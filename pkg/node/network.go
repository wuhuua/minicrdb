package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// DefaultMaxRPCsInFlight is the default value used for pipelining configuration
	// if a zero value is passed. See https://github.com/hashicorp/raft/pull/541
	// for rationale. Note, if this is changed we should update the doc comments
	// below for NetworkTransportConfig.MaxRPCsInFlight.
	DefaultMaxRPCsInFlight = 2

	// connReceiveBufferSize is the size of the buffer we will use for reading RPC requests into
	// on followers
	connReceiveBufferSize = 256 * 1024 // 256KB

	// connSendBufferSize is the size of the buffer we will use for sending RPC request data from
	// the leader to followers.
	connSendBufferSize = 256 * 1024 // 256KB

	// minInFlightForPipelining is a property of our current pipelining
	// implementation and must not be changed unless we change the invariants of
	// that implementation. Roughly speaking even with a zero-length in-flight
	// buffer we still allow 2 requests to be in-flight before we block because we
	// only block after sending and the receiving go-routine always unblocks the
	// chan right after first send. This is a constant just to provide context
	// rather than a magic number in a few places we have to check invariants to
	// avoid panics etc.
	minInFlightForPipelining = 2
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrPipelineShutdown is returned when the pipeline is closed.
	ErrPipelineShutdown = errors.New("append pipeline closed")

	// ErrNoSuchNode is returned when nodeId is unmapped in channel map
	ErrNoSuchNode = errors.New("no such node in this server")

	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// NetworkTransport provides a network based transport that can be
// used to communicate with Raft on remote machines. It requires
// an underlying stream layer to provide a stream abstraction, which can
// be simple TCP, TLS, etc.
//
// This transport is very simple and lightweight. Each RPC request is
// framed by sending a byte that indicates the message type, followed
// by the MsgPack encoded request.
//
// The response is an error string followed by the response object,
// both are encoded using MsgPack.
//
// InstallSnapshot is special, in that after the RPC request we stream
// the entire state. That socket is not re-used as the connection state
// is not known if there is an error.
type NetworkTransport struct {
	connPool     map[raft.ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh       chan raft.RPC
	consumeChannels map[string]chan raft.RPC

	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex

	logger hclog.Logger

	maxPool     int
	maxInFlight int

	serverAddressProvider ServerAddressProvider

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	stream StreamLayer

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int

	msgpackUseNewTimeFormat bool
}

// NetworkTransportConfig encapsulates configuration for the network transport layer.
type NetworkTransportConfig struct {
	// ServerAddressProvider is used to override the target address when establishing a connection to invoke an RPC
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool controls how many connections we will pool
	MaxPool int

	// MaxRPCsInFlight controls the pipelining "optimization" when replicating
	// entries to followers.
	//
	// Setting this to 1 explicitly disables pipelining since no overlapping of
	// request processing is allowed. If set to 1 the pipelining code path is
	// skipped entirely and every request is entirely synchronous.
	//
	// If zero is set (or left as default), DefaultMaxRPCsInFlight is used which
	// is currently 2. A value of 2 overlaps the preparation and sending of the
	// next request while waiting for the previous response, but avoids additional
	// queuing.
	//
	// Historically this was internally fixed at (effectively) 130 however
	// performance testing has shown that in practice the pipelining optimization
	// combines badly with batching and actually has a very large negative impact
	// on commit latency when throughput is high, whilst having very little
	// benefit on latency or throughput in any other case! See
	// [#541](https://github.com/hashicorp/raft/pull/541) for more analysis of the
	// performance impacts.
	//
	// Increasing this beyond 2 is likely to be beneficial only in very
	// high-latency network conditions. HashiCorp doesn't recommend using our own
	// products this way.
	//
	// To maintain the behavior from before version 1.4.1 exactly, set this to
	// 130. The old internal constant was 128 but was used directly as a channel
	// buffer size. Since we send before blocking on the channel and unblock the
	// channel as soon as the receiver is done with the earliest outstanding
	// request, even an unbuffered channel (buffer=0) allows one request to be
	// sent while waiting for the previous one (i.e. 2 inflight). so the old
	// buffer actually allowed 130 RPCs to be inflight at once.
	MaxRPCsInFlight int

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration

	// MsgpackUseNewTimeFormat when set to true, force the underlying msgpack
	// codec to use the new format of time.Time when encoding (used in
	// go-msgpack v1.1.5 by default). Decoding is not affected, as all
	// go-msgpack v2.1.0+ decoders know how to decode both formats.
	MsgpackUseNewTimeFormat bool
}

// ServerAddressProvider is a target address to which we invoke an RPC when establishing a connection
type ServerAddressProvider interface {
	ServerAddr(id raft.ServerID) (raft.ServerAddress, error)
}

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error)
}

type netConn struct {
	target raft.ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

type netPipeline struct {
	conn  *netConn
	trans *NetworkTransport

	doneCh       chan raft.AppendFuture
	inprogressCh chan *appendFuture

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// TCPStreamLayer implements StreamLayer interface for plain TCP.
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewNetworkTransportWithConfig creates a new network transport with the given config struct
func NewNetworkTransportWithConfig(
	config *NetworkTransportConfig,
) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	maxInFlight := config.MaxRPCsInFlight
	if maxInFlight == 0 {
		// Default zero value
		maxInFlight = DefaultMaxRPCsInFlight
	}
	trans := &NetworkTransport{
		connPool:                make(map[raft.ServerAddress][]*netConn),
		consumeCh:               make(chan raft.RPC),
		consumeChannels:         make(map[string]chan raft.RPC),
		logger:                  config.Logger,
		maxPool:                 config.MaxPool,
		maxInFlight:             maxInFlight,
		shutdownCh:              make(chan struct{}),
		stream:                  config.Stream,
		timeout:                 config.Timeout,
		TimeoutScale:            DefaultTimeoutScale,
		serverAddressProvider:   config.ServerAddressProvider,
		msgpackUseNewTimeFormat: config.MsgpackUseNewTimeFormat,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	go trans.listen()

	return trans
}

// NewNetworkTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger creates a new network transport with the given logger, dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

func InitNetwork(bindAddr string,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {

	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	advertise, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	// Verify that we have a usable advertise address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}

	// Create the network transport
	return NewNetworkTransport(stream, maxPool, timeout, logOutput), nil
}

// register new node on network
func (n *NetworkTransport) RegisterNode(nodeId string, consumeCh chan raft.RPC) {
	n.consumeChannels[nodeId] = consumeCh
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// SetHeartbeatHandler is used to set up a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// CloseStreams closes the current streams.
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range n.connPool {
		for _, conn := range e {
			conn.Release()
		}

		delete(n.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	n.streamCtxLock.Lock()
	n.streamCancel()
	n.setupStreamContext()
	n.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.stream.Close()
		n.shutdown = true
	}
	return nil
}

// Consumer implements the Transport interface.
func (n *NetworkTransport) Consumer() <-chan raft.RPC {
	return n.consumeCh
}

// LocalAddr implements the Transport interface.
func (n *NetworkTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(n.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
func (n *NetworkTransport) getPooledConn(target raft.ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider returns a connection from the server address provider if available, or defaults to a connection using the target server address
func (n *NetworkTransport) getConnFromAddressProvider(id raft.ServerID, target raft.ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

func (n *NetworkTransport) getProviderAddressOrFallback(id raft.ServerID, target raft.ServerAddress) raft.ServerAddress {
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddr(id)
		if err != nil {
			n.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn is used to get a connection from the pool.
func (n *NetworkTransport) getConn(target raft.ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, connSendBufferSize),
	}

	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{
		BasicHandle: codec.BasicHandle{
			TimeNotBuiltin: !n.msgpackUseNewTimeFormat,
		},
	})

	// Done
	return netConn, nil
}

// returnConn returns a connection back to the pool.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (n *NetworkTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if n.maxInFlight < minInFlightForPipelining {
		// Pipelining is disabled since no more than one request can be outstanding
		// at once. Skip the whole code path and use synchronous requests.
		return nil, raft.ErrPipelineReplicationNotSupported
	}

	// Get a connection
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// Create the pipeline
	return newNetPipeline(n, conn, n.maxInFlight), nil
}

// AppendEntries implements the Transport interface.
func (n *NetworkTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote implements the Transport interface.
func (n *NetworkTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// genericRPC handles a simple request/response RPC.
func (n *NetworkTransport) genericRPC(id raft.ServerID, target raft.ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	// Get a conn
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	// Set a deadline
	if n.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		n.returnConn(conn)
	}
	return err
}

// InstallSnapshot implements the Transport interface.
func (n *NetworkTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	// Get a conn, always close for InstallSnapshot
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Set a deadline, scaled by request size
	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	// Stream the state
	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// Flush
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// Decode the response, do not return conn
	_, err = decodeResponse(conn, resp)
	return err
}

// EncodePeer implements the Transport interface.
func (n *NetworkTransport) EncodePeer(id raft.ServerID, p raft.ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer implements the Transport interface.
func (n *NetworkTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

// TimeoutNow implements the Transport interface.
func (n *NetworkTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return n.genericRPC(id, target, rpcTimeoutNow, args, resp)
}

// listen is used to handling incoming connections.
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accept incoming connections
		conn, err := n.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !n.IsShutdown() {
				n.logger.Error("failed to accept connection", "error", err)
			}

			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay
		loopDelay = 0

		n.logger.Debug("accepted connection", "local-address", n.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// Handle the connection in dedicated routine
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
func (n *NetworkTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReaderSize(conn, connReceiveBufferSize)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{
		BasicHandle: codec.BasicHandle{
			TimeNotBuiltin: !n.msgpackUseNewTimeFormat,
		},
	})

	for {
		select {
		case <-connCtx.Done():
			n.logger.Debug("stream layer is closed")
			return
		default:
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			n.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command.
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	getTypeStart := time.Now()

	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	metrics.MeasureSince([]string{"raft", "net", "getRPCType"}, getTypeStart)
	decodeStart := time.Now()

	// Create the RPC object
	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		RespChan: respCh,
	}

	// Decode the command
	isHeartbeat := false
	var labels []metrics.Label
	var nodeId string
	switch rpcType {
	case rpcAppendEntries:
		var req raft.AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		leaderAddr := req.RPCHeader.Addr
		if len(leaderAddr) == 0 {
			leaderAddr = req.Leader
		}

		// Check if this is a heartbeat
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		if isHeartbeat {
			labels = []metrics.Label{{Name: "rpcType", Value: "Heartbeat"}}
		} else {
			labels = []metrics.Label{{Name: "rpcType", Value: "AppendEntries"}}
		}
		nodeId = string(req.RPCHeader.ID)
	case rpcRequestVote:
		var req raft.RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestVote"}}
		nodeId = string(req.RPCHeader.ID)
	case rpcInstallSnapshot:
		var req raft.InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)
		labels = []metrics.Label{{Name: "rpcType", Value: "InstallSnapshot"}}
		nodeId = string(req.RPCHeader.ID)
	case rpcTimeoutNow:
		var req raft.TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "TimeoutNow"}}
		nodeId = string(req.RPCHeader.ID)
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	nodeId = parseLastPart(nodeId)
	if nodeId == "" {
		return ErrNoSuchNode
	}

	cCh, ok := n.consumeChannels[nodeId]

	// No such node in this server
	if !ok {
		return ErrNoSuchNode
	}

	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcDecode"}, decodeStart, labels)

	processStart := time.Now()

	// Check for heartbeat fast-path
	if isHeartbeat {
		n.heartbeatFnLock.Lock()
		fn := n.heartbeatFn
		n.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// Dispatch the RPC
	select {
	case cCh <- rpc:
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	// Wait for response
RESP:
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcEnqueue"}, processStart, labels)
	respWaitStart := time.Now()
	select {
	case resp := <-respCh:
		defer metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcRespond"}, respWaitStart, labels)
		// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

// sendRPC is used to encode and send the RPC.
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// Write the request type
	if err := conn.w.WriteByte(rpcType); err != nil {
		conn.Release()
		return err
	}

	// Send the request
	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}

// newNetPipeline is used to construct a netPipeline from a given transport and
// connection. It is a bug to ever call this with maxInFlight less than 2
// (minInFlightForPipelining) and will cause a panic.
func newNetPipeline(trans *NetworkTransport, conn *netConn, maxInFlight int) *netPipeline {
	if maxInFlight < minInFlightForPipelining {
		// Shouldn't happen (tm) since we validate this in the one call site and
		// skip pipelining if it's lower.
		panic("pipelining makes no sense if maxInFlight < 2")
	}
	n := &netPipeline{
		conn:  conn,
		trans: trans,
		// The buffer size is 2 less than the configured max because we send before
		// waiting on the channel and the decode routine unblocks the channel as
		// soon as it's waiting on the first request. So a zero-buffered channel
		// still allows 1 request to be sent even while decode is still waiting for
		// a response from the previous one. i.e. two are inflight at the same time.
		inprogressCh: make(chan *appendFuture, maxInFlight-2),
		doneCh:       make(chan raft.AppendFuture, maxInFlight-2),
		shutdownCh:   make(chan struct{}),
	}
	go n.decodeResponses()
	return n
}

// decodeResponses is a long running routine that decodes the responses
// sent on the connection.
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		select {
		case future := <-n.inprogressCh:
			if timeout > 0 {
				n.conn.conn.SetReadDeadline(time.Now().Add(timeout))
			}

			_, err := decodeResponse(n.conn, future.resp)
			future.respond(err)
			select {
			case n.doneCh <- future:
			case <-n.shutdownCh:
				return
			}
		case <-n.shutdownCh:
			return
		}
	}
}

// AppendEntries is used to pipeline a new append entries request.
func (n *netPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Add a send timeout
	if timeout := n.trans.timeout; timeout > 0 {
		n.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err := sendRPC(n.conn, rpcAppendEntries, future.args); err != nil {
		return nil, err
	}

	// Hand-off for decoding, this can also cause back-pressure
	// to prevent too many inflight requests
	select {
	case n.inprogressCh <- future:
		return future, nil
	case <-n.shutdownCh:
		return nil, ErrPipelineShutdown
	}
}

// Consumer returns a channel that can be used to consume complete futures.
func (n *netPipeline) Consumer() <-chan raft.AppendFuture {
	return n.doneCh
}

// Close is used to shut down the pipeline connection.
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		return nil
	}

	// Release the connection
	n.conn.Release()

	n.shutdown = true
	close(n.shutdownCh)
	return nil
}

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	deferError
	start time.Time
	args  *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *raft.AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *raft.AppendEntriesResponse {
	return a.resp
}

type deferError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = raft.ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

func parseLastPart(s string) string {
	parts := strings.Split(s, "_")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}
