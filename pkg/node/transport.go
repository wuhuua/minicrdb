package node

import (
	"io"

	"github.com/hashicorp/raft"
)

const retainSnapshotCount = 2

type NodeTransport struct {
	nodeId    string
	consumeCh chan raft.RPC
	network   *NetworkTransport
}

func NewNodeTransport(nodeId string, network *NetworkTransport) *NodeTransport {
	consumeCh := make(chan raft.RPC)
	network.RegisterNode(nodeId, consumeCh)
	return &NodeTransport{
		nodeId:    nodeId,
		consumeCh: consumeCh,
		network:   network,
	}
}

// consume and respond to RPC requests.
func (nodeTrans *NodeTransport) Consumer() <-chan raft.RPC {
	return nodeTrans.consumeCh
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (nodeTrans *NodeTransport) LocalAddr() raft.ServerAddress {
	return nodeTrans.network.LocalAddr()
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (nodeTrans *NodeTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return nodeTrans.network.AppendEntriesPipeline(id, target)
}

// AppendEntries sends the appropriate RPC to the target node.
func (nodeTrans *NodeTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return nodeTrans.network.AppendEntries(id, target, args, resp)
}

// RequestVote sends the appropriate RPC to the target node.
func (nodeTrans *NodeTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return nodeTrans.network.RequestVote(id, target, args, resp)
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (nodeTrans *NodeTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return nodeTrans.network.InstallSnapshot(id, target, args, resp, data)
}

// EncodePeer is used to serialize a peer's address.
func (nodeTrans *NodeTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return nodeTrans.network.EncodePeer(id, addr)
}

// DecodePeer is used to deserialize a peer's address.
func (nodeTrans *NodeTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return nodeTrans.network.DecodePeer(buf)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (nodeTrans *NodeTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	nodeTrans.network.SetHeartbeatHandler(cb)
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (nodeTrans *NodeTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return nodeTrans.network.TimeoutNow(id, target, args, resp)
}
