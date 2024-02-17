package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/wuhuua/minicrdb/pkg/httpd"
	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/node"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

const (
	raftPathName = "raft"
	logPathName  = "log"
	metaId       = "m"
	initNodeId   = 0
)

var (
	ErrLocalNotFound = fmt.Errorf("no such range locally")
	ErrNotMetaLeader = fmt.Errorf("not leader")
)

type Cluster struct {
	serverAddrs  []string
	storeDir     string
	raftDir      string
	addr         string
	raftPort     string
	httpPort     string
	raftBind     string
	httpBind     string
	logger       *logger.Logger
	nodes        map[string]*node.KVNode
	nodeMapMutex sync.Mutex
	metaNode     *node.MetaNode
	engine       *storage.Engine
	network      *node.NetworkTransport
	service      *httpd.Service
}

func NewCluster(storeDir string, addr string, raftPort string, serverAddrs []string, httpPort string) (*Cluster, error) {
	logDir := filepath.Join(storeDir, logPathName)
	if err := logger.SetupLogger(logDir); err != nil {
		return nil, err
	}
	Logger := logger.GetLogger("Cluster")

	Logger.Info("Opening storage engine")
	engine, err := storage.NewEngine(storeDir)
	if err != nil {
		Logger.Error(err.Error())
		return nil, err
	}

	Logger.Info("Initializing Network")
	raftBind := addr + ":" + raftPort
	network, err := node.InitNetwork(raftBind, 3, 10*time.Second, os.Stderr)
	if err != nil {
		Logger.Error(err.Error())
		return nil, err
	}

	cluster := &Cluster{
		serverAddrs: serverAddrs,
		storeDir:    storeDir,
		raftDir:     filepath.Join(storeDir, raftPathName),
		nodes:       make(map[string]*node.KVNode),
		engine:      engine,
		addr:        addr,
		raftPort:    raftPort,
		raftBind:    raftBind,
		httpPort:    httpPort,
		httpBind:    addr + ":" + httpPort,
		network:     network,
		logger:      Logger,
	}

	singleNode := len(cluster.serverAddrs) == 0

	metaNode, err := cluster.NewMetaNode(singleNode)
	if err != nil {
		Logger.Error(err.Error())
		return nil, err
	}
	cluster.metaNode = metaNode

	if singleNode {
		err = cluster.NewKVNode(initNodeId, singleNode)
		if err != nil {
			Logger.Error(err.Error())
			return nil, err
		}
		metaNode.WaitUntilWin()
		err = metaNode.SetNodeLeaderAddr(strconv.FormatInt(initNodeId, 10), cluster.httpBind)
		if err != nil {
			Logger.Error(err.Error())
			return nil, err
		}
		err = metaNode.SetNodeLeaderAddr(metaId, cluster.httpBind)
		if err != nil {
			Logger.Error(err.Error())
			return nil, err
		}
	} else {
		err = httpd.CliJoin(serverAddrs[0], metaId, cluster.raftBind)
		if err != nil {
			Logger.Error(err.Error())
			return nil, err
		}
	}

	cluster.service = httpd.New(cluster.httpBind, &ReqHandler{cluster: cluster, logger: Logger})
	err = cluster.service.Start()
	if err != nil {
		Logger.Error(err.Error())
		return nil, err
	}

	return cluster, nil
}

func (cluster *Cluster) NewKVNode(id uint64, singleNode bool) error {
	raftBind := cluster.raftBind
	newNode := node.NewKVNode(cluster.raftDir, cluster.engine, strconv.FormatUint(id, 10),
		raftBind, singleNode, cluster.network)
	err := newNode.InitRaftNode()
	if err != nil {
		cluster.logger.Error(err.Error())
		return err
	}
	cluster.nodeMapMutex.Lock()
	defer cluster.nodeMapMutex.Unlock()
	cluster.nodes[newNode.GetId()] = newNode
	return nil
}

func (cluster *Cluster) NewMetaNode(singleNode bool) (*node.MetaNode, error) {
	raftBind := cluster.addr + ":" + cluster.raftPort
	newNode := node.NewMetaNode(cluster.raftDir, cluster.engine, metaId,
		raftBind, singleNode, cluster.network, cluster.logger)
	return newNode, newNode.InitRaftNode()
}

func (cluster *Cluster) GetLocal(nodeId string, key string) ([]byte, error) {
	node, ok := cluster.nodes[nodeId]
	if !ok {
		return []byte{}, ErrLocalNotFound
	}
	return node.Get(key)
}

func (cluster *Cluster) SetLocal(nodeId string, key string, value []byte) error {
	node, ok := cluster.nodes[nodeId]
	if !ok {
		return ErrLocalNotFound
	}
	return node.Set(key, value)
}

func (cluster *Cluster) GetNodeId(key string) (string, error) {
	id, err := cluster.metaNode.GetNodeId(key)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (cluster *Cluster) GetNodeLeaderAddr(nodeId string) (string, error) {
	addr, err := cluster.metaNode.GetNodeLeaderAddr(nodeId)
	if err != nil {
		return "", err
	}
	return addr, nil
}

func (cluster *Cluster) JoinLocal(nodeId, raftBind string) error {
	if nodeId == metaId {
		return cluster.metaNode.Join(raftBind, nodeId)
	}
	node, ok := cluster.nodes[nodeId]
	if !ok {
		return ErrLocalNotFound
	}
	return node.Join(raftBind, nodeId)
}

// Debug only, do not call other than testing
func (cluster *Cluster) GetClusterKeys() []string {
	return cluster.engine.GetKeyList()
}
