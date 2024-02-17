package node

import (
	"encoding/json"
	"os"

	"github.com/hashicorp/raft"
	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/meta"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

type MetaNode struct {
	nodeMeta     *Node
	config       *raft.Config
	raftDir      string
	store        *storage.Engine
	nodeId       string
	addr         string
	enableSingle bool
	logger       *logger.Logger
	meta         *meta.MetaCmd
	network      *NetworkTransport
}

func NewMetaNode(raftDir string, store *storage.Engine, nodeId string,
	addr string, enableSingle bool, network *NetworkTransport, logger *logger.Logger) *MetaNode {
	return &MetaNode{
		raftDir:      raftDir,
		store:        store,
		nodeId:       nodeId,
		addr:         addr,
		logger:       logger,
		enableSingle: enableSingle,
		network:      network,
	}
}

func (node *MetaNode) InitRaftNode() error {
	config := raft.DefaultConfig()
	config.LocalID = node.genServerID()

	transport := NewNodeTransport(node.nodeId, node.network)

	snapshots, err := raft.NewFileSnapshotStore(node.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		node.logger.Error(err.Error())
		return err
	}

	raftStore := NewRaftStore(node.store, node.nodeId)
	metaCmd := meta.NewClusterMeta(node.store, node.logger)
	err = metaCmd.ReInit()
	if err != nil {
		node.logger.Error(err.Error())
		return err
	}

	raftNode, err := raft.NewRaft(
		config,
		&cmdFSM{handler: metaCmd, logger: node.logger, store: node.store,
			raftDir: node.raftDir, nodeId: node.nodeId},
		raftStore, raftStore, snapshots, transport)
	if err != nil {
		node.logger.Error(err.Error())
		return err
	}

	node.nodeMeta = &Node{
		logger:   node.logger,
		raftNode: raftNode,
	}
	node.config = config
	node.meta = metaCmd

	if node.enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	return nil
}

func (node *MetaNode) Join(raftAddr string, nodeId string) error {
	return node.nodeMeta.Join(raftAddr, nodeId)
}

func (node *MetaNode) CheckPeerNum() uint64 {
	return node.nodeMeta.CheckPeerNumFromConf()
}

func (node *MetaNode) genServerID() raft.ServerID {
	return genServerID(node.addr, node.nodeId)
}

func (node *MetaNode) GetLeaderAddr() string {
	return node.nodeMeta.GetLeaderAddr()
}

func (node *MetaNode) GetNodeId(key string) (string, error) {
	id, err := node.meta.GetNodeId(key)
	if err != nil {
		node.logger.Error(err.Error())
		return "", err
	}
	return id, nil
}

func (node *MetaNode) GetNodeLeaderAddr(nodeId string) (string, error) {
	id, err := node.meta.GetNodeLeaderAddr(nodeId)
	if err != nil {
		node.logger.Error(err.Error())
		return "", err
	}
	return id, nil
}

func (node *MetaNode) SetNodeLeaderAddr(nodeId string, addr string) error {
	if node.nodeMeta.raftNode.State() != raft.Leader {
		err := ErrNotLeader
		node.logger.Error(err.Error())
		return err
	}
	c := &command{
		Op:    "setNodeLeaderAddr",
		Key:   nodeId,
		Value: addr,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := node.nodeMeta.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

func (node *MetaNode) VerifyLeader() bool {
	return node.nodeMeta.VerifyLeader()
}

func (node *MetaNode) LeaderCh() <-chan bool {
	return node.nodeMeta.raftNode.LeaderCh()
}

func (node *MetaNode) WaitUntilWin() {
	for isLeader := range node.LeaderCh() {
		if isLeader {
			return
		}
	}
}
