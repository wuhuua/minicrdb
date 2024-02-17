package node

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hashicorp/raft"
	"github.com/wuhuua/minicrdb/pkg/kv"
	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

type KVNode struct {
	nodeMeta     *Node
	config       *raft.Config
	raftDir      string
	store        *storage.Engine
	nodeId       string
	addr         string
	enableSingle bool
	logger       *logger.Logger
	kv           *kv.KVCmd
	network      *NetworkTransport
}

func NewKVNode(raftDir string, store *storage.Engine, nodeId string,
	addr string, enableSingle bool, network *NetworkTransport) *KVNode {
	return &KVNode{
		raftDir:      raftDir,
		store:        store,
		nodeId:       nodeId,
		addr:         addr,
		logger:       logger.GetLogger("Node" + nodeId),
		enableSingle: enableSingle,
		network:      network,
	}
}

func (node *KVNode) InitRaftNode() error {
	config := raft.DefaultConfig()
	config.LocalID = node.genServerID()

	transport := NewNodeTransport(node.nodeId, node.network)

	snapshots, err := raft.NewFileSnapshotStore(node.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		node.logger.Error(err.Error())
		return err
	}

	raftStore := NewRaftStore(node.store, node.nodeId)
	kvCmd := kv.NewKVCmd(node.store, node.nodeId)

	err = kvCmd.ReInit()
	if err != nil {
		node.logger.Error(err.Error())
		return err
	}

	raftNode, err := raft.NewRaft(
		config,
		&cmdFSM{handler: kvCmd, logger: node.logger, store: node.store,
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
	node.kv = kvCmd

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

func (node *KVNode) GetId() string {
	return node.nodeId
}

func (node *KVNode) Join(raftAddr string, nodeId string) error {
	return node.nodeMeta.Join(raftAddr, nodeId)
}

func (node *KVNode) CheckPeerNum() uint64 {
	return node.nodeMeta.CheckPeerNumFromConf()
}

func (node *KVNode) genServerID() raft.ServerID {
	return genServerID(node.addr, node.nodeId)
}

func (node *KVNode) Get(key string) ([]byte, error) {
	value, err := node.kv.Get(key)
	if err != nil {
		node.logger.Error(err.Error())
		value = nil
	}
	return value, err
}

func (node *KVNode) Set(key string, value []byte) error {
	if node.nodeMeta.raftNode.State() != raft.Leader {
		err := ErrNotLeader
		node.logger.Error(err.Error())
		return err
	}
	c := &command{
		Op:    "set",
		Key:   key,
		Value: string(value),
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := node.nodeMeta.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

func (node *KVNode) Delete(key string) error {
	if node.nodeMeta.raftNode.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "del",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := node.nodeMeta.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

func (node *KVNode) VerifyLeader() bool {
	return node.nodeMeta.VerifyLeader()
}
