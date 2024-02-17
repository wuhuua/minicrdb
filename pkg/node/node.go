package node

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/wuhuua/minicrdb/pkg/logger"
)

type Node struct {
	raftNode *raft.Raft
	logger   *logger.Logger
}

var (
	ErrNotLeader = fmt.Errorf("not leader")
)

func (node *Node) Join(raftAddr string, nodeId string) error {
	node.logger.Info("new join resquest from: " + raftAddr)
	if node.raftNode.State() != raft.Leader {
		err := ErrNotLeader
		node.logger.Error(err.Error())
		return err
	}
	configFuture := node.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		node.logger.Error("failed to get raft configuration: " + err.Error())
		return err
	}
	severID := genServerID(raftAddr, nodeId)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == severID || srv.Address == raft.ServerAddress(raftAddr) {
			node.logger.Info("node " + nodeId + " at " + raftAddr + " already member of cluster, ignoring join request")
			return nil
		}
	}

	f := node.raftNode.AddVoter(severID, raft.ServerAddress(raftAddr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

func (node *Node) CheckPeerNumFromConf() uint64 {
	config := node.raftNode.GetConfiguration()
	return uint64(len(config.Configuration().Servers))
}

func (node *Node) GetLeaderAddr() string {
	addr, _ := node.raftNode.LeaderWithID()
	return string(addr)
}

func (node *Node) LeaderCh() <-chan bool {
	return node.raftNode.LeaderCh()
}

func (node *Node) VerifyLeader() bool {
	return node.raftNode.State() == raft.Leader
}

func genServerID(addr string, nodeId string) raft.ServerID {
	return raft.ServerID(addr + "_" + nodeId)
}
