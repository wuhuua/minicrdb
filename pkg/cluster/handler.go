package cluster

import (
	"fmt"

	"github.com/wuhuua/minicrdb/pkg/httpd"
	"github.com/wuhuua/minicrdb/pkg/logger"
)

type ReqHandler struct {
	cluster *Cluster
	logger  *logger.Logger
}

var (
	ErrLeaderInternal = fmt.Errorf("leader has internal error, avoid recursive calling")
)

func (rh *ReqHandler) Get(key string) (string, error) {
	rh.logger.Debug("handle Get request with key: %s", key)
	nodeId, err := rh.cluster.GetNodeId(key)
	if err != nil {
		rh.logger.Error(err.Error())
		return "", err
	}
	val, err := rh.cluster.GetLocal(nodeId, key)
	if err == nil {
		rh.logger.Debug("get local with key: %s, value: %s", key, string(val))
		return string(val), nil
	}
	addr, err := rh.cluster.GetNodeLeaderAddr(nodeId)
	if err != nil {
		rh.logger.Error(err.Error())
		return "", err
	}
	if addr == rh.cluster.httpBind {
		err = ErrLeaderInternal
		rh.logger.Error(err.Error())
		return "", err
	}
	strVal, err := httpd.CliGet(addr, key)
	if err != nil {
		rh.logger.Error(err.Error())
		return "", err
	}
	return strVal, nil
}

func (rh *ReqHandler) Set(key, value string) error {
	rh.logger.Debug("handle Set request with key: %s, value: %s", key, value)
	nodeId, err := rh.cluster.GetNodeId(key)
	if err != nil {
		rh.logger.Error(err.Error())
		return err
	}
	err = rh.cluster.SetLocal(nodeId, key, []byte(value))
	if err == nil {
		rh.logger.Debug("set local with key: %s, value: %s", key, value)
		return nil
	}
	addr, err := rh.cluster.GetNodeLeaderAddr(nodeId)
	if err != nil {
		rh.logger.Error(err.Error())
		return err
	}
	if addr == rh.cluster.httpBind {
		err = ErrLeaderInternal
		rh.logger.Error(err.Error())
		return err
	}
	err = httpd.CliSet(addr, key, value)
	if err != nil {
		rh.logger.Error(err.Error())
		return err
	}
	return nil
}

func (rh *ReqHandler) Delete(key string) error {
	return nil
}

func (rh *ReqHandler) Join(nodeID string, addr string) error {
	rh.logger.Debug("handle Join request with addr: %s, node id: %s", addr, nodeID)
	err := rh.cluster.JoinLocal(nodeID, addr)
	if err == nil {
		rh.logger.Debug("join local with addr: %s, node id: %s", addr, nodeID)
		return nil
	}
	joinAddr, err := rh.cluster.GetNodeLeaderAddr(nodeID)
	if err != nil {
		rh.logger.Error(err.Error())
		return err
	}
	if joinAddr == rh.cluster.httpBind {
		err = ErrLeaderInternal
		rh.logger.Error(err.Error())
		return err
	}
	err = httpd.CliJoin(joinAddr, nodeID, addr)
	if err != nil {
		rh.logger.Error(err.Error())
		return err
	}
	return nil
}
