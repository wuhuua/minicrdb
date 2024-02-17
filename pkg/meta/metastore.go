package meta

import (
	"strconv"

	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

const (
	physicalPrefix      string = "1"
	clusterPrefix       string = "cluster"
	dataFlagPrefix      string = "1"
	rangeCountPrefix    string = "rcount"
	startKeyPrefix      string = "startkey"
	nodeLeaderKeyPrefix string = "nodeleader"
	startFlagPrefix     string = "0"
	endFlagPrefix       string = "2"
	StartRangeNum       string = "0"
)

type MetaStore struct {
	store  *storage.Engine
	logger *logger.Logger
}

func NewClusterStore(store *storage.Engine, logger *logger.Logger) *MetaStore {
	return &MetaStore{store: store, logger: logger}
}

func (cs *MetaStore) InitClusterStore() error {
	startKey, endKey, startStoreKey, endStoreKey := getStartKey(), getEndKey(), getStoreStartKey(), getStoreEndKey()
	err := cs.store.Set(startKey, []byte(StartRangeNum))
	if err != nil {
		cs.logger.Error(err.Error())
		return err
	}
	err = cs.store.Set(endKey, []byte{})
	if err != nil {
		cs.logger.Error(err.Error())
		return err
	}
	err = cs.store.Set(startStoreKey, []byte{})
	if err != nil {
		cs.logger.Error(err.Error())
		return err
	}
	err = cs.store.Set(endStoreKey, []byte{})
	if err != nil {
		cs.logger.Error(err.Error())
		return err
	}
	return nil
}

func (cs *MetaStore) FindRange(key string) (string, error) {
	startKey, endKey := getStartKey(), getEndKey()
	kvPair, err := cs.store.FindKeysInSpan(&storage.KeySpan{StartKey: startKey, EndKey: endKey})
	if err != nil {
		cs.logger.Error(err.Error())
		return "", err
	}
	nodeId := ""
	searchKey := genSearchKey(key)
	for _, pair := range kvPair {
		if pair.Key > searchKey {
			return nodeId, nil
		}
		nodeId = string(pair.Value)
	}
	return nodeId, nil
}

func (cs *MetaStore) GetNodeLeaderAddr(nodeId string) (string, error) {
	addr, err := cs.store.Get(genNodeLeaderKey(nodeId))
	if err != nil {
		cs.logger.Error(err.Error())
		return "", err
	}
	return string(addr), nil
}

func (cs *MetaStore) SetNodeLeaderAddr(nodeId string, addr string) error {
	err := cs.store.Set(genNodeLeaderKey(nodeId), []byte(addr))
	if err != nil {
		cs.logger.Error(err.Error())
		return err
	}
	return nil
}

func (cs *MetaStore) SetRangeCount(count uint64) error {
	countStr := strconv.FormatUint(count, 10)
	err := cs.store.Set(genRangeCountKey(), []byte(countStr))
	if err != nil {
		cs.logger.Error(err.Error())
		return err
	}
	return nil
}

func (cs *MetaStore) GetRangeCount() (uint64, error) {
	countStr, err := cs.store.Get(genRangeCountKey())
	if err != nil {
		cs.logger.Error(err.Error())
		return 0, err
	}
	count, err := strconv.ParseUint(string(countStr), 10, 64)
	if err != nil {
		cs.logger.Error(err.Error())
		return 0, err
	}
	return count, nil
}

func (cs *MetaStore) GetKeySpan() *storage.KeySpan {
	return &storage.KeySpan{
		StartKey: getStoreStartKey(),
		EndKey:   getStoreEndKey(),
	}
}

func genSearchKey(key string) string {
	return physicalPrefix + "_" + clusterPrefix + "_" + dataFlagPrefix + "_" + startKeyPrefix + "_" + dataFlagPrefix + "_" + key
}

func getStartKey() string {
	return physicalPrefix + "_" + clusterPrefix + "_" + dataFlagPrefix + "_" + startKeyPrefix + "_" + startFlagPrefix
}

func getEndKey() string {
	return physicalPrefix + "_" + clusterPrefix + "_" + dataFlagPrefix + "_" + startKeyPrefix + "_" + endFlagPrefix
}

func genRangeCountKey() string {
	return physicalPrefix + "_" + clusterPrefix + "_" + dataFlagPrefix + "_" + rangeCountPrefix
}

func getStoreStartKey() string {
	return physicalPrefix + "_" + clusterPrefix + "_" + startFlagPrefix
}

func getStoreEndKey() string {
	return physicalPrefix + "_" + clusterPrefix + "_" + endFlagPrefix
}

func genNodeLeaderKey(nodeId string) string {
	return physicalPrefix + "_" + clusterPrefix + "_" + dataFlagPrefix + "_" + nodeLeaderKeyPrefix + "_" + nodeId
}
