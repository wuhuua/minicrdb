package node

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

const (
	physicalPrefix     string = "0"
	raftPrefix         string = "raft"
	rangePrefix        string = "range"
	lowIndexKeyPrefix  string = "lowindex"
	highIndexKeyPrefix string = "highindex"
	logPrefx           string = "log"
	kvPrefix           string = "kv"
	kvIntPrefix        string = "kvint"
)

type RaftStore struct {
	storage *storage.Engine
	nodeId  string
}

func NewRaftStore(storage *storage.Engine, nodeId string) *RaftStore {
	return &RaftStore{
		storage: storage,
		nodeId:  nodeId,
	}
}

func (rs *RaftStore) genRaftKeyPrefix(typePrefix string) string {
	return physicalPrefix + "_" + raftPrefix + "_" + rangePrefix + "_" + rs.nodeId + "_" + typePrefix
}

func appendAdditionalPrefix(prefix string, additional string) string {
	return prefix + "_" + additional
}

// FirstIndex implements the LogStore interface.
func (rs *RaftStore) FirstIndex() (uint64, error) {
	str, err := rs.storage.Get(rs.genRaftKeyPrefix(lowIndexKeyPrefix))
	if err != nil {
		if err == storage.ERROR_NOT_FOUND {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseUint(string(str), 10, 64)
}

// LastIndex implements the LogStore interface.
func (rs *RaftStore) LastIndex() (uint64, error) {
	str, err := rs.storage.Get(rs.genRaftKeyPrefix(highIndexKeyPrefix))
	if err != nil {
		if err == storage.ERROR_NOT_FOUND {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseUint(string(str), 10, 64)
}

// GetLog implements the LogStore interface.
func (rs *RaftStore) GetLog(index uint64, log *raft.Log) error {
	str := strconv.FormatUint(index, 10)
	logPrefix := rs.genRaftKeyPrefix(logPrefx)
	logPrefix = appendAdditionalPrefix(logPrefix, str)
	value, err := rs.storage.Get(logPrefix)
	if err != nil {
		return err
	}
	var l raft.Log
	json.Unmarshal(value, &l)
	if err != nil {
		return err
	}
	*log = l
	return nil
}

// StoreLog implements the LogStore interface.
func (rs *RaftStore) StoreLog(log *raft.Log) error {
	return rs.StoreLogs([]*raft.Log{log})
}

// StoreLogs implements the LogStore interface.
func (rs *RaftStore) StoreLogs(logs []*raft.Log) error {
	logPrefix := rs.genRaftKeyPrefix(logPrefx)
	for _, l := range logs {
		data, err := json.Marshal(l)
		if err != nil {
			return err
		}
		str := strconv.FormatUint(l.Index, 10)
		prefix := appendAdditionalPrefix(logPrefix, str)
		rs.storage.Set(prefix, data)
	}
	return nil
}

// DeleteRange implements the LogStore interface.
func (rs *RaftStore) DeleteRange(min, max uint64) error {
	logPrefix := rs.genRaftKeyPrefix(logPrefx)
	start := strconv.FormatUint(min, 10)
	end := strconv.FormatUint(max, 10)
	return rs.storage.RangeDelete(appendAdditionalPrefix(logPrefix, start),
		appendAdditionalPrefix(logPrefix, end))
}

// Set implements the StableStore interface.
func (rs *RaftStore) Set(key []byte, val []byte) error {
	keyStr := appendAdditionalPrefix(rs.genRaftKeyPrefix(kvPrefix), string(key))
	return rs.storage.Set(keyStr, val)
}

// Get implements the StableStore interface.
func (rs *RaftStore) Get(key []byte) ([]byte, error) {
	keyStr := appendAdditionalPrefix(rs.genRaftKeyPrefix(kvPrefix), string(key))
	val, err := rs.storage.Get(string(keyStr))
	if err == storage.ERROR_NOT_FOUND {
		err = fmt.Errorf("not found")
	}
	return val, err
}

// SetUint64 implements the StableStore interface.
func (rs *RaftStore) SetUint64(key []byte, val uint64) error {
	str := strconv.FormatUint(val, 10)
	keyStr := appendAdditionalPrefix(rs.genRaftKeyPrefix(kvIntPrefix), string(key))
	return rs.storage.Set(keyStr, []byte(str))
}

// GetUint64 implements the StableStore interface.
func (rs *RaftStore) GetUint64(key []byte) (uint64, error) {
	keyStr := appendAdditionalPrefix(rs.genRaftKeyPrefix(kvIntPrefix), string(key))
	str, err := rs.storage.Get(keyStr)
	if err != nil {
		if err == storage.ERROR_NOT_FOUND {
			err = fmt.Errorf("not found")
		}
		return 0, err
	}
	return strconv.ParseUint(string(str), 10, 64)
}
