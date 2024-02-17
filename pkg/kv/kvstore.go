package kv

import "github.com/wuhuua/minicrdb/pkg/storage"

const (
	physicalPrefix  string = "1"
	kvPrefix        string = "storage"
	rangePrefix     string = "range"
	startFlagPrefix string = "0"
	dataFlagPrefix  string = "1"
	endFlagPrefix   string = "2"
)

type KVStore struct {
	nodeId string
	store  *storage.Engine
}

func NewKVStore(store *storage.Engine, nodeId string) *KVStore {
	return &KVStore{
		nodeId: nodeId,
		store:  store,
	}
}

func (kvs *KVStore) genKVKeyPrefix(key string) string {
	return physicalPrefix + "_" + kvPrefix + "_" + rangePrefix + "_" + kvs.nodeId + "_" + dataFlagPrefix + "_" + key
}

func (kvs *KVStore) genKVStartPrefix() string {
	return physicalPrefix + "_" + kvPrefix + "_" + rangePrefix + "_" + kvs.nodeId + "_" + startFlagPrefix
}

func (kvs *KVStore) genKVEndPrefix() string {
	return physicalPrefix + "_" + kvPrefix + "_" + rangePrefix + "_" + kvs.nodeId + "_" + endFlagPrefix
}

func (kvs *KVStore) InitKVStore() error {
	if err := kvs.store.Set(kvs.genKVStartPrefix(), []byte{}); err != nil {
		return err
	}
	if err := kvs.store.Set(kvs.genKVEndPrefix(), []byte{}); err != nil {
		return err
	}
	return nil
}

func (kvs *KVStore) GetKeySpan() *storage.KeySpan {
	return &storage.KeySpan{
		StartKey: kvs.genKVStartPrefix(),
		EndKey:   kvs.genKVEndPrefix(),
	}
}

func (kvs *KVStore) Set(key string, value []byte) error {
	return kvs.store.Set(kvs.genKVKeyPrefix(key), value)
}

func (kvs *KVStore) Get(key string) ([]byte, error) {
	return kvs.store.Get(kvs.genKVKeyPrefix(key))
}

func (kvs *KVStore) Delete(key string) error {
	return kvs.store.Delete(kvs.genKVKeyPrefix(key))
}
