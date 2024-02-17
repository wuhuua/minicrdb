package kv

import (
	"fmt"

	"github.com/wuhuua/minicrdb/pkg/storage"
)

type KVCmd struct {
	store *KVStore
}

func NewKVCmd(store *storage.Engine, nodeId string) *KVCmd {
	return &KVCmd{store: NewKVStore(store, nodeId)}
}

func (kvcmd *KVCmd) Get(key string) ([]byte, error) {
	return kvcmd.store.Get(key)
}

func (kvcmd *KVCmd) GetKeySpan() *storage.KeySpan {
	return kvcmd.store.GetKeySpan()
}

func (kvcmd *KVCmd) InitKVStore() error {
	return kvcmd.store.InitKVStore()
}

func (kvcmd *KVCmd) ReInit() error {
	return kvcmd.InitKVStore()
}

func (kvcmd *KVCmd) HandleCmd(command string, key string, value []byte) error {
	switch command {
	case "set":
		return kvcmd.store.Set(key, value)
	case "del":
		return kvcmd.store.Delete(key)
	default:
		return fmt.Errorf("unknown Command for KVCmd")
	}

}
