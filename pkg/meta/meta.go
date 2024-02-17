package meta

import (
	"fmt"

	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

type MetaCmd struct {
	store *MetaStore
}

func NewClusterMeta(store *storage.Engine, logger *logger.Logger) *MetaCmd {
	return &MetaCmd{store: NewClusterStore(store, logger)}
}

func (metacmd *MetaCmd) HandleCmd(command string, key string, value []byte) error {
	switch command {
	case "setNodeLeaderAddr":
		return metacmd.store.SetNodeLeaderAddr(key, string(value))
	case "del":
		return nil
	default:
		return fmt.Errorf("unknown Command for MetaCmd")
	}

}

func (metacmd *MetaCmd) GetKeySpan() *storage.KeySpan {
	return metacmd.store.GetKeySpan()
}

func (metacmd *MetaCmd) ReInit() error {
	return metacmd.store.InitClusterStore()
}

func (metacmd *MetaCmd) GetNodeId(key string) (string, error) {
	return metacmd.store.FindRange(key)
}

func (metacmd *MetaCmd) GetNodeLeaderAddr(nodeId string) (string, error) {
	return metacmd.store.GetNodeLeaderAddr(nodeId)
}
