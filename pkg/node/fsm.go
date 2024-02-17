package node

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

const (
	raftTimeout = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type CmdHandler interface {
	HandleCmd(command string, key string, value []byte) error
	ReInit() error
	GetKeySpan() *storage.KeySpan
}

type cmdFSM struct {
	logger  *logger.Logger
	handler CmdHandler
	store   *storage.Engine
	raftDir string
	nodeId  string
}

func (s *cmdFSM) Apply(logEntry *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(logEntry.Data, &c); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	if err := s.handler.HandleCmd(c.Op, c.Key, []byte(c.Value)); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

func (s *cmdFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &RaftSnapshot{cmd: s.handler, store: s.store}, nil
}

func (s *cmdFSM) Restore(snapshot io.ReadCloser) error {
	data, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}
	sstName := "snapshot" + "_" + s.nodeId
	path := filepath.Join(s.raftDir, sstName)
	os.WriteFile(path, data, 0644)
	s.store.RestoreFromSSTable([]string{path})
	s.handler.ReInit()
	return nil
}
