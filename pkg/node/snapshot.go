package node

import (
	"github.com/hashicorp/raft"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

type RaftSnapshot struct {
	cmd   CmdHandler
	store *storage.Engine
}

type RaftSnapshotWriter struct {
	writer raft.SnapshotSink
}

func (rw *RaftSnapshotWriter) Write(p []byte) error {
	_, err := rw.writer.Write(p)
	return err
}

func (rw *RaftSnapshotWriter) Finish() error {
	return rw.writer.Close()
}

func (rw *RaftSnapshotWriter) Abort() {
	rw.writer.Cancel()
}

func (rs *RaftSnapshot) Persist(sink raft.SnapshotSink) error {
	keySpans := make([]storage.KeySpan, 0)
	keySpans = append(keySpans, *rs.cmd.GetKeySpan())
	snapshot := rs.store.GetRangeSnapShot(keySpans)
	if err := snapshot.ToSSTable(&RaftSnapshotWriter{writer: sink}); err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (rs *RaftSnapshot) Release() {
	// nothing to do
}
