package storage

import (
	"log"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

var (
	ERROR_NOT_FOUND = pebble.ErrNotFound
)

type Engine struct {
	db *pebble.DB
}

type KeySpan struct {
	StartKey string
	EndKey   string
}

type Snapshot struct {
	efos *pebble.EventuallyFileOnlySnapshot
}

type KVPair struct {
	Key   string
	Value []byte
}

func NewEngine(dataDir string) (*Engine, error) {
	db, err := pebble.Open(dataDir, &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	return &Engine{db}, nil
}

func (engine *Engine) Close() error {
	return engine.db.Close()
}

func (engine *Engine) Set(key string, value []byte) error {
	return engine.db.Set([]byte(key), value, nil)
}

func (engine *Engine) Get(key string) ([]byte, error) {
	val, closer, err := engine.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	return val, closer.Close()
}

func (engine *Engine) Delete(key string) error {
	return engine.db.Delete([]byte(key), nil)
}

func (engine *Engine) RangeDelete(start string, end string) error {
	return engine.db.RangeKeyDelete([]byte(start), []byte(end), nil)
}

func (engine *Engine) GetSnapShot() *pebble.Snapshot {
	return engine.db.NewSnapshot()
}

func (engine *Engine) GetRangeSnapShot(keySpans []KeySpan) *Snapshot {
	engineKeyRanges := make([]pebble.KeyRange, len(keySpans))
	for i := range keySpans {
		engineKeyRanges[i].Start = []byte(keySpans[i].StartKey)
		engineKeyRanges[i].End = []byte(keySpans[i].EndKey)
	}
	return &Snapshot{
		efos: engine.db.NewEventuallyFileOnlySnapshot(engineKeyRanges),
	}
}

func (snapshot *Snapshot) ToSSTable(writer objstorage.Writable) error {
	format := sstable.TableFormatRocksDBv2
	opts := DefaultPebbleOptions().MakeWriterOptions(0, format)
	opts.BlockPropertyCollectors = nil
	opts.FilterPolicy = nil
	opts.BlockSize = 128 << 10
	opts.MergerName = "nullptr"
	fw := sstable.NewWriter(writer, opts)
	iter, _ := snapshot.efos.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		if err := fw.Set(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return fw.Close()
}

func (engine *Engine) RestoreFromSSTable(path []string) error {
	return engine.db.Ingest(path)
}

func (engine *Engine) FindKeysInSpan(keySpan *KeySpan) ([]KVPair, error) {
	iter, err := engine.db.NewIter(nil)
	if err != nil {
		return []KVPair{}, err
	}
	kvPair := make([]KVPair, 0)
	iter.SetBounds([]byte(keySpan.StartKey), []byte(keySpan.EndKey))
	for iter.First(); iter.Valid(); iter.Next() {
		kvPair = append(kvPair, KVPair{Key: string(iter.Key()), Value: iter.Value()})
	}
	return kvPair, nil
}

func (engine *Engine) GetKeyList() []string {
	res := make([]string, 0)
	snapshot := engine.GetSnapShot()
	iter, _ := snapshot.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		res = append(res, string(iter.Key()))
	}
	return res
}
