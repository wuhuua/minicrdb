package storage

import (
	"time"

	"github.com/cockroachdb/pebble"
)

func DefaultPebbleOptions() *pebble.Options {
	opts := &pebble.Options{
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
	}
	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB
	return opts
}
