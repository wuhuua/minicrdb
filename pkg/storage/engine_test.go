package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type testWriter struct {
	writer *os.File
}

func (rw *testWriter) Write(p []byte) error {
	_, err := rw.writer.Write(p)
	return err
}

func (rw *testWriter) Finish() error {
	return rw.writer.Close()
}

func (rw *testWriter) Abort() {
}

const testStorageDir = "./testdb"
const testStorageSSTDir = testStorageDir + "/sst"

func printEngineKeys(engine *Engine) {
	keyList := engine.GetKeyList()
	for _, key := range keyList {
		fmt.Println(key)
	}
}

func TestWriteAndReadSST(t *testing.T) {
	engine, err := NewEngine(testStorageDir)

	if err != nil {
		fmt.Println("Error on new engine")
	}
	engine.Set("a", []byte("m"))
	engine.Set("b", []byte("m"))
	engine.Set("c", []byte("m"))
	engine.Set("d", []byte("m"))

	os.MkdirAll(testStorageSSTDir, 0755)

	snapshot := engine.GetRangeSnapShot([]KeySpan{{StartKey: "a", EndKey: "d"}})

	filePath := filepath.Join(testStorageSSTDir, "test.sst")
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	snapshot.ToSSTable(&testWriter{writer: file})

	engine.Delete("a")
	engine.Delete("b")
	engine.Delete("c")

	fmt.Println("before ingest")
	printEngineKeys(engine)

	err = engine.RestoreFromSSTable([]string{filePath})
	if err != nil {
		panic(err)
	}
	fmt.Println("after ingest")
	printEngineKeys(engine)
	require.Equal(t, len(engine.GetKeyList()), 4)
}
