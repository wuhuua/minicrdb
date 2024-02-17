package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wuhuua/minicrdb/pkg/logger"
	"github.com/wuhuua/minicrdb/pkg/storage"
)

func TestNodeRaftStart(t *testing.T) {
	newNode, err := startNode(1, "13150", "13150", true)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("set key:a, value:m")

	err = newNode.Set("a", []byte("m"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	res, err := newNode.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("get with key:a")
	fmt.Println(string(res))
	require.Equal(t, string(res), "m")
}

func TestMultiRaftStart(t *testing.T) {
	idNode1 := uint64(1)
	idNode2 := uint64(2)
	idNode3 := uint64(3)
	storeDir := "./testdb"
	raftDir := filepath.Join(storeDir, "raft")
	raftBind := "localhost:13150"
	singleNode := true
	engine, err := storage.NewEngine(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	network, err := InitNetwork(raftBind, 3, 10*time.Second, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	logDir := filepath.Join(storeDir, "log")
	logger.SetupLogger(logDir)
	newNode_1 := NewKVNode(raftDir, engine,
		strconv.FormatUint(idNode1, 10), raftBind, singleNode, network)
	newNode_2 := NewKVNode(raftDir, engine,
		strconv.FormatUint(idNode2, 10), raftBind, singleNode, network)
	newNode_3 := NewKVNode(raftDir, engine,
		strconv.FormatUint(idNode3, 10), raftBind, singleNode, network)
	err = newNode_1.InitRaftNode()
	if err != nil {
		t.Fatal(err)
	}
	err = newNode_2.InitRaftNode()
	if err != nil {
		t.Fatal(err)
	}
	err = newNode_3.InitRaftNode()
	if err != nil {
		t.Fatal(err)
	}
	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)
	fmt.Println("set to node_1 key:a, value:m")

	err = newNode_1.Set("a", []byte("m"))
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("set to node_2 key:b, value:n")
	err = newNode_2.Set("b", []byte("n"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	res, err := newNode_1.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("get from node_1 with key:a")
	fmt.Println(string(res))
	require.Equal(t, string(res), "m")

	res, err = newNode_2.Get("b")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("get from node_2 with key:b")
	fmt.Println(string(res))
	require.Equal(t, string(res), "n")
}

func TestNetworkTwoNodes(t *testing.T) {
	node_1, err := startNode(1, "13150", "13150", true)
	if err != nil {
		t.Fatal(err)
	}
	node_2, err := startNode(1, "13151", "13151", false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("set to node_1 key:a, value:m")
	node_1.Set("a", []byte("m"))
	node_1.Join(node_2.addr, node_2.nodeId)
	time.Sleep(3 * time.Second)
	fmt.Println("get from node_2 with key:a")
	res, err := node_2.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(res))
	require.Equal(t, string(res), "m")
}

func TestCheckPeerNum(t *testing.T) {
	node_1, err := startNode(1, "13150", "13150", true)
	if err != nil {
		t.Fatal(err)
	}
	node_2, err := startNode(1, "13151", "13151", false)
	if err != nil {
		t.Fatal(err)
	}
	node_3, err := startNode(1, "13152", "13152", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	fmt.Println("node_1 init")
	num := node_1.CheckPeerNum()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(num)

	err = node_1.Join(node_2.addr, node_2.nodeId)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("node_2 join")
	time.Sleep(3 * time.Second)
	num = node_2.CheckPeerNum()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(num)

	err = node_1.Join(node_3.addr, node_3.nodeId)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("node_3 join")
	time.Sleep(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	num = node_3.CheckPeerNum()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(num)

}

func startNode(nodeId int, port string, storeNum string, singleNode bool) (*KVNode, error) {
	id := uint64(nodeId)
	storeDir := "./testdb" + "_" + storeNum
	raftDir := filepath.Join(storeDir, "raft")
	raftBind := "localhost:" + port
	engine, err := storage.NewEngine(storeDir)
	if err != nil {
		return nil, err
	}
	network, err := InitNetwork(raftBind, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	logDir := filepath.Join(storeDir, "log")
	logger.SetupLogger(logDir)
	newNode := NewKVNode(raftDir, engine,
		strconv.FormatUint(id, 10), raftBind, singleNode, network)
	err = newNode.InitRaftNode()
	if err != nil {
		return nil, err
	}
	// Simple way to ensure there is a leader.
	if singleNode {
		time.Sleep(3 * time.Second)
	}
	return newNode, nil
}
