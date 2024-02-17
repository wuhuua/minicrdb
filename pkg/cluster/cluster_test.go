package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/wuhuua/minicrdb/pkg/httpd"
)

func TestClusterPrintKeys(t *testing.T) {
	cluster_1, err := startCluster("13150", []string{}, "8080")
	if err != nil {
		t.Fatal(err)
	}
	cluster_2, err := startCluster("13151", []string{"localhost:8080"}, "8081")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	fmt.Println("Print cluster_1 info")
	printAllKeys(cluster_1)
	fmt.Println("Print cluster_2 info")
	printAllKeys(cluster_2)
}

func TestClusterWithThreeNode(t *testing.T) {
	_, err := startCluster("13150", []string{}, "8080")
	if err != nil {
		t.Fatal(err)
	}
	_, err = startCluster("13151", []string{"localhost:8080"}, "8081")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	fmt.Println("set to node_2 key:a, value:m")
	err = httpd.CliSet("localhost:8081", "a", "m")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("get from node_1 key:a")
	val, err := httpd.CliGet("localhost:8080", "a")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
}

func startCluster(raftPort string, serverAddrs []string, httpPort string) (*Cluster, error) {
	storeDir := "./testdb" + "_" + raftPort
	addr := "localhost"
	return NewCluster(storeDir, addr, raftPort, serverAddrs, httpPort)
}

func printAllKeys(cluster *Cluster) {
	keys := cluster.GetClusterKeys()
	for _, key := range keys {
		fmt.Println(key)
	}
}
