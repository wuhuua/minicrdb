package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wuhuua/minicrdb/pkg/cluster"
)

var (
	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start an minicrdb instance",
		Long:  "start an minicrdb instance, without --join to init as single node",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Starting minicrdb")
			joinAddrList := splitAndTrim(joinAddrs)
			if len(joinAddrList) == 0 {
				fmt.Println("start as a single node")
			} else {
				fmt.Println("[join servers below]")
				for _, joinAddr := range joinAddrList {
					fmt.Println(joinAddr)
				}
			}
			fmt.Println("[data storage directory]: " + storeDir)
			fmt.Println("[listen and advertise on]: " + addr)
			fmt.Println("[cluster communicates on port]: " + raftPort)
			fmt.Println("[cluster http service on port]: " + httpPort)

			_, err := cluster.NewCluster(storeDir, addr, raftPort, joinAddrList, httpPort)
			if err != nil {
				fmt.Println("Init Cluster error")
				fmt.Println(err)
				return
			}
			fmt.Println("Successfully init")

			terminate := make(chan os.Signal, 1)
			signal.Notify(terminate, os.Interrupt)
			<-terminate
			fmt.Println("minicrdb exiting")
		},
	}
	joinAddrs = ""
	storeDir  = "./minicrdb_data"
	addr      = "localhost"
	raftPort  = "13150"
	httpPort  = "8080"
)

func init() {
	startCmd.Flags().StringVar(&joinAddrs, "join", "", "server to join")
	startCmd.Flags().StringVar(&storeDir, "store", "./minicrdb_data", "data storage directory")
	startCmd.Flags().StringVar(&addr, "addr", "localhost", "network address")
	startCmd.Flags().StringVar(&raftPort, "node-port", "13150", "port for raft")
	startCmd.Flags().StringVar(&httpPort, "http-port", "8080", "port for http")
	rootCmd.AddCommand(startCmd)
}

func splitAndTrim(s string) []string {
	if len(s) == 0 {
		return []string{}
	}
	parts := strings.Split(s, ",")
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}
	return parts
}
