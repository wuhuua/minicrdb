package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "minicrdb",
		Short: "A distribute database based on raft",
		Long:  "minicrdb is based on raft and similar to cockroachdb, https://github.com/cockroachdb/cockroach",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Welcome to minicrdb")
			fmt.Println("For more details: https://github.com/wuhuua/minicrdb")
		},
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("config", "", "Config file path")
}
