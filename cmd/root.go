package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pg-monitoring [command]",
	Short: "This is the root command",
	Long:  "This is the root command",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Greetings!\nType pg-monitoring --help to see available commands")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
