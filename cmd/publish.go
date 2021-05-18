package cmd

import (
	"github.com/pkbhowmick/pg-monitoring/pkg/producer"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(publishCmd)
}

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "It will publish the database info to producer",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		producer.Publish()
	},
}
