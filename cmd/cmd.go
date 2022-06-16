package cmd

import (
	"os"

	"github.com/psanford/esbuddy/count"
	"github.com/psanford/esbuddy/search"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "esbuddy",
	Short: "Elasticsearch buddy",
}

func Execute() error {

	rootCmd.AddCommand(search.Command())
	rootCmd.AddCommand(count.Command())
	rootCmd.AddCommand(completionCommand())

	return rootCmd.Execute()
}

func completionCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "completion",
		Short: "Generates bash completion scripts",
		Long: `To load completion run

. <(esbuddy completion)

To configure your bash shell to load completions for each session add to your bashrc

# ~/.bashrc or ~/.profile
. <(esbuddy completion)
`,
		Run: func(cmd *cobra.Command, args []string) {
			rootCmd.GenBashCompletion(os.Stdout)
		},
	}

	return cmd
}
