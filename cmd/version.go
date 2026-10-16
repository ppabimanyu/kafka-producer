package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"kpub/internal/ui"
)

var (
	Version   = "v1.0.0"
	GitCommit = "none"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version, commit, and runtime build info of kpub",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("%s %s\n", ui.Cyan("kpub version:"), ui.White(Version))
		fmt.Printf("%s %s\n", ui.Cyan("git commit:  "), ui.White(GitCommit))
		fmt.Printf("%s %s\n", ui.Cyan("build date:  "), ui.White(BuildDate))
		fmt.Printf("%s %s/%s (%s)\n", ui.Cyan("runtime:     "), runtime.GOOS, runtime.GOARCH, runtime.Version())
		return nil
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
