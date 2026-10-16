package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"kpub/internal/kafka"
	"kpub/internal/ui"
)

var pingCmd = &cobra.Command{
	Use:     "ping",
	Aliases: []string{"check", "test-connection"},
	Short:   "Test connectivity and health of Kafka broker(s)",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := GetGlobalConfig()

		fmt.Printf("%s Testing connectivity to brokers: %s\n",
			ui.Cyan("⏳"), ui.Yellow(strings.Join(cfg.Brokers, ", ")))

		start := time.Now()
		client, err := kafka.NewClient(cfg)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()

		if err := kafka.Ping(ctx, client); err != nil {
			return fmt.Errorf("ping failed: %w", err)
		}

		duration := time.Since(start)
		fmt.Printf("%s Successfully connected to Kafka brokers! Roundtrip latency: %s\n",
			ui.Green("✔"), ui.White(duration.Round(time.Millisecond).String()))
		return nil
	},
}

func init() {
	RootCmd.AddCommand(pingCmd)
}
