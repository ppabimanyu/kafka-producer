package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"kpub/internal/config"
	"kpub/internal/kafka"
	"kpub/internal/ui"
)

var interactiveCmd = &cobra.Command{
	Use:     "interactive",
	Aliases: []string{"ui", "wizard", "prompt"},
	Short:   "Launch an interactive prompt wizard to compose and publish messages",
	RunE:    runInteractive,
}

func runInteractive(cmd *cobra.Command, args []string) error {
	cfg := GetGlobalConfig()

	session, err := ui.RunInteractivePrompt(cfg)
	if err != nil {
		return err
	}

	// Apply chosen broker and topic
	cfg.Brokers = config.ParseBrokers(session.Broker)
	cfg.Topic = session.Topic
	cfg.Partition = session.Partition

	fmt.Printf("\n%s Connecting to Kafka broker(s) and publishing message...\n", ui.Cyan("⏳"))

	client, err := kafka.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := kafka.PublishMessage(ctx, client, &kafka.RecordInput{
		Topic:     session.Topic,
		Partition: session.Partition,
		Key:       session.Key,
		Value:     session.Payload,
		Headers:   session.Headers,
	})
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	fmt.Println()
	ui.PrintSuccess(res)
	return nil
}

func init() {
	RootCmd.AddCommand(interactiveCmd)
}
