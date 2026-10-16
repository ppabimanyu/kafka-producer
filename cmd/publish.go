package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"kpub/internal/config"
	"kpub/internal/input"
	"kpub/internal/kafka"
	"kpub/internal/ui"
)

var (
	pubTopic        string
	pubMessage      string
	pubKey          string
	pubHeaders      []string
	pubPartition    int32
	pubFile         string
	pubBatch        bool
	pubRepeat       int
	pubInterval     time.Duration
	pubRate         int
	pubValidateJSON bool
)

var publishCmd = &cobra.Command{
	Use:     "publish",
	Aliases: []string{"pub", "p", "send"},
	Short:   "Publish a message or stream of messages to a Kafka topic",
	Example: `  # Publish a simple JSON message
  kpub publish -t my-topic -m '{"user": "alice", "action": "login"}' -k "user-123"

  # Publish with custom headers
  kpub publish -t my-topic -m '{"event": "ping"}' -H "source=web" -H "env=prod"

  # Publish from a JSON file
  kpub publish -t my-topic -f ./message.json

  # Publish lines from a file in batch
  kpub publish -t my-topic -f ./events.jsonl --batch

  # Pipe message from STDIN
  cat event.json | kpub publish -t my-topic

  # Repeat sending message with 500ms delay for streaming simulation
  kpub publish -t my-topic -m '{"status": "OK"}' --repeat 10 --interval 500ms`,
	RunE: runPublish,
}

func init() {
	RootCmd.AddCommand(publishCmd)

	publishCmd.Flags().StringVarP(&pubTopic, "topic", "t",
		config.GetEnv("KAFKA_TOPIC", ""),
		"Destination Kafka topic name (required, env: KAFKA_TOPIC)")

	publishCmd.Flags().StringVarP(&pubMessage, "message", "m", "",
		"Message payload string")

	publishCmd.Flags().StringVarP(&pubKey, "key", "k", "",
		"Optional partition key")

	publishCmd.Flags().StringArrayVarP(&pubHeaders, "header", "H", []string{},
		"Message headers in key=value format (can be specified multiple times)")

	publishCmd.Flags().Int32VarP(&pubPartition, "partition", "p", -1,
		"Target partition number (-1 for automatic partitioning)")

	publishCmd.Flags().StringVarP(&pubFile, "file", "f", "",
		"Path to file containing payload")

	publishCmd.Flags().BoolVarP(&pubBatch, "batch", "B", false,
		"Treat input file as newline-delimited batch (JSONL / multi-line)")

	publishCmd.Flags().IntVarP(&pubRepeat, "repeat", "r", 1,
		"Number of times to publish the message (repeat mode)")

	publishCmd.Flags().DurationVar(&pubInterval, "interval", 0,
		"Interval between repeated messages (e.g. 100ms, 1s)")

	publishCmd.Flags().IntVar(&pubRate, "rate", 0,
		"Maximum messages per second (rate limit, overrides interval if set)")

	publishCmd.Flags().BoolVar(&pubValidateJSON, "validate-json", false,
		"Validate that the message payload is valid JSON before publishing")
}

func runPublish(cmd *cobra.Command, args []string) error {
	if strings.TrimSpace(pubTopic) == "" {
		pubTopic = config.GetEnv("KAFKA_TOPIC", "")
	}
	if strings.TrimSpace(pubTopic) == "" {
		return fmt.Errorf("missing required topic name. Specify via -t/--topic or KAFKA_TOPIC env")
	}

	cfg := GetGlobalConfig()
	cfg.Topic = pubTopic
	cfg.Partition = pubPartition

	headersMap, err := input.ParseHeaders(pubHeaders)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// Initialize Kafka client
	client, err := kafka.NewClient(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	// Case 1: Batch from file
	if pubFile != "" && (pubBatch || strings.HasSuffix(pubFile, ".jsonl")) {
		return handleBatchFilePublish(client, headersMap)
	}

	// Case 2: Obtain single payload (from flag, file, or stdin)
	payload, err := resolvePayload()
	if err != nil {
		return err
	}

	if pubValidateJSON {
		if err := input.ValidateJSON(payload); err != nil {
			return err
		}
	}

	// Case 3: Repeat publish simulation
	if pubRepeat > 1 {
		return handleRepeatPublish(client, payload, headersMap)
	}

	// Case 4: Single publish
	recordInput := &kafka.RecordInput{
		Topic:     pubTopic,
		Partition: pubPartition,
		Key:       pubKey,
		Value:     payload,
		Headers:   headersMap,
	}

	res, err := kafka.PublishMessage(ctx, client, recordInput)
	if err != nil {
		return err
	}

	ui.PrintSuccess(res)
	return nil
}

func resolvePayload() ([]byte, error) {
	if pubMessage != "" {
		return []byte(pubMessage), nil
	}

	if pubFile != "" {
		return input.ReadFile(pubFile)
	}

	if input.HasStdinData() {
		data, err := input.ReadStdin()
		if err != nil {
			return nil, fmt.Errorf("failed to read from STDIN: %w", err)
		}
		if len(data) > 0 {
			return data, nil
		}
	}

	return nil, fmt.Errorf("no payload specified. Provide via -m/--message, -f/--file, or pipe data to STDIN")
}

func handleBatchFilePublish(client *kgo.Client, headers map[string]string) error {
	lines, err := input.ReadLinesFromFile(pubFile)
	if err != nil {
		return err
	}

	if len(lines) == 0 {
		return fmt.Errorf("batch file '%s' is empty", pubFile)
	}

	fmt.Printf("%s Starting batch publish of %s messages from '%s'...\n",
		ui.Cyan("ℹ"), ui.Yellow(fmt.Sprintf("%d", len(lines))), pubFile)

	startTime := time.Now()
	sleepDelay := pubInterval
	if pubRate > 0 {
		sleepDelay = time.Second / time.Duration(pubRate)
	}

	for i, line := range lines {
		if pubValidateJSON {
			if err := input.ValidateJSON(line); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := kafka.PublishMessage(ctx, client, &kafka.RecordInput{
			Topic:     pubTopic,
			Partition: pubPartition,
			Key:       pubKey,
			Value:     line,
			Headers:   headers,
		})
		cancel()

		if err != nil {
			return fmt.Errorf("failed producing line %d: %w", i+1, err)
		}

		fmt.Printf(" [%d/%d] Topic: %s | Partition: %d | Offset: %d | Size: %d B\n",
			i+1, len(lines), res.Topic, res.Partition, res.Offset, res.PayloadSize)

		if sleepDelay > 0 && i < len(lines)-1 {
			time.Sleep(sleepDelay)
		}
	}

	ui.PrintBatchSummary(len(lines), time.Since(startTime))
	return nil
}

func handleRepeatPublish(client *kgo.Client, payload []byte, headers map[string]string) error {
	fmt.Printf("%s Publishing %s repeated messages to topic '%s'...\n",
		ui.Cyan("ℹ"), ui.Yellow(fmt.Sprintf("%d", pubRepeat)), pubTopic)

	startTime := time.Now()
	sleepDelay := pubInterval
	if pubRate > 0 {
		sleepDelay = time.Second / time.Duration(pubRate)
	}

	for i := 1; i <= pubRepeat; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := kafka.PublishMessage(ctx, client, &kafka.RecordInput{
			Topic:     pubTopic,
			Partition: pubPartition,
			Key:       pubKey,
			Value:     payload,
			Headers:   headers,
		})
		cancel()

		if err != nil {
			return fmt.Errorf("failed producing message #%d: %w", i, err)
		}

		fmt.Printf(" [%d/%d] Partition: %d | Offset: %d | Time: %s\n",
			i, pubRepeat, res.Partition, res.Offset, res.Timestamp.Format("15:04:05.000"))

		if sleepDelay > 0 && i < pubRepeat {
			time.Sleep(sleepDelay)
		}
	}

	ui.PrintBatchSummary(pubRepeat, time.Since(startTime))
	return nil
}
