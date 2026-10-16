package ui

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/manifoldco/promptui"
	"kpub/internal/config"
	"kpub/internal/input"
	"kpub/internal/kafka"
)

var (
	Cyan    = color.New(color.FgCyan, color.Bold).SprintFunc()
	Green   = color.New(color.FgGreen, color.Bold).SprintFunc()
	Yellow  = color.New(color.FgYellow).SprintFunc()
	Red     = color.New(color.FgRed, color.Bold).SprintFunc()
	White   = color.New(color.FgHiWhite).SprintFunc()
	Magenta = color.New(color.FgMagenta, color.Bold).SprintFunc()
)

// PrintBanner prints the CLI banner.
func PrintBanner() {
	fmt.Println(Cyan("╔═════════════════════════════════════════════════════════════╗"))
	fmt.Println(Cyan("║                    kpub - Kafka Producer                    ║"))
	fmt.Println(Cyan("╚═════════════════════════════════════════════════════════════╝"))
}

// PrintSuccess prints single message publication details.
func PrintSuccess(res *kafka.PublishResult) {
	fmt.Println(Green("✔ Message successfully published to Kafka!"))
	fmt.Printf("  • %-12s: %s\n", Cyan("Topic"), White(res.Topic))
	fmt.Printf("  • %-12s: %s\n", Cyan("Partition"), Yellow(fmt.Sprintf("%d", res.Partition)))
	fmt.Printf("  • %-12s: %s\n", Cyan("Offset"), Yellow(fmt.Sprintf("%d", res.Offset)))
	if res.Key != "" {
		fmt.Printf("  • %-12s: %s\n", Cyan("Key"), White(res.Key))
	}
	fmt.Printf("  • %-12s: %s bytes\n", Cyan("Size"), Yellow(fmt.Sprintf("%d", res.PayloadSize)))
	fmt.Printf("  • %-12s: %s\n", Cyan("Timestamp"), White(res.Timestamp.Format(time.RFC3339Nano)))
}

// PrintBatchSummary prints stats of a batch or repeat publish.
func PrintBatchSummary(count int, duration time.Duration) {
	rate := float64(count) / duration.Seconds()
	fmt.Println(Green("\n✔ Batch publish finished successfully!"))
	fmt.Printf("  • %-14s: %s messages\n", Cyan("Total Sent"), Yellow(fmt.Sprintf("%d", count)))
	fmt.Printf("  • %-14s: %s\n", Cyan("Total Time"), White(duration.Round(time.Millisecond).String()))
	fmt.Printf("  • %-14s: %s msg/sec\n", Cyan("Throughput"), Yellow(fmt.Sprintf("%.2f", rate)))
}

// InteractiveSession holds user choices gathered interactively.
type InteractiveSession struct {
	Broker    string
	Topic     string
	Partition int32
	Key       string
	Headers   map[string]string
	Payload   []byte
}

// RunInteractivePrompt prompts the user step-by-step for publication parameters.
func RunInteractivePrompt(cfg *config.Config) (*InteractiveSession, error) {
	PrintBanner()
	fmt.Println(White("Follow the interactive prompts to publish your message:\n"))

	defaultBroker := "localhost:9092"
	if len(cfg.Brokers) > 0 && cfg.Brokers[0] != "" {
		defaultBroker = strings.Join(cfg.Brokers, ",")
	}

	// 1. Broker
	brokerPrompt := promptui.Prompt{
		Label:   "Kafka Broker(s)",
		Default: defaultBroker,
		Validate: func(input string) error {
			if strings.TrimSpace(input) == "" {
				return errors.New("broker cannot be empty")
			}
			return nil
		},
	}
	broker, err := brokerPrompt.Run()
	if err != nil {
		return nil, err
	}

	// 2. Topic
	topicDefault := cfg.Topic
	topicPrompt := promptui.Prompt{
		Label:   "Kafka Topic",
		Default: topicDefault,
		Validate: func(input string) error {
			if strings.TrimSpace(input) == "" {
				return errors.New("topic name is required")
			}
			return nil
		},
	}
	topic, err := topicPrompt.Run()
	if err != nil {
		return nil, err
	}

	// 3. Partition (optional)
	partitionPrompt := promptui.Prompt{
		Label:   "Partition (leave blank for automatic/hash)",
		Default: "",
		Validate: func(in string) error {
			in = strings.TrimSpace(in)
			if in == "" {
				return nil
			}
			_, err := strconv.ParseInt(in, 10, 32)
			if err != nil {
				return errors.New("partition must be a non-negative integer")
			}
			return nil
		},
	}
	partitionStr, err := partitionPrompt.Run()
	if err != nil {
		return nil, err
	}
	partition := int32(-1)
	if strings.TrimSpace(partitionStr) != "" {
		p, _ := strconv.ParseInt(strings.TrimSpace(partitionStr), 10, 32)
		partition = int32(p)
	}

	// 4. Message Key (optional)
	keyPrompt := promptui.Prompt{
		Label:   "Message Key (optional)",
		Default: "",
	}
	key, err := keyPrompt.Run()
	if err != nil {
		return nil, err
	}

	// 5. Headers (optional)
	headersPrompt := promptui.Prompt{
		Label:   "Headers (optional, comma-separated e.g. key1=val1,key2=val2)",
		Default: "",
	}
	headersRaw, err := headersPrompt.Run()
	if err != nil {
		return nil, err
	}
	var headers map[string]string
	if strings.TrimSpace(headersRaw) != "" {
		headerItems := strings.Split(headersRaw, ",")
		headers, err = input.ParseHeaders(headerItems)
		if err != nil {
			return nil, fmt.Errorf("invalid headers: %w", err)
		}
	}

	// 6. Payload Input Source
	sourceSelect := promptui.Select{
		Label: "Payload Source",
		Items: []string{
			"Type/paste message inline",
			"Read from file",
		},
	}
	idx, _, err := sourceSelect.Run()
	if err != nil {
		return nil, err
	}

	var payload []byte
	if idx == 0 {
		payloadPrompt := promptui.Prompt{
			Label: "Message Payload (JSON / String)",
			Validate: func(in string) error {
				if strings.TrimSpace(in) == "" {
					return errors.New("payload cannot be empty")
				}
				return nil
			},
		}
		rawPayload, err := payloadPrompt.Run()
		if err != nil {
			return nil, err
		}
		payload = []byte(rawPayload)
	} else {
		filePrompt := promptui.Prompt{
			Label: "File Path",
			Validate: func(in string) error {
				if strings.TrimSpace(in) == "" {
					return errors.New("file path cannot be empty")
				}
				return nil
			},
		}
		filePath, err := filePrompt.Run()
		if err != nil {
			return nil, err
		}
		payload, err = input.ReadFile(strings.TrimSpace(filePath))
		if err != nil {
			return nil, err
		}
	}

	// Confirm prompt
	fmt.Printf("\n%s\n", Cyan("Summary of message to send:"))
	fmt.Printf("  • Topic:     %s\n", topic)
	fmt.Printf("  • Broker:    %s\n", broker)
	if partition >= 0 {
		fmt.Printf("  • Partition: %d\n", partition)
	} else {
		fmt.Printf("  • Partition: auto\n")
	}
	if key != "" {
		fmt.Printf("  • Key:       %s\n", key)
	}
	fmt.Printf("  • Payload:   %s (bytes: %d)\n", string(payload), len(payload))

	confirmPrompt := promptui.Prompt{
		Label:     "Publish this message to Kafka",
		IsConfirm: true,
	}
	confirm, err := confirmPrompt.Run()
	if err != nil || strings.ToLower(confirm) != "y" {
		return nil, fmt.Errorf("publishing cancelled by user")
	}

	return &InteractiveSession{
		Broker:    broker,
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Headers:   headers,
		Payload:   payload,
	}, nil
}
