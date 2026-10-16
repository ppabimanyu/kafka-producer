package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"kpub/internal/config"
	"kpub/internal/ui"
)

var (
	cfgBrokers       string
	cfgSASLMech      string
	cfgSASLUser      string
	cfgSASLPass      string
	cfgTLSEnabled    bool
	cfgTLSInsecure   bool
	cfgTLSCAFile     string
	cfgTLSCertFile   string
	cfgTLSKeyFile    string
	cfgTimeout       time.Duration
	cfgInteractive   bool

	globalConfig *config.Config
)

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "kpub",
	Short: "kpub - A lightweight, modern CLI tool for publishing messages to Apache Kafka",
	Long: ui.Cyan(`
  _  ______  _   _ ____  
 | |/ /  _ \| | | | __ ) 
 | ' /| |_) | | | |  _ \ 
 | . \|  __/| |_| | |_) |
 |_|\_\_|    \___/|____/ 
`) + `
kpub is a high-performance, developer-friendly Kafka CLI producer.
Supports single messages, file input, STDIN streaming, batch/repeat simulation,
and interactive mode with SASL/TLS authentication.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s %v\n", ui.Red("Error:"), err)
		os.Exit(1)
	}
}

func init() {
	// Auto load .env if present in current directory
	config.LoadEnv()

	RootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		if cfgInteractive {
			return runInteractive(cmd, args)
		}
		// If no subcommand or flags are passed, show help
		return cmd.Help()
	}

	// Persistent global flags with ENV fallback
	RootCmd.PersistentFlags().StringVarP(&cfgBrokers, "brokers", "b",
		config.GetEnv("KAFKA_BROKERS", "localhost:9092"),
		"Comma-separated list of Kafka broker addresses (env: KAFKA_BROKERS)")

	RootCmd.PersistentFlags().StringVar(&cfgSASLMech, "sasl-mechanism",
		config.GetEnv("KAFKA_SASL_MECHANISM", "none"),
		"SASL mechanism: none, plain, scram-sha-256, scram-sha-512 (env: KAFKA_SASL_MECHANISM)")

	RootCmd.PersistentFlags().StringVar(&cfgSASLUser, "sasl-username",
		config.GetEnv("KAFKA_SASL_USERNAME", ""),
		"SASL username (env: KAFKA_SASL_USERNAME)")

	RootCmd.PersistentFlags().StringVar(&cfgSASLPass, "sasl-password",
		config.GetEnv("KAFKA_SASL_PASSWORD", ""),
		"SASL password (env: KAFKA_SASL_PASSWORD)")

	RootCmd.PersistentFlags().BoolVar(&cfgTLSEnabled, "tls",
		config.GetEnvBool("KAFKA_TLS_ENABLED", false),
		"Enable TLS/SSL encryption (env: KAFKA_TLS_ENABLED)")

	RootCmd.PersistentFlags().BoolVar(&cfgTLSInsecure, "tls-insecure",
		config.GetEnvBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
		"Skip TLS certificate verification (env: KAFKA_TLS_INSECURE_SKIP_VERIFY)")

	RootCmd.PersistentFlags().StringVar(&cfgTLSCAFile, "tls-ca",
		config.GetEnv("KAFKA_TLS_CA_CERT", ""),
		"Path to custom CA certificate PEM file (env: KAFKA_TLS_CA_CERT)")

	RootCmd.PersistentFlags().StringVar(&cfgTLSCertFile, "tls-cert",
		config.GetEnv("KAFKA_TLS_CLIENT_CERT", ""),
		"Path to client certificate PEM file for mTLS (env: KAFKA_TLS_CLIENT_CERT)")

	RootCmd.PersistentFlags().StringVar(&cfgTLSKeyFile, "tls-key",
		config.GetEnv("KAFKA_TLS_CLIENT_KEY", ""),
		"Path to client private key PEM file for mTLS (env: KAFKA_TLS_CLIENT_KEY)")

	RootCmd.PersistentFlags().DurationVar(&cfgTimeout, "timeout",
		10*time.Second,
		"Kafka operation timeout (dial, produce, ping)")

	RootCmd.Flags().BoolVarP(&cfgInteractive, "interactive", "i", false,
		"Launch interactive wizard mode")
}

// GetGlobalConfig returns the parsed Config based on flags and ENV.
func GetGlobalConfig() *config.Config {
	if globalConfig != nil {
		return globalConfig
	}

	brokers := cfgBrokers
	if !RootCmd.PersistentFlags().Changed("brokers") {
		brokers = config.GetEnv("KAFKA_BROKERS", "localhost:9092")
	}

	saslMech := cfgSASLMech
	if !RootCmd.PersistentFlags().Changed("sasl-mechanism") {
		saslMech = config.GetEnv("KAFKA_SASL_MECHANISM", "none")
	}

	saslUser := cfgSASLUser
	if !RootCmd.PersistentFlags().Changed("sasl-username") {
		saslUser = config.GetEnv("KAFKA_SASL_USERNAME", "")
	}

	saslPass := cfgSASLPass
	if !RootCmd.PersistentFlags().Changed("sasl-password") {
		saslPass = config.GetEnv("KAFKA_SASL_PASSWORD", "")
	}

	tlsEnabled := cfgTLSEnabled
	if !RootCmd.PersistentFlags().Changed("tls") {
		tlsEnabled = config.GetEnvBool("KAFKA_TLS_ENABLED", false)
	}

	tlsInsecure := cfgTLSInsecure
	if !RootCmd.PersistentFlags().Changed("tls-insecure") {
		tlsInsecure = config.GetEnvBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false)
	}

	tlsCA := cfgTLSCAFile
	if !RootCmd.PersistentFlags().Changed("tls-ca") {
		tlsCA = config.GetEnv("KAFKA_TLS_CA_CERT", "")
	}

	tlsCert := cfgTLSCertFile
	if !RootCmd.PersistentFlags().Changed("tls-cert") {
		tlsCert = config.GetEnv("KAFKA_TLS_CLIENT_CERT", "")
	}

	tlsKey := cfgTLSKeyFile
	if !RootCmd.PersistentFlags().Changed("tls-key") {
		tlsKey = config.GetEnv("KAFKA_TLS_CLIENT_KEY", "")
	}

	globalConfig = &config.Config{
		Brokers:               config.ParseBrokers(brokers),
		Topic:                 config.GetEnv("KAFKA_TOPIC", ""),
		SASLMechanism:         saslMech,
		SASLUsername:          saslUser,
		SASLPassword:          saslPass,
		TLSEnabled:            tlsEnabled,
		TLSInsecureSkipVerify: tlsInsecure,
		TLSCAFile:             tlsCA,
		TLSCertFile:           tlsCert,
		TLSKeyFile:            tlsKey,
		Timeout:               cfgTimeout,
		ClientID:              config.GetEnv("KAFKA_CLIENT_ID", "kpub"),
		Partition:             -1,
	}

	return globalConfig
}
