package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"kpub/internal/config"
)

// NewClient initializes a new Franz-Go Kafka client with configured options.
func NewClient(cfg *config.Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.Timeout),
		kgo.ProduceRequestTimeout(cfg.Timeout),
		kgo.RecordDeliveryTimeout(cfg.Timeout),
		kgo.RequestTimeoutOverhead(cfg.Timeout),
	}

	// Use manual partitioner only when an explicit partition is specified (>= 0).
	// Otherwise, leave unset to let Franz-Go use its default UniformBytesPartitioner.
	if cfg.Partition >= 0 {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}

	// SASL Authentication
	if cfg.SASLMechanism != "" && !strings.EqualFold(cfg.SASLMechanism, "none") {
		mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL mechanism: %w", err)
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	// TLS / SSL Configuration
	if cfg.TLSEnabled {
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return client, nil
}

// Ping checks connectivity with Kafka broker cluster.
func Ping(ctx context.Context, client *kgo.Client) error {
	return client.Ping(ctx)
}

func buildSASLMechanism(mech, user, pass string) (sasl.Mechanism, error) {
	switch strings.ToLower(mech) {
	case "plain":
		return plain.Auth{
			User: user,
			Pass: pass,
		}.AsMechanism(), nil
	case "scram-sha-256", "scram256":
		return scram.Auth{
			User: user,
			Pass: pass,
		}.AsSha256Mechanism(), nil
	case "scram-sha-512", "scram512":
		return scram.Auth{
			User: user,
			Pass: pass,
		}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s (supported: plain, scram-sha-256, scram-sha-512)", mech)
	}
}

func buildTLSConfig(cfg *config.Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
	}

	// Custom Root CA certificate
	if cfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file '%s': %w", cfg.TLSCAFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from PEM in '%s'", cfg.TLSCAFile)
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Client certificates for mutual TLS (mTLS)
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client key pair (%s, %s): %w", cfg.TLSCertFile, cfg.TLSKeyFile, err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
