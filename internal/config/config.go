package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds Kafka connection and security configuration.
type Config struct {
	Brokers               []string
	Topic                 string
	SASLMechanism         string
	SASLUsername          string
	SASLPassword          string
	TLSEnabled            bool
	TLSInsecureSkipVerify bool
	TLSCAFile             string
	TLSCertFile           string
	TLSKeyFile            string
	Timeout               time.Duration
	ClientID              string
	Partition             int32
}

func init() {
	LoadEnv()
}

// LoadEnv loads .env from the current working directory if available.
func LoadEnv() {
	_ = godotenv.Load()
}

// GetEnv retrieves an environment variable or returns defaultVal if empty.
func GetEnv(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

// GetEnvBool retrieves a boolean environment variable or returns defaultVal.
func GetEnvBool(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return defaultVal
	}
	return parsed
}

// ParseBrokers parses a comma-separated list of broker addresses.
func ParseBrokers(brokersStr string) []string {
	if brokersStr == "" {
		return []string{"localhost:9092"}
	}
	raw := strings.Split(brokersStr, ",")
	var result []string
	for _, b := range raw {
		trimmed := strings.TrimSpace(b)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return []string{"localhost:9092"}
	}
	return result
}
