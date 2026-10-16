package config

import (
	"reflect"
	"testing"
)

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single broker",
			input:    "localhost:9092",
			expected: []string{"localhost:9092"},
		},
		{
			name:     "multiple brokers with spaces",
			input:    "kafka1:9092, kafka2:9092 , kafka3:9092",
			expected: []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
		},
		{
			name:     "empty string fallback to default",
			input:    "",
			expected: []string{"localhost:9092"},
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: []string{"localhost:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseBrokers(tt.input)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ParseBrokers(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
