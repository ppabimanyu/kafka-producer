package input

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// HasStdinData returns true if data is being piped into standard input.
func HasStdinData() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	// Check if stdin is a pipe or regular file redirection
	return (stat.Mode() & os.ModeCharDevice) == 0
}

// ReadStdin reads all bytes from STDIN.
func ReadStdin() ([]byte, error) {
	return io.ReadAll(os.Stdin)
}

// ReadFile reads the entire content of a file.
func ReadFile(filePath string) ([]byte, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file '%s': %w", filePath, err)
	}
	return data, nil
}

// ReadLinesFromFile reads a file line by line, skipping blank lines.
// Useful for batch publishing (JSONL or plain lines).
func ReadLinesFromFile(filePath string) ([][]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", filePath, err)
	}
	defer f.Close()

	var lines [][]byte
	scanner := bufio.NewScanner(f)
	// Allow up to 10MB per line
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		// Copy bytes because scanner reuses the internal buffer
		lineCopy := make([]byte, len(line))
		copy(lineCopy, line)
		lines = append(lines, lineCopy)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading lines from '%s': %w", filePath, err)
	}

	return lines, nil
}

// ParseHeaders converts a slice of "key=value" strings into a map[string]string.
func ParseHeaders(headerSlice []string) (map[string]string, error) {
	headers := make(map[string]string)
	for _, h := range headerSlice {
		parts := strings.SplitN(h, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header format '%s', expected 'key=value'", h)
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("empty header key in '%s'", h)
		}
		headers[key] = val
	}
	return headers, nil
}

// ValidateJSON checks if a byte slice is valid JSON.
func ValidateJSON(payload []byte) error {
	var js json.RawMessage
	if err := json.Unmarshal(payload, &js); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}
	return nil
}
