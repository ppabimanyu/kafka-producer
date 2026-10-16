package input

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseHeaders(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    map[string]string
		wantErr bool
	}{
		{
			name:  "valid single header",
			input: []string{"trace-id=abc-123"},
			want:  map[string]string{"trace-id": "abc-123"},
		},
		{
			name:  "valid multiple headers",
			input: []string{"env=production", "app=kpub", "version=1.0.0"},
			want:  map[string]string{"env": "production", "app": "kpub", "version": "1.0.0"},
		},
		{
			name:  "header with equals in value",
			input: []string{"formula=a=b+c"},
			want:  map[string]string{"formula": "a=b+c"},
		},
		{
			name:    "invalid header without equals",
			input:   []string{"invalid_header"},
			wantErr: true,
		},
		{
			name:    "empty key",
			input:   []string{"=value"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseHeaders(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseHeaders() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				for k, v := range tt.want {
					if got[k] != v {
						t.Errorf("header %s = %v, want %v", k, got[k], v)
					}
				}
			}
		})
	}
}

func TestValidateJSON(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantErr bool
	}{
		{
			name:    "valid object",
			payload: []byte(`{"message": "hello", "code": 200}`),
			wantErr: false,
		},
		{
			name:    "valid array",
			payload: []byte(`[1, 2, 3, "foo"]`),
			wantErr: false,
		},
		{
			name:    "invalid json string",
			payload: []byte(`{message: hello}`),
			wantErr: true,
		},
		{
			name:    "empty bytes",
			payload: []byte(""),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateJSON(tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReadLinesFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.jsonl")

	content := `{"id": 1}

{"id": 2}
{"id": 3}
`
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp test file: %v", err)
	}

	lines, err := ReadLinesFromFile(testFile)
	if err != nil {
		t.Fatalf("ReadLinesFromFile() failed: %v", err)
	}

	if len(lines) != 3 {
		t.Fatalf("expected 3 non-empty lines, got %d", len(lines))
	}

	if string(lines[0]) != `{"id": 1}` {
		t.Errorf("expected line 0 to be '{\"id\": 1}', got '%s'", string(lines[0]))
	}
}
