BINARY_NAME=kpub
BIN_DIR=bin
VERSION ?= v1.0.0
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS = -ldflags "-s -w -X kpub/cmd.Version=$(VERSION) -X kpub/cmd.GitCommit=$(COMMIT) -X kpub/cmd.BuildDate=$(DATE)"

.PHONY: all build clean test install help

all: build

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

build: ## Build the kpub binary
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/$(BINARY_NAME) .
	@echo "Build complete: $(BIN_DIR)/$(BINARY_NAME)"

install: build ## Install the binary to GOPATH/bin
	go install $(LDFLAGS) .
	@echo "Installed $(BINARY_NAME) to $(shell go env GOPATH)/bin"

clean: ## Remove built binaries and artifacts
	rm -rf $(BIN_DIR)
	@echo "Cleaned $(BIN_DIR)"

test: ## Run unit tests
	go test -v ./...
