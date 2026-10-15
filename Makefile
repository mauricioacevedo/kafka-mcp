VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test lint demo docker

build: ## Build both binaries into ./bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

test: ## Run tests with the race detector (uses an in-process fake Kafka, no Docker needed)
	go test -race -cover ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

demo: ## Start Kafka, demo traffic and kafka-mcp (HTTP on :8080) with Docker
	docker compose up --build

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t kafka-mcp:$(VERSION) .
