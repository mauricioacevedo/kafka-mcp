# kafka-mcp

MCP server (Go) that inspects Kafka clusters: topics, consumer groups, lag and messages.

## Commands
- `make test`: `go test -race -cover ./...`. Uses kfake (in-process Kafka), so no Docker is needed. Run it before every commit.
- `make lint`: golangci-lint v2 (config in `.golangci.yml`).
- `make demo`: real Kafka, demo traffic and kafka-mcp over HTTP via docker compose.

## Layout
- `internal/inspector`: all Kafka logic and every safety rule (read-only mode, topic allowlist, hide-values, limits), so every transport gets them.
- `internal/mcpserver`: thin tool definitions. Handlers return Go errors, and the SDK turns them into `IsError` results the model can read.

## Conventions
- Tool errors must tell the model how to recover (e.g. "use list_topics", "restart with --allow-produce").
- Peeking must never join a consumer group or commit offsets.
- Lag results include a plain-language `diagnosis`; add a case to `TestDiagnose` when changing it.
- New tools need annotations (`ReadOnlyHint` etc.) and a test in `internal/mcpserver/server_test.go`.
- Never read credentials from flags; SASL credentials come from the environment only.
- Never write to stdout outside the MCP transport: in stdio mode it carries the protocol. Log to stderr.
