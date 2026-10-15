# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/kafka-mcp ./cmd/kafka-mcp \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/demo-producer ./cmd/demo-producer

# Demo traffic generator used by docker-compose.
FROM gcr.io/distroless/static-debian12:nonroot AS demo
COPY --from=build /out/demo-producer /demo-producer
ENTRYPOINT ["/demo-producer"]

# Default image: the MCP server.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kafka-mcp /kafka-mcp
ENTRYPOINT ["/kafka-mcp"]
