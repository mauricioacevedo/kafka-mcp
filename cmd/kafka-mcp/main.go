// Command kafka-mcp is an MCP server that lets AI assistants inspect Kafka
// clusters: topics, consumer groups, lag and messages.
//
// It speaks MCP over stdio (for Claude Code and Claude Desktop) or over
// Streamable HTTP (for remote clients such as ChatGPT and claude.ai).
package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/mauricioacevedo/kafka-mcp/internal/inspector"
	"github.com/mauricioacevedo/kafka-mcp/internal/mcpserver"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kafka-mcp:", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run() error {
	var (
		transport     = flag.String("transport", "stdio", "MCP transport: stdio or http")
		listen        = flag.String("listen", "127.0.0.1:8080", "address for the HTTP transport")
		httpToken     = flag.String("http-token", os.Getenv("KAFKA_MCP_HTTP_TOKEN"), "bearer token required by the HTTP transport (env KAFKA_MCP_HTTP_TOKEN)")
		brokers       = flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated seed brokers (env KAFKA_BROKERS)")
		saslMechanism = flag.String("sasl-mechanism", os.Getenv("KAFKA_SASL_MECHANISM"), "PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 (env KAFKA_SASL_MECHANISM)")
		useTLS        = flag.Bool("tls", false, "connect to brokers using TLS")
		tlsCA         = flag.String("tls-ca", "", "PEM file with CA certificates (system roots by default)")
		tlsSkipVerify = flag.Bool("tls-skip-verify", false, "skip TLS certificate verification (development only)")
		allowTopics   = flag.String("allow-topics", "", "comma-separated topic patterns whose messages may be read or written; empty allows all non-internal topics")
		allowProduce  = flag.Bool("allow-produce", false, "allow produce_message to write messages (read-only by default)")
		hideValues    = flag.Bool("hide-values", false, "never return message values, only keys, headers and sizes")
		maxMessages   = flag.Int("max-messages", 50, "maximum messages returned by one peek")
		maxValueBytes = flag.Int("max-value-bytes", 4096, "truncate keys and values longer than this")
		timeout       = flag.Duration("timeout", 15*time.Second, "timeout for each operation")
		showVersion   = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}

	// stdout carries the MCP protocol in stdio mode, so logs go to stderr.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	var topics []string
	if *allowTopics != "" {
		topics = strings.Split(*allowTopics, ",")
	}
	in, err := inspector.New(inspector.Options{
		Conn: inspector.ConnConfig{
			Brokers:       strings.Split(*brokers, ","),
			SASLMechanism: *saslMechanism,
			// Credentials come only from the environment so they never appear
			// in process listings or client config files.
			SASLUsername:  os.Getenv("KAFKA_SASL_USERNAME"),
			SASLPassword:  os.Getenv("KAFKA_SASL_PASSWORD"),
			TLS:           *useTLS,
			TLSCAFile:     *tlsCA,
			TLSSkipVerify: *tlsSkipVerify,
			ClientID:      "kafka-mcp",
		},
		AllowedTopics: topics,
		AllowProduce:  *allowProduce,
		HideValues:    *hideValues,
		MaxMessages:   *maxMessages,
		MaxValueBytes: *maxValueBytes,
		Timeout:       *timeout,
	})
	if err != nil {
		return err
	}
	defer in.Close()

	server := mcpserver.New(in, version)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting", "version", version, "transport", *transport, "brokers", *brokers,
		"allow_produce", *allowProduce, "hide_values", *hideValues)
	switch *transport {
	case "stdio":
		return server.Run(ctx, &mcp.StdioTransport{})
	case "http":
		return serveHTTP(ctx, logger, server, *listen, *httpToken)
	default:
		return fmt.Errorf("unknown transport %q (want stdio or http)", *transport)
	}
}

func serveHTTP(ctx context.Context, logger *slog.Logger, server *mcp.Server, addr, token string) error {
	mux := http.NewServeMux()
	mux.Handle("/mcp", requireToken(token, mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if token == "" {
		logger.Warn("HTTP transport has no --http-token; anyone who can reach it can use it")
	}
	logger.Info("listening", "url", "http://"+addr+"/mcp")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func requireToken(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
