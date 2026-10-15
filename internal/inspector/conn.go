package inspector

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ConnConfig describes how to reach and authenticate with a Kafka cluster.
type ConnConfig struct {
	Brokers []string

	// SASLMechanism is "", "PLAIN", "SCRAM-SHA-256" or "SCRAM-SHA-512".
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string

	TLS           bool
	TLSCAFile     string // optional PEM bundle; system roots are used otherwise
	TLSSkipVerify bool

	ClientID string
}

// ClientOptions turns c into franz-go client options.
func (c ConnConfig) ClientOptions() ([]kgo.Opt, error) {
	var brokers []string
	for _, b := range c.Brokers {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required (--brokers or KAFKA_BROKERS)")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	if c.ClientID != "" {
		opts = append(opts, kgo.ClientID(c.ClientID))
	}

	switch strings.ToUpper(c.SASLMechanism) {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q (want PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)", c.SASLMechanism)
	}

	if c.TLS {
		cfg := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: c.TLSSkipVerify, //nolint:gosec // opt-in flag for development clusters
		}
		if c.TLSCAFile != "" {
			pem, err := os.ReadFile(c.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("reading TLS CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("no certificates found in %s", c.TLSCAFile)
			}
			cfg.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	return opts, nil
}
