package inspector

import (
	"fmt"
	"path"
	"strings"
)

// TopicPolicy decides which topics may have their messages read or written.
// Metadata (partitions, offsets, lag) is always visible; message contents are
// what the policy protects. Patterns use path.Match syntax ("orders.*").
type TopicPolicy struct {
	patterns []string
}

// NewTopicPolicy validates patterns. An empty list allows every topic except
// Kafka's internal ones (names starting with "__").
func NewTopicPolicy(patterns []string) (*TopicPolicy, error) {
	p := &TopicPolicy{}
	for _, raw := range patterns {
		pattern := strings.TrimSpace(raw)
		if pattern == "" {
			continue
		}
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid topic pattern %q: %w", raw, err)
		}
		p.patterns = append(p.patterns, pattern)
	}
	return p, nil
}

// Check returns an error if the messages of topic may not be accessed.
func (p *TopicPolicy) Check(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic is required")
	}
	if len(p.patterns) == 0 {
		if strings.HasPrefix(topic, "__") {
			return fmt.Errorf("messages of internal topic %q are not accessible; allow it explicitly with --allow-topics", topic)
		}
		return nil
	}
	for _, pattern := range p.patterns {
		if ok, _ := path.Match(pattern, topic); ok {
			return nil
		}
	}
	return fmt.Errorf("messages of topic %q are not accessible; allowed patterns: %s (change with --allow-topics)",
		topic, strings.Join(p.patterns, ", "))
}
