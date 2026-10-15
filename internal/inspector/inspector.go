// Package inspector reads Kafka cluster state (brokers, topics, consumer groups
// and lag) and messages, enforcing the safety policy configured by the operator.
package inspector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ErrProduceDisabled is returned by Produce when the server runs in read-only mode.
var ErrProduceDisabled = errors.New("producing messages is disabled; restart kafka-mcp with --allow-produce to enable it")

// Limits that apply even if the operator configures larger values.
const (
	hardMaxMessages   = 500
	hardMaxValueBytes = 1 << 20
)

// Options configure an Inspector.
type Options struct {
	Conn ConnConfig
	// AllowedTopics are path.Match patterns for topics whose messages may be
	// read or written. Empty allows all non-internal topics.
	AllowedTopics []string
	// AllowProduce enables Produce. When false the inspector is read-only.
	AllowProduce bool
	// HideValues omits message values from peek results (keys, headers and
	// sizes are still returned). Useful when topics carry personal data.
	HideValues bool
	// MaxMessages caps how many messages one peek returns. Defaults to 50.
	MaxMessages int
	// MaxValueBytes truncates keys and values longer than this. Defaults to 4 KiB.
	MaxValueBytes int
	// Timeout bounds every operation. Defaults to 15s.
	Timeout time.Duration
}

// Inspector serves inspection requests against one cluster. It is safe for
// concurrent use.
type Inspector struct {
	opts       Options
	policy     *TopicPolicy
	clientOpts []kgo.Opt
	client     *kgo.Client
	admin      *kadm.Client
}

// New validates opts and returns an Inspector. It does not contact the
// cluster until the first request.
func New(opts Options) (*Inspector, error) {
	if opts.MaxMessages <= 0 {
		opts.MaxMessages = 50
	}
	opts.MaxMessages = min(opts.MaxMessages, hardMaxMessages)
	if opts.MaxValueBytes <= 0 {
		opts.MaxValueBytes = 4 << 10
	}
	opts.MaxValueBytes = min(opts.MaxValueBytes, hardMaxValueBytes)
	if opts.Timeout <= 0 {
		opts.Timeout = 15 * time.Second
	}
	policy, err := NewTopicPolicy(opts.AllowedTopics)
	if err != nil {
		return nil, err
	}
	clientOpts, err := opts.Conn.ClientOptions()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka client: %w", err)
	}
	return &Inspector{
		opts:       opts,
		policy:     policy,
		clientOpts: clientOpts,
		client:     client,
		admin:      kadm.NewClient(client),
	}, nil
}

// ProduceAllowed reports whether Produce is enabled.
func (i *Inspector) ProduceAllowed() bool { return i.opts.AllowProduce }

// MaxMessages is the most messages a single peek returns.
func (i *Inspector) MaxMessages() int { return i.opts.MaxMessages }

// Close releases the underlying client.
func (i *Inspector) Close() { i.client.Close() }

func (i *Inspector) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, i.opts.Timeout)
}

// ClusterInfo describes the cluster and its brokers.
type ClusterInfo struct {
	ClusterID    string       `json:"cluster_id"`
	ControllerID int32        `json:"controller_id"`
	Brokers      []BrokerInfo `json:"brokers"`
	Topics       int          `json:"topics"`
}

// BrokerInfo describes one broker.
type BrokerInfo struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
	Rack string `json:"rack,omitempty"`
}

// DescribeCluster returns the cluster ID, controller and brokers.
func (i *Inspector) DescribeCluster(ctx context.Context) (ClusterInfo, error) {
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	md, err := i.admin.Metadata(ctx)
	if err != nil {
		return ClusterInfo{}, fmt.Errorf("fetching cluster metadata: %w", err)
	}
	info := ClusterInfo{ClusterID: md.Cluster, ControllerID: md.Controller, Topics: len(md.Topics)}
	for _, b := range md.Brokers {
		bi := BrokerInfo{ID: b.NodeID, Host: b.Host, Port: b.Port}
		if b.Rack != nil {
			bi.Rack = *b.Rack
		}
		info.Brokers = append(info.Brokers, bi)
	}
	return info, nil
}

// TopicSummary is one row of ListTopics.
type TopicSummary struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	Internal          bool   `json:"internal,omitempty"`
	// Messages is end offset minus start offset summed over partitions. It is
	// an upper bound for compacted or transactional topics.
	Messages int64 `json:"messages"`
}

// ListTopics returns topics sorted by name. filter is a case-insensitive substring.
func (i *Inspector) ListTopics(ctx context.Context, includeInternal bool, filter string) ([]TopicSummary, error) {
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	var (
		topics kadm.TopicDetails
		err    error
	)
	if includeInternal {
		topics, err = i.admin.ListTopicsWithInternal(ctx)
	} else {
		topics, err = i.admin.ListTopics(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	filter = strings.ToLower(filter)
	var names []string
	for name := range topics {
		if strings.Contains(strings.ToLower(name), filter) {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	counts := map[string]int64{}
	if len(names) > 0 {
		counts, err = i.messageCounts(ctx, names...)
		if err != nil {
			return nil, err
		}
	}
	out := []TopicSummary{}
	for _, name := range names {
		t := topics[name]
		out = append(out, TopicSummary{
			Name:              name,
			Partitions:        len(t.Partitions),
			ReplicationFactor: t.Partitions.NumReplicas(),
			Internal:          t.IsInternal,
			Messages:          counts[name],
		})
	}
	return out, nil
}

func (i *Inspector) messageCounts(ctx context.Context, topics ...string) (map[string]int64, error) {
	starts, ends, err := i.offsets(ctx, topics...)
	if err != nil {
		return nil, err
	}
	counts := map[string]int64{}
	ends.Each(func(end kadm.ListedOffset) {
		if start, ok := starts.Lookup(end.Topic, end.Partition); ok && end.Err == nil && start.Err == nil {
			counts[end.Topic] += max(end.Offset-start.Offset, 0)
		}
	})
	return counts, nil
}

func (i *Inspector) offsets(ctx context.Context, topics ...string) (starts, ends kadm.ListedOffsets, err error) {
	starts, err = i.admin.ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, nil, fmt.Errorf("listing start offsets: %w", err)
	}
	ends, err = i.admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, nil, fmt.Errorf("listing end offsets: %w", err)
	}
	return starts, ends, nil
}

// TopicInfo describes a topic in detail.
type TopicInfo struct {
	Name                      string            `json:"name"`
	Internal                  bool              `json:"internal,omitempty"`
	ReplicationFactor         int               `json:"replication_factor"`
	TotalMessages             int64             `json:"total_messages"`
	UnderReplicatedPartitions int               `json:"under_replicated_partitions"`
	Partitions                []PartitionInfo   `json:"partitions"`
	Configs                   map[string]string `json:"configs"`
}

// PartitionInfo describes one partition of a topic.
type PartitionInfo struct {
	Partition       int32   `json:"partition"`
	Leader          int32   `json:"leader"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offline_replicas,omitempty"`
	UnderReplicated bool    `json:"under_replicated,omitempty"`
	StartOffset     int64   `json:"start_offset"`
	EndOffset       int64   `json:"end_offset"`
	Messages        int64   `json:"messages"`
	Error           string  `json:"error,omitempty"`
}

// DescribeTopic returns partitions, offsets, replica health and the configs
// that differ from the broker defaults.
func (i *Inspector) DescribeTopic(ctx context.Context, topic string) (TopicInfo, error) {
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	detail, err := i.topicDetail(ctx, topic)
	if err != nil {
		return TopicInfo{}, err
	}
	starts, ends, err := i.offsets(ctx, topic)
	if err != nil {
		return TopicInfo{}, err
	}

	info := TopicInfo{
		Name:              topic,
		Internal:          detail.IsInternal,
		ReplicationFactor: detail.Partitions.NumReplicas(),
		Partitions:        []PartitionInfo{},
		Configs:           map[string]string{},
	}
	for _, p := range detail.Partitions.Sorted() {
		pi := PartitionInfo{
			Partition:       p.Partition,
			Leader:          p.Leader,
			Replicas:        p.Replicas,
			ISR:             p.ISR,
			OfflineReplicas: p.OfflineReplicas,
			UnderReplicated: len(p.ISR) < len(p.Replicas),
		}
		if p.Err != nil {
			pi.Error = p.Err.Error()
		}
		start, _ := starts.Lookup(topic, p.Partition)
		end, _ := ends.Lookup(topic, p.Partition)
		pi.StartOffset, pi.EndOffset = start.Offset, end.Offset
		pi.Messages = max(end.Offset-start.Offset, 0)
		info.TotalMessages += pi.Messages
		if pi.UnderReplicated {
			info.UnderReplicatedPartitions++
		}
		info.Partitions = append(info.Partitions, pi)
	}

	configs, err := i.admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return TopicInfo{}, fmt.Errorf("describing topic configs: %w", err)
	}
	for _, rc := range configs {
		for _, c := range rc.Configs {
			if c.Source == kmsg.ConfigSourceDefaultConfig || c.Source == kmsg.ConfigSourceStaticBrokerConfig {
				continue
			}
			switch {
			case c.Sensitive:
				info.Configs[c.Key] = "[redacted]"
			case c.Value != nil:
				info.Configs[c.Key] = *c.Value
			}
		}
	}
	return info, nil
}

// topicDetail returns metadata for one topic, with a helpful error if it does not exist.
func (i *Inspector) topicDetail(ctx context.Context, topic string) (kadm.TopicDetail, error) {
	if topic == "" {
		return kadm.TopicDetail{}, errors.New("topic is required")
	}
	topics, err := i.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return kadm.TopicDetail{}, fmt.Errorf("fetching topic metadata: %w", err)
	}
	detail, ok := topics[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return kadm.TopicDetail{}, fmt.Errorf("topic %q not found; use list_topics to see available topics", topic)
	}
	if detail.Err != nil {
		return kadm.TopicDetail{}, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}
	return detail, nil
}

// sortedPartitions returns the partition numbers of a topic in order.
func sortedPartitions(detail kadm.TopicDetail) []int32 {
	ps := make([]int32, 0, len(detail.Partitions))
	for p := range detail.Partitions {
		ps = append(ps, p)
	}
	slices.Sort(ps)
	return ps
}
