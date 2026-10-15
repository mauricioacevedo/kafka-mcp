// Package mcpserver exposes an inspector.Inspector as a set of MCP tools.
package mcpserver

import (
	"context"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/mauricioacevedo/kafka-mcp/internal/inspector"
)

const instructions = `Inspect a Kafka cluster: brokers, topics, consumer groups, lag and messages.
To answer "is anything behind?" start with find_lagging_groups, then describe_consumer_group for details.
peek_messages reads without joining a consumer group, so it never moves real consumers' offsets.
produce_message only works when the server was started with --allow-produce.`

// New returns an MCP server exposing the inspector's capabilities as tools.
func New(in *inspector.Inspector, version string) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "kafka-mcp", Title: "Kafka Inspector", Version: version},
		&mcp.ServerOptions{Instructions: instructions},
	)
	h := &handlers{in: in}
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: ptr(true)}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_cluster",
		Title:       "Describe the Kafka cluster",
		Description: "Cluster ID, controller, brokers and topic count.",
		Annotations: readOnly,
	}, h.describeCluster)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_topics",
		Title:       "List topics",
		Description: "Topics with partition count, replication factor and approximate message count.",
		Annotations: readOnly,
	}, h.listTopics)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_topic",
		Title:       "Describe a topic",
		Description: "Partitions with leader, replicas, in-sync replicas and offsets, under-replication, and configs that differ from the defaults.",
		Annotations: readOnly,
	}, h.describeTopic)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_consumer_groups",
		Title:       "List consumer groups",
		Description: "Consumer groups with their state (Stable, Empty, rebalancing, ...).",
		Annotations: readOnly,
	}, h.listGroups)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_consumer_group",
		Title:       "Describe a consumer group",
		Description: "Members and their assignments, committed offset and lag per partition, total lag, and a plain-language diagnosis.",
		Annotations: readOnly,
	}, h.describeGroup)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "find_lagging_groups",
		Title:       "Find lagging consumer groups",
		Description: "Every consumer group whose total lag is at least min_lag, most behind first, with a diagnosis for each.",
		Annotations: readOnly,
	}, h.findLagging)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "peek_messages",
		Title: "Peek at messages",
		Description: fmt.Sprintf("Read up to %d messages from a topic without joining a consumer group. "+
			"JSON values are decoded, text is returned as is and binary data as base64.", in.MaxMessages()),
		Annotations: readOnly,
	}, h.peek)

	produceDescription := "Write one message to a topic. The partition is chosen from the key."
	if !in.ProduceAllowed() {
		produceDescription += " Currently disabled: the server is running in read-only mode."
	}
	mcp.AddTool(server, &mcp.Tool{
		Name:        "produce_message",
		Title:       "Produce a message",
		Description: produceDescription,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: ptr(false), OpenWorldHint: ptr(true)},
	}, h.produce)

	return server
}

type handlers struct {
	in *inspector.Inspector
}

type empty struct{}

func (h *handlers) describeCluster(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, inspector.ClusterInfo, error) {
	info, err := h.in.DescribeCluster(ctx)
	return nil, info, err
}

type ListTopicsInput struct {
	IncludeInternal bool   `json:"include_internal,omitempty" jsonschema:"include internal topics such as __consumer_offsets"`
	Filter          string `json:"filter,omitempty" jsonschema:"only topics whose name contains this text (case-insensitive)"`
}

type ListTopicsOutput struct {
	Topics []inspector.TopicSummary `json:"topics"`
}

func (h *handlers) listTopics(ctx context.Context, _ *mcp.CallToolRequest, in ListTopicsInput) (*mcp.CallToolResult, ListTopicsOutput, error) {
	topics, err := h.in.ListTopics(ctx, in.IncludeInternal, in.Filter)
	return nil, ListTopicsOutput{Topics: topics}, err
}

type TopicInput struct {
	Topic string `json:"topic" jsonschema:"topic name"`
}

func (h *handlers) describeTopic(ctx context.Context, _ *mcp.CallToolRequest, in TopicInput) (*mcp.CallToolResult, inspector.TopicInfo, error) {
	info, err := h.in.DescribeTopic(ctx, in.Topic)
	return nil, info, err
}

type ListGroupsOutput struct {
	Groups []inspector.GroupSummary `json:"groups"`
}

func (h *handlers) listGroups(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, ListGroupsOutput, error) {
	groups, err := h.in.ListGroups(ctx)
	return nil, ListGroupsOutput{Groups: groups}, err
}

type GroupInput struct {
	Group string `json:"group" jsonschema:"consumer group ID"`
}

func (h *handlers) describeGroup(ctx context.Context, _ *mcp.CallToolRequest, in GroupInput) (*mcp.CallToolResult, inspector.GroupInfo, error) {
	info, err := h.in.DescribeGroup(ctx, in.Group)
	return nil, info, err
}

type FindLaggingInput struct {
	MinLag int64 `json:"min_lag,omitempty" jsonschema:"minimum total lag to report; defaults to 1"`
}

type FindLaggingOutput struct {
	Groups []inspector.LaggingGroup `json:"groups"`
}

func (h *handlers) findLagging(ctx context.Context, _ *mcp.CallToolRequest, in FindLaggingInput) (*mcp.CallToolResult, FindLaggingOutput, error) {
	minLag := in.MinLag
	if minLag <= 0 {
		minLag = 1
	}
	groups, err := h.in.FindLaggingGroups(ctx, minLag)
	return nil, FindLaggingOutput{Groups: groups}, err
}

type PeekInput struct {
	Topic     string `json:"topic" jsonschema:"topic to read"`
	Partition *int32 `json:"partition,omitempty" jsonschema:"read only this partition; all partitions when omitted"`
	From      string `json:"from,omitempty" jsonschema:"where to start: latest (default, the most recent messages), earliest, offset or timestamp"`
	Offset    int64  `json:"offset,omitempty" jsonschema:"start offset when from is offset (requires partition)"`
	Timestamp string `json:"timestamp,omitempty" jsonschema:"RFC 3339 time when from is timestamp, e.g. 2026-01-15T10:00:00Z"`
	Limit     int    `json:"limit,omitempty" jsonschema:"number of messages to return; defaults to 10"`
}

func (h *handlers) peek(ctx context.Context, _ *mcp.CallToolRequest, in PeekInput) (*mcp.CallToolResult, inspector.PeekResult, error) {
	req := inspector.PeekRequest{
		Topic:     in.Topic,
		Partition: in.Partition,
		From:      in.From,
		Offset:    in.Offset,
		Limit:     in.Limit,
	}
	if in.Timestamp != "" {
		ts, err := time.Parse(time.RFC3339, in.Timestamp)
		if err != nil {
			return nil, inspector.PeekResult{}, fmt.Errorf("timestamp must be RFC 3339, e.g. 2026-01-15T10:00:00Z: %w", err)
		}
		req.Timestamp = ts
	}
	result, err := h.in.Peek(ctx, req)
	return nil, result, err
}

type ProduceInput struct {
	Topic   string            `json:"topic" jsonschema:"topic to write to"`
	Key     string            `json:"key,omitempty" jsonschema:"message key; determines the partition"`
	Value   string            `json:"value" jsonschema:"message value"`
	Headers map[string]string `json:"headers,omitempty" jsonschema:"message headers"`
}

func (h *handlers) produce(ctx context.Context, _ *mcp.CallToolRequest, in ProduceInput) (*mcp.CallToolResult, inspector.ProduceResult, error) {
	result, err := h.in.Produce(ctx, inspector.ProduceRequest{Topic: in.Topic, Key: in.Key, Value: in.Value, Headers: in.Headers})
	return nil, result, err
}

func ptr[T any](v T) *T { return &v }
