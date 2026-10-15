package mcpserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mauricioacevedo/kafka-mcp/internal/inspector"
	"github.com/mauricioacevedo/kafka-mcp/internal/mcpserver"
)

// connect starts an in-process Kafka cluster with an "orders" topic (2
// partitions, 10 messages) and a "billing" group 4 messages behind, then
// returns an MCP client session wired to kafka-mcp.
func connect(t *testing.T, opts inspector.Options) *mcp.ClientSession {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	for i := range 10 {
		r := &kgo.Record{Topic: "orders", Partition: int32(i % 2), Value: []byte(fmt.Sprintf(`{"order_id":"ord-%d"}`, i))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	offsets := kadm.Offsets{}
	offsets.AddOffset("orders", 0, 5, -1) // caught up
	offsets.AddOffset("orders", 1, 1, -1) // 4 behind
	if _, err := kadm.NewClient(cl).CommitOffsets(ctx, "billing", offsets); err != nil {
		t.Fatal(err)
	}

	opts.Conn.Brokers = cluster.ListenAddrs()
	in, err := inspector.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(in.Close)

	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := mcpserver.New(in, "test").Connect(ctx, serverT, nil); err != nil {
		t.Fatal(err)
	}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "test"}, nil).Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func call(t *testing.T, s *mcp.ClientSession, tool string, args map[string]any) (*mcp.CallToolResult, map[string]any) {
	t.Helper()
	res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	var out map[string]any
	if res.StructuredContent != nil {
		b, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(b, &out)
	}
	return res, out
}

func text(res *mcp.CallToolResult) string {
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String()
}

func TestToolsAreListedWithAnnotations(t *testing.T) {
	s := connect(t, inspector.Options{})
	res, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
		if tool.Name == "produce_message" {
			if tool.Annotations.ReadOnlyHint || !strings.Contains(tool.Description, "read-only mode") {
				t.Errorf("produce_message = %+v", tool)
			}
		} else if !tool.Annotations.ReadOnlyHint {
			t.Errorf("%s should be marked read-only", tool.Name)
		}
	}
	slices.Sort(names)
	want := []string{"describe_cluster", "describe_consumer_group", "describe_topic", "find_lagging_groups",
		"list_consumer_groups", "list_topics", "peek_messages", "produce_message"}
	if !slices.Equal(names, want) {
		t.Errorf("tools = %v, want %v", names, want)
	}
}

func TestLagInvestigationFlow(t *testing.T) {
	s := connect(t, inspector.Options{})

	res, out := call(t, s, "find_lagging_groups", nil)
	if res.IsError {
		t.Fatalf("find_lagging_groups: %s", text(res))
	}
	groups := out["groups"].([]any)
	if len(groups) != 1 || groups[0].(map[string]any)["total_lag"] != float64(4) {
		t.Fatalf("groups = %v", groups)
	}

	res, out = call(t, s, "describe_consumer_group", map[string]any{"group": "billing"})
	if res.IsError {
		t.Fatalf("describe_consumer_group: %s", text(res))
	}
	if out["state"] != "Empty" || !strings.Contains(fmt.Sprint(out["diagnosis"]), "No active consumers") {
		t.Errorf("group = %v", out)
	}

	// Read exactly the messages billing has not processed yet.
	res, out = call(t, s, "peek_messages", map[string]any{"topic": "orders", "partition": 1, "from": "offset", "offset": 1})
	if res.IsError {
		t.Fatalf("peek_messages: %s", text(res))
	}
	if out["count"] != float64(4) {
		t.Errorf("peek = %v", out)
	}

	res, out = call(t, s, "describe_topic", map[string]any{"topic": "orders"})
	if res.IsError || out["total_messages"] != float64(10) {
		t.Errorf("describe_topic = %s", text(res))
	}
	res, out = call(t, s, "list_topics", map[string]any{"filter": "ord"})
	if res.IsError || len(out["topics"].([]any)) != 1 {
		t.Errorf("list_topics = %s", text(res))
	}
	res, out = call(t, s, "list_consumer_groups", nil)
	if res.IsError || len(out["groups"].([]any)) != 1 {
		t.Errorf("list_consumer_groups = %s", text(res))
	}
	res, out = call(t, s, "describe_cluster", nil)
	if res.IsError || len(out["brokers"].([]any)) != 1 {
		t.Errorf("describe_cluster = %s", text(res))
	}
}

func TestProduceThroughMCP(t *testing.T) {
	s := connect(t, inspector.Options{AllowProduce: true})
	res, out := call(t, s, "produce_message", map[string]any{"topic": "orders", "key": "cus-1", "value": `{"order_id":"ord-new"}`})
	if res.IsError {
		t.Fatalf("produce_message: %s", text(res))
	}
	if out["topic"] != "orders" {
		t.Errorf("produce = %v", out)
	}
}

func TestErrorsAreReturnedToTheModel(t *testing.T) {
	s := connect(t, inspector.Options{})
	for _, tc := range []struct {
		tool    string
		args    map[string]any
		wantErr string
	}{
		{"produce_message", map[string]any{"topic": "orders", "value": "x"}, "--allow-produce"},
		{"describe_topic", map[string]any{"topic": "nope"}, "list_topics"},
		{"describe_consumer_group", map[string]any{"group": "nope"}, "list_consumer_groups"},
		{"peek_messages", map[string]any{"topic": "orders", "from": "timestamp", "timestamp": "yesterday"}, "RFC 3339"},
	} {
		res, _ := call(t, s, tc.tool, tc.args)
		if !res.IsError || !strings.Contains(text(res), tc.wantErr) {
			t.Errorf("%s: IsError=%v %q, want error containing %q", tc.tool, res.IsError, text(res), tc.wantErr)
		}
	}
}
