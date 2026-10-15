package inspector_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mauricioacevedo/kafka-mcp/internal/inspector"
)

// fixture is an in-process Kafka cluster seeded with known data:
//
//	orders   3 partitions, 30 records (10 per partition), JSON values
//	audit    1 partition, 3 records: text, binary and a tombstone
//	empty    1 partition, no records
type fixture struct {
	brokers []string
	admin   *kadm.Client
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(3, "orders"),
		kfake.SeedTopics(1, "audit", "empty"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	ctx := context.Background()
	base := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	var records []*kgo.Record
	for i := range 30 {
		records = append(records, &kgo.Record{
			Topic:     "orders",
			Partition: int32(i % 3),
			Key:       []byte(fmt.Sprintf("cus-%d", i%3)),
			Value:     []byte(fmt.Sprintf(`{"order_id":"ord-%d","amount":%d}`, i, 10*i)),
			Timestamp: base.Add(time.Duration(i) * time.Minute),
			Headers:   []kgo.RecordHeader{{Key: "source", Value: []byte("web")}},
		})
	}
	records = append(records,
		&kgo.Record{Topic: "audit", Value: []byte("user logged in"), Timestamp: base},
		&kgo.Record{Topic: "audit", Value: []byte{0xff, 0x00, 0xfe}, Timestamp: base.Add(time.Second)},
		&kgo.Record{Topic: "audit", Key: []byte("deleted"), Value: nil, Timestamp: base.Add(2 * time.Second)},
	)
	if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return &fixture{brokers: cluster.ListenAddrs(), admin: kadm.NewClient(cl)}
}

func (f *fixture) inspector(t *testing.T, opts inspector.Options) *inspector.Inspector {
	t.Helper()
	opts.Conn.Brokers = f.brokers
	in, err := inspector.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(in.Close)
	return in
}

// commit creates a consumer group with no members and the given offsets on orders.
func (f *fixture) commit(t *testing.T, group string, offsets map[int32]int64) {
	t.Helper()
	os := kadm.Offsets{}
	for p, o := range offsets {
		os.AddOffset("orders", p, o, -1)
	}
	resp, err := f.admin.CommitOffsets(context.Background(), group, os)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatal(err)
	}
}

func TestDescribeCluster(t *testing.T) {
	in := newFixture(t).inspector(t, inspector.Options{})
	info, err := in.DescribeCluster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Brokers) != 1 || info.Topics < 3 {
		t.Errorf("cluster = %+v", info)
	}
}

func TestListTopics(t *testing.T) {
	in := newFixture(t).inspector(t, inspector.Options{})
	ctx := context.Background()

	topics, err := in.ListTopics(ctx, false, "")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]inspector.TopicSummary{}
	var names []string
	for _, tp := range topics {
		byName[tp.Name] = tp
		names = append(names, tp.Name)
	}
	if !slices.IsSorted(names) {
		t.Errorf("topics not sorted: %v", names)
	}
	if o := byName["orders"]; o.Partitions != 3 || o.Messages != 30 || o.ReplicationFactor != 1 {
		t.Errorf("orders = %+v", o)
	}
	if e := byName["empty"]; e.Messages != 0 {
		t.Errorf("empty = %+v", e)
	}

	filtered, err := in.ListTopics(ctx, false, "ORD")
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0].Name != "orders" {
		t.Errorf("filtered = %+v", filtered)
	}
}

func TestDescribeTopic(t *testing.T) {
	in := newFixture(t).inspector(t, inspector.Options{})
	ctx := context.Background()

	info, err := in.DescribeTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if info.TotalMessages != 30 || len(info.Partitions) != 3 || info.UnderReplicatedPartitions != 0 {
		t.Errorf("orders = %+v", info)
	}
	for _, p := range info.Partitions {
		if p.StartOffset != 0 || p.EndOffset != 10 || p.Messages != 10 || p.Leader < 0 {
			t.Errorf("partition = %+v", p)
		}
	}

	_, err = in.DescribeTopic(ctx, "nope")
	if err == nil || !strings.Contains(err.Error(), "list_topics") {
		t.Errorf("unknown topic err = %v", err)
	}
}

func TestDescribeGroupWithoutMembers(t *testing.T) {
	f := newFixture(t)
	f.commit(t, "billing", map[int32]int64{0: 10, 1: 4, 2: 0})
	in := f.inspector(t, inspector.Options{})
	ctx := context.Background()

	g, err := in.DescribeGroup(ctx, "billing")
	if err != nil {
		t.Fatal(err)
	}
	if g.TotalLag != 16 {
		t.Errorf("total lag = %d, want 16 (0 + 6 + 10)", g.TotalLag)
	}
	if len(g.TopicLag) != 1 || g.TopicLag[0] != (inspector.TopicLag{Topic: "orders", Lag: 16}) {
		t.Errorf("topic lag = %+v", g.TopicLag)
	}
	lagByPartition := map[int32]int64{}
	for _, p := range g.Partitions {
		lagByPartition[p.Partition] = p.Lag
	}
	if lagByPartition[0] != 0 || lagByPartition[1] != 6 || lagByPartition[2] != 10 {
		t.Errorf("partition lag = %v", lagByPartition)
	}
	if g.State != "Empty" || !containsText(g.Diagnosis, "No active consumers, but the group is 16 messages behind") {
		t.Errorf("state %q diagnosis %v", g.State, g.Diagnosis)
	}

	_, err = in.DescribeGroup(ctx, "missing-group")
	if err == nil || !strings.Contains(err.Error(), "list_consumer_groups") {
		t.Errorf("missing group err = %v", err)
	}
}

func TestStableGroupIsHealthy(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(f.brokers...),
		kgo.ConsumerGroup("shipping"),
		kgo.ConsumeTopics("orders"),
		kgo.ClientID("shipping-1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for consumed := 0; consumed < 30; {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consumed only %d records: %v", consumed, err)
		}
		consumed += fetches.NumRecords()
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatal(err)
	}

	in := f.inspector(t, inspector.Options{})
	g, err := in.DescribeGroup(ctx, "shipping")
	if err != nil {
		t.Fatal(err)
	}
	if g.State != "Stable" || g.TotalLag != 0 || len(g.Members) != 1 {
		t.Fatalf("group = %+v", g)
	}
	m := g.Members[0]
	if m.ClientID != "shipping-1" || len(m.Assignments) != 1 || len(m.Assignments[0].Partitions) != 3 {
		t.Errorf("member = %+v", m)
	}
	if !containsText(g.Diagnosis, "Healthy") {
		t.Errorf("diagnosis = %v", g.Diagnosis)
	}
}

func TestFindLaggingGroups(t *testing.T) {
	f := newFixture(t)
	f.commit(t, "far-behind", map[int32]int64{0: 0, 1: 0, 2: 0})     // lag 30
	f.commit(t, "a-bit-behind", map[int32]int64{0: 9, 1: 10, 2: 10}) // lag 1
	f.commit(t, "caught-up", map[int32]int64{0: 10, 1: 10, 2: 10})   // lag 0
	in := f.inspector(t, inspector.Options{})
	ctx := context.Background()

	groups, err := in.FindLaggingGroups(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, g := range groups {
		got = append(got, fmt.Sprintf("%s=%d", g.Group, g.TotalLag))
	}
	if want := []string{"far-behind=30", "a-bit-behind=1"}; !slices.Equal(got, want) {
		t.Errorf("lagging = %v, want %v", got, want)
	}

	groups, err = in.FindLaggingGroups(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Group != "far-behind" {
		t.Errorf("min_lag 10 = %+v", groups)
	}

	listed, err := in.ListGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 3 || listed[0].Group != "a-bit-behind" {
		t.Errorf("listed = %+v", listed)
	}
}

func TestPeekPositions(t *testing.T) {
	in := newFixture(t).inspector(t, inspector.Options{})
	ctx := context.Background()
	p1 := int32(1)

	tests := []struct {
		name      string
		req       inspector.PeekRequest
		wantCount int
		wantFirst string // order_id of the first message
		wantLast  string
	}{
		{"latest default", inspector.PeekRequest{Topic: "orders"}, 10, "ord-20", "ord-29"},
		{"latest 3", inspector.PeekRequest{Topic: "orders", Limit: 3}, 3, "ord-27", "ord-29"},
		{"earliest", inspector.PeekRequest{Topic: "orders", From: inspector.FromEarliest, Limit: 4}, 4, "ord-0", "ord-3"},
		{"one partition", inspector.PeekRequest{Topic: "orders", Partition: &p1, From: inspector.FromEarliest, Limit: 2}, 2, "ord-1", "ord-4"},
		{"offset", inspector.PeekRequest{Topic: "orders", Partition: &p1, From: inspector.FromOffset, Offset: 8}, 2, "ord-25", "ord-28"},
		{
			"timestamp",
			inspector.PeekRequest{Topic: "orders", From: inspector.FromTimestamp, Timestamp: time.Date(2026, 1, 15, 10, 27, 0, 0, time.UTC)},
			3, "ord-27", "ord-29",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := in.Peek(ctx, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if res.Count != tc.wantCount || len(res.Messages) != tc.wantCount {
				t.Fatalf("count = %d, want %d", res.Count, tc.wantCount)
			}
			first := res.Messages[0].Value.Data.(map[string]any)["order_id"]
			last := res.Messages[len(res.Messages)-1].Value.Data.(map[string]any)["order_id"]
			if first != tc.wantFirst || last != tc.wantLast {
				t.Errorf("first, last = %v, %v; want %s, %s", first, last, tc.wantFirst, tc.wantLast)
			}
		})
	}
}

func TestPeekRendering(t *testing.T) {
	in := newFixture(t).inspector(t, inspector.Options{})
	res, err := in.Peek(context.Background(), inspector.PeekRequest{Topic: "audit", From: inspector.FromEarliest})
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 3 {
		t.Fatalf("count = %d", res.Count)
	}
	text, binary, tombstone := res.Messages[0], res.Messages[1], res.Messages[2]
	if text.Value.Encoding != "text" || text.Value.Data != "user logged in" || text.Key != nil {
		t.Errorf("text = %+v", text)
	}
	if binary.Value.Encoding != "base64" || binary.Value.Data != "/wD+" || binary.Value.Bytes != 3 {
		t.Errorf("binary = %+v", binary.Value)
	}
	if tombstone.Value.Encoding != "null" || tombstone.Key.Data != "deleted" {
		t.Errorf("tombstone = %+v", tombstone)
	}

	orders, err := in.Peek(context.Background(), inspector.PeekRequest{Topic: "orders", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	m := orders.Messages[0]
	if m.Value.Encoding != "json" || m.Headers["source"] != "web" || m.Timestamp != "2026-01-15T10:29:00Z" {
		t.Errorf("order message = %+v", m)
	}
}

func TestPeekLimitsAndPrivacy(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	capped := f.inspector(t, inspector.Options{MaxMessages: 5, MaxValueBytes: 10})
	res, err := capped.Peek(ctx, inspector.PeekRequest{Topic: "orders", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 5 {
		t.Errorf("count = %d, want the max of 5", res.Count)
	}
	if v := res.Messages[0].Value; !v.Truncated || v.Encoding != "text" || len(v.Data.(string)) != 10 {
		t.Errorf("value = %+v, want truncated text", v)
	}

	hidden := f.inspector(t, inspector.Options{HideValues: true})
	res, err = hidden.Peek(ctx, inspector.PeekRequest{Topic: "orders", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if v := res.Messages[0].Value; v.Encoding != "hidden" || v.Data != nil || v.Bytes == 0 {
		t.Errorf("hidden value = %+v", v)
	}
	if res.Messages[0].Key == nil || !strings.Contains(res.Note, "hide-values") {
		t.Errorf("keys should stay visible and the note should explain: %+v", res)
	}
}

func TestPeekErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	in := f.inspector(t, inspector.Options{})
	p0, p9 := int32(0), int32(9)

	tests := map[string]struct {
		req     inspector.PeekRequest
		wantErr string
	}{
		"unknown topic":     {inspector.PeekRequest{Topic: "nope"}, "list_topics"},
		"internal topic":    {inspector.PeekRequest{Topic: "__consumer_offsets"}, "internal topic"},
		"bad partition":     {inspector.PeekRequest{Topic: "orders", Partition: &p9}, "no partition 9"},
		"offset no part":    {inspector.PeekRequest{Topic: "orders", From: inspector.FromOffset}, "requires a partition"},
		"offset range":      {inspector.PeekRequest{Topic: "orders", Partition: &p0, From: inspector.FromOffset, Offset: 99}, "outside"},
		"timestamp missing": {inspector.PeekRequest{Topic: "orders", From: inspector.FromTimestamp}, "requires a timestamp"},
		"unknown from":      {inspector.PeekRequest{Topic: "orders", From: "middle"}, "unknown from"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := in.Peek(ctx, tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}

	empty, err := in.Peek(ctx, inspector.PeekRequest{Topic: "empty"})
	if err != nil || empty.Count != 0 || empty.Note == "" {
		t.Errorf("empty topic = %+v, %v", empty, err)
	}

	restricted := f.inspector(t, inspector.Options{AllowedTopics: []string{"aud*"}})
	if _, err := restricted.Peek(ctx, inspector.PeekRequest{Topic: "orders"}); err == nil || !strings.Contains(err.Error(), "--allow-topics") {
		t.Errorf("restricted err = %v", err)
	}
	if _, err := restricted.Peek(ctx, inspector.PeekRequest{Topic: "audit"}); err != nil {
		t.Errorf("allowed topic err = %v", err)
	}
}

func TestProduce(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	req := inspector.ProduceRequest{Topic: "audit", Key: "k", Value: `{"event":"test"}`, Headers: map[string]string{"by": "kafka-mcp"}}

	readOnly := f.inspector(t, inspector.Options{})
	if _, err := readOnly.Produce(ctx, req); !errors.Is(err, inspector.ErrProduceDisabled) {
		t.Errorf("read-only err = %v", err)
	}

	in := f.inspector(t, inspector.Options{AllowProduce: true})
	res, err := in.Produce(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Topic != "audit" || res.Offset != 3 {
		t.Errorf("result = %+v", res)
	}
	peek, err := in.Peek(ctx, inspector.PeekRequest{Topic: "audit", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if m := peek.Messages[0]; m.Value.Encoding != "json" || m.Headers["by"] != "kafka-mcp" {
		t.Errorf("produced message = %+v", m)
	}

	if _, err := in.Produce(ctx, inspector.ProduceRequest{Topic: "nope", Value: "x"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("unknown topic err = %v", err)
	}
}

func containsText(notes []string, text string) bool {
	for _, n := range notes {
		if strings.Contains(n, text) {
			return true
		}
	}
	return false
}
