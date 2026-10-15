package inspector

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestTopicPolicy(t *testing.T) {
	open, err := NewTopicPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	restricted, err := NewTopicPolicy([]string{"orders.*", "__consumer_offsets"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		policy  *TopicPolicy
		topic   string
		allowed bool
	}{
		{open, "orders", true},
		{open, "__consumer_offsets", false},
		{open, "", false},
		{restricted, "orders.v1", true},
		{restricted, "orders", false},
		{restricted, "payments", false},
		{restricted, "__consumer_offsets", true},
	} {
		if err := tc.policy.Check(tc.topic); (err == nil) != tc.allowed {
			t.Errorf("Check(%q) err = %v, want allowed=%v", tc.topic, err, tc.allowed)
		}
	}
	if _, err := NewTopicPolicy([]string{"[bad"}); err == nil {
		t.Error("expected invalid pattern error")
	}
}

func TestClientOptions(t *testing.T) {
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		cfg     ConnConfig
		wantErr string
	}{
		"plaintext":     {ConnConfig{Brokers: []string{"localhost:9092"}}, ""},
		"plain":         {ConnConfig{Brokers: []string{"b:9092"}, SASLMechanism: "plain", SASLUsername: "u", SASLPassword: "p"}, ""},
		"scram-256":     {ConnConfig{Brokers: []string{"b:9092"}, SASLMechanism: "SCRAM-SHA-256"}, ""},
		"scram-512+tls": {ConnConfig{Brokers: []string{"b:9092"}, SASLMechanism: "SCRAM-SHA-512", TLS: true}, ""},
		"no brokers":    {ConnConfig{Brokers: []string{" ", ""}}, "at least one broker"},
		"bad mechanism": {ConnConfig{Brokers: []string{"b:9092"}, SASLMechanism: "GSSAPI"}, "unsupported SASL"},
		"missing ca":    {ConnConfig{Brokers: []string{"b:9092"}, TLS: true, TLSCAFile: "/does/not/exist"}, "reading TLS CA"},
		"invalid ca":    {ConnConfig{Brokers: []string{"b:9092"}, TLS: true, TLSCAFile: ca}, "no certificates"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := tc.cfg.ClientOptions()
			if tc.wantErr == "" && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestRenderPayload(t *testing.T) {
	for name, tc := range map[string]struct {
		in        string
		max       int
		encoding  string
		data      any
		truncated bool
	}{
		"json object":       {`{"a":1}`, 100, "json", map[string]any{"a": float64(1)}, false},
		"json array":        {` [1,2]`, 100, "json", []any{float64(1), float64(2)}, false},
		"json scalar stays": {`123`, 100, "text", "123", false},
		"text":              {"hello", 100, "text", "hello", false},
		"long json is text": {`{"a":"bcdef"}`, 5, "text", `{"a":`, true},
		"utf8 not split":    {"añb", 2, "text", "a", true},
		"binary":            {"\xff\xfe", 100, "base64", "//4=", false},
	} {
		t.Run(name, func(t *testing.T) {
			p := renderPayload([]byte(tc.in), tc.max)
			if p.Encoding != tc.encoding || p.Truncated != tc.truncated || p.Bytes != len(tc.in) {
				t.Errorf("payload = %+v", p)
			}
			if !reflect.DeepEqual(p.Data, tc.data) {
				t.Errorf("data = %#v, want %#v", p.Data, tc.data)
			}
		})
	}
}

func TestDiagnose(t *testing.T) {
	for name, tc := range map[string]struct {
		group GroupInfo
		want  string
	}{
		"stopped consumers":  {GroupInfo{State: "Empty", TotalLag: 5}, "No active consumers, but the group is 5 messages behind"},
		"idle and caught up": {GroupInfo{State: "Empty"}, "caught up"},
		"rebalancing":        {GroupInfo{State: "PreparingRebalance"}, "rebalancing"},
		"healthy":            {GroupInfo{State: "Stable"}, "Healthy"},
		"active but behind":  {GroupInfo{State: "Stable", TotalLag: 7}, "keeps growing across calls"},
		"hot partition": {GroupInfo{State: "Stable", TotalLag: 5000, Partitions: []PartitionLag{
			{Topic: "orders", Partition: 0, Lag: 4900, ClientID: "c1"},
			{Topic: "orders", Partition: 1, Lag: 100, ClientID: "c2"},
		}}, "concentrated on orders partition 0"},
		"unassigned lag": {GroupInfo{State: "Stable", TotalLag: 3, Partitions: []PartitionLag{
			{Topic: "old-topic", Partition: 0, Lag: 3},
		}}, "no assigned member"},
	} {
		t.Run(name, func(t *testing.T) {
			notes := diagnose(tc.group)
			if !strings.Contains(strings.Join(notes, " | "), tc.want) {
				t.Errorf("diagnosis %q does not mention %q", notes, tc.want)
			}
		})
	}
}
