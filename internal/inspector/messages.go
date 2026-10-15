package inspector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Peek start positions.
const (
	FromLatest    = "latest"
	FromEarliest  = "earliest"
	FromOffset    = "offset"
	FromTimestamp = "timestamp"
)

// idleTimeout ends a peek when no new records arrive for this long. It covers
// partitions whose last offsets are transaction markers or compacted away,
// where the end offset is never reached by a visible record.
const idleTimeout = 2 * time.Second

// PeekRequest selects messages to read.
type PeekRequest struct {
	Topic string
	// Partition limits the read to one partition; nil reads all partitions.
	Partition *int32
	// From is FromLatest (default), FromEarliest, FromOffset or FromTimestamp.
	From      string
	Offset    int64     // for FromOffset; requires Partition
	Timestamp time.Time // for FromTimestamp
	Limit     int
}

// PeekResult holds the messages read by Peek.
type PeekResult struct {
	Topic    string    `json:"topic"`
	From     string    `json:"from"`
	Count    int       `json:"count"`
	Messages []Message `json:"messages"`
	Note     string    `json:"note,omitempty"`
}

// Message is one record, rendered for humans and models.
type Message struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp string            `json:"timestamp"`
	Key       *Payload          `json:"key,omitempty"`
	Value     Payload           `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// Payload is a key or value. JSON payloads are decoded so they read naturally,
// other UTF-8 text is returned as is, and binary data is base64 encoded.
type Payload struct {
	Encoding  string `json:"encoding"` // json, text, base64, hidden or null
	Data      any    `json:"data,omitempty"`
	Bytes     int    `json:"bytes"`
	Truncated bool   `json:"truncated,omitempty"`
}

// Peek reads messages without joining a consumer group, so it never commits
// offsets or disturbs real consumers.
func (i *Inspector) Peek(ctx context.Context, req PeekRequest) (PeekResult, error) {
	if err := i.policy.Check(req.Topic); err != nil {
		return PeekResult{}, err
	}
	if req.From == "" {
		req.From = FromLatest
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	limit = min(limit, i.opts.MaxMessages)

	ctx, cancel := i.withTimeout(ctx)
	defer cancel()

	plan, err := i.planPeek(ctx, req, limit)
	if err != nil {
		return PeekResult{}, err
	}
	result := PeekResult{Topic: req.Topic, From: req.From, Messages: []Message{}}
	if len(plan) == 0 {
		result.Note = "No messages in the selected range."
		return result, nil
	}

	records, err := i.consume(ctx, req.Topic, plan, limit, req.From != FromLatest)
	if err != nil {
		return PeekResult{}, err
	}
	sort.Slice(records, func(a, b int) bool {
		ra, rb := records[a], records[b]
		if !ra.Timestamp.Equal(rb.Timestamp) {
			return ra.Timestamp.Before(rb.Timestamp)
		}
		if ra.Partition != rb.Partition {
			return ra.Partition < rb.Partition
		}
		return ra.Offset < rb.Offset
	})
	if len(records) > limit {
		if req.From == FromLatest {
			records = records[len(records)-limit:]
		} else {
			records = records[:limit]
		}
	}
	for _, r := range records {
		result.Messages = append(result.Messages, i.render(r))
	}
	result.Count = len(result.Messages)
	if i.opts.HideValues {
		result.Note = "Message values are hidden by the server configuration (--hide-values)."
	}
	return result, nil
}

// span is the half-open offset range [from, to) to read from one partition.
type span struct{ from, to int64 }

func (i *Inspector) planPeek(ctx context.Context, req PeekRequest, limit int) (map[int32]span, error) {
	detail, err := i.topicDetail(ctx, req.Topic)
	if err != nil {
		return nil, err
	}
	partitions := sortedPartitions(detail)
	if req.Partition != nil {
		if _, ok := detail.Partitions[*req.Partition]; !ok {
			return nil, fmt.Errorf("topic %q has no partition %d (it has %d partitions)", req.Topic, *req.Partition, len(partitions))
		}
		partitions = []int32{*req.Partition}
	}
	starts, ends, err := i.offsets(ctx, req.Topic)
	if err != nil {
		return nil, err
	}

	var fromTS kadm.ListedOffsets
	switch req.From {
	case FromLatest, FromEarliest:
	case FromOffset:
		if req.Partition == nil {
			return nil, errors.New(`from "offset" requires a partition`)
		}
	case FromTimestamp:
		if req.Timestamp.IsZero() {
			return nil, errors.New(`from "timestamp" requires a timestamp (RFC 3339, e.g. 2026-01-15T10:00:00Z)`)
		}
		fromTS, err = i.admin.ListOffsetsAfterMilli(ctx, req.Timestamp.UnixMilli(), req.Topic)
		if err != nil {
			return nil, fmt.Errorf("looking up offsets for timestamp: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown from %q (want latest, earliest, offset or timestamp)", req.From)
	}

	plan := map[int32]span{}
	for _, p := range partitions {
		start, _ := starts.Lookup(req.Topic, p)
		end, _ := ends.Lookup(req.Topic, p)
		if start.Err != nil || end.Err != nil {
			continue
		}
		s := span{from: start.Offset, to: end.Offset}
		switch req.From {
		case FromLatest:
			s.from = max(start.Offset, end.Offset-int64(limit))
		case FromOffset:
			if req.Offset < start.Offset || req.Offset >= end.Offset {
				return nil, fmt.Errorf("offset %d is outside partition %d's range [%d, %d)", req.Offset, p, start.Offset, end.Offset)
			}
			s.from = req.Offset
		case FromTimestamp:
			if o, ok := fromTS.Lookup(req.Topic, p); ok && o.Err == nil && o.Offset >= 0 {
				s.from = o.Offset
			} else {
				s.from = end.Offset // nothing at or after the timestamp
			}
		}
		if s.from < s.to {
			plan[p] = s
		}
	}
	return plan, nil
}

// consume reads the planned spans with a short-lived, group-less client.
// When stopAtLimit is set, reading stops as soon as limit records arrived.
func (i *Inspector) consume(ctx context.Context, topic string, plan map[int32]span, limit int, stopAtLimit bool) ([]*kgo.Record, error) {
	offsets := map[int32]kgo.Offset{}
	for p, s := range plan {
		offsets[p] = kgo.NewOffset().At(s.from)
	}
	opts := append([]kgo.Opt{}, i.clientOpts...)
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating consumer: %w", err)
	}
	defer cl.Close()

	var records []*kgo.Record
	remaining := len(plan)
	done := map[int32]bool{}
	for remaining > 0 && (!stopAtLimit || len(records) < limit) {
		pollCtx, cancel := context.WithTimeout(ctx, idleTimeout)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			break // overall timeout: return what we have
		}
		got := 0
		var fetchErr error
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				fetchErr = err
			}
		})
		if fetchErr != nil {
			return nil, fmt.Errorf("reading messages: %w", fetchErr)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			s := plan[r.Partition]
			if done[r.Partition] || r.Offset >= s.to {
				return
			}
			records = append(records, r)
			got++
			if r.Offset+1 >= s.to {
				done[r.Partition] = true
				remaining--
			}
		})
		if got == 0 {
			break // idle: nothing more is coming within the planned ranges
		}
	}
	return records, nil
}

func (i *Inspector) render(r *kgo.Record) Message {
	m := Message{
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp.UTC().Format(time.RFC3339Nano),
	}
	if r.Key != nil {
		k := renderPayload(r.Key, i.opts.MaxValueBytes)
		m.Key = &k
	}
	switch {
	case r.Value == nil:
		m.Value = Payload{Encoding: "null"} // a tombstone in compacted topics
	case i.opts.HideValues:
		m.Value = Payload{Encoding: "hidden", Bytes: len(r.Value)}
	default:
		m.Value = renderPayload(r.Value, i.opts.MaxValueBytes)
	}
	if len(r.Headers) > 0 {
		m.Headers = map[string]string{}
		for _, h := range r.Headers {
			if utf8.Valid(h.Value) {
				m.Headers[h.Key] = string(h.Value)
			} else {
				m.Headers[h.Key] = "base64:" + base64.StdEncoding.EncodeToString(h.Value)
			}
		}
	}
	return m
}

func renderPayload(b []byte, maxBytes int) Payload {
	p := Payload{Bytes: len(b)}
	if len(b) <= maxBytes && json.Valid(b) && looksLikeJSONDocument(b) {
		var v any
		if err := json.Unmarshal(b, &v); err == nil {
			p.Encoding, p.Data = "json", v
			return p
		}
	}
	if len(b) > maxBytes {
		b = truncateUTF8(b[:maxBytes])
		p.Truncated = true
	}
	if utf8.Valid(b) {
		p.Encoding, p.Data = "text", string(b)
	} else {
		p.Encoding, p.Data = "base64", base64.StdEncoding.EncodeToString(b)
	}
	return p
}

// truncateUTF8 drops a multi-byte character split by truncation, so text
// stays text. Binary data is returned unchanged.
func truncateUTF8(b []byte) []byte {
	for n := 1; n < utf8.UTFMax && n < len(b); n++ {
		if utf8.Valid(b[:len(b)-n]) && !utf8.Valid(b) {
			return b[:len(b)-n]
		}
	}
	return b
}

// looksLikeJSONDocument limits decoding to objects and arrays, so plain
// strings such as `123` or `true` stay text.
func looksLikeJSONDocument(b []byte) bool {
	b = bytes.TrimSpace(b)
	return len(b) > 0 && (b[0] == '{' || b[0] == '[')
}

// ProduceRequest is a message to write.
type ProduceRequest struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

// ProduceResult reports where a produced message landed.
type ProduceResult struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
}

// Produce writes one message. The partition is chosen from the key.
func (i *Inspector) Produce(ctx context.Context, req ProduceRequest) (ProduceResult, error) {
	if !i.opts.AllowProduce {
		return ProduceResult{}, ErrProduceDisabled
	}
	if err := i.policy.Check(req.Topic); err != nil {
		return ProduceResult{}, err
	}
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	// Check first: producing to a missing topic would otherwise retry until timeout.
	if _, err := i.topicDetail(ctx, req.Topic); err != nil {
		return ProduceResult{}, err
	}

	rec := &kgo.Record{Topic: req.Topic, Value: []byte(req.Value)}
	if req.Key != "" {
		rec.Key = []byte(req.Key)
	}
	for k, v := range req.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	sort.Slice(rec.Headers, func(a, b int) bool { return rec.Headers[a].Key < rec.Headers[b].Key })

	r, err := i.client.ProduceSync(ctx, rec).First()
	if err != nil {
		return ProduceResult{}, fmt.Errorf("producing to %q: %w", req.Topic, err)
	}
	return ProduceResult{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp.UTC().Format(time.RFC3339Nano),
	}, nil
}
