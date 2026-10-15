package inspector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// GroupSummary is one row of ListGroups.
type GroupSummary struct {
	Group        string `json:"group"`
	State        string `json:"state"`
	ProtocolType string `json:"protocol_type"`
}

// ListGroups returns consumer groups sorted by name.
func (i *Inspector) ListGroups(ctx context.Context) ([]GroupSummary, error) {
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	groups, err := i.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", err)
	}
	out := []GroupSummary{}
	for _, g := range groups.Sorted() {
		out = append(out, GroupSummary{Group: g.Group, State: g.State, ProtocolType: g.ProtocolType})
	}
	return out, nil
}

// GroupInfo describes a consumer group, its members and its lag.
type GroupInfo struct {
	Group        string         `json:"group"`
	State        string         `json:"state"`
	ProtocolType string         `json:"protocol_type"`
	Protocol     string         `json:"protocol,omitempty"`
	Coordinator  int32          `json:"coordinator"`
	Members      []MemberInfo   `json:"members"`
	TotalLag     int64          `json:"total_lag"`
	TopicLag     []TopicLag     `json:"topic_lag"`
	Partitions   []PartitionLag `json:"partitions"`
	// Diagnosis holds plain-language observations about the group's health.
	Diagnosis []string `json:"diagnosis"`
}

// MemberInfo describes one member of a group.
type MemberInfo struct {
	MemberID    string            `json:"member_id"`
	InstanceID  string            `json:"instance_id,omitempty"`
	ClientID    string            `json:"client_id"`
	ClientHost  string            `json:"client_host"`
	Assignments []TopicPartitions `json:"assignments"`
}

// TopicPartitions lists partitions of one topic.
type TopicPartitions struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// TopicLag is a group's total lag on one topic.
type TopicLag struct {
	Topic string `json:"topic"`
	Lag   int64  `json:"lag"`
}

// PartitionLag is a group's position on one partition.
type PartitionLag struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	EndOffset       int64  `json:"end_offset"`
	Lag             int64  `json:"lag"`
	ClientID        string `json:"client_id,omitempty"`
	Error           string `json:"error,omitempty"`
}

// DescribeGroup returns the members, committed offsets and lag of a group.
func (i *Inspector) DescribeGroup(ctx context.Context, group string) (GroupInfo, error) {
	if group == "" {
		return GroupInfo{}, fmt.Errorf("group is required")
	}
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	lags, err := i.admin.Lag(ctx, group)
	if err != nil {
		return GroupInfo{}, fmt.Errorf("fetching lag for group %q: %w", group, err)
	}
	lag, ok := lags[group]
	// Older brokers report unknown groups as Dead; newer ones return GROUP_ID_NOT_FOUND.
	if !ok || (lag.State == "Dead" && lag.Lag.IsEmpty()) || errors.Is(lag.Error(), kerr.GroupIDNotFound) {
		return GroupInfo{}, fmt.Errorf("consumer group %q not found; use list_consumer_groups to see available groups", group)
	}
	if err := lag.Error(); err != nil {
		return GroupInfo{}, fmt.Errorf("group %q: %w", group, err)
	}
	return groupInfo(lag), nil
}

// LaggingGroup is one row of FindLaggingGroups.
type LaggingGroup struct {
	Group     string     `json:"group"`
	State     string     `json:"state"`
	Members   int        `json:"members"`
	TotalLag  int64      `json:"total_lag"`
	TopicLag  []TopicLag `json:"topic_lag"`
	Diagnosis []string   `json:"diagnosis"`
}

// FindLaggingGroups returns every group whose total lag is at least minLag,
// most-lagging first.
func (i *Inspector) FindLaggingGroups(ctx context.Context, minLag int64) ([]LaggingGroup, error) {
	ctx, cancel := i.withTimeout(ctx)
	defer cancel()
	listed, err := i.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", err)
	}
	out := []LaggingGroup{}
	if len(listed) == 0 {
		return out, nil
	}
	lags, err := i.admin.Lag(ctx, listed.Groups()...)
	if err != nil {
		return nil, fmt.Errorf("fetching lag: %w", err)
	}
	for _, lag := range lags {
		if lag.Error() != nil {
			continue
		}
		info := groupInfo(lag)
		if info.TotalLag < minLag {
			continue
		}
		out = append(out, LaggingGroup{
			Group:     info.Group,
			State:     info.State,
			Members:   len(info.Members),
			TotalLag:  info.TotalLag,
			TopicLag:  info.TopicLag,
			Diagnosis: info.Diagnosis,
		})
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].TotalLag != out[b].TotalLag {
			return out[a].TotalLag > out[b].TotalLag
		}
		return out[a].Group < out[b].Group
	})
	return out, nil
}

func groupInfo(l kadm.DescribedGroupLag) GroupInfo {
	info := GroupInfo{
		Group:        l.Group,
		State:        l.State,
		ProtocolType: l.ProtocolType,
		Protocol:     l.Protocol,
		Coordinator:  l.Coordinator.NodeID,
		Members:      []MemberInfo{},
		TopicLag:     []TopicLag{},
		Partitions:   []PartitionLag{},
	}
	for _, m := range l.Members {
		mi := MemberInfo{
			MemberID:    m.MemberID,
			ClientID:    m.ClientID,
			ClientHost:  strings.TrimPrefix(m.ClientHost, "/"), // Kafka reports "/10.0.0.1"
			Assignments: []TopicPartitions{},
		}
		if m.InstanceID != nil {
			mi.InstanceID = *m.InstanceID
		}
		if assigned, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range assigned.Topics {
				mi.Assignments = append(mi.Assignments, TopicPartitions{Topic: t.Topic, Partitions: t.Partitions})
			}
		}
		info.Members = append(info.Members, mi)
	}

	for _, pl := range l.Lag.Sorted() {
		p := PartitionLag{
			Topic:           pl.Topic,
			Partition:       pl.Partition,
			CommittedOffset: pl.Commit.At,
			EndOffset:       pl.End.Offset,
			Lag:             pl.Lag,
		}
		if pl.Member != nil {
			p.ClientID = pl.Member.ClientID
		}
		if pl.Err != nil {
			p.Error = pl.Err.Error()
		}
		info.Partitions = append(info.Partitions, p)
	}
	for _, t := range l.Lag.TotalByTopic().Sorted() {
		info.TopicLag = append(info.TopicLag, TopicLag{Topic: t.Topic, Lag: t.Lag})
	}
	info.TotalLag = l.Lag.Total()
	info.Diagnosis = diagnose(info)
	return info
}

// hotPartitionMinLag avoids flagging skew on groups that are barely behind.
const hotPartitionMinLag = 1000

// diagnose turns raw lag numbers into observations an operator (or a model)
// can act on.
func diagnose(g GroupInfo) []string {
	notes := []string{}
	switch g.State {
	case "Empty":
		if g.TotalLag > 0 {
			notes = append(notes, fmt.Sprintf("No active consumers, but the group is %d messages behind: its consumers are stopped or crashing.", g.TotalLag))
		} else {
			notes = append(notes, "No active consumers; the group is caught up.")
		}
	case "PreparingRebalance", "CompletingRebalance":
		notes = append(notes, "The group is rebalancing; consumption is paused until it completes. Frequent rebalances usually mean consumers exceed max.poll.interval.ms or are restarting.")
	case "Dead":
		notes = append(notes, "The group is dead (no members and no committed offsets).")
	}

	var (
		maxLag      PartitionLag
		laggingPart int
		unassigned  int
	)
	for _, p := range g.Partitions {
		if p.Lag > maxLag.Lag {
			maxLag = p
		}
		if p.Lag > 0 {
			laggingPart++
			if p.ClientID == "" && g.State == "Stable" {
				unassigned++
			}
		}
	}
	if unassigned > 0 {
		notes = append(notes, fmt.Sprintf("%d lagging partition(s) have no assigned member: committed offsets exist for topics the current members no longer consume.", unassigned))
	}
	if laggingPart > 1 && maxLag.Lag >= hotPartitionMinLag && float64(maxLag.Lag) > 0.8*float64(g.TotalLag) {
		notes = append(notes, fmt.Sprintf("Lag is concentrated on %s partition %d (%d of %d messages): look for a hot key or a stuck consumer (%s).",
			maxLag.Topic, maxLag.Partition, maxLag.Lag, g.TotalLag, valueOr(maxLag.ClientID, "unassigned")))
	}
	if g.State == "Stable" {
		if g.TotalLag == 0 {
			notes = append(notes, "Healthy: all members are consuming and the group is caught up.")
		} else {
			notes = append(notes, fmt.Sprintf("Members are active but %d messages behind. A small lag is normal between offset commits; "+
				"if it keeps growing across calls, the consumers are too slow (add members, up to one per partition, or speed up processing).", g.TotalLag))
		}
	}
	return notes
}

func valueOr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
