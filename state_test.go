package txshardv2

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestState_Clone(t *testing.T) {
	old := NewState(&StateConfig{
		NodePrefix:      "/node/",
		SelfNodeID:      10,
		SelfNodeAddress: "localhost:1122",
	})
	old.leaseID = 832
	old.leaderID = 3
	old.nodes = map[NodeID]Node{
		9: {
			ID:          9,
			Address:     "localhost:2200",
			ModRevision: 44,
		},
	}

	newState := old.Clone()
	assert.Same(t, newState.config, old.config)
	assert.Equal(t, newState.leaseID, old.leaseID)
	assert.Equal(t, newState.leaderID, old.leaderID)
	assert.Equal(t, newState.nodes, old.nodes)

	newState.nodes[5] = Node{
		ID:          5,
		Address:     "localhost:5500",
		ModRevision: 55,
	}
	assert.NotEqual(t, newState.nodes, old.nodes)
}

func TestHandleLeaderEvent(t *testing.T) {
	table := []struct {
		name   string
		event  NodeID
		before func(s *State)
		after  func(s *State)
		output HandleOutput
	}{
		{
			name:  "no-previous-data",
			event: 13,
			after: func(s *State) {
				s.leaderID = 13
			},
		},
		{
			name:  "had-nodes",
			event: 20,
			before: func(s *State) {
				s.nodes = map[NodeID]Node{
					13: {
						ID:          13,
						Address:     "localhost:2233",
						ModRevision: 123,
					},
				}
			},
			after: func(s *State) {
				s.leaderID = 20
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/0",
						Value: "13",
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/1",
						Value: "13",
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/2",
						Value: "13",
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/3",
						Value: "13",
					},
				},
			},
		},
		{
			name:  "had-nodes-not-leader",
			event: 13,
			before: func(s *State) {
				s.nodes = map[NodeID]Node{
					13: {
						ID:          13,
						Address:     "localhost:2233",
						ModRevision: 123,
					},
				}
			},
			after: func(s *State) {
				s.leaderID = 13
			},
			output: HandleOutput{},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			conf := &StateConfig{
				ExpectedPartitionPrefix: "/partition/expected/",

				PartitionCount:  4,
				SelfNodeID:      20,
				SelfNodeAddress: "localhost:2233",
				NodePrefix:      "/node/",
			}

			old := NewState(conf)
			if e.before != nil {
				e.before(old)
			}

			expected := old.Clone()
			if e.after != nil {
				e.after(expected)
			}

			s, output := handleLeaderEvent(old, e.event)
			assert.Equal(t, expected, s)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestHandleLeaseEvent(t *testing.T) {
	table := []struct {
		name   string
		event  LeaseID
		before func(s *State)
		after  func(s *State)
		output HandleOutput
	}{
		{
			name:  "normal",
			event: 5566,
			after: func(s *State) {
				s.leaseID = 5566
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:    EventTypePut,
						Key:     "/node/30",
						Value:   "localhost:6122",
						LeaseID: 5566,
					},
				},
			},
		},
		{
			name:  "lease-id-same",
			event: 5566,
			before: func(s *State) {
				s.leaseID = 5566
			},
			after: func(s *State) {
				s.leaseID = 5566
			},
			output: HandleOutput{},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			conf := &StateConfig{
				PartitionCount:  4,
				SelfNodeID:      30,
				SelfNodeAddress: "localhost:6122",
				NodePrefix:      "/node/",
			}

			oldState := NewState(conf)
			if e.before != nil {
				e.before(oldState)
			}

			expected := oldState.Clone()
			if e.after != nil {
				e.after(expected)
			}

			s, output := handleLeaseEvent(oldState, e.event)
			assert.Equal(t, expected, s)
			assert.Equal(t, e.output, output)
		})
	}
}
