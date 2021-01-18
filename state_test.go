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
	old.partitions = []Partition{
		{
			Running: true,
		},
	}

	newState := old.Clone()
	assert.Same(t, newState.config, old.config)
	assert.Equal(t, newState.leaseID, old.leaseID)
	assert.Equal(t, newState.leaderID, old.leaderID)
	assert.Equal(t, newState.nodes, old.nodes)
	assert.Equal(t, newState.partitions, old.partitions)

	newState.nodes[5] = Node{
		ID:          5,
		Address:     "localhost:5500",
		ModRevision: 55,
	}
	old.partitions[0].Running = false

	assert.NotEqual(t, newState.nodes, old.nodes)
	assert.NotEqual(t, newState.partitions, old.partitions)
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
				s.nodes = map[NodeID]Node{
					30: {
						ID: 30,
					},
				}
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

func TestComputeHandleOutput(t *testing.T) {
	table := []struct {
		name           string
		partitionCount PartitionID
		before         func(s *State)
		after          func(s *State)
		output         HandleOutput
	}{
		{
			name: "nothing",
		},
		{
			name: "lease-id-changed",
			after: func(s *State) {
				s.leaseID = 5511
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:    EventTypePut,
						Key:     "/node/12",
						Value:   "self-address",
						LeaseID: 5511,
					},
				},
			},
		},
		{
			name: "lease-id-not-changed",
			before: func(s *State) {
				s.leaseID = 5511
				s.nodes = map[NodeID]Node{
					12: {
						ID:      12,
						Address: "self-address",
					},
				}
			},
			after: func(s *State) {
				s.leaseID = 5511
			},
			output: HandleOutput{},
		},
		{
			name: "lease-not-changed.self-node-deleted",
			before: func(s *State) {
				s.leaseID = 5511
				s.nodes = map[NodeID]Node{
					12: {
						ID:      12,
						Address: "self-address",
					},
				}
			},
			after: func(s *State) {
				s.nodes = map[NodeID]Node{}
				s.leaseID = 5511
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:    EventTypePut,
						Key:     "/node/12",
						Value:   "self-address",
						LeaseID: 5511,
					},
				},
			},
		},
		{
			name:           "leader-changed",
			partitionCount: 3,
			before: func(s *State) {
				s.nodes = map[NodeID]Node{
					12: {
						ID: 12,
					},
					13: {
						ID: 13,
					},
				}
			},
			after: func(s *State) {
				s.leaderID = 12
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/0",
						Value: "12",
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/1",
						Value: "12",
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/2",
						Value: "13",
					},
				},
			},
		},
		{
			name:           "leader-not-changed",
			partitionCount: 3,
			before: func(s *State) {
				s.leaderID = 12
				s.nodes = map[NodeID]Node{
					12: {
						ID: 12,
					},
					13: {
						ID: 13,
					},
				}
			},
			after: func(s *State) {
				s.leaderID = 12
			},
			output: HandleOutput{},
		},
		{
			name:           "leader-changed.not-self",
			partitionCount: 3,
			before: func(s *State) {
				s.nodes = map[NodeID]Node{
					12: {
						ID: 12,
					},
					13: {
						ID: 13,
					},
				}
			},
			after: func(s *State) {
				s.leaderID = 13
			},
			output: HandleOutput{},
		},
		{
			name:           "leader-not-changed.expected-change",
			partitionCount: 3,
			before: func(s *State) {
				s.nodes = map[NodeID]Node{
					12: {
						ID: 12,
					},
					13: {
						ID: 13,
					},
				}
				s.leaderID = 12
			},
			after: func(s *State) {
				s.partitions[0].Expected = PartitionData{
					Persisted:   true,
					NodeID:      15,
					ModRevision: 222,
				}
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:        EventTypePut,
						Key:         "/partition/expected/0",
						Value:       "12",
						ModRevision: 222,
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/1",
						Value: "12",
					},
					{
						Type:  EventTypePut,
						Key:   "/partition/expected/2",
						Value: "13",
					},
				},
			},
		},
		{
			name:           "leader-not-changed.expected-not-change.nodes-changed",
			partitionCount: 3,
			before: func(s *State) {
				s.nodes = map[NodeID]Node{
					12: {
						ID: 12,
					},
					13: {
						ID: 13,
					},
				}
				s.leaderID = 12

				s.partitions[0].Expected = PartitionData{
					Persisted:   true,
					NodeID:      12,
					ModRevision: 200,
				}
				s.partitions[1].Expected = PartitionData{
					Persisted:   true,
					NodeID:      12,
					ModRevision: 201,
				}
				s.partitions[2].Expected = PartitionData{
					Persisted:   true,
					NodeID:      13,
					ModRevision: 202,
				}
			},
			after: func(s *State) {
				s.nodes = map[NodeID]Node{
					14: {
						ID: 14,
					},
					12: {
						ID: 12,
					},
					13: {
						ID: 13,
					},
				}
			},
			output: HandleOutput{
				Kvs: []CASKeyValue{
					{
						Type:        EventTypePut,
						Key:         "/partition/expected/1",
						Value:       "14",
						ModRevision: 201,
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			oldState := NewState(&StateConfig{
				ExpectedPartitionPrefix: "/partition/expected/",
				NodePrefix:              "/node/",
				PartitionCount:          e.partitionCount,

				SelfNodeID:      12,
				SelfNodeAddress: "self-address",
			})

			if e.before != nil {
				e.before(oldState)
			}

			newState := oldState.Clone()
			if e.after != nil {
				e.after(newState)
			}

			output := computeHandleOutput(oldState, newState)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestNodesEqual(t *testing.T) {
	table := []struct {
		name   string
		a      map[NodeID]Node
		b      map[NodeID]Node
		output bool
	}{
		{
			name:   "both-empty",
			output: true,
		},
		{
			name:   "both-empty",
			a:      map[NodeID]Node{},
			b:      map[NodeID]Node{},
			output: true,
		},
		{
			name: "diff-size",
			a: map[NodeID]Node{
				10: {},
			},
			b:      map[NodeID]Node{},
			output: false,
		},
		{
			name: "diff-key",
			a: map[NodeID]Node{
				12: {},
				10: {},
			},
			b: map[NodeID]Node{
				12: {},
				13: {},
			},
			output: false,
		},
		{
			name: "diff-value",
			a: map[NodeID]Node{
				10: {
					ID:          10,
					ModRevision: 100,
				},
			},
			b: map[NodeID]Node{
				10: {
					ID:          10,
					ModRevision: 101,
				},
			},
			output: false,
		},
		{
			name: "same",
			a: map[NodeID]Node{
				10: {
					ID:          10,
					ModRevision: 100,
				},
			},
			b: map[NodeID]Node{
				10: {
					ID:          10,
					ModRevision: 100,
				},
			},
			output: true,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			output := nodesEqual(e.a, e.b)
			assert.Equal(t, e.output, output)
		})
	}
}
