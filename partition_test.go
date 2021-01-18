package txshardv2

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAllocatePartitions(t *testing.T) {
	table := []struct {
		name       string
		nodes      map[NodeID]Node
		count      PartitionID
		partitions []Partition
		output     []updatedPartition
	}{
		{
			name:  "empty",
			nodes: map[NodeID]Node{},
			count: 4,
			partitions: []Partition{
				{},
				{},
				{},
				{},
			},
		},
		{
			name: "simple",
			nodes: map[NodeID]Node{
				11: {
					ID: 11,
				},
			},
			count: 4,
			partitions: []Partition{
				{},
				{},
				{},
				{},
			},
			output: []updatedPartition{
				{
					id:     0,
					nodeID: 11,
				},
				{
					id:     1,
					nodeID: 11,
				},
				{
					id:     2,
					nodeID: 11,
				},
				{
					id:     3,
					nodeID: 11,
				},
			},
		},
		{
			name: "persisted",
			nodes: map[NodeID]Node{
				11: {
					ID: 11,
				},
			},
			count: 4,
			partitions: []Partition{
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      15,
						ModRevision: 100,
					},
				},
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      11,
						ModRevision: 200,
					},
				},
				{},
				{},
			},
			output: []updatedPartition{
				{
					id:          0,
					nodeID:      11,
					modRevision: 100,
				},
				{
					id:     2,
					nodeID: 11,
				},
				{
					id:     3,
					nodeID: 11,
				},
			},
		},
		{
			name: "7-partitions-4-1-0",
			nodes: map[NodeID]Node{
				13: {
					ID: 13,
				},
				11: {
					ID: 11,
				},
				12: {
					ID: 12,
				},
			},
			count: 7,
			partitions: []Partition{
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      15,
						ModRevision: 100,
					},
				},
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      11,
						ModRevision: 200,
					},
				},
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      11,
						ModRevision: 201,
					},
				},
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      11,
						ModRevision: 202,
					},
				},
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      11,
						ModRevision: 203,
					},
				},
				{
					Expected: PartitionData{
						Persisted:   true,
						NodeID:      12,
						ModRevision: 204,
					},
				},
				{},
			},
			output: []updatedPartition{
				{
					id:          0,
					nodeID:      12,
					modRevision: 100,
				},
				{
					id:     6,
					nodeID: 13,
				},
				{
					id:          4,
					nodeID:      13,
					modRevision: 203,
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			output := allocatePartitions(e.partitions, e.nodes, e.count)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestPartitionDataEqual(t *testing.T) {
	table := []struct {
		name   string
		a      PartitionData
		b      PartitionData
		output bool
	}{
		{
			name:   "both-empty",
			output: true,
		},
		{
			name: "a-persisted",
			a: PartitionData{
				Persisted: true,
			},
			output: false,
		},
		{
			name: "b-persisted",
			b: PartitionData{
				Persisted: true,
			},
			output: false,
		},
		{
			name: "both-persisted.same-id-mod",
			a: PartitionData{
				Persisted:   true,
				NodeID:      20,
				ModRevision: 212,
			},
			b: PartitionData{
				Persisted:   true,
				NodeID:      20,
				ModRevision: 212,
			},
			output: true,
		},
		{
			name: "both-persisted.same-id-not-mod",
			a: PartitionData{
				Persisted:   true,
				NodeID:      20,
				ModRevision: 212,
			},
			b: PartitionData{
				Persisted:   true,
				NodeID:      20,
				ModRevision: 222,
			},
			output: false,
		},
		{
			name: "both-persisted.same-mod-not-id",
			a: PartitionData{
				Persisted:   true,
				NodeID:      30,
				ModRevision: 222,
			},
			b: PartitionData{
				Persisted:   true,
				NodeID:      20,
				ModRevision: 222,
			},
			output: false,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			output := partitionDataEqual(e.a, e.b)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestPartitionExpectedEqual(t *testing.T) {
	table := []struct {
		name   string
		a      []Partition
		b      []Partition
		output bool
	}{
		{
			name:   "both-empty",
			output: true,
		},
		{
			name: "diff-size",
			a: []Partition{
				{
				},
			},
			output: false,
		},
		{
			name: "diff-one",
			a: []Partition{
				{
					Expected: PartitionData{
						Persisted: true,
					},
				},
			},
			b: []Partition{
				{
				},
			},
			output: false,
		},
		{
			name: "same",
			a: []Partition{
				{
					Expected: PartitionData{
						Persisted: true,
						NodeID:    11,
					},
				},
			},
			b: []Partition{
				{
					Expected: PartitionData{
						Persisted: true,
						NodeID:    11,
					},
				},
			},
			output: true,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			output := partitionExpectedEqual(e.a, e.b)
			assert.Equal(t, e.output, output)
		})
	}
}
