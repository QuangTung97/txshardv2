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
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			output := allocatePartitions(e.partitions, e.nodes, e.count)
			assert.Equal(t, e.output, output)
		})
	}
}
