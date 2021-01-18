package txshardv2

import "sort"

type updatedPartition struct {
	id          PartitionID
	nodeID      NodeID
	modRevision Revision
}

type sortNodeID []NodeID

var _ sort.Interface = sortNodeID{}

func (s sortNodeID) Len() int {
	return len(s)
}

func (s sortNodeID) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s sortNodeID) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func allocatePartitions(partitions []Partition, nodes map[NodeID]Node, partitionCount PartitionID) []updatedPartition {
	if len(nodes) == 0 {
		return nil
	}

	type freePartition struct {
		id          PartitionID
		modRevision Revision
	}

	var nodeIDs []NodeID
	nodeIDSet := make(map[NodeID]struct{})
	for nodeID := range nodes {
		nodeIDs = append(nodeIDs, nodeID)
		nodeIDSet[nodeID] = struct{}{}
	}
	sort.Sort(sortNodeID(nodeIDs))

	allocationMin := int(partitionCount) / len(nodes)
	allocationMaxCount := int(partitionCount) - allocationMin*len(nodes)

	allocatedMap := make(map[NodeID][]freePartition)
	var freePartitions []freePartition
	for id := PartitionID(0); id < partitionCount; id++ {
		partition := partitions[id]
		modRevision := Revision(0)

		if partition.Expected.Persisted {
			nodeID := partition.Expected.NodeID
			modRevision = partition.Expected.ModRevision

			_, existed := nodeIDSet[nodeID]
			if existed {
				allocatedMap[nodeID] = append(allocatedMap[nodeID], freePartition{
					id:          id,
					modRevision: modRevision,
				})
				continue
			}
		}

		freePartitions = append(freePartitions, freePartition{
			id:          id,
			modRevision: modRevision,
		})
	}

	for i, nodeID := range nodeIDs {
		allocated := allocatedMap[nodeID]
		expectedPartitionCount := allocationMin
		if i < allocationMaxCount {
			expectedPartitionCount = allocationMin + 1
		}

		if len(allocated) > expectedPartitionCount {
			freePartitions = append(freePartitions, allocated[expectedPartitionCount:]...)
			allocatedMap[nodeID] = allocated[:expectedPartitionCount]
		}
	}

	var result []updatedPartition
	for i, nodeID := range nodeIDs {
		allocated := allocatedMap[nodeID]
		expectedPartitionCount := allocationMin
		if i < allocationMaxCount {
			expectedPartitionCount = allocationMin + 1
		}

		if len(allocated) < expectedPartitionCount {
			num := expectedPartitionCount - len(allocated)
			for _, partition := range freePartitions[:num] {
				result = append(result, updatedPartition{
					id:          partition.id,
					nodeID:      nodeID,
					modRevision: partition.modRevision,
				})
			}
			freePartitions = freePartitions[num:]
		}
	}

	return result
}
