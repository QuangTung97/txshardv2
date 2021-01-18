package txshardv2

import (
	"strconv"
)

// StateConfig ...
type StateConfig struct {
	ExpectedPartitionPrefix string
	CurrentPartitionPrefix  string
	NodePrefix              string
	PartitionCount          PartitionID

	SelfNodeID      NodeID
	SelfNodeAddress string
}

// State ...
type State struct {
	config *StateConfig

	leaseID    LeaseID
	leaderID   NodeID
	nodes      map[NodeID]Node
	partitions []Partition
}

// HandleOutput ...
type HandleOutput struct {
	Kvs []CASKeyValue
}

// NewState ...
func NewState(conf *StateConfig) *State {
	return &State{
		config:     conf,
		nodes:      map[NodeID]Node{},
		partitions: make([]Partition, conf.PartitionCount),
	}
}

func cloneNodes(nodes map[NodeID]Node) map[NodeID]Node {
	result := make(map[NodeID]Node)
	for k, v := range nodes {
		result[k] = v
	}
	return result
}

func clonePartitions(partitions []Partition) []Partition {
	result := make([]Partition, len(partitions))
	copy(result, partitions)
	return result
}

// Clone ...
func (s *State) Clone() *State {
	return &State{
		config:     s.config,
		leaseID:    s.leaseID,
		leaderID:   s.leaderID,
		nodes:      cloneNodes(s.nodes),
		partitions: clonePartitions(s.partitions),
	}
}

func formatNodeID(id NodeID) string {
	return strconv.FormatUint(uint64(id), 10)
}

func formatPartitionID(id PartitionID) string {
	return strconv.FormatUint(uint64(id), 10)
}

func computeExpectedPartitionKvs(newState *State) []CASKeyValue {
	conf := newState.config
	if newState.leaderID != conf.SelfNodeID || len(newState.nodes) == 0 {
		return nil
	}

	updatedPartitions := allocatePartitions(newState.partitions, newState.nodes, conf.PartitionCount)

	var kvs []CASKeyValue
	for _, updated := range updatedPartitions {
		kvs = append(kvs, CASKeyValue{
			Type:        EventTypePut,
			Key:         conf.ExpectedPartitionPrefix + formatPartitionID(updated.id),
			Value:       formatNodeID(updated.nodeID),
			ModRevision: updated.modRevision,
		})
	}
	return kvs
}

func computeHandleOutput(oldState *State, newState *State) HandleOutput {
	conf := oldState.config

	var kvs []CASKeyValue
	_, selfNodeExisted := newState.nodes[conf.SelfNodeID]
	if newState.leaseID != oldState.leaseID || !selfNodeExisted {
		if newState.leaseID != 0 {
			kvs = append(kvs, CASKeyValue{
				Type:    EventTypePut,
				Key:     conf.NodePrefix + formatNodeID(conf.SelfNodeID),
				Value:   conf.SelfNodeAddress,
				LeaseID: newState.leaseID,
			})
		}
	}

	if newState.leaderID != oldState.leaderID ||
		!partitionExpectedEqual(newState.partitions, oldState.partitions) ||
		!nodesEqual(newState.nodes, oldState.nodes) {
		kvs = append(kvs, computeExpectedPartitionKvs(newState)...)
	}

	return HandleOutput{
		Kvs: kvs,
	}
}

func handleLeaseEvent(s *State, leaseID LeaseID) (*State, HandleOutput) {
	newState := s.Clone()
	newState.leaseID = leaseID

	return newState, computeHandleOutput(s, newState)
}

func handleLeaderEvent(s *State, leaderID NodeID) (*State, HandleOutput) {
	newState := s.Clone()
	newState.leaderID = leaderID

	return newState, computeHandleOutput(s, newState)
}

func nodesEqual(a, b map[NodeID]Node) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		_, existed := b[k]
		if !existed {
			return false
		}
		if v != b[k] {
			return false
		}
	}
	return true
}
