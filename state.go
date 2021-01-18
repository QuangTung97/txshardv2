package txshardv2

import "strconv"

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

	leaseID  LeaseID
	leaderID NodeID
	nodes    map[NodeID]Node
}

type HandleOutput struct {
	Kvs []CASKeyValue
}

// NewState ...
func NewState(conf *StateConfig) *State {
	return &State{
		config: conf,
	}
}

func cloneNodes(nodes map[NodeID]Node) map[NodeID]Node {
	result := make(map[NodeID]Node)
	for k, v := range nodes {
		result[k] = v
	}
	return result
}

// Clone ...
func (s *State) Clone() *State {
	return &State{
		config:   s.config,
		leaseID:  s.leaseID,
		leaderID: s.leaderID,
		nodes:    cloneNodes(s.nodes),
	}
}

func formatNodeID(id NodeID) string {
	return strconv.FormatUint(uint64(id), 10)
}

func formatPartitionID(id PartitionID) string {
	return strconv.FormatUint(uint64(id), 10)
}

func computeNodeKvs(newState *State) []CASKeyValue {
	conf := newState.config
	if newState.leaderID != conf.SelfNodeID || len(newState.nodes) == 0 {
		return nil
	}

	var kvs []CASKeyValue
	for partitionID := PartitionID(0); partitionID < conf.PartitionCount; partitionID++ {
		kvs = append(kvs, CASKeyValue{
			Type:  EventTypePut,
			Key:   conf.ExpectedPartitionPrefix + formatPartitionID(partitionID),
			Value: formatNodeID(conf.SelfNodeID),
		})
	}
	return kvs
}

func computeHandleOutput(oldState *State, newState *State) HandleOutput {
	conf := oldState.config

	var kvs []CASKeyValue
	if newState.leaseID != oldState.leaseID {
		kvs = append(kvs, CASKeyValue{
			Type:    EventTypePut,
			Key:     conf.NodePrefix + formatNodeID(conf.SelfNodeID),
			Value:   conf.SelfNodeAddress,
			LeaseID: newState.leaseID,
		})
	}

	if newState.leaderID != oldState.leaderID {
		kvs = append(kvs, computeNodeKvs(newState)...)
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
