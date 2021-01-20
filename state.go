package txshardv2

import (
	"context"
	"strconv"
	"time"
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

	StartPartitions []PartitionID
	StopPartitions  []PartitionID
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
	_, selfNodeOldExisted := oldState.nodes[conf.SelfNodeID]
	_, selfNodeNewExisted := newState.nodes[conf.SelfNodeID]
	if newState.leaseID != oldState.leaseID || (selfNodeOldExisted && !selfNodeNewExisted) {
		if newState.leaseID != 0 {
			kvs = append(kvs, CASKeyValue{
				Type:    EventTypePut,
				Key:     conf.NodePrefix + formatNodeID(conf.SelfNodeID),
				Value:   conf.SelfNodeAddress,
				LeaseID: newState.leaseID,
			})
		}
	}

	nodesChanged := !nodesEqual(newState.nodes, oldState.nodes)

	if newState.leaderID != oldState.leaderID ||
		!partitionExpectedEqual(newState.partitions, oldState.partitions) ||
		nodesChanged {
		kvs = append(kvs, computeExpectedPartitionKvs(newState)...)
	}

	var startPartitions []PartitionID
	var stopPartitions []PartitionID

	for id := PartitionID(0); id < conf.PartitionCount; id++ {
		newPartition := newState.partitions[id]
		oldPartition := oldState.partitions[id]
		if newState.leaseID == oldState.leaseID &&
			partitionDataEqual(newPartition.Expected, oldPartition.Expected) &&
			partitionDataEqual(newPartition.Current, oldPartition.Current) {
			continue
		}

		modRevision := Revision(0)
		if newPartition.Current.Persisted {
			modRevision = newPartition.Current.ModRevision
		}

		actions := computePartitionActions(newPartition, conf.SelfNodeID)
		if actions.put && newState.leaseID != 0 {
			kvs = append(kvs, CASKeyValue{
				Type:        EventTypePut,
				Key:         conf.CurrentPartitionPrefix + formatPartitionID(id),
				Value:       formatNodeID(conf.SelfNodeID),
				LeaseID:     newState.leaseID,
				ModRevision: modRevision,
			})
		}
		if actions.delete {
			kvs = append(kvs, CASKeyValue{
				Type:        EventTypeDelete,
				Key:         conf.CurrentPartitionPrefix + formatPartitionID(id),
				ModRevision: modRevision,
			})
		}
		if actions.start {
			startPartitions = append(startPartitions, id)
		}
		if actions.stop {
			stopPartitions = append(stopPartitions, id)
		}
	}

	return HandleOutput{
		Kvs:             kvs,
		StartPartitions: startPartitions,
		StopPartitions:  stopPartitions,
	}
}

func computeHandleOutputForRetry(oldState *State) HandleOutput {
	conf := oldState.config

	var kvs []CASKeyValue

	_, selfNodeExisted := oldState.nodes[conf.SelfNodeID]
	if !selfNodeExisted {
		if oldState.leaseID != 0 {
			kvs = append(kvs, CASKeyValue{
				Type:    EventTypePut,
				Key:     conf.NodePrefix + formatNodeID(conf.SelfNodeID),
				Value:   conf.SelfNodeAddress,
				LeaseID: oldState.leaseID,
			})
		}
	}

	kvs = append(kvs, computeExpectedPartitionKvs(oldState)...)

	var startPartitions []PartitionID
	var stopPartitions []PartitionID

	for id := PartitionID(0); id < conf.PartitionCount; id++ {
		oldPartition := oldState.partitions[id]

		modRevision := Revision(0)
		if oldPartition.Current.Persisted {
			modRevision = oldPartition.Current.ModRevision
		}

		actions := computePartitionActions(oldPartition, conf.SelfNodeID)
		if actions.put && oldState.leaseID != 0 {
			kvs = append(kvs, CASKeyValue{
				Type:        EventTypePut,
				Key:         conf.CurrentPartitionPrefix + formatPartitionID(id),
				Value:       formatNodeID(conf.SelfNodeID),
				LeaseID:     oldState.leaseID,
				ModRevision: modRevision,
			})
		}
		if actions.delete {
			kvs = append(kvs, CASKeyValue{
				Type:        EventTypeDelete,
				Key:         conf.CurrentPartitionPrefix + formatPartitionID(id),
				ModRevision: modRevision,
			})
		}
		if actions.start {
			startPartitions = append(startPartitions, id)
		}
		if actions.stop {
			stopPartitions = append(stopPartitions, id)
		}
	}

	return HandleOutput{
		Kvs:             kvs,
		StartPartitions: startPartitions,
		StopPartitions:  stopPartitions,
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

func handleNodeEvents(s *State, nodeEvents NodeEvents) (*State, HandleOutput) {
	newState := s.Clone()
	for _, event := range nodeEvents.Events {
		if event.Type == EventTypePut {
			newState.nodes[event.NodeID] = Node{
				ID:          event.NodeID,
				Address:     event.Address,
				ModRevision: event.ModRevision,
			}
		} else {
			delete(newState.nodes, event.NodeID)
		}
	}

	return newState, computeHandleOutput(s, newState)
}

func handleExpectedPartitionEvents(s *State, events ExpectedPartitionEvents) (*State, HandleOutput) {
	newState := s.Clone()
	for _, event := range events.Events {
		if event.Type == EventTypePut {
			newState.partitions[event.Partition].Expected = PartitionData{
				Persisted:   true,
				NodeID:      event.NodeID,
				ModRevision: event.ModRevision,
			}
		} else {
			newState.partitions[event.Partition].Expected = PartitionData{
				Persisted: false,
			}
		}
	}

	return newState, computeHandleOutput(s, newState)
}

func handleCurrentPartitionEvents(s *State, events CurrentPartitionEvents) (*State, HandleOutput) {
	newState := s.Clone()
	for _, event := range events.Events {
		if event.Type == EventTypePut {
			newState.partitions[event.Partition].Current = PartitionData{
				Persisted:   true,
				NodeID:      event.NodeID,
				ModRevision: event.ModRevision,
			}
		} else {
			newState.partitions[event.Partition].Current = PartitionData{
				Persisted: false,
			}
		}
	}

	return newState, computeHandleOutput(s, newState)
}

func handleRunnerEvents(s *State, events RunnerEvents) (*State, HandleOutput) {
	newState := s.Clone()

	for _, event := range events.Events {
		if event.Type == RunnerEventTypeStart {
			newState.partitions[event.PartitionID].Running = true
		} else {
			newState.partitions[event.PartitionID].Running = false
		}
	}

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

func handleEvents(ctx context.Context, oldState *State,
	leaseChan <-chan LeaseID,
	leaderChan <-chan NodeID,
	nodeEventsChan <-chan NodeEvents,
	expectedPartitionChan <-chan ExpectedPartitionEvents,
	currentPartitionChan <-chan CurrentPartitionEvents,
	runnerChan <-chan RunnerEvents,
	after <-chan time.Time,
) (*State, HandleOutput) {
	select {
	case leaseID := <-leaseChan:
		return handleLeaseEvent(oldState, leaseID)

	case leaderID := <-leaderChan:
		return handleLeaderEvent(oldState, leaderID)

	case nodeEvents := <-nodeEventsChan:
		return handleNodeEvents(oldState, nodeEvents)

	case events := <-expectedPartitionChan:
		return handleExpectedPartitionEvents(oldState, events)

	case events := <-currentPartitionChan:
		return handleCurrentPartitionEvents(oldState, events)

	case runnerEvents := <-runnerChan:
		return handleRunnerEvents(oldState, runnerEvents)

	case <-after:
		return oldState, computeHandleOutputForRetry(oldState)

	case <-ctx.Done():
		return oldState, HandleOutput{}
	}
}
