package txshardv2

// PartitionID ...
type PartitionID uint32

// NodeID ...
type NodeID uint32

// LeaseID ...
type LeaseID int64

// Revision ...
type Revision int64

// EventType ...
type EventType int

const (
	// EventTypePut ...
	EventTypePut EventType = 1
	// EventTypeDelete ...
	EventTypeDelete EventType = 2
)

// RunnerEventType ...
type RunnerEventType int

const (
	// RunnerEventTypeStart ...
	RunnerEventTypeStart RunnerEventType = 1
	// RunnerEventTypeStop ...
	RunnerEventTypeStop RunnerEventType = 2
)

// CASKeyValue ...
type CASKeyValue struct {
	Type        EventType
	Key         string
	Value       string
	LeaseID     LeaseID
	ModRevision Revision
}

// Runner ...
type Runner func(partitionID PartitionID)

//============================================
// Events
//============================================

// PartitionEvent ...
type PartitionEvent struct {
	Type        EventType
	Partition   PartitionID
	NodeID      NodeID
	ModRevision Revision
}

// ExpectedPartitionEvents ...
type ExpectedPartitionEvents struct {
	Events []PartitionEvent
}

// CurrentPartitionEvents ...
type CurrentPartitionEvents struct {
	Events []PartitionEvent
}

// RunnerEvent ...
type RunnerEvent struct {
	Type        RunnerEventType
	PartitionID PartitionID
}

// RunnerEvents ...
type RunnerEvents struct {
	Events []RunnerEvent
}

//============================================
// State
//============================================

// PartitionData ...
type PartitionData struct {
	Persisted   bool
	NodeID      NodeID
	ModRevision Revision
}

// Partition ...
type Partition struct {
	Expected PartitionData
	Current  PartitionData
	Running  bool
}

// Node ...
type Node struct {
	ID          NodeID
	Address     string
	ModRevision Revision
}
