package txshardv2

import "context"

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

// NodeEvent ...
type NodeEvent struct {
	Type        EventType
	NodeID      NodeID
	Address     string
	ModRevision Revision
}

// NodeEvents ...
type NodeEvents struct {
	Events []NodeEvent
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

//============================================
// Interfaces
//============================================

// Runner ...
type Runner func(ctx context.Context, partitionID PartitionID)

// EtcdClient ...
type EtcdClient interface {
	WatchLease(ctx context.Context) <-chan LeaseID
	WatchLeader(ctx context.Context) <-chan NodeID
	WatchNodes(ctx context.Context, prefix string) <-chan NodeEvents
	WatchExpectedPartitions(ctx context.Context, prefix string) <-chan ExpectedPartitionEvents
	WatchCurrentPartitions(ctx context.Context, prefix string) <-chan CurrentPartitionEvents
	CompareAndSet(ctx context.Context, kvs []CASKeyValue) error
}
