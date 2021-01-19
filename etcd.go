package txshardv2

import (
	"context"
	"errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

// EtcdManager ...
type EtcdManager struct {
	client       *clientv3.Client
	logger       *zap.Logger
	leaderPrefix string
	selfNodeID   NodeID

	leaseChan  chan LeaseID
	leaderChan chan NodeID
}

var _ EtcdClient = &EtcdManager{}

// ErrTxnFailed ...
var ErrTxnFailed = errors.New("etcd txn failed")

// NewEtcdManager ...
func NewEtcdManager(logger *zap.Logger, conf clientv3.Config, appName string, selfNodeID NodeID) *EtcdManager {
	client, err := clientv3.New(conf)
	if err != nil {
		panic(err)
	}

	logger.Debug("EtcdManager created")

	return &EtcdManager{
		client:       client,
		logger:       logger,
		leaderPrefix: "/" + appName + "/leader",
		selfNodeID:   selfNodeID,

		leaseChan:  make(chan LeaseID, 1),
		leaderChan: make(chan NodeID, 1),
	}
}

// WatchLease ...
func (m *EtcdManager) WatchLease(context.Context) <-chan LeaseID {
	return m.leaseChan
}

// WatchLeader ...
func (m *EtcdManager) WatchLeader(context.Context) <-chan NodeID {
	return m.leaderChan
}

// Run ...
func (m *EtcdManager) Run(originalCtx context.Context) {
	for {
		ctx, cancel := context.WithCancel(originalCtx)

		sess, err := concurrency.NewSession(m.client)
		if err != nil {
			m.logger.Error("concurrency.NewSession", zap.Error(err))
			continue
		}

		m.leaseChan <- LeaseID(sess.Lease())

		go func() {
			election := concurrency.NewElection(sess, m.leaderPrefix)
			observeChan := election.Observe(ctx)

			go func() {
				for res := range observeChan {
					for _, kv := range res.Kvs {
						num := getNumber(string(kv.Value))
						m.leaderChan <- NodeID(num)
					}
				}
			}()

			err := election.Campaign(ctx, formatNodeID(m.selfNodeID))
			if err != nil {
				m.logger.Error("campaign expired")
				cancel()
			}
		}()

		select {
		case <-sess.Done():
			m.logger.Error("lease expired")
			_ = sess.Close()
			continue
		case <-ctx.Done():
			_ = sess.Close()
			if originalCtx.Err() != nil {
				return
			}
			continue
		}
	}
}

// CompareAndSet ...
func (m *EtcdManager) CompareAndSet(ctx context.Context, kvs []CASKeyValue) error {
	m.logger.Debug("CompareAndSet", zap.Any("etcd.kvs", kvs))

	var compares []clientv3.Cmp
	var ops []clientv3.Op
	for _, kv := range kvs {
		if kv.ModRevision != 0 {
			compares = append(compares,
				clientv3.Compare(clientv3.ModRevision(kv.Key), "=", int64(kv.ModRevision)),
			)
		}

		if kv.Type == EventTypePut {
			var op clientv3.Op
			if kv.LeaseID != 0 {
				op = clientv3.OpPut(kv.Key, kv.Value,
					clientv3.WithLease(clientv3.LeaseID(kv.LeaseID)))
			} else {
				op = clientv3.OpPut(kv.Key, kv.Value)
			}

			ops = append(ops, op)
		} else {
			op := clientv3.OpDelete(kv.Key)
			ops = append(ops, op)
		}
	}

	res, err := m.client.Txn(ctx).If(compares...).Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return ErrTxnFailed
	}

	return nil
}

func etcdEventTypeFromLib(eventType mvccpb.Event_EventType) EventType {
	switch eventType {
	case mvccpb.PUT:
		return EventTypePut
	case mvccpb.DELETE:
		return EventTypeDelete
	default:
		panic("unrecognized event type")
	}
}

func getNumberWithPrefix(s string, prefix string) int64 {
	num, err := strconv.ParseInt(strings.TrimPrefix(s, prefix), 10, 64)
	if err != nil {
		panic(err)
	}
	return num
}

func getNumber(s string) int64 {
	num, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return num
}

// WatchNodes ...
func (m *EtcdManager) WatchNodes(ctx context.Context, prefix string) <-chan NodeEvents {
	res, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	rev := res.Header.Revision

	result := make(chan NodeEvents, 1)

	var events []NodeEvent

	for _, kv := range res.Kvs {
		event := NodeEvent{
			Type:        EventTypePut,
			NodeID:      NodeID(getNumberWithPrefix(string(kv.Key), prefix)),
			Address:     string(kv.Value),
			ModRevision: Revision(kv.ModRevision),
		}
		m.logger.Debug("WatchNodes", zap.Any("node.event", event))
		events = append(events, event)
	}

	result <- NodeEvents{
		Events: events,
	}

	ch := m.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))

	go func() {
		for wr := range ch {
			var events []NodeEvent
			for _, e := range wr.Events {
				eventType := etcdEventTypeFromLib(e.Type)

				event := NodeEvent{
					Type:        eventType,
					NodeID:      NodeID(getNumberWithPrefix(string(e.Kv.Key), prefix)),
					Address:     string(e.Kv.Value),
					ModRevision: Revision(e.Kv.ModRevision),
				}
				m.logger.Debug("WatchNodes", zap.Any("node.event", event))
				events = append(events, event)
			}
			result <- NodeEvents{
				Events: events,
			}
		}
	}()

	return result
}

// WatchExpectedPartitions ...
func (m *EtcdManager) WatchExpectedPartitions(ctx context.Context, prefix string) <-chan ExpectedPartitionEvents {
	res, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	rev := res.Header.Revision

	var events []PartitionEvent

	for _, kv := range res.Kvs {
		events = append(events, PartitionEvent{
			Type:        EventTypePut,
			Partition:   PartitionID(getNumberWithPrefix(string(kv.Key), prefix)),
			NodeID:      NodeID(getNumber(string(kv.Value))),
			ModRevision: Revision(kv.ModRevision),
		})
	}

	result := make(chan ExpectedPartitionEvents, 1)
	if len(events) > 0 {
		m.logger.Debug("WatchExpectedPartitions", zap.Any("partition.expected.events", events))
		result <- ExpectedPartitionEvents{
			Events: events,
		}
	}

	ch := m.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))

	go func() {
		for wr := range ch {
			var events []PartitionEvent
			for _, e := range wr.Events {
				evenType := etcdEventTypeFromLib(e.Type)
				nodeID := NodeID(0)
				if evenType == EventTypePut {
					nodeID = NodeID(getNumber(string(e.Kv.Value)))
				}

				events = append(events, PartitionEvent{
					Type:        evenType,
					Partition:   PartitionID(getNumberWithPrefix(string(e.Kv.Key), prefix)),
					NodeID:      nodeID,
					ModRevision: Revision(e.Kv.ModRevision),
				})
			}
			m.logger.Debug("WatchExpectedPartitions", zap.Any("partition.expected.events", events))
			result <- ExpectedPartitionEvents{
				Events: events,
			}
		}
	}()

	return result
}

// WatchCurrentPartitions ...
func (m *EtcdManager) WatchCurrentPartitions(ctx context.Context, prefix string) <-chan CurrentPartitionEvents {
	res, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	rev := res.Header.Revision

	var events []PartitionEvent

	for _, kv := range res.Kvs {
		events = append(events, PartitionEvent{
			Type:        EventTypePut,
			Partition:   PartitionID(getNumberWithPrefix(string(kv.Key), prefix)),
			NodeID:      NodeID(getNumber(string(kv.Value))),
			ModRevision: Revision(kv.ModRevision),
		})
	}

	result := make(chan CurrentPartitionEvents, 1)
	if len(events) > 0 {
		m.logger.Debug("WatchCurrentPartitions", zap.Any("partition.current.events", events))
		result <- CurrentPartitionEvents{
			Events: events,
		}
	}

	ch := m.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))

	go func() {
		for wr := range ch {
			var events []PartitionEvent
			for _, e := range wr.Events {
				evenType := etcdEventTypeFromLib(e.Type)
				nodeID := NodeID(0)
				if evenType == EventTypePut {
					nodeID = NodeID(getNumber(string(e.Kv.Value)))
				}

				events = append(events, PartitionEvent{
					Type:        evenType,
					Partition:   PartitionID(getNumberWithPrefix(string(e.Kv.Key), prefix)),
					NodeID:      nodeID,
					ModRevision: Revision(e.Kv.ModRevision),
				})
			}
			m.logger.Debug("WatchCurrentPartitions", zap.Any("partition.current.events", events))
			result <- CurrentPartitionEvents{
				Events: events,
			}
		}
	}()

	return result
}
