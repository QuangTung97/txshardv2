package txshardv2

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"sync"
	"time"
)

// System ...
type System struct {
	conf    *StateConfig
	runner  Runner
	logger  *zap.Logger
	manager *EtcdManager
}

// NewSystem ...
func NewSystem(nodeID NodeID, address string,
	appName string, partitionCount PartitionID,
	runner Runner, etcdConf clientv3.Config, logger *zap.Logger,
) *System {
	return &System{
		conf: &StateConfig{
			ExpectedPartitionPrefix: "/" + appName + "/partition/expected/",
			CurrentPartitionPrefix:  "/" + appName + "/partition/current/",
			NodePrefix:              "/" + appName + "/node/",
			PartitionCount:          partitionCount,
			SelfNodeID:              nodeID,
			SelfNodeAddress:         address,
		},
		runner:  runner,
		logger:  logger,
		manager: NewEtcdManager(logger, etcdConf, appName, nodeID),
	}
}

type watchChannels struct {
	leaseChan             <-chan LeaseID
	leaderChan            <-chan NodeID
	nodeChan              <-chan NodeEvents
	expectedPartitionChan <-chan ExpectedPartitionEvents
	currentPartitionChan  <-chan CurrentPartitionEvents
}

// Run ...
func (s *System) Run(originalCtx context.Context) {
	managerCtx, managerCancel := context.WithCancel(context.Background())
	runCtx, runCancel := context.WithCancel(context.Background())

	var managerWg sync.WaitGroup
	managerWg.Add(1)

	go func() {
		defer managerWg.Done()

		s.manager.Run(managerCtx)
	}()

	var runWg sync.WaitGroup
	runWg.Add(1)

	channels := &watchChannels{
		leaseChan:             s.manager.WatchLease(managerCtx),
		leaderChan:            s.manager.WatchLeader(managerCtx),
		nodeChan:              s.manager.WatchNodes(managerCtx, s.conf.NodePrefix),
		expectedPartitionChan: s.manager.WatchExpectedPartitions(managerCtx, s.conf.ExpectedPartitionPrefix),
		currentPartitionChan:  s.manager.WatchCurrentPartitions(managerCtx, s.conf.CurrentPartitionPrefix),
	}

	go func() {
		defer runWg.Done()

		runLoop(runCtx, s.logger, time.Minute, s.runner, s.conf, s.manager, channels)
	}()

	<-originalCtx.Done()

	runCancel()
	runWg.Wait()

	managerCancel()
	managerWg.Wait()

	s.logger.Info("System Stopped")
}

type activeRunner struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

func runLoop(ctx context.Context, logger *zap.Logger,
	timeoutDuration time.Duration,
	runner Runner, conf *StateConfig, client EtcdClient,
	channels *watchChannels,
) {
	runnerChan := make(chan RunnerEvents, conf.PartitionCount)
	state := NewState(conf)
	activeMap := make(map[PartitionID]activeRunner)

	var after <-chan time.Time
	for {
		var output HandleOutput
		state, output = handleEvents(ctx,
			state,
			channels.leaseChan,
			channels.leaderChan,
			channels.nodeChan,
			channels.expectedPartitionChan,
			channels.currentPartitionChan,
			runnerChan,
			after,
		)
		if ctx.Err() != nil {
			for _, runner := range activeMap {
				runner.cancel()
				<-runner.done
			}
			activeMap = make(map[PartitionID]activeRunner)

			return
		}

		var runnerEvents []RunnerEvent

		for _, partition := range output.StartPartitions {
			_, existed := activeMap[partition]
			if existed {
				continue
			}

			id := partition

			ctx, cancel := context.WithCancel(ctx)
			done := make(chan struct{}, 1)

			go func() {
				runner(ctx, id)
				done <- struct{}{}
			}()

			activeMap[partition] = activeRunner{
				cancel: cancel,
				done:   done,
			}

			runnerEvents = append(runnerEvents, RunnerEvent{
				Type:        RunnerEventTypeStart,
				PartitionID: partition,
			})
		}

		for _, partition := range output.StopPartitions {
			_, existed := activeMap[partition]
			if !existed {
				continue
			}

			activeMap[partition].cancel()
		}

		for _, partition := range output.StopPartitions {
			_, existed := activeMap[partition]
			if !existed {
				continue
			}

			<-activeMap[partition].done
			delete(activeMap, partition)

			runnerEvents = append(runnerEvents, RunnerEvent{
				Type:        RunnerEventTypeStop,
				PartitionID: partition,
			})
		}

		if len(runnerEvents) > 0 {
			runnerChan <- RunnerEvents{
				Events: runnerEvents,
			}
		}

		if len(output.Kvs) > 0 {
			err := client.CompareAndSet(ctx, output.Kvs)
			if err != nil {
				logger.Error("client.CompareAndSet", zap.Error(err),
					zap.Any("kvs", output.Kvs),
				)
				after = time.After(timeoutDuration)
				continue
			}
		}
	}
}
