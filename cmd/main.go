package main

import (
	"context"
	"github.com/QuangTung97/txshardv2"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"os/signal"
	"strconv"
	"sync"
)

func main() {
	if len(os.Args) <= 2 {
		panic("must provide node_id and address")
	}

	num, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	nodeID := txshardv2.NodeID(num)
	address := os.Args[2]

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)

	zapConf := zap.NewProductionConfig()
	zapConf.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	logger, err := zapConf.Build()
	if err != nil {
		panic(err)
	}

	system := txshardv2.NewSystem(nodeID, address,
		"sample", 4,
		func(ctx context.Context, partitionID txshardv2.PartitionID) {

		},
		clientv3.Config{
			Endpoints: []string{
				"localhost:2379",
			},
		}, logger,
	)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		system.Run(ctx)
	}()

	<-done
	cancel()
	wg.Wait()
}
