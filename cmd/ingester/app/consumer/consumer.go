// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"sync"

	"github.com/Shopify/sarama"
	sc "github.com/bsm/sarama-cluster"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/ingester/app/processor"
	"github.com/jaegertracing/jaeger/pkg/kafka/consumer"
)

// Params are the parameters of a Consumer
type Params struct {
	ProcessorFactory ProcessorFactory
	Factory          metrics.Factory
	Logger           *zap.Logger
	InternalConsumer consumer.Consumer
}

// Consumer uses sarama to consume and handle messages from kafka
type Consumer struct {
	metricsFactory metrics.Factory
	logger         *zap.Logger

	internalConsumer consumer.Consumer
	processorFactory ProcessorFactory

	close                     chan struct{}
	partitionToProcessorState map[int32]*sync.WaitGroup
}

// New is a constructor for a Consumer
func New(params Params) (*Consumer, error) {
	return &Consumer{
		metricsFactory:   params.Factory,
		logger:           params.Logger,
		close:            make(chan struct{}, 1),
		internalConsumer: params.InternalConsumer,
		processorFactory: params.ProcessorFactory,
	}, nil
}

// Start begins consuming messages in a go routine
func (c *Consumer) Start() {
	c.logger.Info("Starting main loop")
	go c.mainLoop()
}

// Close closes the Consumer and underlying sarama consumer
func (c *Consumer) Close() error {
	c.logger.Info("Closing consumer")

	for k, wg := range c.partitionToProcessorState {
		c.logger.Info("Waiting to close partition", zap.Int32("partition", k))
		wg.Wait()
		c.logger.Info("Closed partition", zap.Int32("partition", k))
	}

	err := c.internalConsumer.Close()
	if err != nil {
		return nil
	}

	return nil
}

func (c *Consumer) mainLoop() {
	for {
		select {
		case pc := <-c.internalConsumer.Partitions():

			if wg, ok := c.partitionToProcessorState[pc.Partition()]; ok {
				wg.Wait()
				delete(c.partitionToProcessorState, pc.Partition())
			}
			c.partitionToProcessorState[pc.Partition()] = &sync.WaitGroup{}

			c.partitionToProcessorState[pc.Partition()].Add(1)
			go c.handleMessages(pc)

			c.partitionToProcessorState[pc.Partition()].Add(1)
			go c.handleErrors(pc.Partition(), pc.Errors())

		case <-c.close:
			c.logger.Info("Closing main loop")
			return
		}
	}
}

func (c *Consumer) handleMessages(pc sc.PartitionConsumer) {
	c.logger.Info("Starting message handler")
	defer c.closePartition(pc)
	defer c.partitionToProcessorState[pc.Partition()].Done()

	msgMetrics := c.newMsgMetrics(pc.Partition())
	var msgProcessor processor.SpanProcessor

	for msg := range pc.Messages() {
		c.logger.Debug("Got msg", zap.Any("msg", msg))
		msgMetrics.counter.Inc(1)
		msgMetrics.offsetGauge.Update(msg.Offset)
		msgMetrics.lagGauge.Update(pc.HighWaterMarkOffset() - msg.Offset - 1)

		if msgProcessor == nil {
			msgProcessor = c.processorFactory.new(pc.Partition(), msg.Offset-1)
			defer msgProcessor.Close()
		}

		msgProcessor.Process(&saramaMessageWrapper{msg})
		c.logger.Info("finished processing messages")
	}
	c.logger.Info("finished handling messages")
}

func (c *Consumer) closePartition(partitionConsumer sc.PartitionConsumer) {
	c.logger.Info("Closing partition consumer", zap.Int32("partition", partitionConsumer.Partition()))
	partitionConsumer.Close() // blocks until messages channel is drained
	c.logger.Info("Closed partition consumer", zap.Int32("partition", partitionConsumer.Partition()))
}

func (c *Consumer) handleErrors(partition int32, errChan <-chan *sarama.ConsumerError) {
	c.logger.Info("Starting error handler")
	defer c.partitionToProcessorState[partition].Done()

	errMetrics := c.newErrMetrics(partition)
	for err := range errChan {
		errMetrics.errCounter.Inc(1)
		c.logger.Error("Error consuming from Kafka", zap.Error(err))
	}
}
