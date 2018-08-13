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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/testutils"
	"go.uber.org/zap"

	kmocks "github.com/jaegertracing/jaeger/cmd/ingester/app/consumer/mocks"
	pmocks "github.com/jaegertracing/jaeger/cmd/ingester/app/processor/mocks"
	"github.com/Shopify/sarama/mocks"
	"fmt"
)

//go:generate mockery -dir ../../../../pkg/kafka/config/ -name Consumer
//go:generate mockery -dir ../../../../../vendor/github.com/bsm/sarama-cluster/ -name PartitionConsumer

type consumerTest struct {
	saramaConsumer    *kmocks.Consumer
	consumer          *Consumer
	partitionConsumer *kmocks.PartitionConsumer
}

func TestConstructor(t *testing.T) {
	newConsumer, err := New(Params{})
	assert.NoError(t, err)
	assert.NotNil(t, newConsumer)
}

func withWrappedConsumer(fn func(c *consumerTest)) {
	sc := &kmocks.Consumer{}
	logger, _ := zap.NewDevelopment()
	metricsFactory := metrics.NewLocalFactory(0)
	c := &consumerTest{
		saramaConsumer: sc,
		consumer: &Consumer{
			metricsFactory: metricsFactory,
			logger:         logger,
			close:          make(chan struct{}),
			partitionToProcessorState: make(map[int32]*sync.WaitGroup),
			internalConsumer:          sc,
			processorFactory: ProcessorFactory{
				topic:          "topic",
				consumer:       sc,
				metricsFactory: metricsFactory,
				logger:         logger,
				baseProcessor:  &pmocks.SpanProcessor{},
				parallelism:    1,
			},
		},
	}

	c.partitionConsumer = &kmocks.PartitionConsumer{}
	pcha := make(chan cluster.PartitionConsumer, 1)
	pcha <- c.partitionConsumer
	c.saramaConsumer.On("Partitions").Return((<-chan cluster.PartitionConsumer)(pcha))
	c.saramaConsumer.On("Close").Return(nil)
	c.saramaConsumer.On("MarkPartitionOffset", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	fn(c)
}

func TestSaramaConsumerWrapper_MarkPartitionOffset(t *testing.T) {
	withWrappedConsumer(func(c *consumerTest) {
		topic := "morekuzambu"
		partition := int32(316)
		offset := int64(1111110111111)
		metadata := "meatbag"
		c.saramaConsumer.On("MarkPartitionOffset", topic, partition, offset, metadata).Return()

		c.saramaConsumer.MarkPartitionOffset(topic, partition, offset, metadata)

		c.saramaConsumer.AssertCalled(t, "MarkPartitionOffset", topic, partition, offset, metadata)
	})
}

func TestSaramaConsumerWrapper_start_Messages(t *testing.T) {
	withWrappedConsumer(func(c *consumerTest) {
		msg := &sarama.ConsumerMessage{}
		msg.Offset = 0
		msgCh := make(chan *sarama.ConsumerMessage, 1)
		msgCh <- msg

		errCh := make(chan *sarama.ConsumerError, 1)
		c.partitionConsumer.On("Partition").Return(int32(0))
		c.partitionConsumer.On("Errors").Return((<-chan *sarama.ConsumerError)(errCh))
		c.partitionConsumer.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgCh))
		c.partitionConsumer.On("HighWaterMarkOffset").Return(int64(1234))
		c.partitionConsumer.On("Close").Return(nil)

		mp := &pmocks.SpanProcessor{}
		mp.On("Process", &saramaMessageWrapper{msg}).Return(nil)
		c.consumer.processorFactory.baseProcessor = mp

		c.consumer.Start()
		time.Sleep(100 * time.Millisecond)
		close(msgCh)
		close(errCh)
		c.consumer.Close()

		mp.AssertExpectations(t)

		f := (c.consumer.metricsFactory).(*metrics.LocalFactory)
		partitionTag := map[string]string{"partition": "0"}
		testutils.AssertCounterMetrics(t, f, testutils.ExpectedMetric{
			Name:  "sarama-consumer.messages",
			Tags:  partitionTag,
			Value: 1,
		})
		testutils.AssertGaugeMetrics(t, f, testutils.ExpectedMetric{
			Name:  "sarama-consumer.current-offset",
			Tags:  partitionTag,
			Value: 0,
		})
		testutils.AssertGaugeMetrics(t, f, testutils.ExpectedMetric{
			Name:  "sarama-consumer.offset-lag",
			Tags:  partitionTag,
			Value: 1233,
		})
	})
}

func TestSaramaConsumerWrapper_start_Errors(t *testing.T) {
	withWrappedConsumer(func(c *consumerTest) {
		errCh := make(chan *sarama.ConsumerError, 1)
		errCh <- &sarama.ConsumerError{
			Topic: "some-topic",
			Err:   errors.New("some error"),
		}

		msgCh := make(chan *sarama.ConsumerMessage)

		c.partitionConsumer.On("Partition").Return(int32(0))
		c.partitionConsumer.On("Errors").Return((<-chan *sarama.ConsumerError)(errCh))
		c.partitionConsumer.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgCh))
		c.partitionConsumer.On("Close").Return(nil)

		c.consumer.Start()
		time.Sleep(100 * time.Millisecond)
		close(msgCh)
		close(errCh)
		c.consumer.Close()
		f := (c.consumer.metricsFactory).(*metrics.LocalFactory)
		partitionTag := map[string]string{"partition": "0"}
		testutils.AssertCounterMetrics(t, f, testutils.ExpectedMetric{
			Name:  "sarama-consumer.errors",
			Tags:  partitionTag,
			Value: 1,
		})
	})
}

func makePartitionConsumer(mockConsumer *mocks.Consumer, topic string, partition int32) saramaMockBackedClusterPartitionConsumer {
	fmt.Println("1")
	partitionConsumer := mockConsumer.ExpectConsumePartition(topic, partition, 0)
	fmt.Println("2")
	go func() {
		time.Sleep(1 * time.Second)
		partitionConsumer.YieldMessage(&sarama.ConsumerMessage{
			Key:            nil,
			Value:          nil,
			Topic:          topic,
			Partition:      partition,
			Offset:         0,
			Timestamp:      time.Now(),
			BlockTimestamp: time.Now(),
		})
		time.Sleep(1 * time.Second)
		fmt.Println("Closing partition consumer")
		partitionConsumer.Close()
	}()

	fmt.Println("3")
	pc , _ := mockConsumer.ConsumePartition(topic, partition, 0)
	return saramaMockBackedClusterPartitionConsumer{
		PartitionConsumer: pc,
		topic:             topic,
		partition:         partition,
	}
}

type saramaMockBackedClusterPartitionConsumer struct {
	sarama.PartitionConsumer
	topic     string
	partition int32
}

func (s saramaMockBackedClusterPartitionConsumer) Topic() string {
	return s.topic
}

func (s saramaMockBackedClusterPartitionConsumer) Partition() int32 {
	return s.partition
}

func TestStartStop(t *testing.T) {
	sc := &kmocks.Consumer{}
	logger, _ := zap.NewDevelopment()
	metricsFactory := metrics.NewLocalFactory(0)

	mp := &pmocks.SpanProcessor{}
	mp.On("Process", mock.Anything).Return(nil)

	testingConsumer := &Consumer{
		metricsFactory: metricsFactory,
		logger:         logger,
		close:          make(chan struct{}),
		partitionToProcessorState: make(map[int32]*sync.WaitGroup),
		internalConsumer:          sc,
		processorFactory: ProcessorFactory{
			topic:          "topic",
			consumer:       sc,
			metricsFactory: metricsFactory,
			logger:         logger,
			baseProcessor:  mp,
			parallelism:    1,
		}}

	mockConsumer := mocks.NewConsumer(t, &sarama.Config{})
	mockConsumer.Close()

	pcha := make(chan cluster.PartitionConsumer, 1)
	fmt.Println("boo")
	pc := makePartitionConsumer(mockConsumer, "boop", 1)
	pcha <- pc
	fmt.Println("booing")
	sc.On("Partitions").Return((<-chan cluster.PartitionConsumer)(pcha))
	sc.On("Close").Return(nil)
	sc.On("MarkPartitionOffset", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	testingConsumer.Start()
	time.Sleep(10 * time.Second)
	// testingConsumer.Close()

	// c.partitionConsumer = &kmocks.PartitionConsumer{}
	// pcha := make(chan cluster.PartitionConsumer, 1)
	// pcha <- c.partitionConsumer
	// c.saramaConsumer.On("Partitions").Return((<-chan cluster.PartitionConsumer)(pcha))
	// c.saramaConsumer.On("Close").Return(nil)
	// c.saramaConsumer.On("MarkPartitionOffset", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// msg := &sarama.ConsumerMessage{}
	// msg.Offset = 0
	// msgCh := make(chan *sarama.ConsumerMessage, 1)
	// msgCh <- msg
	//
	// pcha := make(chan cluster.PartitionConsumer, 1)
	// pcha <- mockedConsumer
	// // go func() {
	// // 	pcha <- c.partitionConsumer
	// // }()
	// c.saramaConsumer.On("Partitions").Return((<-chan cluster.PartitionConsumer)(pcha))
	//
	// errCh := make(chan *sarama.ConsumerError, 1)
	// c.partitionConsumer.On("Partition").Return(int32(0))
	// c.partitionConsumer.On("Errors").Return((<-chan *sarama.ConsumerError)(errCh))
	// c.partitionConsumer.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgCh))
	// c.partitionConsumer.On("HighWaterMarkOffset").Return(int64(1234))
	// c.partitionConsumer.On("Close").Return(nil)
	//
	//
	// c.consumer.Start()
	// time.Sleep(1000 * time.Millisecond)
	// c.consumer.Close()
}
