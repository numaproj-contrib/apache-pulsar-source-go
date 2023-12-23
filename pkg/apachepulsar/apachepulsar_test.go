/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apachepulsar

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/numaproj/numaflow-go/pkg/sourcer"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj-contrib/apache-pulsar-source-go/pkg/mocks"
)

var (
	pulsarClient pulsar.Client
	resource     *dockertest.Resource
	pool         *dockertest.Pool
)

const (
	host             = "pulsar://localhost:6650"
	topic            = "test-topic"
	subscriptionName = "test-subscription"
)

func sendMessage(client pulsar.Client, ctx context.Context) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: []byte("hello"),
			})
			if err != nil {
				log.Println("Failed to publish message", err)
			}
		}
	}
}

func TestMain(m *testing.M) {
	var err error
	p, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker ;is it running ? %s", err)
	}
	pool = p
	opts := dockertest.RunOptions{
		Repository:   "apachepulsar/pulsar",
		Tag:          "3.1.1",
		ExposedPorts: []string{"6650/tcp", "8080/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"6650/tcp": {
				{HostIP: "127.0.0.1", HostPort: "6650"},
			},
			"8080/tcp": {
				{HostIP: "127.0.0.1", HostPort: "8080"},
			},
		},
		Mounts: []string{"pulsardata:/pulsar/data", "pulsarconf:/pulsar/conf"},
		Cmd:    []string{"bin/pulsar", "standalone"},
	}
	resource, err = pool.RunWithOptions(&opts)
	if err != nil {
		log.Println(err)
		_ = pool.Purge(resource)
		log.Fatalf("could not start resource %s", err)
	}
	if err != nil {
		log.Fatalf("error -%s", err)
	}
	if err := pool.Retry(func() error {
		pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:               host,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
		})
		if err != nil {
			log.Fatalf("failed to create pulsar client: %v", err)
		}
		return nil
	}); err != nil {
		if resource != nil {
			_ = pool.Purge(resource)
		}
		log.Fatalf("could not connect to apache pulsar %s", err)
	}
	defer pulsarClient.Close()
	code := m.Run()
	if resource != nil {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Couln't purge resource %s", err)
		}
	}
	os.Exit(code)
}

func TestPulsarSource_Read(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sendMessage(pulsarClient, ctx)
	messageCh := make(chan sourcer.Message, 20)
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pulsarSource := NewPulsarSource(pulsarClient, consumer)
	pulsarSource.Read(ctx, mocks.ReadRequest{
		CountValue: 5,
		Timeout:    20 * time.Second,
	}, messageCh)
	assert.Equal(t, 5, len(messageCh))
	// Try reading 4 more messages
	// Since the previous batch didn't get acked, the data source shouldn't allow us to read more messages
	// We should get 0 messages, meaning the channel only holds the previous 5 messages

	pulsarSource.Read(ctx, mocks.ReadRequest{
		CountValue: 4,
		Timeout:    time.Second,
	}, messageCh)
	assert.Equal(t, 5, len(messageCh))

	msg1 := <-messageCh
	msg2 := <-messageCh
	msg3 := <-messageCh
	msg4 := <-messageCh
	msg5 := <-messageCh
	fmt.Println(msg1.Offset().Value(), msg2.Offset().Value(), msg3.Offset().Value(), msg4.Offset().Value(), msg5.Offset().Value())

	pulsarSource.Ack(ctx, mocks.TestAckRequest{
		OffsetsValue: []sourcer.Offset{msg1.Offset(), msg2.Offset(), msg3.Offset(), msg4.Offset(), msg5.Offset()},
	})

	// Read 6 more messages
	pulsarSource.Read(ctx, mocks.ReadRequest{
		CountValue: 6,
		Timeout:    time.Second,
	}, messageCh)
	assert.Equal(t, 6, len(messageCh))
}

func TestPulsarSource_Partitions(t *testing.T) {
	os.Setenv("PULSAR_ADMIN_ENDPOINT", "http://localhost:8080")
	os.Setenv("PULSAR_TOPIC", topic)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pulsarSource := NewPulsarSource(pulsarClient, consumer)
	partitions := pulsarSource.Partitions(ctx)
	assert.GreaterOrEqual(t, 1, len(partitions))
}

func TestPulsarSource_Pending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sendMessage(pulsarClient, ctx)
	messageCh := make(chan sourcer.Message, 20)
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pulsarSource := NewPulsarSource(pulsarClient, consumer)
	// Reading 4 messages
	pulsarSource.Read(ctx, mocks.ReadRequest{
		CountValue: 4,
		Timeout:    time.Second,
	}, messageCh)
	// pending should return 4 as there are 4 messages to be acknowledged
	pending := pulsarSource.Pending(ctx)
	assert.Equal(t, int64(4), pending)
}
