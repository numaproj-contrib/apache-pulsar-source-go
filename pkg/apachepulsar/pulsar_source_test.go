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
	"os/exec"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/numaproj/numaflow-go/pkg/sourcer"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	pulsaradmin "github.com/streamnative/pulsar-admin-go"
	"github.com/streamnative/pulsar-admin-go/pkg/utils"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj-contrib/apache-pulsar-source-go/pkg/payloads"
)

var (
	pulsarClient pulsar.Client
	pulsarAdmin  pulsaradmin.Client
	resource     *dockertest.Resource
	pool         *dockertest.Pool
)

const (
	host                = "pulsar://localhost:6650"
	topic               = "testTopic"
	pendingTopic        = "testPendingTopic" // specifically for testing pending messages
	subscriptionName    = "testSubscription"
	pulsarAdminEndPoint = "http://localhost:8080"
	tenant              = "public"
	namespace           = "test-namespace"
	confDir             = "pulsarConf"
	dataDir             = "pulsarDatDir"
	numOfPartitions     = 2
)

func initProducer(client pulsar.Client, topic string) (pulsar.Producer, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	return producer, err
}

func createTopic(tenant, namespace, topic string, partitions int) error {
	topicPulsar, _ := utils.GetTopicName(fmt.Sprintf("%s/%s/%s", tenant, namespace, topic))
	err := pulsarAdmin.Topics().Create(*topicPulsar, partitions)
	if err != nil {
		log.Printf("error creating topic %s", err)
		return err
	}
	return nil
}

func sendMessage(producer pulsar.Producer, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte("hello"),
			})
			if err != nil {
				log.Println("Failed to publish message", err)
			}
		}
	}
}

func sendCountedMessage(producer pulsar.Producer, ctx context.Context, count int) {
	for i := 0; i < count; i++ {
		_, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte("hello"),
		})
		if err != nil {
			log.Println("Failed to publish message", err)
		}
		// delay confirming message reaches to consumer
		time.Sleep(1 * time.Second)
	}
}

func removeDockerVolume(volumeName string) {
	cmd := exec.Command("docker", "volume", "rm", volumeName)
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to remove Docker volume %s: %s", volumeName, err)
	}
}
func setEnv() {
	os.Setenv("PULSAR_ADMIN_ENDPOINT", pulsarAdminEndPoint)
	os.Setenv("PULSAR_TENANT", tenant)
	os.Setenv("PULSAR_NAMESPACE", namespace)
	os.Setenv("PULSAR_SUBSCRIPTION", subscriptionName)
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
		Mounts: []string{fmt.Sprintf("%s:/pulsar/data", dataDir), fmt.Sprintf("%s:/pulsar/conf", confDir)},
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
		cfg := &pulsaradmin.Config{
			WebServiceURL: pulsarAdminEndPoint,
		}
		pulsarAdmin, err = pulsaradmin.NewClient(cfg)
		if err != nil {
			log.Fatalf("failed to create pulsar admin client: %v", err)
		}

		return nil
	}); err != nil {
		if resource != nil {
			_ = pool.Purge(resource)
		}
		log.Fatalf("could not connect to apache pulsar %s", err)
	}
	// waiting for pulsar admin to be ready
	time.Sleep(10 * time.Second)
	// Create a new namespace
	err = pulsarAdmin.Namespaces().CreateNamespace(fmt.Sprintf("%s/%s", tenant, namespace))
	if err != nil {
		log.Fatalf("failed to create pulsar namespace: %v", err)

	}
	err = createTopic(tenant, namespace, topic, numOfPartitions)
	if err != nil {
		log.Fatalf("failed to create pulsar topic %s: %v", topic, err)
	}

	// creating topic for checking pending messages
	err = createTopic(tenant, namespace, pendingTopic, 0)
	if err != nil {
		log.Fatalf("failed to create pulsar topic %s: %v", pendingTopic, err)
	}

	setEnv()
	code := m.Run()
	defer pulsarClient.Close()
	if resource != nil {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Couln't purge resource %s", err)
		}
	}
	// removing persistent docker volumes
	removeDockerVolume(dataDir)
	removeDockerVolume(confDir)
	os.Exit(code)
}

func TestPulsarSource_Read(t *testing.T) {
	os.Setenv("PULSAR_TOPIC", topic)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := initProducer(pulsarClient, fmt.Sprintf("%s/%s/%s", tenant, namespace, topic))
	assert.Nil(t, err)
	defer producer.Close()
	go sendMessage(producer, ctx)
	messageCh := make(chan sourcer.Message, 20)
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            fmt.Sprintf("%s/%s/%s", tenant, namespace, topic),
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pulsarSource := NewPulsarSource(pulsarClient, pulsarAdmin, consumer)
	pulsarSource.Read(ctx, payloads.ReadRequest{
		CountValue: 5,
		Timeout:    20 * time.Second,
	}, messageCh)
	assert.Equal(t, 5, len(messageCh))
	// Try reading 4 more messages
	// Since the previous batch didn't get acked, the data source shouldn't allow us to read more messages
	// We should get 0 messages, meaning the channel only holds the previous 5 messages
	pulsarSource.Read(ctx, payloads.ReadRequest{
		CountValue: 4,
		Timeout:    time.Second,
	}, messageCh)
	assert.Equal(t, 5, len(messageCh))

	msg1 := <-messageCh
	msg2 := <-messageCh
	msg3 := <-messageCh
	msg4 := <-messageCh
	msg5 := <-messageCh
	pulsarSource.Ack(ctx, payloads.TestAckRequest{
		OffsetsValue: []sourcer.Offset{msg1.Offset(), msg2.Offset(), msg3.Offset(), msg4.Offset(), msg5.Offset()},
	})

	// Read 6 more messages
	pulsarSource.Read(ctx, payloads.ReadRequest{
		CountValue: 6,
		Timeout:    time.Second,
	}, messageCh)
	assert.Equal(t, 6, len(messageCh))
	msg6 := <-messageCh
	msg7 := <-messageCh
	msg8 := <-messageCh
	msg9 := <-messageCh
	msg10 := <-messageCh
	msg11 := <-messageCh
	pulsarSource.Ack(ctx, payloads.TestAckRequest{
		OffsetsValue: []sourcer.Offset{msg6.Offset(), msg7.Offset(), msg8.Offset(), msg9.Offset(), msg10.Offset(), msg11.Offset()},
	})
}

func TestPulsarSource_Partitions(t *testing.T) {
	os.Setenv("PULSAR_TOPIC", topic)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            fmt.Sprintf("%s/%s/%s", tenant, namespace, topic),
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pulsarSource := NewPulsarSource(pulsarClient, pulsarAdmin, consumer)
	partitionIndices := pulsarSource.Partitions(ctx)
	assert.Equal(t, numOfPartitions, len(partitionIndices))
	for i, v := range partitionIndices {
		assert.Equal(t, i, int(v))
	}
}

func TestPulsarSource_Pending(t *testing.T) {
	os.Setenv("PULSAR_TOPIC", pendingTopic)
	messagesCount := 20
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := initProducer(pulsarClient, fmt.Sprintf("%s/%s/%s", tenant, namespace, pendingTopic))
	assert.Nil(t, err)
	defer producer.Close()
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            fmt.Sprintf("%s/%s/%s", tenant, namespace, pendingTopic),
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	sendCountedMessage(producer, ctx, messagesCount)
	pulsarSource := NewPulsarSource(pulsarClient, pulsarAdmin, consumer)
	pending := pulsarSource.Pending(ctx)
	assert.Equal(t, int64(messagesCount), pending)
}
