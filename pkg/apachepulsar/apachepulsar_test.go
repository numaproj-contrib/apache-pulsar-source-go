package apachepulsar

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/numaproj/numaflow-go/pkg/sourcer"
	"github.com/stretchr/testify/assert"

	"github.com/shubhamdixit863/apache-pulsar-source-go/pkg/mocks"
)

func SendMessage(client pulsar.Client) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("hello"),
	})
	defer producer.Close()
	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")
}

func initClient() pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	return client
}

func TestPulsarSource_Read(t *testing.T) {
	client := initClient()
	defer client.Close()
	SendMessage(client)
	messageCh := make(chan sourcer.Message, 20)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "topic-1",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	pulsarSource := NewPulsarSource(client, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pulsarSource.Read(ctx, mocks.ReadRequest{
		CountValue: 5,
		Timeout:    20 * time.Second,
	}, messageCh)
	assert.Equal(t, 5, len(messageCh))
}
