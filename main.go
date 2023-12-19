package main

import (
	"context"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/numaproj/numaflow-go/pkg/sourcer"

	"github.com/shubhamdixit863/apache-pulsar-source-go/pkg/apachepulsar"
)

func main() {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "topic-1",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pulsarSource := apachepulsar.NewPulsarSource(client, consumer)
	err = sourcer.NewServer(pulsarSource).Start(context.Background())
	if err != nil {
		log.Panic("failed to start source server : ", err)
	}

}
