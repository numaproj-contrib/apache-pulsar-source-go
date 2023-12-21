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

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/numaproj/numaflow-go/pkg/sourcer"

	"github.com/shubhamdixit863/apache-pulsar-source-go/pkg/apachepulsar"
)

func main() {
	topic := os.Getenv("PULSAR_TOPIC")
	subscriptionName := os.Getenv("PULSAR_SUBSCRIPTION_NAME")
	host := os.Getenv("PULSAR_HOST")

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               fmt.Sprintf("pulsar://%s", host),
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatalf("could not create topic subscription: %v", err)
	}
	defer consumer.Close()
	pulsarSource := apachepulsar.NewPulsarSource(client, consumer)
	err = sourcer.NewServer(pulsarSource).Start(context.Background())
	if err != nil {
		log.Panic("failed to start source server : ", err)
	}
}
