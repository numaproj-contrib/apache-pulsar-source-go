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
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	sourcesdk "github.com/numaproj/numaflow-go/pkg/sourcer"
	pulsaradmin "github.com/streamnative/pulsar-admin-go"
	"github.com/streamnative/pulsar-admin-go/pkg/utils"
)

type PulsarSource struct {
	client      pulsar.Client
	adminClient pulsaradmin.Client
	consumer    pulsar.Consumer
	toAckSet    map[string]pulsar.Message
	lock        *sync.Mutex
}

func NewPulsarSource(client pulsar.Client, adminClient pulsaradmin.Client, consumer pulsar.Consumer) *PulsarSource {
	return &PulsarSource{client: client, adminClient: adminClient, consumer: consumer, toAckSet: map[string]pulsar.Message{}, lock: new(sync.Mutex)}
}

func (ps *PulsarSource) Read(_ context.Context, readRequest sourcesdk.ReadRequest, messageCh chan<- sourcesdk.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), readRequest.TimeOut())
	defer cancel()
	// If we have un-acked data, we return without reading any new data.
	if len(ps.toAckSet) > 0 {
		return
	}
	for i := 0; i < int(readRequest.Count()); i++ {
		select {
		case <-ctx.Done():
			return
		default:
			ps.lock.Lock()
			msg, err := ps.consumer.Receive(ctx)
			if msg == nil {
				log.Println("received empty message")
				ps.lock.Unlock()
				continue
			}
			if err != nil {
				log.Printf("error receiving message %s", err)
				ps.lock.Unlock()
				continue
			}
			messageCh <- sourcesdk.NewMessage(
				msg.Payload(),
				sourcesdk.NewOffset([]byte(msg.ID().String()), 0),
				msg.PublishTime(),
			)
			ps.toAckSet[msg.ID().String()] = msg
			ps.lock.Unlock()
		}
	}
}

func (ps *PulsarSource) Pending(_ context.Context) int64 {
	topic, _ := utils.GetTopicName(fmt.Sprintf("%s/%s/%s", os.Getenv("PULSAR_TENANT"), os.Getenv("PULSAR_NAMESPACE"), os.Getenv("PULSAR_TOPIC")))
	stats, err := ps.adminClient.Topics().GetStats(*topic)
	if err != nil {
		log.Printf("error getting pending count %s", err)
		return 0
	}
	pendingMessages := stats.Subscriptions[os.Getenv("PULSAR_SUBSCRIPTION")].UnAckedMessages
	return pendingMessages
}

func (ps *PulsarSource) Ack(ctx context.Context, request sourcesdk.AckRequest) {
	for _, offset := range request.Offsets() {
		select {
		case <-ctx.Done():
			return
		default:
			func() {
				ps.lock.Lock()
				defer ps.lock.Unlock()
				message := ps.toAckSet[string(offset.Value())]
				err := ps.consumer.Ack(message)
				if err != nil {
					log.Printf("error in acknowledging message %s", err)
					return
				}
				delete(ps.toAckSet, string(offset.Value()))
			}()
		}
	}
}

func (ps *PulsarSource) Partitions(ctx context.Context) []int32 {
	topic, err := utils.GetTopicName(fmt.Sprintf("%s/%s/%s", os.Getenv("PULSAR_TENANT"), os.Getenv("PULSAR_NAMESPACE"), os.Getenv("PULSAR_TOPIC")))
	if err != nil {
		log.Printf("error getting partitions from admin endpoint %s", err)
		return sourcesdk.DefaultPartitions()
	}
	stats, err := ps.adminClient.Topics().GetMetadata(*topic)
	if err != nil {
		log.Printf("error getting partitions from admin endpoint %s", err)
		return sourcesdk.DefaultPartitions()
	}
	var partitions []int32
	for i := 0; i < stats.Partitions; i++ {
		partitions = append(partitions, int32(i))
	}
	return partitions
}
