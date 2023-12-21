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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	sourcesdk "github.com/numaproj/numaflow-go/pkg/sourcer"
)

type PulsarSource struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	toAckSet map[string]pulsar.Message
	lock     *sync.Mutex
}

func NewPulsarSource(client pulsar.Client, consumer pulsar.Consumer) *PulsarSource {
	return &PulsarSource{client: client, consumer: consumer, toAckSet: map[string]pulsar.Message{}, lock: new(sync.Mutex)}
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
			if err != nil {
				log.Printf("error receiving message %s", err)
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
	return int64(len(ps.toAckSet))
}

func (ps *PulsarSource) Ack(ctx context.Context, request sourcesdk.AckRequest) {
	for _, offset := range request.Offsets() {
		select {
		case <-ctx.Done():
			return
		default:
			ps.lock.Lock()
			message := ps.toAckSet[string(offset.Value())]
			err := ps.consumer.Ack(message)
			if err != nil {
				log.Printf("error in acknowledging message %s", err)
				ps.lock.Unlock()
				// continue here as we won't delete message from ackSet
				continue
			}
			delete(ps.toAckSet, string(offset.Value()))
			ps.lock.Unlock()
		}
	}
}

func (ps *PulsarSource) Partitions(ctx context.Context) []int32 {
	restApiEndpoint := fmt.Sprintf("%s/admin/v2/persistent/public/default/%s/partitions", os.Getenv("PULSAR_ADMIN_ENDPOINT"), os.Getenv("PULSAR_TOPIC"))
	resp, err := http.Get(restApiEndpoint)
	if err != nil {
		log.Printf("error getting partitions from admin endpoint %s", err)
		return sourcesdk.DefaultPartitions()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error parsing data  from admin endpoint %s", err)
		return sourcesdk.DefaultPartitions()
	}
	var partitions map[string]interface{}
	if err := json.Unmarshal(body, &partitions); err != nil {
		log.Printf("error unmarshalling data  from admin endpoint %s", err)
		return sourcesdk.DefaultPartitions()
	}
	partition := partitions["partitions"].(float64)
	return []int32{int32(partition)}
}
