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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/numaproj-contrib/numaflow-utils-go/testing/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	host             = "pulsar://localhost:6650"
	topic            = "test-topic"
	subscriptionName = "test-subscription"
)

type ApachePulsarSuite struct {
	fixtures.E2ESuite
}

func sendMessage(client pulsar.Client, ctx context.Context) error {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: []byte("hello"),
			})
			defer producer.Close()
			if err != nil {
				fmt.Println("Failed to publish message", err)
			}
			fmt.Println("Published message")
		}
	}
}

func (suite *ApachePulsarSuite) SetupTest() {

	suite.T().Log("e2e Api resources are ready")
	suite.StartPortForward("e2e-api-pod", 8378)

	// Create Redis Resource
	redisDeleteCmd := fmt.Sprintf("kubectl delete -k ../../config/apps/redis -n %s --ignore-not-found=true", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", redisDeleteCmd}, fixtures.OutputRegexp(""))
	redisCreateCmd := fmt.Sprintf("kubectl apply -k ../../config/apps/redis -n %s", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", redisCreateCmd}, fixtures.OutputRegexp("service/redis created"))

	suite.T().Log("Redis resources are ready")

	// Create Pulsar resources used for mocking AWS APIs.
	apachePulsarDeleteCmd := fmt.Sprintf("kubectl delete -k ../../config/apps/apachepulsar -n %s --ignore-not-found=true", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", apachePulsarDeleteCmd}, fixtures.OutputRegexp(""))
	apachePulsarCreateCmd := fmt.Sprintf("kubectl apply -k ../../config/apps/apachepulsar -n %s", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", apachePulsarCreateCmd}, fixtures.OutputRegexp("service/pulsar-broker created"))
	pulsarLabelSelector := fmt.Sprintf("app=%s", "pulsar-broker")
	//suite.Given().When().WaitForStatefulSetReady(pulsarLabelSelector)
	suite.Given().When().WaitForPodReady("pulsar-broker-0", pulsarLabelSelector)
	suite.T().Log("apache pulsar resources are ready")
	suite.T().Log("port forwarding apache pulsar service")
	suite.StartPortForward("pulsar-broker-0", 6650)
}

func initClient() (pulsar.Client, error) {
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               host,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return pulsarClient, nil
}
func (suite *ApachePulsarSuite) TestApachePulsarSource() {
	var testMessage = "testing"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := initClient()
	assert.Nil(suite.T(), err)
	stopChan := make(chan struct{})
	workflow := suite.Given().Pipeline("@testdata/apachepulsar_source.yaml").When().CreatePipelineAndWait()
	workflow.Expect().VertexPodsRunning()

	go func() {
		for {
			sendErr := sendMessage(client, ctx)
			if sendErr != nil {
				log.Fatalf("Error in Sending Message: %s", sendErr)
			}
			select {
			case <-stopChan:
				log.Println("Stopped sending messages to pulsar subscriber.")
				return
			default:
				// Continue sending messages at a specific interval, if needed
				time.Sleep(1 * time.Second)
			}
		}
	}()
	assert.Nil(suite.T(), err)
	defer workflow.DeletePipelineAndWait()
	workflow.Expect().SinkContains("redis-sink", testMessage, fixtures.WithTimeout(2*time.Minute))
	stopChan <- struct{}{}
}

func TestApachePulsarSourceSuite(t *testing.T) {
	suite.Run(t, new(ApachePulsarSuite))
}
