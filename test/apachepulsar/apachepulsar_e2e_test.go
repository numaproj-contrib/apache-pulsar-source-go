//go:build test

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

package sqs

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/numaproj-contrib/numaflow-utils-go/testing/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SqsSourceSuite struct {
	fixtures.E2ESuite
}

const (
	queueName = "testing"
)

func setupQueue(client *sqs.Client, queueName string, ctx context.Context) (*string, error) {
	params := &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	}
	response, err := client.CreateQueue(ctx, params)
	if err != nil {
		fmt.Println("Error creating queue:", err)
		return nil, err
	}
	return response.QueueUrl, nil
}

func initClient(ctx context.Context) (*sqs.Client, error) {
	// Load default configs for aws based on env variable provided based on
	// https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/#specifying-credentials
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed loading aws config, err: %v", err)
	}
	client := sqs.NewFromConfig(cfg, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(os.Getenv("AWS_ENDPOINT_URL"))
	})
	return client, nil
}

func sendMessages(client *sqs.Client, queueURL *string, testMessage string, ctx context.Context) error {
	// Check if queueURL is not nil and not an empty string
	if queueURL == nil || *queueURL == "" {
		return fmt.Errorf("invalid queue URL: %v", queueURL)
	}
	sendParams := &sqs.SendMessageInput{
		QueueUrl:    queueURL, // Ensure QueueUrl is set correctly here
		MessageBody: aws.String(testMessage),
	}
	_, err := client.SendMessage(ctx, sendParams)
	if err != nil {
		return err
	}
	return nil
}
func (suite *SqsSourceSuite) SetupTest() {

	suite.T().Log("e2e Api resources are ready")
	suite.StartPortForward("e2e-api-pod", 8378)

	// Create Redis Resource
	redisDeleteCmd := fmt.Sprintf("kubectl delete -k ../../config/apps/redis -n %s --ignore-not-found=true", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", redisDeleteCmd}, fixtures.OutputRegexp(""))
	redisCreateCmd := fmt.Sprintf("kubectl apply -k ../../config/apps/redis -n %s", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", redisCreateCmd}, fixtures.OutputRegexp("service/redis created"))

	suite.T().Log("Redis resources are ready")

	// Create Moto resources used for mocking AWS APIs.
	motoDeleteCmd := fmt.Sprintf("kubectl delete -k ../../config/apps/moto -n %s --ignore-not-found=true", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", motoDeleteCmd}, fixtures.OutputRegexp(""))
	motoCreateCmd := fmt.Sprintf("kubectl apply -k ../../config/apps/moto -n %s", fixtures.Namespace)
	suite.Given().When().Exec("sh", []string{"-c", motoCreateCmd}, fixtures.OutputRegexp("service/moto created"))
	motoLabelSelector := fmt.Sprintf("app=%s", "moto")
	suite.Given().When().WaitForStatefulSetReady(motoLabelSelector)
	suite.Given().When().WaitForPodReady("moto-0", motoLabelSelector)
	suite.T().Log("Moto resources are ready")
	suite.T().Log("port forwarding moto service")
	suite.StartPortForward("moto-0", 5000)
}

func (suite *SqsSourceSuite) TestSqsSource() {
	var testMessage = "aws_Sqs"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := initClient(ctx)
	assert.Nil(suite.T(), err)
	// Create SQS client
	queueURL, err := setupQueue(client, queueName, ctx)
	assert.Nil(suite.T(), err)

	stopChan := make(chan struct{})
	workflow := suite.Given().Pipeline("@testdata/sqs_source.yaml").When().CreatePipelineAndWait()
	workflow.Expect().VertexPodsRunning()

	go func() {
		for {
			sendErr := sendMessages(client, queueURL, testMessage, ctx)
			if sendErr != nil {
				log.Fatalf("Error in Sending Message: %s", sendErr)
			}
			select {
			case <-stopChan:
				log.Println("Stopped sending messages to queue.")
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

func TestSqsSourceSuite(t *testing.T) {
	suite.Run(t, new(SqsSourceSuite))
}
