// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integration

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/suite"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	cheramiclient "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

// Note: you need to start ZooKeeper/Kafka on your local machine to run following tests.
// If running on Mac and java 1.7 for ZooKeeper/Kafka, run following command before starting Kafka:
// echo "127.0.0.1 $HOSTNAME" | sudo tee -a /etc/hosts

type KfCIntegrationSuite struct {
	testBase

	cheramiConsumer   cheramiclient.Consumer
	cheramiConsumerCh cheramiclient.Delivery
	kafkaProducer     sarama.SyncProducer
	kafkaTopics       []string
}

func TestKfCIntegrationSuite(t *testing.T) {
	s := new(KfCIntegrationSuite)
	s.testBase.SetupSuite(t)
	suite.Run(t, s)
}

func (s *KfCIntegrationSuite) setupTest() {

	destPath := "/kafka_test_dest/kfc"
	cgPath := "/kafka_test_cg/kfc"
	s.kafkaTopics = []string{"kfc0", "kfc1"}

	// cherami client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-kfc-integration", ipaddr, portNum, nil)

	// create cherami kfc destination
	destDesc, err := cheramiClient.CreateDestination(&cherami.CreateDestinationRequest{
		Path: common.StringPtr(destPath),
		Type: cherami.DestinationTypePtr(cherami.DestinationType_KAFKA),
		ConsumedMessagesRetention:   common.Int32Ptr(60),
		UnconsumedMessagesRetention: common.Int32Ptr(120),
		ChecksumOption:              cherami.ChecksumOption_CRC32IEEE,
		OwnerEmail:                  common.StringPtr("cherami-test-kfc-integration@uber.com"),
		IsMultiZone:                 common.BoolPtr(false),
		KafkaCluster:                common.StringPtr("local"),
		KafkaTopics:                 kafkaTopics,
	})
	s.NotNil(destDesc)
	s.NoError(err)

	// create cherami kfc consumer group
	cgDesc, err := cheramiClient.CreateConsumerGroup(&cherami.CreateConsumerGroupRequest{
		ConsumerGroupName:    common.StringPtr(cgPath),
		DestinationPath:      common.StringPtr(destPath),
		LockTimeoutInSeconds: common.Int32Ptr(30),
		MaxDeliveryCount:     common.Int32Ptr(1),
		OwnerEmail:           common.StringPtr("cherami-test-kfc-integration@uber.com"),
	})

	s.NoError(err)
	s.NotNil(cgDesc)

	s.cheramiConsumer = cheramiClient.CreateConsumer(&cheramiclient.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "KfCIntegration",
		PrefetchCount:     1,
		Options:           &cheramiclient.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	})
	s.NotNil(s.cheramiConsumer)

	// setup kafka producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	brokers := []string{"localhost:9092"}

	s.kafkaProducer, err = sarama.NewSyncProducer(brokers, config)
	s.NoError(err)

	s.cheramiMsgsCh, err = s.cheramiConsumer.Open(make(chan cheramiclient.Delivery, 1))
	s.NoError(err)

	var wgConsumer sync.WaitGroup

	log := common.GetDefaultLogger()

	wgConsumer.Add(1)
	go func() {
		defer wgConsumer.Done()

	ReadLoop:
		for {
			timeout := time.NewTimer(time.Second * 45)

			select {
			case msg := <-msgsCh:
				log.Infof("consumer: recv msg id: %v [addr=%x]", msg.GetMessage().Payload.GetID(), msg.GetMessage().GetAddress())
				msg.Ack()

			case <-timeout.C:
				log.Errorf("consumer: timed-out")
				break ReadLoop
			}
		}
	}()
}

func (s *KfCIntegrationSuite) cleanupTest() {

	err := s.kafkaProducer.Close()
	s.NoError(err)

	s.cheramiConsumer.Close()
}

func (s *KfCIntegrationSuite) consumeFromCherami() {
}

func (s *KfCIntegrationSuite) publishToKafka(topic, msg string) (partition int32, offset int64, err error) {

	kmsg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msg)}
	partition, offset, err = s.kafkaProducer.SendMessage(kmsg)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	} else {
		log.Printf("Sent message to partition %d at offset %d: %s\n", partition, offset, kmsg)
	}
	s.Nil(err)

	return
}
