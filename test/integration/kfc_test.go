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
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"net"
	"strconv"
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

	destPath, cgPath string
	cheramiConsumer  cheramiclient.Consumer
	cheramiMsgsCh    chan cheramiclient.Delivery
	kafkaProducer    sarama.SyncProducer
	kafkaTopics      []string
}

func TestKfCIntegrationSuite(t *testing.T) {
	s := new(KfCIntegrationSuite)
	s.testBase.SetupSuite(t)
	suite.Run(t, s)
}

func (s *KfCIntegrationSuite) TestKafkaForCherami() {

	s.destPath, s.cgPath = "/kafka_test_dest/kfc", "/kafka_test_cg/kfc"
	s.kafkaTopics = []string{uuid.New(), uuid.New(), uuid.New()}

	// cherami client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-kfc-integration", ipaddr, portNum, nil)

	// create cherami kfc destination
	destDesc, err := cheramiClient.CreateDestination(&cherami.CreateDestinationRequest{
		Path: common.StringPtr(s.destPath),
		Type: cherami.DestinationTypePtr(cherami.DestinationType_KAFKA),
		ConsumedMessagesRetention:   common.Int32Ptr(60),
		UnconsumedMessagesRetention: common.Int32Ptr(120),
		ChecksumOption:              cherami.ChecksumOption_CRC32IEEE,
		OwnerEmail:                  common.StringPtr("cherami-test-kfc-integration@uber.com"),
		IsMultiZone:                 common.BoolPtr(false),
		KafkaCluster:                common.StringPtr("local"),
		KafkaTopics:                 s.kafkaTopics,
	})
	s.NotNil(destDesc)
	s.NoError(err)

	// create cherami kfc consumer group
	cgDesc, err := cheramiClient.CreateConsumerGroup(&cherami.CreateConsumerGroupRequest{
		ConsumerGroupName:    common.StringPtr(s.cgPath),
		DestinationPath:      common.StringPtr(s.destPath),
		LockTimeoutInSeconds: common.Int32Ptr(30),
		MaxDeliveryCount:     common.Int32Ptr(1),
		OwnerEmail:           common.StringPtr("cherami-test-kfc-integration@uber.com"),
	})

	s.NoError(err)
	s.NotNil(cgDesc)

	s.cheramiConsumer = cheramiClient.CreateConsumer(&cheramiclient.CreateConsumerRequest{
		Path:              s.destPath,
		ConsumerGroupName: s.cgPath,
		ConsumerName:      "KfCIntegration",
		PrefetchCount:     1,
		Options:           &cheramiclient.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	})
	s.NotNil(s.cheramiConsumer)
	defer s.cheramiConsumer.Close()

	// setup kafka producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	s.kafkaProducer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	s.NoError(err)
	defer s.kafkaProducer.Close()

	s.cheramiMsgsCh, err = s.cheramiConsumer.Open(make(chan cheramiclient.Delivery, 1))
	s.NoError(err)

	const numMsgs = 10

	type kafkaMsg struct {
		topic string
		key   string
		val   []byte
		part  int32
		offs  int64
	}

	msgs := make(map[string]*kafkaMsg)

	// publish messages to kafka
	for i := 0; i < numMsgs; i++ {

		const minSize, maxSize = 512, 8192

		var topic = s.kafkaTopics[rand.Intn(len(s.kafkaTopics))]   // pick one of the topics at random
		var key = uuid.New()                                       // random key
		var val = make([]byte, minSize+rand.Intn(maxSize-minSize)) // random buf
		rand.Read(val)                                             // fill 'val' with random bytes

		part, offs, err :=
			s.kafkaProducer.SendMessage(
				&sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.StringEncoder(key),
					Value: sarama.ByteEncoder(val),
				},
			)

		s.NoError(err)

		msgs[key] = &kafkaMsg{topic: topic, key: key, val: val, part: part, offs: offs}
	}

	// consume messages from cherami
loop:
	for i := 0; i < numMsgs; i++ {

		select {
		case cmsg := <-s.cheramiMsgsCh:
			payload := cmsg.GetMessage().Payload
			uc := payload.GetUserContext()

			msg := msgs[uc["key"]]

			s.Equal(msg.topic, uc["topic"])
			s.Equal(msg.key, uc["key"])
			s.Equal(len(msg.val), len(payload.GetData()))
			s.EqualValues(msg.val, payload.GetData())
			s.Equal(strconv.Itoa(int(msg.part)), uc["partition"])
			s.Equal(strconv.Itoa(int(msg.offs)), uc["offset"])

			cmsg.Ack()

		case <-time.After(45 * time.Second):
			s.Fail("cherami-consumer: timed out")
			break loop
		}
	}

	return
}
