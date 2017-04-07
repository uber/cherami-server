// Copyright (c) 2016 Uber Technologies, Inc.
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

package configure

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestLoadKafkaConfig(t *testing.T) {
	KafkaConfig := NewCommonKafkaConfig()
	KafkaConfig.KafkaClusterConfigFile = "../../config/local_kafka_clusters.yaml"
	clusters := KafkaConfig.GetKafkaClusters()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, "local", clusters[0])

	clusterConfig, ok := KafkaConfig.GetKafkaClusterConfig("local")
	assert.True(t, ok)
	assert.Equal(t, 1, len(clusterConfig.Brokers))
	assert.Equal(t, "localhost", clusterConfig.Brokers[0])
	assert.Equal(t, 1, len(clusterConfig.Zookeepers))
	assert.Equal(t, "localhost", clusterConfig.Zookeepers[0])
	assert.Equal(t, 0, len(clusterConfig.Chroot))

	clusterConfig2, ok2 := KafkaConfig.GetKafkaClusterConfig("not_existing_cluster")
	assert.False(t, ok2)
	assert.Equal(t, 0, len(clusterConfig2.Brokers))
	assert.Equal(t, 0, len(clusterConfig2.Zookeepers))
	assert.Equal(t, 0, len(clusterConfig2.Chroot))
}