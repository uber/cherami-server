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
	"io/ioutil"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// KafkaConfig holds the configuration for the Kafka client
type KafkaConfig struct {
	kafkaClusterConfigFile string `yaml:kafkaClusterConfigFile`
	clustersConfig clustersConfig
}

type clustersConfig struct {
	clusters []cluster `yaml:clusters`
}

type cluster struct {
	name       string
	brokers    []string `yaml:brokers`
	zookeepers []string `yaml:zookeepers`
	chroot     string   `yaml:chroot`
}

// NewCommonKafkaConfig instantiates a Kafka config
func NewCommonKafkaConfig() *KafkaConfig {
	config := &KafkaConfig{}
	config.loadClusterConfigFile()
	return config
}

// GetKafkaClusters returns all kafka cluster names
func (r *KafkaConfig) GetKafkaClusters() []string {
	var ret = []string{}
	for _, cluster := range r.clustersConfig.clusters {
		ret = append(ret, cluster.name)
	}
	return ret
}

func (r *KafkaConfig) loadClusterConfigFile() {
	// TODO do we need to detect file change and reload on file change
	if len(r.kafkaClusterConfigFile) == 0 {
		log.Warnf("Could not load kafka configu because kafka cluster config file is not configured")
		return
	}

	contents, err := ioutil.ReadFile(r.kafkaClusterConfigFile)
	if err != nil {
		log.Warnf("Failed to load kafka cluster config file %s: %v", r.kafkaClusterConfigFile, err)
	}

	if err := yaml.Unmarshal(contents, &r.clustersConfig); err != nil {
		log.Warnf("Failed to parse kafka cluster config file %s: %v", r.kafkaClusterConfigFile, err)
	}
}