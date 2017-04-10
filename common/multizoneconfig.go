package common

import (
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	// MultiZoneDynamicConfig contains the configs for multi_zone consumer groups
	MultiZoneDynamicConfig struct {
		ActiveZone   string `name:"activeZone"`
		FailoverMode string `name:"failoverMode" default:"enabled"`
	}
)

// ShouldConsumeInZone indicated whether we should consume from this zone for a multi_zone consumer group
func ShouldConsumeInZone(zone string, cgDesc *shared.ConsumerGroupDescription, dConfig MultiZoneDynamicConfig) bool {
	return true
}
