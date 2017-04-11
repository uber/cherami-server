package dconfig

import (
	"github.com/uber/cherami-server/common"
)

// ConfigUpdater is a daemon that can be used to update the config based on information from some external source
type (
	ConfigUpdater interface {
		common.Daemon
	}

	dummyConfigUpdater struct {
	}
)

// NewDummyConfigUpdater creates a dummy config updater
func NewDummyConfigUpdater() ConfigUpdater {
	return &dummyConfigUpdater{}
}

func (d *dummyConfigUpdater) Start() {
	return
}

func (d *dummyConfigUpdater) Stop() {
	return
}
