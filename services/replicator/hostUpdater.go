package replicator

import (
	"github.com/uber/cherami-server/common"
)

// HostUpdater is a daemon that can be used to update the replicator host.
type (
	HostUpdater interface {
		common.Daemon
	}

	dummyHostUpdater struct {
	}
)

// NewDummyHostUpdater creates a dummy replicator host updater
func NewDummyHostUpdater() HostUpdater {
	return &dummyHostUpdater{}
}

func (d *dummyHostUpdater) Start() {
	return
}

func (d *dummyHostUpdater) Stop() {
	return
}
