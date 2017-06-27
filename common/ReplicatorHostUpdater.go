package common

// ReplicatorHostUpdater is a daemon that can be used to update the replicator host.
type (
	ReplicatorHostUpdater interface {
		Daemon
	}

	dummyReplicatorHostUpdater struct {
	}
)

// NewDummyReplicatorHostUpdater creates a dummy replicator host updater
func NewDummyReplicatorHostUpdater() ReplicatorHostUpdater {
	return &dummyZoneFailoverManager{}
}

func (d *dummyReplicatorHostUpdater) Start() {
	return
}

func (d *dummyReplicatorHostUpdater) Stop() {
	return
}
