package replicator

import (
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/tchannel-go/thrift"
)

// DumpConnectionStatus implements the admin API
func (r *Replicator) DumpConnectionStatus(ctx thrift.Context) (*admin.ReplicatorConnectionStatus, error) {
	status := admin.NewReplicatorConnectionStatus()

	r.remoteReplicatorConnMutex.Lock()
	defer r.remoteReplicatorConnMutex.Unlock()
	for extent, conn := range r.remoteReplicatorConn {
		status.RemoteReplicatorConn = append(status.RemoteReplicatorConn, &admin.ReplicatorConnection{
			ExtentUUID: common.StringPtr(extent),
			StartTime:  common.Int64Ptr(conn.startTime),
		})
	}

	r.storehostConnMutex.Lock()
	defer r.storehostConnMutex.Unlock()
	for extent, conn := range r.storehostConn{
		status.StorehostConn = append(status.StorehostConn, &admin.ReplicatorConnection{
			ExtentUUID: common.StringPtr(extent),
			StartTime:  common.Int64Ptr(conn.startTime),
		})
	}

	return status, nil
}
