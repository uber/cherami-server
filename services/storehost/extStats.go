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

package storehost

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	reportChanBufLen = 1024
	reportInterval   = time.Duration(time.Minute)
)

// ReportInterval determines how often we report; it is expoerted to allow integration test to modify this value
var ReportInterval = reportInterval

type (
	ExtStatsReporter struct {
		hostID  string
		xMgr    *ExtentManager
		mClient metadata.TChanMetadataService
		logger  bark.Logger

		sync.RWMutex
		extents    map[string]*extStatsContext
		reportPool sync.Pool
		reportC    chan *report

		wg    sync.WaitGroup
		stopC chan struct{} // internal channel to stop
	}

	extStatsContext struct {
		ext        *extentContext
		ref        int64
		lastReport *report
	}

	report struct {
		ctx       *extStatsContext
		extentID  uuid.UUID
		timestamp int64

		firstSeqNum, lastSeqNum int64
		sealed                  bool
	}
)

func NewExtStatsReporter(hostID string, xMgr *ExtentManager, mClient metadata.TChanMetadataService, logger bark.Logger) *ExtStatsReporter {

	return &ExtStatsReporter{
		hostID:  hostID,
		xMgr:    xMgr,
		mClient: mClient,
		logger:  logger,

		extents:    make(map[string]*extStatsContext),
		reportPool: sync.Pool{New: func() interface{} { return &report{} }},
		reportC:    make(chan *report, reportChanBufLen),
		stopC:      make(chan struct{}),
	}
}

func (t *ExtStatsReporter) Start() {

	t.logger.Info("extStatsReporter: started")

	t.wg.Add(1)
	go t.reporterPump()

	t.wg.Add(1)
	go t.schedulerPump()
}

func (t *ExtStatsReporter) Stop() {

	close(t.stopC)
	t.wg.Wait()

	t.logger.Info("extStatsReporter: stopped")
}

func (t *ExtStatsReporter) trySendReport(extentID uuid.UUID, ext *extentContext, ctx *extStatsContext) bool {

	// if paused, do nothing
	if atomic.LoadInt32(&extStatsReporterPause) == 1 {
		return false
	}

	r := t.reportPool.Get().(*report)

	r.ctx = ctx
	r.extentID = extentID
	r.timestamp = time.Now().UnixNano()

	// get a snapshot of various extent stats
	r.firstSeqNum, r.lastSeqNum = ext.getBeginSeqNum(), ext.getLastSeqNum()
	r.sealed, _ = ext.isSealed()

	// create and send report non-blockingly
	select {
	case t.reportC <- r:
		return true // sent

	default:
		// drop the report, if we are running behind
		t.reportPool.Put(ctx.lastReport) // return old 'lastReport' to pool
		ctx.lastReport = r               // update lastReport to this one

		return false // not sent
	}
}

func (t *ExtStatsReporter) schedulerPump() {

	defer t.wg.Done()

	ticker := time.NewTicker(ReportInterval)

pump:
	for {
		select {
		case <-ticker.C:

			// list of extents that can be deleted
			deleteExtents := make([]string, 8)

			// pause between extents to spread out the calls
			pause := time.Duration(ReportInterval.Nanoseconds() / int64(1+len(t.extents)))

			// get lock shared, while iterating through map
			t.RLock()

			// iterate through the extents and prepare/send report
			for id, ctx := range t.extents {

				t.RUnlock()

				if atomic.LoadInt64(&ctx.ref) > 0 {

					ext := ctx.ext

					// trySendReport will be accessing 'ext' members,
					// some of which require holding 'ext' shared.
					ext.RLock()
					t.trySendReport(uuid.UUID(id), ext, ctx)
					ext.RUnlock()

				} else {

					// collect extents to delete; and delete them in
					// one go with the lock held exclusive.
					deleteExtents = append(deleteExtents, id)
				}

				time.Sleep(pause) // take a short nap

				t.RLock()
			}

			t.RUnlock()

			// get lock exclusive, and delete extents from the map whose ref has dropped to zero
			t.Lock()
			for _, id := range deleteExtents {
				if ctx, ok := t.extents[string(id)]; ok && atomic.LoadInt64(&ctx.ref) == 0 {
					delete(t.extents, id)
				}
			}
			t.Unlock()

		case <-t.stopC:
			break pump
		}
	}
}

func (t *ExtStatsReporter) extentOpened(id uuid.UUID, ext *extentContext, intent OpenIntent) {

	if intent != OpenIntentAppendStream &&
		intent != OpenIntentSealExtent &&
		intent != OpenIntentPurgeMessages &&
		intent != OpenIntentReplicateExtent {

		return // ignore, if not any of the interesting 'intents'
	}

	// get exclusive lock, since we could be adding to the map
	t.Lock()
	defer t.Unlock()

	if ctx, ok := t.extents[string(id)]; ok && ctx.ext == ext {

		atomic.AddInt64(&ctx.ref, 1) // add ref

	} else {

		// create a new context
		t.extents[string(id)] = &extStatsContext{ext: ext, ref: 1}
	}

	return
}

func (t *ExtStatsReporter) extentClosed(id uuid.UUID, ext *extentContext, intent OpenIntent) (done bool) {

	if intent != OpenIntentAppendStream &&
		intent != OpenIntentSealExtent &&
		intent != OpenIntentPurgeMessages &&
		intent != OpenIntentReplicateExtent {

		return true // ignore, if not any of the interesting 'intents'
	}

	// get lock shared, when reading through the map
	t.RLock()
	defer t.RUnlock()

	if ctx, ok := t.extents[string(id)]; ok {

		// remove ref, if it drops to zero, then send out one last report
		if atomic.AddInt64(&ctx.ref, -1) == 0 {
			t.trySendReport(id, ext, ctx)
		}

	} else {

		// assert(ok) //
	}

	return true // go ahead with the cleanup
}

func (t *ExtStatsReporter) reporterPump() {

	defer t.wg.Done()

pump:
	for {
		select {
		case report := <-t.reportC:

			var extentID, lastReport = report.extentID, report.ctx.lastReport

			var lastSeqRate = common.CalculateRate(
				common.SequenceNumber(lastReport.lastSeqNum),
				common.SequenceNumber(report.lastSeqNum),
				common.UnixNanoTime(lastReport.timestamp),
				common.UnixNanoTime(report.timestamp),
			)

			var extReplStatus = shared.ExtentReplicaStatus_OPEN
			if report.sealed {
				extReplStatus = shared.ExtentReplicaStatus_SEALED
			}

			extReplStats := &shared.ExtentReplicaStats{
				StoreUUID:  common.StringPtr(t.hostID),
				ExtentUUID: common.StringPtr(extentID.String()),
				// BeginAddress:  common.Int64Ptr(report.firstAddress),
				// LastAddress: common.Int64Ptr(report.lastAddress),
				BeginSequence: common.Int64Ptr(report.firstSeqNum),
				LastSequence:  common.Int64Ptr(report.lastSeqNum),
				// BeginEnqueueTimeUtc: common.Int64Ptr(report.firstTimestamp),
				// LastEnqueueTimeUtc: common.Int64Ptr(report.lastTimestamp),
				// SizeInBytes: common.Int64Ptr(report.size),
				Status:                shared.ExtentReplicaStatusPtr(extReplStatus),
				AvailableSequenceRate: common.Float64Ptr(lastSeqRate),
				LastSequenceRate:      common.Float64Ptr(lastSeqRate),
				// AvailableAddress
				// AvailableSequence
				// LastSequenceRate
				// SizeInBytesRate
				// BeginTime
				// EndTime
				// WriteTime
			}

			req := &metadata.UpdateStoreExtentReplicaStatsRequest{
				ExtentUUID:   common.StringPtr(extentID.String()),
				ReplicaStats: []*shared.ExtentReplicaStats{extReplStats},
			}

			err := t.mClient.UpdateStoreExtentReplicaStats(nil, req)

			if err != nil {
				t.logger.WithFields(bark.Fields{
					common.TagExt:  extentID,
					common.TagStor: t.hostID,
				}).Error(`UpdateStoreExtentReplicaStats failed`)
			}

			t.logger.WithFields(bark.Fields{ // #perfdisable
				common.TagExt:        extentID,                                // #perfdisable
				`begin-seq`:          extReplStats.GetBeginSequence(),         // #perfdisable
				`last-seq`:           extReplStats.GetLastSequence(),          // #perfdisable
				`last-seq-rate`:      extReplStats.GetLastSequenceRate(),      // #perfdisable
				`available-seq`:      extReplStats.GetAvailableSequence(),     // #perfdisable
				`available-seq-rate`: extReplStats.GetAvailableSequenceRate(), // #perfdisable
			}).Info("extStatsReporter: report") // #perfdisable

			report.ctx.lastReport = report // update last-report
			t.reportPool.Put(lastReport)   // return old one to pool

		case <-t.stopC:
			break pump
		}
	}
}

var extStatsReporterPause int32

func ExtStatsReporterPause() {
	atomic.StoreInt32(&extStatsReporterPause, 1)
}

func ExtStatsReporterUnpause() {
	atomic.StoreInt32(&extStatsReporterPause, 0)
}
