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

package controllerhost

import (
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	a "github.com/uber/cherami-thrift/.generated/go/admin"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const failBackoffInterval = int64(time.Millisecond * 100)

var (
	// TTL after which the cache entry is due for refresh
	// The entry won't be evicted immediately afte the TTL
	// We can keep serving stale entries for up to an hour,
	// when we cannot refresh the cache (say, due to cassandra failure)
	outputCacheTTL = 5 * time.Second
)

type cgExtentsByCategory struct {
	open        map[string]struct{}
	openHealthy map[string]struct{}
	consumed    map[string]struct{}
	openBad     []*m.ConsumerGroupExtentLite
}

func validatCGStatus(cgDesc *shared.ConsumerGroupDescription) error {
	switch cgDesc.GetStatus() {
	case shared.ConsumerGroupStatus_ENABLED:
		return nil
	case shared.ConsumerGroupStatus_DELETED:
		return ErrConsumerGroupNotExists
	default:
		return ErrConsumerGroupDisabled
	}
}

func newCGExtentsByCategory() *cgExtentsByCategory {
	return &cgExtentsByCategory{
		open:        make(map[string]struct{}),
		openHealthy: make(map[string]struct{}),
		consumed:    make(map[string]struct{}),
		openBad:     make([]*m.ConsumerGroupExtentLite, 0),
	}
}

func maxExtentsToConsumeForDst(context *Context, dstPath, cgName string, dstType dstType, zoneConfigs []*shared.DestinationZoneConfig) int {
	switch dstType {
	case dstTypeTimer:
		return maxExtentsToConsumeForDstTimer
	case dstTypeDLQ:
		return maxExtentsToConsumeForDstDLQ
	}

	logFn := func() bark.Logger {
		return context.log.WithFields(bark.Fields{
			common.TagDstPth: common.FmtDstPth(dstPath),
			common.TagCnsPth: common.FmtCnsPth(cgName),
			common.TagModule: `extentAssign`})
	}

	ruleKey := dstPath + `/` + cgName
	var remoteZones, remoteExtentTarget, consumeExtentTarget int
	if len(zoneConfigs) > 0 {
		totalZones := 0
		for _, zone := range zoneConfigs {
			if zone.GetAllowPublish() {
				totalZones++
			}
		}
		remoteZones = common.MaxInt(0, totalZones-1)
	}

	cfgIface, err := context.cfgMgr.Get(common.ControllerServiceName, `*`, `*`, `*`)
	if err != nil {
		logFn().WithFields(bark.Fields{common.TagErr: err}).Error(`Couldn't get extent target configuration`)
		return defaultMinConsumeExtents
	}

	cfg, ok := cfgIface.(ControllerDynamicConfig)
	if !ok {
		logFn().Error(`Couldn't cast cfg to ExtentAssignmentConfig`)
		return defaultMinConsumeExtents
	}

	if remoteZones > 0 {
		remoteExtentTarget = int(common.OverrideValueByPrefix(logFn, ruleKey, cfg.NumRemoteConsumerExtentsByPath, defaultRemoteExtents, `NumRemoteConsumerExtentsByPath`))
	}

	consumeExtentTarget = int(common.OverrideValueByPrefix(logFn, ruleKey, cfg.NumConsumerExtentsByPath, defaultMinConsumeExtents, `NumConsumerExtentsByPath`))
	return consumeExtentTarget + remoteExtentTarget*remoteZones
}

func hostInfoMapToSlice(hosts map[string]*common.HostInfo) ([]string, []string) {
	count := 0
	uuids := make([]string, len(hosts))
	addrs := make([]string, len(hosts))
	for k, v := range hosts {
		uuids[count] = k
		addrs[count] = v.Addr
		count++
	}
	return uuids, addrs
}

func pickOutputHostForStoreHosts(context *Context, storeUUIDs []string) (*common.HostInfo, error) {
	var storeHosts []*common.HostInfo
	for _, uuid := range storeUUIDs {
		if addr, err := context.rpm.ResolveUUID(common.StoreServiceName, uuid); err != nil {
			context.log.WithFields(bark.Fields{common.TagStor: common.FmtStor(uuid), common.TagErr: err}).Warn("Failed to resolve store uuid")
		} else {
			storeHosts = append(storeHosts, &common.HostInfo{
				Addr: addr,
				UUID: uuid,
			})
		}
	}

	return context.placement.PickOutputHost(storeHosts)
}

func canConsumeDstExtent(context *Context, ext *m.DestinationExtent, consumedCGExtents map[string]struct{}) bool {
	extID := ext.GetExtentUUID()
	if _, ok := consumedCGExtents[extID]; ok {
		return false
	}
	if !isAnyStoreHealthy(context, ext.GetStoreUUIDs()) {
		return false
	}
	return true
}

func reassignOutHost(context *Context, dstUUID string, cgUUID string, extent *m.ConsumerGroupExtentLite, m3Scope int) *common.HostInfo {
	outhost, err := pickOutputHostForStoreHosts(context, extent.GetStoreUUIDs())
	if err != nil {
		context.m3Client.IncCounter(m3Scope, metrics.ControllerErrPickOutHostCounter)
		return nil
	}
	err = context.mm.UpdateOutHost(dstUUID, cgUUID, extent.GetExtentUUID(), outhost.UUID)
	if err != nil {
		context.m3Client.IncCounter(m3Scope, metrics.ControllerErrMetadataUpdateCounter)
		context.log.WithField(common.TagErr, err).Debug("Failed to update outhost for consumer group")
		return nil
	}

	context.log.WithFields(bark.Fields{
		common.TagDst:  common.FmtDst(dstUUID),
		common.TagExt:  common.FmtExt(extent.GetExtentUUID()),
		common.TagOut:  common.FmtOut(outhost.UUID),
		common.TagCnsm: common.FmtCnsm(cgUUID),
		`oldOuthID`:    common.FmtOut(extent.GetOutputHostUUID()),
	}).Info("Reassigned output host")
	return outhost
}

// notifyOutputHostsForConsumerGroup sends a reconfigure notification to all outputhosts for a particular consumer
// group.
func notifyOutputHostsForConsumerGroup(context *Context, dstUUID, cgUUID, reason, reasonContext string, m3Scope int) (err error) {
	outputHosts := make(map[string]struct{})

	filterBy := []m.ConsumerGroupExtentStatus{m.ConsumerGroupExtentStatus_OPEN}
	openCGExtents, err := listConsumerGroupExtents(context, dstUUID, cgUUID, m3Scope, filterBy)
	if err != nil {
		return
	}

	// Deduplicate the outputhosts
	for _, ext := range openCGExtents {
		outputHosts[ext.GetOutputHostUUID()] = struct{}{}
	}

	// Send notifications to the various outputhosts
	for hostID := range outputHosts {
		event := NewOutputHostNotificationEvent(dstUUID, cgUUID, hostID, reason, reasonContext, a.NotificationType_HOST)
		if !context.eventPipeline.Add(event) {
			context.log.WithFields(bark.Fields{
				common.TagDst:  common.FmtDst(dstUUID),
				common.TagCnsm: common.FmtCnsm(cgUUID),
				common.TagOut:  common.FmtOut(hostID),
				`reason`:       reason,
				`context`:      reasonContext,
			}).Error("Dropping OutputHostNotificationEvent after repairing extent, event queue full")
		}
	}

	return
}

// repairExtentsAndUpdateOutputHosts repairs unhealthy consumer group exents by
// reassigning output hosts. This method must be called while holding the destination
// lock
func repairExtentsAndUpdateOutputHosts(
	context *Context,
	dstUUID string,
	cgUUID string,
	cgExtents *cgExtentsByCategory,
	maxToRepair int,
	outputHosts map[string]*common.HostInfo,
	m3Scope int) int {

	nRepaired := 0
	for i := 0; i < len(cgExtents.openBad); i++ {
		toRepair := cgExtents.openBad[i]
		outHost := reassignOutHost(context, dstUUID, cgUUID, toRepair, m3Scope)
		if outHost != nil {
			outputHosts[outHost.UUID] = outHost
			event := NewOutputHostNotificationEvent(dstUUID, cgUUID, outHost.UUID,
				notifyExtentRepaired, toRepair.GetExtentUUID(), a.NotificationType_HOST)
			if !context.eventPipeline.Add(event) {
				context.log.WithFields(bark.Fields{
					common.TagDst:  common.FmtDst(dstUUID),
					common.TagCnsm: common.FmtCnsm(cgUUID),
					common.TagOut:  common.FmtOut(outHost.UUID),
				}).Error("Dropping OutputHostNotificationEvent after repairing extent, event queue full")
			}
		}
		nRepaired++
		cgExtents.openHealthy[toRepair.GetExtentUUID()] = struct{}{}
		// Limit the repair to a few extents per call
		if nRepaired > maxToRepair {
			break
		}
	}
	return nRepaired
}

func addExtentsToConsumerGroup(context *Context, dstUUID string, cgUUID string, newExtents []*m.DestinationExtent, outputHosts map[string]*common.HostInfo, m3Scope int) int {
	nAdded := 0

	for _, ext := range newExtents {
		outhost, err := pickOutputHostForStoreHosts(context, ext.GetStoreUUIDs())
		if err != nil {
			context.m3Client.IncCounter(m3Scope, metrics.ControllerErrPickOutHostCounter)
			context.log.WithFields(bark.Fields{
				common.TagExt: common.FmtExt(ext.GetExtentUUID()),
				common.TagErr: err,
			}).Warn("Failed to pick outhost for extent")
			continue
		}

		err = context.mm.AddExtentToConsumerGroup(dstUUID, cgUUID, ext.GetExtentUUID(), outhost.UUID, ext.GetStoreUUIDs())
		if err != nil {
			context.m3Client.IncCounter(m3Scope, metrics.ControllerErrPickOutHostCounter)
			context.log.WithField(common.TagErr, err).Warn("Failed to add open extent to consumer group")
			continue
		}

		nAdded++
		outputHosts[outhost.UUID] = outhost

		// Schedule an async notification to outhost to
		// load the newly created extent
		event := NewConsGroupUpdatedEvent(dstUUID, cgUUID, ext.GetExtentUUID(), outhost.UUID)
		context.eventPipeline.Add(event)

		context.log.WithFields(bark.Fields{
			common.TagDst:  common.FmtDst(dstUUID),
			common.TagExt:  common.FmtExt(ext.GetExtentUUID()),
			common.TagOut:  common.FmtIn(outhost.UUID),
			common.TagCnsm: common.FmtCnsm(cgUUID),
		}).Info("Extent added to consumer group")
	}

	return nAdded
}

func fetchClassifyOpenCGExtents(context *Context, dstUUID string, cgUUID string, m3Scope int) (
	cgExtents *cgExtentsByCategory,
	outputHosts map[string]*common.HostInfo,
	err error,
) {

	cgExtents = newCGExtentsByCategory()
	outputHosts = make(map[string]*common.HostInfo)
	filterBy := []m.ConsumerGroupExtentStatus{m.ConsumerGroupExtentStatus_OPEN}
	openCGExtentsList, err := listConsumerGroupExtents(context, dstUUID, cgUUID, m3Scope, filterBy)
	if err != nil {
		return
	}

	for _, ext := range openCGExtentsList {

		extID := ext.GetExtentUUID()
		cgExtents.open[extID] = struct{}{}

		// if atleast one store is healthy, this
		// extent is consumable, inc the consumable count
		if !isAnyStoreHealthy(context, ext.GetStoreUUIDs()) {
			continue
		}

		hostID := ext.GetOutputHostUUID()
		addr, e2 := context.rpm.ResolveUUID(common.OutputServiceName, hostID)
		if e2 != nil {
			cgExtents.openBad = append(cgExtents.openBad, ext)
			continue
		}

		cgExtents.openHealthy[extID] = struct{}{}
		outputHosts[hostID] = &common.HostInfo{UUID: hostID, Addr: addr}
	}

	return
}

// Given the set of current open consumer_group_extents,
// this method picks the next set of extents to consume
// for the given consumer group. It does the following:
//
//   * Determines the optimal target number of cgExtents
//       * Based on utilization & backlog metrics (TODO)
//       * 25% of this is always reserved for DLQ extents (if present)
//   * Runs the algorithm to pick dst extents to add to CG (to achieve target)
//       * Gives out extents by created_time
//       * Gives out one extent from each zone each time
//       * until all quota is used. This is to ensure we
//       * select similar number of extents from each zone
//       * If there are no DLQ extents currently being
//         consumed and there is one available, it is
//         always picked, even if the target number is
//         already reached
//
//  Rationale for special treatment of DLQExtents:
//  Two Goals:
//
//   (1) DLQ extents become available only when a customer
//   merges their dlq to their normal destination. When this
//   happens, the customer expectation is to start seeing
//   messages from the merge operation immediately.
//   (2) Avoid all consumed extents being DLQ extents.
//   This is because, a merge could potentially bring in
//   a lot of dlq extents and in case, these are poison
//   pills, the customer will make no progress w.r.t their
//   backlog.
func selectNextExtentsToConsume(
	context *Context,
	dstDesc *shared.DestinationDescription,
	cgDesc *shared.ConsumerGroupDescription,
	cgExtents *cgExtentsByCategory,
	m3Scope int) ([]*m.DestinationExtent, int, error) {

	dstID := dstDesc.GetDestinationUUID()
	cgID := cgDesc.GetConsumerGroupUUID()

	filterBy := []shared.ExtentStatus{shared.ExtentStatus_SEALED, shared.ExtentStatus_OPEN}
	dstExtents, err := context.mm.ListDestinationExtentsByStatus(dstID, filterBy)
	if err != nil {
		context.m3Client.IncCounter(m3Scope, metrics.ControllerErrMetadataReadCounter)
		return []*m.DestinationExtent{}, 0, err
	}

	dedupMap := make(map[string]struct{})

	var nCGDlqExtents int
	var dstDlqExtents []*m.DestinationExtent
	dstExtentsCount := 0
	dstExtentsByZone := make(map[string][]*m.DestinationExtent)

	sortExtentStatsByTime(dstExtents)

	for _, ext := range dstExtents {

		extID := ext.GetExtentUUID()

		if _, ok := dedupMap[extID]; ok {
			continue
		}

		dedupMap[extID] = struct{}{}

		if !canConsumeDstExtent(context, ext, cgExtents.consumed) {
			continue
		}

		visibility := ext.GetConsumerGroupVisibility()

		if _, ok := cgExtents.open[extID]; ok {
			if len(visibility) > 0 {
				nCGDlqExtents++
			}
			continue
		}

		if len(visibility) > 0 {
			if visibility == cgID {
				dstDlqExtents = append(dstDlqExtents, ext)
			}
			continue
		}

		dstExtentsByZone[ext.GetOriginZone()] = append(dstExtentsByZone[ext.GetOriginZone()], ext)
		dstExtentsCount++
	}

	var zones []string
	for zone := range dstExtentsByZone {
		zones = append(zones, zone)
	}

	// capacity is the target number of cgextents to achieve
	capacity := maxExtentsToConsumeForDst(context, dstDesc.GetPath(), cgDesc.GetConsumerGroupName(), getDstType(dstDesc), dstDesc.GetZoneConfigs())
	dlqQuota := common.MaxInt(1, capacity/4)
	dlqQuota = common.MaxInt(0, dlqQuota-nCGDlqExtents)

	nAvailable := dstExtentsCount + len(dstDlqExtents)
	nConsumable := dstExtentsCount + common.MinInt(dlqQuota, len(dstDlqExtents))

	capacity = common.MaxInt(0, capacity-len(cgExtents.openHealthy))
	capacity = common.MinInt(capacity, nConsumable)

	if capacity == 0 {
		if nCGDlqExtents == 0 && len(dstDlqExtents) > 0 {
			// there is no room for new cgextents, however,
			// we have a dlq extent available now (and there
			// is none currently consumed). So pick the
			// dlq extent and bail out
			return []*m.DestinationExtent{dstDlqExtents[0]}, nAvailable, nil
		}
		return []*m.DestinationExtent{}, nAvailable, nil
	}

	nZone := 0
	remDstExtents := dstExtentsCount

	nDstDlqExtents := 0
	remDstDlqExtents := len(dstDlqExtents)

	result := make([]*m.DestinationExtent, capacity)

	for i := 0; i < capacity; i++ {
		if remDstDlqExtents > 0 {
			if nDstDlqExtents < dlqQuota {
				result[i] = dstDlqExtents[nDstDlqExtents]
				nDstDlqExtents++
				remDstDlqExtents--
				continue
			}
		}

		if remDstExtents > 0 {
			// iterate until we find out a zone that has available extent
			for len(dstExtentsByZone[zones[nZone]]) == 0 {
				nZone = (nZone + 1) % len(zones)
			}
			result[i] = dstExtentsByZone[zones[nZone]][0]
			dstExtentsByZone[zones[nZone]] = dstExtentsByZone[zones[nZone]][1:]
			nZone = (nZone + 1) % len(zones)

			remDstExtents--
			continue
		}
	}

	return result, nAvailable, nil
}

func refreshCGExtents(context *Context,
	dstDesc *shared.DestinationDescription,
	cgDesc *shared.ConsumerGroupDescription,
	cgExtents *cgExtentsByCategory,
	outputHosts map[string]*common.HostInfo,
	m3Scope int) (int, error) {

	dstID := dstDesc.GetDestinationUUID()
	cgID := cgDesc.GetConsumerGroupUUID()

	// generate map of consumed CG Extents
	filterBy := []m.ConsumerGroupExtentStatus{m.ConsumerGroupExtentStatus_CONSUMED}
	consumedCGExtentsList, err := listConsumerGroupExtents(context, dstID, cgID, m3Scope, filterBy)
	if err != nil {
		return 0, err
	}

	cgExtents.consumed = make(map[string]struct{})
	for _, ext := range consumedCGExtentsList {
		cgExtents.consumed[ext.GetExtentUUID()] = struct{}{}
	}

	newExtents, nAvailable, err := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, m3Scope)
	if err != nil {
		return 0, err
	}

	if len(cgExtents.openHealthy) == 0 && len(newExtents) == 0 {

		nBacklog := nAvailable + len(cgExtents.open)
		maxExtentsToConsume := maxExtentsToConsumeForDst(context, dstDesc.GetPath(), cgDesc.GetConsumerGroupName(), getDstType(dstDesc), dstDesc.GetZoneConfigs())

		if nBacklog < maxExtentsToConsume {
			// No consumable extents for this destination, create one
			extentID, _, storehosts, e := createExtent(context, dstID, dstDesc.GetIsMultiZone(), m3Scope)
			if e != nil {
				context.m3Client.IncCounter(m3Scope, metrics.ControllerErrCreateExtentCounter)
				return 0, e
			}
			storeids := make([]string, len(storehosts))
			for i := 0; i < len(storehosts); i++ {
				storeids[i] = storehosts[i].UUID
			}
			ext := &m.DestinationExtent{
				ExtentUUID: common.StringPtr(extentID),
				StoreUUIDs: storeids,
			}
			newExtents = append(newExtents, ext)
		}
	}

	return addExtentsToConsumerGroup(context, dstID, cgID, newExtents, outputHosts, m3Scope), nil
}

// refreshOutputHostsForConsGroup refreshes the output hosts for the given consumer group
func refreshOutputHostsForConsGroup(context *Context,
	dstID string,
	cgID string,
	cacheEntry resultCacheReadResult,
	now int64) ([]string, error) {

	var m3Scope = metrics.RefreshOutputHostsForConsGroupScope
	context.m3Client.IncCounter(m3Scope, metrics.ControllerRequests)

	dstDesc, err := readDestination(context, dstID, m3Scope)
	if err != nil {
		context.m3Client.IncCounter(m3Scope, metrics.ControllerFailures)
		return nil, err
	}

	if err = validateDstStatus(dstDesc); err != nil {
		return nil, err
	}

	var nConsumable int
	var dstType = getDstType(dstDesc)
	var outputAddrs []string
	var outputIDs []string
	var outputHosts map[string]*common.HostInfo

	cgDesc, err := context.mm.ReadConsumerGroup(dstID, "", cgID, "")
	if err != nil {
		context.m3Client.IncCounter(m3Scope, metrics.ControllerErrMetadataReadCounter)
		context.m3Client.IncCounter(m3Scope, metrics.ControllerFailures)
		return nil, err
	}
	if err := validatCGStatus(cgDesc); err != nil {
		return nil, err
	}

	var maxExtentsToConsume = maxExtentsToConsumeForDst(context, dstDesc.GetPath(), cgDesc.GetConsumerGroupName(), dstType, dstDesc.GetZoneConfigs())

	writeToCache := func(ttl int64) {

		outputIDs, outputAddrs = hostInfoMapToSlice(outputHosts)

		context.resultCache.write(cgID,
			resultCacheParams{
				dstType:    dstType,
				nExtents:   nConsumable,
				maxExtents: maxExtentsToConsume,
				hostIDs:    outputIDs,
				expiry:     now + ttl,
			})
	}

	cgExtents, outputHosts, err := fetchClassifyOpenCGExtents(context, dstID, cgID, m3Scope)
	if err != nil {
		context.m3Client.IncCounter(m3Scope, metrics.ControllerFailures)
		return nil, err
	}

	nConsumable = len(cgExtents.openHealthy)

	// If we have enough extents and nothing changed since last refresh,
	// short circuit and return
	if nConsumable >= maxExtentsToConsume && nConsumable == cacheEntry.nExtents {
		// Logic to avoid leaving too many open extents for the
		// consumer group. Goal is for the consumer to keep up
		// with producer, so we try to keep twice the number of
		// published extents open for consumption at any given
		// point of time.
		if dstType == dstTypeTimer {
			// If we indeed hit this limit for TIMERs, its time
			// to alarm, because the limit is too high for timers
			context.m3Client.IncCounter(m3Scope, metrics.ControllerErrTooManyOpenCGExtents)
			context.log.WithFields(bark.Fields{
				common.TagDst:  common.FmtDst(dstID),
				common.TagCnsm: common.FmtCnsm(cgID),
			}).Warn("Too many open consumer group extents")
		}
		return cacheEntry.cachedResult, nil
	}

	// repair unhealthy extents before making a decision on whether to create a new extent or not
	if len(cgExtents.openBad) > 0 {
		nRepaired := repairExtentsAndUpdateOutputHosts(context, dstID, cgID, cgExtents, maxExtentsToConsume, outputHosts, m3Scope)
		nConsumable += nRepaired
		if nRepaired != len(cgExtents.openBad) && nConsumable > 0 {
			// if we cannot repair all of the bad extents,
			// we will likely won't be able to create new
			// consumer group extents, short circuit
			writeToCache(int64(outputCacheTTL))
			return outputAddrs, nil
		}
	}

	// A this point, we do a full refresh i.e we will scan the destination extents,
	// not just existing consumer group extents. This is a 'full scan'
	nAdded, _ := refreshCGExtents(context, dstDesc, cgDesc, cgExtents, outputHosts, m3Scope)
	nConsumable += nAdded
	writeToCache(failBackoffInterval)
	return outputAddrs, err
}
