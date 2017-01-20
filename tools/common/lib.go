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

package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/codegangsta/cli"
	ccli "github.com/uber/cherami-client-go/client/cherami"
	mcli "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/clients/outputhost"
	"github.com/uber/cherami-server/clients/storehost"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// GlobalOptions are options shared by most command line
type GlobalOptions struct {
	hyperbahn              bool
	hyperbahnBootstrapFile string
	env                    string
	frontendHost           string
	frontendPort           int
	timeoutSecs            int
}

const (
	// DefaultPageSize is the default page size for data base access for command line
	DefaultPageSize = 1000

	// UnknownUUID is the suffix added to a host when the host uuid does not exist in cassandra
	UnknownUUID = " UNKNOWN_HOST_UUID"
	// DestinationType is the name for entity type for destination in listEntityOps
	DestinationType = "DST"
	// ConsumerGroupType is the name for entity type for consumer group in listEntityOps
	ConsumerGroupType = "CG"
)

const (
	strNotEnoughArgs        = "Not enough arguments. Try \"--help\""
	strNoChange             = "Update must update something. Try \"--help\""
	strCGSpecIncorrectArgs  = "Incorrect consumer group specification. Use \"<cg_uuid>\" or \"<dest_path> <cg_name>\""
	strDestStatus           = "Destination status must be \"enabled\", \"disabled\", \"sendonly\", or \"recvonly\""
	strCGStatus             = "Consumer group status must be \"enabled\", or \"disabled\""
	strWrongDestZoneConfig  = "Format of destination zone config is wrong, should be \"ZoneName,AllowPublish,AllowConsume,ReplicaCount\". For example: \"zone1,true,true,3\""
	strWrongReplicaCount    = "Replica count must be within 1 to 3"
	strWrongZoneConfigCount = "Multi zone destination must have at least 2 zone configs"
)

var uuidRegex, _ = regexp.Compile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)

// ExitIfError exit while err is not nil and print the calling stack also
func ExitIfError(err error) {
	const stacksEnv = `CHERAMI_SHOW_STACKS`
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		if os.Getenv(stacksEnv) != `` {
			debug.PrintStack()
		} else {
			fmt.Fprintf(os.Stderr, "('export %s=1' to see stack traces)\n", stacksEnv)
		}
		os.Exit(1)
	}
}

// newGlobalOptionsFromCLIContext return GlobalOptions based on the cli.Context
func newGlobalOptionsFromCLIContext(c *cli.Context) *GlobalOptions {
	host, port, err := common.SplitHostPort(c.GlobalString("hostport"))
	ExitIfError(err)
	environment := c.GlobalString("env")
	if strings.HasPrefix(environment, `prod`) {
		environment = ``
	}
	return &GlobalOptions{
		hyperbahn:              c.GlobalBool("hyperbahn"),
		hyperbahnBootstrapFile: c.GlobalString("hyperbahn_bootstrap_file"),
		env:          environment,
		frontendHost: host,
		frontendPort: port,
		timeoutSecs:  c.GlobalInt("timeout"),
	}
}

// GetCClient return a cherami.Client
func GetCClient(c *cli.Context, serviceName string) ccli.Client {
	gOpts := newGlobalOptionsFromCLIContext(c)
	var cClient ccli.Client
	var err error
	cOpts := ccli.ClientOptions{
		Timeout:       time.Duration(gOpts.timeoutSecs) * time.Second,
		DeploymentStr: gOpts.env,
	}

	if !(len(gOpts.frontendHost) > 0 || gOpts.frontendPort > 0) && gOpts.hyperbahn {
		cClient, err = ccli.NewHyperbahnClient(serviceName, gOpts.hyperbahnBootstrapFile, &cOpts)
	} else {
		cClient, err = ccli.NewClient(serviceName, gOpts.frontendHost, gOpts.frontendPort, &cOpts)
	}

	ExitIfError(err)
	return cClient
}

// GetMClient return a metadata.Client
func GetMClient(c *cli.Context, serviceName string) mcli.Client {
	gOpts := newGlobalOptionsFromCLIContext(c)
	var mClient mcli.Client
	var err error
	cOpts := ccli.ClientOptions{
		Timeout:       time.Duration(gOpts.timeoutSecs) * time.Second,
		DeploymentStr: gOpts.env,
	}

	if !(len(gOpts.frontendHost) > 0 || gOpts.frontendPort > 0) && gOpts.hyperbahn {
		mClient, err = mcli.NewHyperbahnClient(serviceName, gOpts.hyperbahnBootstrapFile, &cOpts)
	} else {
		mClient, err = mcli.NewClient(serviceName, gOpts.frontendHost, gOpts.frontendPort, &cOpts)
	}

	ExitIfError(err)
	return mClient
}

// Jsonify return the json string based on the obj
func Jsonify(obj thrift.TStruct) string {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTSimpleJSONProtocol(transport)
	obj.Write(protocol)
	protocol.Flush()
	transport.Flush()
	return transport.String()
}

func getChecksumOptionParam(optionStr string) cherami.ChecksumOption {
	checksumstr := strings.ToLower(optionStr)
	if checksumstr == "md5" {
		return cherami.ChecksumOption_MD5
	}

	return cherami.ChecksumOption_CRC32IEEE
}

// CreateDestination create destination
func CreateDestination(c *cli.Context, cClient ccli.Client, cliHelper common.CliHelper) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	dType := cherami.DestinationType_PLAIN
	if c.String("type") == "timer" {
		dType = cherami.DestinationType_TIMER
	}

	path := c.Args().First()
	consumedMessagesRetention := int32(c.Int("consumed_messages_retention"))
	unconsumedMessagesRetention := int32(c.Int("unconsumed_messages_retention"))
	checksumOption := getChecksumOptionParam(string(c.String("checksum_option")))
	ownerEmail := string(c.String("owner_email"))
	if len(ownerEmail) == 0 {
		cliHelper.GetDefaultOwnerEmail()
	}

	var zoneConfigs cherami.DestinationZoneConfigs
	configs := c.StringSlice("zone_config")
	for _, config := range configs {
		parts := strings.Split(config, `,`)
		if len(parts) != 4 {
			ExitIfError(errors.New(strWrongDestZoneConfig))
		}

		zone, err := cliHelper.GetCanonicalZone(parts[0])
		ExitIfError(err)

		allowPublish, err := strconv.ParseBool(parts[1])
		ExitIfError(err)
		allowConsume, err := strconv.ParseBool(parts[2])
		ExitIfError(err)
		replicaCount, err := strconv.ParseInt(parts[3], 10, 0)
		ExitIfError(err)

		if replicaCount < 1 || replicaCount > 3 {
			ExitIfError(errors.New(strWrongReplicaCount))
		}

		zoneConfigs.Configs = append(zoneConfigs.Configs, &cherami.DestinationZoneConfig{
			Zone:                   common.StringPtr(zone),
			AllowPublish:           common.BoolPtr(allowPublish),
			AllowConsume:           common.BoolPtr(allowConsume),
			RemoteExtentReplicaNum: common.Int32Ptr(int32(replicaCount)),
		})
	}

	if len(zoneConfigs.Configs) == 1 {
		ExitIfError(errors.New(strWrongZoneConfigCount))
	}

	desc, err := cClient.CreateDestination(&cherami.CreateDestinationRequest{
		Path: &path,
		Type: &dType,
		ConsumedMessagesRetention:   &consumedMessagesRetention,
		UnconsumedMessagesRetention: &unconsumedMessagesRetention,
		ChecksumOption:              checksumOption,
		OwnerEmail:                  &ownerEmail,
		IsMultiZone:                 common.BoolPtr(len(zoneConfigs.Configs) > 0),
		ZoneConfigs:                 &zoneConfigs,
	})

	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

// UpdateDestination update destination based on cli
func UpdateDestination(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	path := c.Args().First()

	status := cherami.DestinationStatus_ENABLED
	switch c.String("status") {
	case "disabled":
		status = cherami.DestinationStatus_DISABLED
	case "sendonly":
		status = cherami.DestinationStatus_SENDONLY
	case "recvonly:":
		status = cherami.DestinationStatus_RECEIVEONLY
	case "enabled":
	default:
		if c.IsSet(`status`) {
			ExitIfError(errors.New(strDestStatus))
		}
	}

	setCount := 0

	request := &cherami.UpdateDestinationRequest{
		Path: &path,
		ConsumedMessagesRetention:   getIfSetInt32(c, `consumed_messages_retention`, &setCount),
		UnconsumedMessagesRetention: getIfSetInt32(c, `unconsumed_messages_retention`, &setCount),
		OwnerEmail:                  getIfSetString(c, `owner_email`, &setCount),
	}

	if c.IsSet(`status`) {
		setCount++
		request.Status = &status
	}

	if c.IsSet(`checksum_option`) {
		checksumOption := getChecksumOptionParam(string(c.String("checksum_option")))
		request.ChecksumOption = &checksumOption
		setCount++
	}

	if setCount == 0 {
		ExitIfError(errors.New(strNoChange))
	}

	desc, err := cClient.UpdateDestination(request)

	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

// CreateConsumerGroup create consumer group based on cli.Context
func CreateConsumerGroup(c *cli.Context, cClient ccli.Client, cliHelper common.CliHelper) {
	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	path := c.Args()[0]
	name := c.Args()[1]
	// StartFrom is a 64-bit value with nano-second precision
	startTime := int64(c.Int("start_time")) * 1e9
	lockTimeout := int32(c.Int("lock_timeout_seconds"))
	maxDelivery := int32(c.Int("max_delivery_count"))
	skipOlder := int32(c.Int("skip_older_messages_in_seconds"))
	ownerEmail := string(c.String("owner_email"))

	// Override default startFrom for DLQ consumer groups
	if common.UUIDRegex.MatchString(path) && int64(c.Int("start_time")) > time.Now().Unix()-60 {
		fmt.Printf(`Start_time defaulted to zero, while creating DLQ consumer group`)
		startTime = 0
	}

	if len(ownerEmail) == 0 {
		ownerEmail = cliHelper.GetDefaultOwnerEmail()
	}
	desc, err := cClient.CreateConsumerGroup(&cherami.CreateConsumerGroupRequest{
		DestinationPath:            &path,
		ConsumerGroupName:          &name,
		StartFrom:                  &startTime,
		LockTimeoutInSeconds:       &lockTimeout,
		MaxDeliveryCount:           &maxDelivery,
		SkipOlderMessagesInSeconds: &skipOlder,
		OwnerEmail:                 &ownerEmail,
	})

	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

// UpdateConsumerGroup update the consumer group based on cli.Context
func UpdateConsumerGroup(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	path := c.Args()[0]
	name := c.Args()[1]
	status := cherami.ConsumerGroupStatus_ENABLED
	if c.String("status") == "disabled" {
		status = cherami.ConsumerGroupStatus_DISABLED
	} else if c.String("status") != "enabled" {
		if c.IsSet(`status`) {
			ExitIfError(errors.New(strCGStatus))
		}
	}

	setCount := 0

	uReq := &cherami.UpdateConsumerGroupRequest{
		DestinationPath:            &path,
		ConsumerGroupName:          &name,
		LockTimeoutInSeconds:       getIfSetInt32(c, `lock_timeout_seconds`, &setCount),
		MaxDeliveryCount:           getIfSetInt32(c, `max_delivery_count`, &setCount),
		SkipOlderMessagesInSeconds: getIfSetInt32(c, `skip_older_messages_in_seconds`, &setCount),
		OwnerEmail:                 getIfSetString(c, `owner_email`, &setCount),
	}

	if c.IsSet(`status`) {
		setCount++
		uReq.Status = &status
	}

	if setCount == 0 {
		ExitIfError(errors.New(strNoChange))
	}

	desc, err := cClient.UpdateConsumerGroup(uReq)
	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

// UnloadConsumerGroup unloads the CG based on cli.Context
func UnloadConsumerGroup(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]
	cgUUID := c.String("cg_uuid")

	if !uuidRegex.MatchString(cgUUID) {
		ExitIfError(errors.New("specify a valid cg UUID"))
	}

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	outputClient, err := outputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer outputClient.Close()

	cgUnloadReq := admin.NewUnloadConsumerGroupsRequest()
	cgUnloadReq.CgUUIDs = []string{cgUUID}

	err = outputClient.UnloadConsumerGroups(cgUnloadReq)
	ExitIfError(err)
}

type destDescJSONOutputFields struct {
	Path                        string                   `json:"path"`
	UUID                        string                   `json:"uuid"`
	Status                      shared.DestinationStatus `json:"status"`
	Type                        shared.DestinationType   `json:"type"`
	ChecksumOption              string                   `json:"checksum_option"`
	OwnerEmail                  string                   `json:"owner_email"`
	ConsumedMessagesRetention   int32                    `json:"consumed_messages_retention"`
	UnconsumedMessagesRetention int32                    `json:"unconsumed_messages_retention"`
	DLQCGUUID                   string                   `json:"dlq_cg_uuid"`
	DLQPurgeBefore              int64                    `json:"dlq_purge_before"`
	DLQMergeBefore              int64                    `json:"dlq_merge_before"`
}

func printDest(dest *shared.DestinationDescription) {
	output := &destDescJSONOutputFields{
		Path:                        dest.GetPath(),
		UUID:                        dest.GetDestinationUUID(),
		Status:                      dest.GetStatus(),
		Type:                        dest.GetType(),
		OwnerEmail:                  dest.GetOwnerEmail(),
		ConsumedMessagesRetention:   dest.GetConsumedMessagesRetention(),
		UnconsumedMessagesRetention: dest.GetUnconsumedMessagesRetention(),
		DLQCGUUID:                   dest.GetDLQConsumerGroupUUID(),
		DLQPurgeBefore:              dest.GetDLQPurgeBefore(),
		DLQMergeBefore:              dest.GetDLQMergeBefore(),
	}

	switch dest.GetChecksumOption() {
	case shared.ChecksumOption_CRC32IEEE:
		output.ChecksumOption = "CRC32_IEEE"
	case shared.ChecksumOption_MD5:
		output.ChecksumOption = "MD5"
	}

	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// ReadDestination return the detail for dest, and also consumer group for this dest
func ReadDestination(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	path := c.Args().First()
	showCG := string(c.String("showcg"))
	desc, err := mClient.ReadDestination(&metadata.ReadDestinationRequest{
		Path: &path,
	})
	ExitIfError(err)
	printDest(desc)

	// only show cg info if showCG flag is true
	if showCG == "true" {
		// read all the consumer group for this destination, including deleted ones
		destUUID := desc.GetDestinationUUID()
		req := &metadata.ListConsumerGroupRequest{
			Limit: common.Int64Ptr(DefaultPageSize),
		}
		var cgsInfo = make([]*shared.ConsumerGroupDescription, 0)
		for {
			resp, err1 := mClient.ListAllConsumerGroups(req)
			ExitIfError(err1)

			for _, cg := range resp.GetConsumerGroups() {
				if destUUID == cg.GetDestinationUUID() {
					cgsInfo = append(cgsInfo, cg)
				}
			}

			if len(resp.GetConsumerGroups()) < DefaultPageSize {
				break
			} else {
				req.PageToken = resp.NextPageToken
			}
		}
		// print out all the consumer groups for this destination
		for _, cg := range cgsInfo {
			printCG(cg)
		}
	}
}

// ReadDlq return the info for dlq dest and related consumer group
func ReadDlq(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	dlqUUID := c.Args().First()
	desc, err0 := mClient.ReadDestination(&metadata.ReadDestinationRequest{
		Path: &dlqUUID,
	})

	ExitIfError(err0)
	printDest(desc)

	cgUUID := desc.GetDLQConsumerGroupUUID()
	if len(cgUUID) <= 0 {
		ExitIfError(errors.New("no dlqConsumerGroupUUID for this destination. Please ensure it is a dlq destination"))
	}

	req := &metadata.ReadConsumerGroupRequest{
		ConsumerGroupUUID: &cgUUID,
	}

	resp, err := mClient.ReadConsumerGroupByUUID(req)
	ExitIfError(err)
	printCG(resp)
}

// ReadCgBacklog reads the CG back log
func ReadCgBacklog(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	cgUUID := c.Args()[0]

	if !uuidRegex.MatchString(cgUUID) {
		ExitIfError(errors.New("specify a valid cg UUID"))
	}
	backlog, err := cClient.GetQueueDepthInfo(&cherami.GetQueueDepthInfoRequest{
		Key: &cgUUID,
	})
	if err != nil {
		fmt.Printf("Cannot get backlog for %s. Please check the dashboard.", cgUUID)
		os.Exit(1)
	}

	fmt.Println(backlog.GetValue())
}

// DeleteDestination delete the destination based on Cli.Context
func DeleteDestination(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args().First()
	err := cClient.DeleteDestination(&cherami.DeleteDestinationRequest{
		Path: &path,
	})

	ExitIfError(err)
}

// DeleteConsumerGroup delete the consumer group based on Cli.Context
func DeleteConsumerGroup(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args()[0]
	name := c.Args()[1]

	err := cClient.DeleteConsumerGroup(&cherami.DeleteConsumerGroupRequest{
		DestinationPath:   &path,
		ConsumerGroupName: &name,
	})

	ExitIfError(err)
}

type cgJSONOutputFields struct {
	CGName                   string                     `json:"consumer_group_name"`
	DestUUID                 string                     `json:"destination_uuid"`
	CGUUID                   string                     `json:"consumer_group_uuid"`
	Status                   shared.ConsumerGroupStatus `json:"consumer_group_status"`
	StartFrom                int64                      `json:"startFrom"`
	LockTimeoutSeconds       int32                      `json:"lock_timeout_seconds"`
	MaxDeliveryCount         int32                      `json:"max_delivery_count"`
	SkipOlderMessagesSeconds int32                      `json:"skip_older_msg_seconds"`
	CGEmail                  string                     `json:"owner_email"`
	CGDlq                    string                     `json:"dlqUUID"`
}

func printCG(cg *shared.ConsumerGroupDescription) {
	output := &cgJSONOutputFields{
		CGName:                   cg.GetConsumerGroupName(),
		DestUUID:                 cg.GetDestinationUUID(),
		CGUUID:                   cg.GetConsumerGroupUUID(),
		Status:                   cg.GetStatus(),
		StartFrom:                cg.GetStartFrom(),
		LockTimeoutSeconds:       cg.GetLockTimeoutSeconds(),
		MaxDeliveryCount:         cg.GetMaxDeliveryCount(),
		SkipOlderMessagesSeconds: cg.GetSkipOlderMessagesSeconds(),
		CGEmail:                  cg.GetOwnerEmail(),
		CGDlq:                    cg.GetDeadLetterQueueDestinationUUID()}
	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// ReadConsumerGroup return the consumer group information
func ReadConsumerGroup(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strCGSpecIncorrectArgs))
	}

	if len(c.Args()) == 2 {
		path := c.Args()[0]
		name := c.Args()[1]

		cgDesc, err := mClient.ReadConsumerGroup(&metadata.ReadConsumerGroupRequest{
			DestinationPath:   &path,
			ConsumerGroupName: &name,
		})

		ExitIfError(err)
		printCG(cgDesc)
		return
	}
	if len(c.Args()) == 1 {
		cgUUID := c.Args()[0]

		cgDesc, err := mClient.ReadConsumerGroupByUUID(&metadata.ReadConsumerGroupRequest{
			ConsumerGroupUUID: common.StringPtr(cgUUID)})
		ExitIfError(err)
		printCG(cgDesc)
	}
}

// MergeDLQForConsumerGroup return the consumer group information
func MergeDLQForConsumerGroup(c *cli.Context, cClient ccli.Client) {
	var err error
	switch len(c.Args()) {
	default:
		err = errors.New(strCGSpecIncorrectArgs)
	case 2:
		path := c.Args()[0]
		name := c.Args()[1]

		err = cClient.MergeDLQForConsumerGroup(&cherami.MergeDLQForConsumerGroupRequest{
			DestinationPath:   common.StringPtr(path),
			ConsumerGroupName: common.StringPtr(name),
		})
	case 1:
		cgUUID := c.Args()[0]
		err = cClient.MergeDLQForConsumerGroup(&cherami.MergeDLQForConsumerGroupRequest{
			ConsumerGroupName: common.StringPtr(cgUUID),
		})
	}
	ExitIfError(err)
}

// PurgeDLQForConsumerGroup return the consumer group information
func PurgeDLQForConsumerGroup(c *cli.Context, cClient ccli.Client) {
	var err error
	switch len(c.Args()) {
	default:
		err = errors.New(strCGSpecIncorrectArgs)
	case 2:
		path := c.Args()[0]
		name := c.Args()[1]

		err = cClient.PurgeDLQForConsumerGroup(&cherami.PurgeDLQForConsumerGroupRequest{
			DestinationPath:   common.StringPtr(path),
			ConsumerGroupName: common.StringPtr(name),
		})
	case 1:
		cgUUID := c.Args()[0]
		err = cClient.PurgeDLQForConsumerGroup(&cherami.PurgeDLQForConsumerGroupRequest{
			ConsumerGroupName: common.StringPtr(cgUUID),
		})
	}
	ExitIfError(err)
}

type destJSONOutputFields struct {
	DestinationName             string                   `json:"destination_name"`
	DestinationUUID             string                   `json:"destination_uuid"`
	Status                      shared.DestinationStatus `json:"status"`
	Type                        shared.DestinationType   `json:"type"`
	OwnerEmail                  string                   `json:"owner_email"`
	TotalExts                   int                      `json:"total_ext"`
	OpenExts                    int                      `json:"open"`
	SealedExts                  int                      `json:"sealed"`
	ConsumedExts                int                      `json:"consumed"`
	DeletedExts                 int                      `json:"Deleted"`
	ConsumedMessagesRetention   int32                    `json:"consumed_messages_retention"`
	UnconsumedMessagesRetention int32                    `json:"unconsumed_messages_retention"`
}

func matchDestStatus(status string, wantStatus shared.DestinationStatus) bool {

	switch status {
	case "enabled":
		return wantStatus == shared.DestinationStatus_ENABLED
	case "disabled":
		return wantStatus == shared.DestinationStatus_DISABLED
	case "sendonly":
		return wantStatus == shared.DestinationStatus_SENDONLY
	case "recvonly":
		return wantStatus == shared.DestinationStatus_RECEIVEONLY
	default:
		ExitIfError(errors.New("Please use status among: enabled | disabled | sendonly | recvonly"))
	}
	return false
}

// ReadMessage implement for show msg command line
func ReadMessage(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 2 {
		ExitIfError(errors.New("not enough arguments, need to specify both extent uuid and message address"))
	}

	uuidStr := c.Args()[0]
	descExtent, err := mClient.ReadExtentStats(&metadata.ReadExtentStatsRequest{
		ExtentUUID: &uuidStr,
	})
	ExitIfError(err)
	extentStats := descExtent.GetExtentStats()
	extent := extentStats.GetExtent()

	addressStr := c.Args()[1]
	address, err1 := strconv.ParseInt(addressStr, 16, 64)
	ExitIfError(err1)

	storeHostUUIDs := extent.GetStoreUUIDs()
	for _, storeUUID := range storeHostUUIDs {
		storeHostAddr, err2 := mClient.UUIDToHostAddr(storeUUID)
		if err2 != nil {
			storeHostAddr = storeUUID + UnknownUUID
		}

		sClient, err3 := storehost.NewClient(storeUUID, storeHostAddr)
		ExitIfError(err3)
		defer sClient.Close()

		req := store.NewReadMessagesRequest()
		req.ExtentUUID = common.StringPtr(string(uuidStr))
		req.StartAddress = common.Int64Ptr(address)
		req.StartAddressInclusive = common.BoolPtr(true)
		req.NumMessages = common.Int32Ptr(1)

		// query storage to find address of the message with the given timestamp
		resp, err4 := sClient.ReadMessages(req)
		ExitIfError(err4)

		// print out msg from all store hosts
		readMessage := resp.GetMessages()[0].GetMessage()
		message := readMessage.GetMessage()

		ipAndPort := strings.Split(storeHostAddr, ":")

		output := &messageJSONOutputFields{
			StoreAddr:      ipAndPort[0],
			StoreUUID:      storeUUID,
			MessageAddress: readMessage.GetAddress(),
			SequenceNumber: message.GetSequenceNumber(),
			EnqueueTimeUtc: time.Unix(0, message.GetEnqueueTimeUtc()*1000000),
		}

		outputStr, _ := json.Marshal(output)
		fmt.Fprintln(os.Stdout, string(outputStr))
		fmt.Fprintf(os.Stdout, "%v\n", message.GetPayload())
	}
}

type messageJSONOutputFields struct {
	StoreAddr      string    `json:"storehost_addr"`
	StoreUUID      string    `json:"storehost_uuid"`
	MessageAddress int64     `json:"address"`
	SequenceNumber int64     `json:"sequenceNumber,omitempty"`
	EnqueueTimeUtc time.Time `json:"enqueueTimeUtc,omitempty"`
}

// ListDestinations return destinations based on the Cli.Context
func ListDestinations(c *cli.Context, mClient mcli.Client) {

	prefix := string(c.String("prefix"))
	included := string(c.String("include"))
	excluded := string(c.String("exclude"))
	destStatus := string(c.String("status"))

	req := &shared.ListDestinationsRequest{
		Prefix: &prefix,
		Limit:  common.Int64Ptr(DefaultPageSize),
	}

	inReg, errI := regexp.Compile(included)
	ExitIfError(errI)
	exReg, errE := regexp.Compile(excluded)
	ExitIfError(errE)

	var destsInfo = make(map[shared.DestinationStatus][]*destJSONOutputFields, 0)
	for {
		resp, err := mClient.ListDestinations(req)
		ExitIfError(err)

		for _, desc := range resp.GetDestinations() {

			if len(included) > 0 && !inReg.MatchString(desc.GetPath()) {
				continue
			}
			if len(excluded) > 0 && exReg.MatchString(desc.GetPath()) {
				continue
			}
			nTotal := 0
			nOpen := 0
			nSealed := 0
			nConsumed := 0
			nDeleted := 0
			listExtentsStats := &shared.ListExtentsStatsRequest{
				DestinationUUID: desc.DestinationUUID,
				Limit:           common.Int64Ptr(DefaultPageSize),
			}

			for {
				listExtentStatsResult, err1 := mClient.ListExtentsStats(listExtentsStats)
				ExitIfError(err1)

				for _, stats := range listExtentStatsResult.ExtentStatsList {
					//extent := stats.GetExtent()
					nTotal++
					if stats.GetStatus() == shared.ExtentStatus_OPEN {
						nOpen++
					} else if stats.GetStatus() == shared.ExtentStatus_SEALED {
						nSealed++
					} else if stats.GetStatus() == shared.ExtentStatus_CONSUMED {
						nConsumed++
					} else if stats.GetStatus() == shared.ExtentStatus_DELETED {
						nDeleted++
					}
				}

				if len(listExtentStatsResult.GetNextPageToken()) == 0 {
					break
				} else {
					listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
				}
			}

			// if --status option not provide, show all the destination path
			status := desc.GetStatus()
			if len(destStatus) == 0 || matchDestStatus(destStatus, status) {
				outputDest := &destJSONOutputFields{
					DestinationName: desc.GetPath(),
					DestinationUUID: desc.GetDestinationUUID(),
					Status:          status,
					Type:            desc.GetType(),
					OwnerEmail:      desc.GetOwnerEmail(),
					TotalExts:       nTotal,

					OpenExts:                    nOpen,
					SealedExts:                  nSealed,
					ConsumedExts:                nConsumed,
					DeletedExts:                 nDeleted,
					ConsumedMessagesRetention:   desc.GetConsumedMessagesRetention(),
					UnconsumedMessagesRetention: desc.GetUnconsumedMessagesRetention(),
				}
				destsInfo[status] = append(destsInfo[status], outputDest)
			}
		}

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			req.PageToken = resp.GetNextPageToken()
		}
	}
	// print output here based on the destination status
	for _, dests := range destsInfo {
		for _, dest := range dests {
			outputStr, _ := json.Marshal(dest)
			fmt.Fprintln(os.Stdout, string(outputStr))
		}
	}
}

// ListConsumerGroups return the consumer groups based on the destination provided
func ListConsumerGroups(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args()[0]
	var name string
	if len(c.Args()) > 1 {
		name = c.Args()[1]
	}

	req := &cherami.ListConsumerGroupRequest{
		DestinationPath:   &path,
		ConsumerGroupName: &name,
		Limit:             common.Int64Ptr(DefaultPageSize),
	}

	for {
		resp, err := cClient.ListConsumerGroups(req)
		ExitIfError(err)

		for _, desc := range resp.GetConsumerGroups() {
			fmt.Printf("%s %s %s\n", desc.GetDestinationPath(), desc.GetConsumerGroupName(), desc.GetConsumerGroupUUID())
		}

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			req.PageToken = resp.GetNextPageToken()
		}
	}
}

// Publish start to pusblish to the destination provided
func Publish(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args().First()

	publisher := cClient.CreatePublisher(&ccli.CreatePublisherRequest{
		Path: path,
	})

	err := publisher.Open()
	ExitIfError(err)

	receiptCh := make(chan *ccli.PublisherReceipt)
	receiptWg := sync.WaitGroup{}
	go func() {
		for receipt := range receiptCh {
			if receipt.Error != nil {
				fmt.Fprintf(os.Stdout, "Error for publish ID %s is %s\n", receipt.ID, receipt.Error.Error())
			} else {
				fmt.Fprintf(os.Stdout, "Receipt for publish ID %s is %s\n", receipt.ID, receipt.Receipt)
			}
			receiptWg.Done()
		}
	}()

	fmt.Fprintf(os.Stdout, "Enter messages to publish, one per line. Ctrl-D to finish.\n")

	var readErr error
	var line []byte
	bio := bufio.NewReader(os.Stdin)

	for readErr == nil {

		line, readErr = bio.ReadBytes('\n')
		if len(line) == 0 { // When readErr != nil, line can still be non-empty
			break
		}

		id, err := publisher.PublishAsync(&ccli.PublisherMessage{
			Data: line,
		}, receiptCh)

		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			break
		}

		fmt.Fprintf(os.Stdout, "Local publish ID: %s\n", id)
		receiptWg.Add(1)
	}

	if !common.AwaitWaitGroup(&receiptWg, time.Minute) {
		fmt.Fprintf(os.Stderr, "Timed out waiting for receipt.\n")
	}
	publisher.Close()
	close(receiptCh)
}

// Consume start to consume from the destination
func Consume(c *cli.Context, cClient ccli.Client) {
	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	gOpts := newGlobalOptionsFromCLIContext(c)
	path := c.Args()[0]
	name := c.Args()[1]

	consumer := cClient.CreateConsumer(&ccli.CreateConsumerRequest{
		Path:              path,
		ConsumerGroupName: name,
		ConsumerName:      "",
		PrefetchCount:     c.Int("prefetch_count"),
		Options: &ccli.ClientOptions{
			Timeout:       time.Duration(gOpts.timeoutSecs) * time.Second,
			DeploymentStr: gOpts.env,
		},
	})

	autoAck := c.BoolT("autoack")
	ch := make(chan ccli.Delivery, common.MaxInt(c.Int("prefetch_count")*2, 1))
	ch, err := consumer.Open(ch)
	ExitIfError(err)

	if !autoAck {
		// read ack tokens from Stdin
		go func() {
			var err error
			var line []byte
			bio := bufio.NewReader(os.Stdin)
			for err == nil {
				line, err = bio.ReadBytes('\n')
				if len(line) > 0 {
					consumer.AckDelivery(string(line))
				}
			}
		}()
	}

	fmt.Fprintf(os.Stdout, "%s, %s\n", `Enqueue Time (nanoseconds)`, `Message Data`)

	for delivery := range ch {
		msg := delivery.GetMessage()
		_, err = fmt.Fprintf(os.Stdout, "%v, %s\n", msg.GetEnqueueTimeUtc(), string(msg.GetPayload().GetData()))
		if autoAck {
			delivery.Ack()
		} else {
			fmt.Fprintf(os.Stdout, "%s\n", delivery.GetDeliveryToken())
		}

		if err != nil {
			if e, ok := err.(*os.PathError); ok {
				if ee, ok1 := e.Err.(syscall.Errno); ok1 && ee == syscall.EPIPE {
					return
				}
			}

			fmt.Fprintf(os.Stderr, "%T %v\n", err, err)
			return
		}
	}
}

func getIfSetInt32(c *cli.Context, p string, setCount *int) (r *int32) {
	if c.IsSet(p) {
		v := int32(c.Int(p))
		*setCount++
		return &v
	}
	return
}

func getIfSetString(c *cli.Context, p string, setCount *int) (r *string) {
	if c.IsSet(p) {
		v := string(c.String(p))
		*setCount++
		return &v
	}
	return
}
