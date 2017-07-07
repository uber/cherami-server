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
	"time"

	"github.com/codegangsta/cli"
	"github.com/uber/cherami-server/common"
	toolscommon "github.com/uber/cherami-server/tools/common"
)

const (
	strLockTimeoutSeconds = `Acknowledgement timeout for prefetched/received messages`
	strMaxDeliveryCount = "Maximum number of times a message is delivered before it is sent to the DLQ (dead-letter queue)"
	strSkipOlderMessagesInSeconds = `Skip messages older than this duration in seconds ('0' to skip none)`
	strDelaySeconds = `Delay, in seconds, to defer all messages by`
)

// GetCommonFlags get the common flags for both cli and admin commands
func GetCommonFlags() []cli.Flag {
	return []cli.Flag{
		cli.BoolTFlag{
			Name:  "hyperbahn",
			Usage: "Use hyperbahn",
		},
		cli.IntFlag{
			Name:  "timeout, t",
			Value: 60,
			Usage: "Timeout in seconds",
		},
		cli.StringFlag{
			Name:  "env",
			Value: "staging",
			Usage: "Deployment to connect to. By default connects to staging. Use \"prod\" to connect to production",
		},
		cli.StringFlag{
			Name:   "hyperbahn_bootstrap_file, hbfile",
			Value:  "/etc/uber/hyperbahn/hosts.json",
			Usage:  "hyperbahn boostrap file",
			EnvVar: "HYPERBAHN_BOOSTRAP_FILE",
		},
		cli.StringFlag{
			Name:   "hostport",
			Value:  "",
			Usage:  "Host:port for frontend host",
			EnvVar: "CHERAMI_FRONTEND_HOSTPORT",
		},
	}
}

// GetCommonCommands get the common commands for both cli and admin commands
func GetCommonCommands(serviceName string) []cli.Command {
	cliHelper := common.NewCliHelper()
	// SetCanonicalZones. For now just "zone1", "zone2", "z1"
	// and "z2" are valid and they map to "zone1" and "zone2"
	// canonical zones.
	// We can use this API to set any valid zones
	cliHelper.SetCanonicalZones(map[string]string{
		"zone1": "zone1",
		"zone2": "zone2",
		"z1":    "zone1",
		"z2":    "zone2",
	})

	return []cli.Command{
		{
			Name:    "create",
			Aliases: []string{"c", "cr"},
			Usage:   "create (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "create destination <path> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "type, t",
							Value: "plain",
							Usage: "Type of the destination: 'plain', 'timer', or 'kafka'",
						},
						cli.IntFlag{
							Name:  "consumed_messages_retention, cr",
							Value: toolscommon.DefaultConsumedMessagesRetention,
							Usage: "Consumed messages retention period specified in seconds. Default is 1 hour.",
						},
						cli.IntFlag{
							Name:  "unconsumed_messages_retention, ur",
							Value: toolscommon.DefaultUnconsumedMessagesRetention,
							Usage: "Unconsumed messages retention period specified in seconds. Default is two hours.",
						},
						cli.StringFlag{
							Name:  "checksum_option, co",
							Value: "crcIEEE",
							Usage: "Checksum_options, can be one of the crcIEEE, md5",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: cliHelper.GetDefaultOwnerEmail(),
							Usage: "The owner's email. Default is the $USER@uber.com",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi_zone destinations. Format for each zone should be \"ZoneName,AllowPublish,AllowConsume,ReplicaCount\". For example: \"zone1,true,true,3\"",
						},
						cli.StringFlag{
							Name:  "kafka_cluster, kc",
							Usage: "Name of the Kafka cluster to attach to",
						},
						cli.StringSliceFlag{
							Name:  "kafka_topics, kt",
							Usage: "List of kafka topics to subscribe to. Use multiple times, e.g. \"-kafka_topics topic_a -kafka_topics topic_b\"",
						},
					},
					Action: func(c *cli.Context) {
						toolscommon.CreateDestination(c, cliHelper, serviceName)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "create consumergroup [<destination_path>|<DLQ_UUID>] <consumer_group_name> [options]",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "start_time, s",
							Value: int(time.Now().Unix()),
							Usage: `Consume messages newer than this time in unix-nanos (default: Now; ie, consume no previously published messages)`,
						},
						cli.IntFlag{
							Name:  "lock_timeout_seconds, l",
							Value: toolscommon.DefaultLockTimeoutSeconds,
							Usage: strLockTimeoutSeconds,
						},
						cli.IntFlag{
							Name:  "max_delivery_count, m",
							Value: toolscommon.DefaultMaxDeliveryCount,
							Usage: strMaxDeliveryCount,
						},
						cli.IntFlag{
							Name:  "skip_older_messages_in_seconds, k",
							Value: toolscommon.DefaultSkipOlderMessageSeconds,
							Usage: strSkipOlderMessagesInSeconds,
						},
						cli.IntFlag{
							Name:  "delay_seconds, d",
							Value: toolscommon.DefaultDelayMessageSeconds,
							Usage: strDelaySeconds,
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: cliHelper.GetDefaultOwnerEmail(),
							Usage: "Owner email",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi-zone CG. For each zone, specify \"Zone,PreferedActiveZone\"; ex: \"zone1,false\"",
						},
					},
					Action: func(c *cli.Context) {
						toolscommon.CreateConsumerGroup(c, cliHelper, serviceName)
					},
				},
			},
		},
		{
			Name:    "show",
			Aliases: []string{"s", "sh", "info", "i"},
			Usage:   "show (destination | consumergroup | message | dlq | cgBacklog)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "show destination <name>",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "showcg, cg",
							Usage: "show consumer groups for the destination",
						},
					},
					Action: func(c *cli.Context) {
						toolscommon.ReadDestination(c, serviceName)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "show consumergroup (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Action: func(c *cli.Context) {
						toolscommon.ReadConsumerGroup(c, serviceName)
					},
				},
				{
					Name:    "message",
					Aliases: []string{"m"},
					Usage:   "show message <extent_uuid> <address>",
					Action: func(c *cli.Context) {
						toolscommon.ReadMessage(c, serviceName)
					},
				},
				{
					Name:    "dlq",
					Aliases: []string{"dl"},
					Usage:   "show dlq <uuid>",
					Action: func(c *cli.Context) {
						toolscommon.ReadDlq(c, serviceName)
					},
				},
				{
					Name:    "cgBacklog",
					Aliases: []string{"cgb", "cb"},
					Usage:   "show cgBacklog (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Action: func(c *cli.Context) {
						toolscommon.ReadCgBacklog(c, serviceName)
					},
				},
			},
		},
		{
			Name:    "update",
			Aliases: []string{"u"},
			Usage:   "update (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "update destination <name>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Usage: "status: enabled | disabled | sendonly | recvonly",
						},
						cli.IntFlag{
							Name:  "consumed_messages_retention, cr",
							Usage: "Consumed messages retention period specified in seconds. Default is one hour.",
						},
						cli.IntFlag{
							Name:  "unconsumed_messages_retention, ur",
							Usage: "Unconsumed messages retention period specified in seconds. Default is two hours.",
						},
						cli.StringFlag{
							Name:  "checksum_option, co",
							Usage: "Checksum_options, can be one of the crcIEEE, md5",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Usage: "The updated owner's email",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi_zone destinations. Format for each zone should be \"ZoneName,AllowPublish,AllowConsume,ReplicaCount\". For example: \"zone1,true,true,3\"",
						},
					},
					Action: func(c *cli.Context) {
						toolscommon.UpdateDestination(c, cliHelper, serviceName)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "update consumergroup (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Usage: "status: enabled | disabled",
						},
						cli.IntFlag{
							Name:  "lock_timeout_seconds, l",
							Usage: strLockTimeoutSeconds,
						},
						cli.IntFlag{
							Name:  "max_delivery_count, m",
							Usage: strMaxDeliveryCount,
						},
						cli.IntFlag{
							Name:  "skip_older_messages_in_seconds, k",
							Usage: strSkipOlderMessagesInSeconds,
						},
						cli.IntFlag{
							Name:  "delay_seconds, d",
							Usage: strDelaySeconds,
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Usage: "The updated owner's email",
						},
						cli.StringFlag{
							Name:  "active_zone, az",
							Usage: "The updated active zone",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi_zone consumer group. Format for each zone should be \"ZoneName,PreferedActiveZone\". For example: \"zone1,false\"",
						},
					},
					Action: func(c *cli.Context) {
						toolscommon.UpdateConsumerGroup(c, cliHelper, serviceName)
					},
				},
			},
		},
		{
			Name:    "delete",
			Aliases: []string{"d"},
			Usage:   "delete (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "delete destination <name>",
					Action: func(c *cli.Context) {
						toolscommon.DeleteDestination(c, serviceName)
						println("deleted destination: ", c.Args().First())
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "delete consumergroup [<destination_path>|<DLQ_UUID>] <consumer_group_name>",
					Action: func(c *cli.Context) {
						toolscommon.DeleteConsumerGroup(c, serviceName)
						println("deleted consumergroup: ", c.Args()[0], c.Args()[1])
					},
				},
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l", "ls"},
			Usage:   "list (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "list destination [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "prefix, pf",
							Value: "/",
							Usage: "only show destinations of prefix",
						},
						cli.StringFlag{
							Name:  "status, s",
							Value: "",
							Usage: "status: enabled | disabled | sendonly | recvonly, if empty, return all",
						},
					},
					Action: func(c *cli.Context) {
						toolscommon.ListDestinations(c, serviceName)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "list consumergroup <destination_path> [<consumer_group>]",
					Action: func(c *cli.Context) {
						toolscommon.ListConsumerGroups(c, serviceName)
					},
				},
			},
		},
		{
			Name:    "publish",
			Aliases: []string{"p", "pub", "w", "write"},
			Usage:   "publish <destination_name>",
			Action: func(c *cli.Context) {
				toolscommon.Publish(c, serviceName)
			},
		},
		{
			Name:    "consume",
			Aliases: []string{"sub", "r", "read"},
			Usage:   "consume <destination_name> <consumer_group_name> [options]",
			Flags: []cli.Flag{
				cli.BoolTFlag{
					Name:  "autoack, a",
					Usage: "automatically ack each message as it's printed",
				},
				cli.IntFlag{
					Name:  "prefetch_count, p",
					Value: 1,
					Usage: "prefetch count",
				},
			},
			Action: func(c *cli.Context) {
				toolscommon.Consume(c, serviceName)
			},
		},
		{
			Name:    "merge_dlq",
			Aliases: []string{"mdlq"},
			Usage:   "merge_dlq  (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",

			Action: func(c *cli.Context) {
				toolscommon.MergeDLQForConsumerGroup(c, serviceName)
			},
		},
		{
			Name:    "purge_dlq",
			Aliases: []string{"pdlq"},
			Usage:   "purge_dlq  (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",

			Action: func(c *cli.Context) {
				toolscommon.PurgeDLQForConsumerGroup(c, serviceName)
			},
		},
	}
}
