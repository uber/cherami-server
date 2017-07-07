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

package main

import (
	"os"

	"github.com/codegangsta/cli"
	lib "github.com/uber/cherami-server/cmd/tools/common"
	"github.com/uber/cherami-server/tools/admin"
)

const (
	adminToolService = "cherami-admin"
)

func main() {
	app := cli.NewApp()
	app.Name = "cherami"
	app.Usage = "A command-line tool for cherami developer, including debugging tool"
	app.Version = "1.2.1"
	app.Flags = lib.GetCommonFlags()
	app.Flags = append(app.Flags, cli.BoolTFlag{
		Name:  "admin_mode",
		Usage: "use admin mode (bypass range checking for input arguments)",
	})

	app.Commands = lib.GetCommonCommands(adminToolService)

	showCommand := getCommand(app.Commands, "show")
	showCommand.Usage = "show (destination | consumergroup | extent | storehost | message | dlq | cgAckID | cgqueue | destqueue | cgBacklog)"
	showCommand.Subcommands = append(showCommand.Subcommands, []cli.Command{
		{
			Name:    "extent",
			Aliases: []string{"e"},
			Usage:   "show extent <extent_uuid>",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "showcg, sc",
					Value: "false",
					Usage: "show consumer group(false, true), default to false",
				},
			},
			Action: func(c *cli.Context) {
				admin.ReadExtent(c)
			},
		},
		{
			Name:    "storehost",
			Aliases: []string{"s"},
			Usage:   "show storehost <storehostAddr>",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "top, tp",
					Value: 5,
					Usage: "show the top k heavy extents in this storehost",
				},
			},
			Action: func(c *cli.Context) {
				admin.ReadStoreHost(c)
			},
		}, {
			Name:    "cgAckID",
			Aliases: []string{"aid"},
			Usage:   "show cgAckID <cgAckID>",
			Action: func(c *cli.Context) {
				admin.ReadCgAckID(c)
			},
		},
		{
			Name:    "cgqueue",
			Aliases: []string{"cq", "cgq"},
			Usage:   "show cgqueue (<consumer_group_uuid> | <consumer_group_uuid> <extent_uuid>)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "status, s",
					Value: "",
					Usage: "status: open | consumed | deleted, if empty, return all",
				},
			},
			Action: func(c *cli.Context) {
				admin.ReadCgQueue(c)
			},
		},
		{
			Name:    "destqueue",
			Aliases: []string{"dq", "destq"},
			Usage:   "show destqueue (<destination_uuid> | <destination_path>)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "status, s",
					Value: "open",
					Usage: "status: open | sealed | consumed archived | deleted, if empty, return all",
				},
			},
			Action: func(c *cli.Context) {
				admin.ReadDestQueue(c)
			},
		},
	}...)

	listCommand := getCommand(app.Commands, "list")
	listCommand.Usage = "list (destination | consumergroup | extents | consumergroupextents | hosts)"
	listCommand.Subcommands = append(listCommand.Subcommands, []cli.Command{
		{
			Name:    "extents",
			Aliases: []string{"e", "es"},
			Usage:   "list extents <destination_path>",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "prefix, pf",
					Usage: "only show extents of prefix",
				},
			},
			Action: func(c *cli.Context) {
				admin.ListExtents(c)
			},
		},
		{
			Name:    "consumergroupextents",
			Aliases: []string{"cge", "cges"},
			Usage:   "list consumergroupextents <destination_path> <consumergroup_path>",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "limit, lm",
					Value: 10,
					Usage: "show top n consumer group extents",
				},
			},
			Action: func(c *cli.Context) {
				admin.ListConsumerGroupExtents(c)
			},
		},
		{
			Name:    "hosts",
			Aliases: []string{"h", "hs"},
			Usage:   "list hosts [options] ",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "service, s",
					Usage: "only show hosts of service(input,output,frontend,store,controller)",
				},
				cli.StringFlag{
					Name:  "type, t",
					Value: "active",
					Usage: "show hosts from specific table(active, history), default to active",
				},
			},
			Action: func(c *cli.Context) {
				admin.ListHosts(c)
			},
		},
	}...)

	app.Commands = append(app.Commands, []cli.Command{
		{
			Name:    "listAll",
			Aliases: []string{"la", "lsa"},
			Usage:   "listAll (destination)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst"},
					Usage:   "listAll destination [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "include, id",
							Value: "",
							Usage: "only show destinations match include regexp",
						},
						cli.StringFlag{
							Name:  "exclude, ed",
							Value: "",
							Usage: "only show destinations not match excluded regexp",
						},
						cli.StringFlag{
							Name:  "status, s",
							Value: "",
							Usage: "status: enabled | disabled | sendonly | recvonly | deleting | deleted, if empty, return all",
						},
					},
					Action: func(c *cli.Context) {
						admin.ListAllDestinations(c)
					},
				},
			},
		},
		{
			Name:    "uuid2hostport",
			Aliases: []string{"u2h"},
			Usage:   "uuid2hostport <uuid>",
			Action: func(c *cli.Context) {
				admin.UUID2hostAddr(c)
			},
		},
		{
			Name:    "audit",
			Aliases: []string{"at"},
			Usage:   "audit",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "uuid",
					Value: "",
					Usage: "destination uuid",
				},
				cli.StringFlag{
					Name:  "name",
					Value: "",
					Usage: "destination path",
				},
				cli.IntFlag{
					Name:  "limit",
					Value: 100,
					Usage: "maximum returned ops number",
				},
			},
			Action: func(c *cli.Context) {
				admin.ListEntityOps(c)
			},
		},
		{
			Name:    "hostport2uuid",
			Aliases: []string{"h2u"},
			Usage:   "hostport2uuid <host:port>",
			Action: func(c *cli.Context) {
				admin.HostAddr2uuid(c)
			},
		},
		{
			Name:    "serviceconfig",
			Aliases: []string{"cfg"},
			Usage:   "serviceconfig (get|set|delete)",
			Description: `Manage service configs. Supported service names, config keys and usage are:
"cherami-controllerhost", "numPublisherExtentsByPath",      comma-separated list of "prefix=extent_count"
"cherami-controllerhost", "numConsumerExtentsByPath",       comma-separated list of "prefix=extent_count"
"cherami-controllerhost", "numRemoteConsumerExtentsByPath", comma-separated list of "prefix=extent_count"
"cherami-controllerhost", "activeZone",                     the active zone for multi-zone consumers
"cherami-controllerhost", "failoverMode",                   "enabled" or "disabled"
"cherami-storehost",      "adminStatus",                    if set to anything other than "enabled", will prevent placement of new extent on this store
"cherami-storehost",      "minFreeDiskSpaceBytes",          integer, minimum required free disk space in bytes to place a new extent
"cherami-outputhost",     "messagecachesize",               comma-separated list of "destination/CG_name=value" for message cache size
			`,
			Subcommands: []cli.Command{
				{
					Name:    "get",
					Aliases: []string{"g"},
					Usage:   "serviceconfig get <service-name> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "key, k",
							Value: "",
							Usage: "The config key whose value is to be fetched",
						},
					},
					Action: func(c *cli.Context) {
						admin.GetServiceConfig(c)
					},
				},
				{
					Name:    "set",
					Aliases: []string{"s"},
					Usage:   "serviceconfig set <service-name.version.sku.hostname.config-key> <config-value>",
					Action: func(c *cli.Context) {
						admin.SetServiceConfig(c)
					},
				},
				{
					Name:    "delete",
					Aliases: []string{"d"},
					Usage:   "serviceconfig delete <service-name.version.sku.hostname.config-key>",
					Action: func(c *cli.Context) {
						admin.DeleteServiceConfig(c)
					},
				},
			},
		},
		{
			Name:    "outputhost",
			Aliases: []string{"oh"},
			Usage:   "outputhost (cgstate|listAllCgs|unloadcg)",
			Subcommands: []cli.Command{
				{
					Name:    "cgstate",
					Aliases: []string{"cgs"},
					Usage:   "outputhost cgstate <hostport> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "cg_uuid, cg",
							Value: "",
							Usage: "The UUID of the consumer group whose state will be dumped",
						},
					},
					Action: func(c *cli.Context) {
						admin.GetCgState(c)
					},
				},
				{
					Name:    "listAllCgs",
					Aliases: []string{"ls"},
					Usage:   "outputhost listAllCgs <hostport>",
					Action: func(c *cli.Context) {
						admin.ListAllCgs(c)
					},
				},
				{
					Name:    "unloadcg",
					Aliases: []string{"uc"},
					Usage:   "outputhost unloadcg <hostport> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "cg_uuid, cg",
							Value: "",
							Usage: "The consumergroup UUID which should be unloaded",
						},
					},
					Action: func(c *cli.Context) {
						admin.UnloadConsumerGroup(c)
					},
				},
			},
		},
		{
			Name:    "inputhost",
			Aliases: []string{"ih"},
			Usage:   "inputhost (deststate|listAllDests|unloaddest)",
			Subcommands: []cli.Command{
				{
					Name:    "deststate",
					Aliases: []string{"dests"},
					Usage:   "inputhost deststate <hostport> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "dest_uuid, dest",
							Value: "",
							Usage: "The UUID of the destination whose state will be dumped",
						},
					},
					Action: func(c *cli.Context) {
						admin.GetDestinationState(c)
					},
				},
				{
					Name:    "listAllDests",
					Aliases: []string{"ls"},
					Usage:   "inputhost listAllDests <hostport>",
					Action: func(c *cli.Context) {
						admin.ListAllLoadedDestinations(c)
					},
				},
				{
					Name:    "unloaddest",
					Aliases: []string{"ud"},
					Usage:   "inputhost unloaddest <hostport> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "dest_uuid, dest",
							Value: "",
							Usage: "The destination UUID which should be unloaded",
						},
					},
					Action: func(c *cli.Context) {
						admin.UnloadDestination(c)
					},
				},
			},
		},
		{
			Name:    "seal-check",
			Aliases: []string{"sc"},
			Usage:   "seal-check <dest> [-seal]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "prefix, pf",
					Value: "/",
					Usage: "only process destinations with prefix",
				},
				cli.BoolFlag{
					Name:  "seal",
					Usage: "seal extents on replica that are not sealed",
				},
				cli.BoolFlag{
					Name:  "verbose, v",
					Usage: "verbose output",
				},
				cli.BoolFlag{
					Name:  "veryverbose, vv",
					Usage: "very verbose output",
				},
			},
			Action: func(c *cli.Context) {
				admin.SealConsistencyCheck(c)
			},
		},
		{
			Name:    "store-seal",
			Aliases: []string{"seal"},
			Usage:   "seal <store_uuid> <extent_uuid> [<seqnum>]",
			Action: func(c *cli.Context) {
				admin.StoreSealExtent(c)
			},
		},
		{
			Name:    "store-isextentsealed",
			Aliases: []string{"issealed"},
			Usage:   "issealed <store_uuid> <extent_uuid>",
			Action: func(c *cli.Context) {
				admin.StoreIsExtentSealed(c)
			},
		},
		{
			Name:    "store-gaft",
			Aliases: []string{"gaft"},
			Usage:   "gaft <store_uuid> <extent_uuid> <timestamp>",
			Action: func(c *cli.Context) {
				admin.StoreGetAddressFromTimestamp(c)
			},
		},
		{
			Name:    "store-purgeextent",
			Aliases: []string{"purge"},
			Usage:   "purge <store_uuid> <extent_uuid> [<address> | -entirely]",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "entirely",
					Usage: "deletes extent entirely",
				},

				cli.Int64Flag{
					Name:  "address, a",
					Value: 0,
					Usage: "address to delete upto",
				},
			},
			Action: func(c *cli.Context) {
				admin.StorePurgeMessages(c)
			},
		},
		{
			Name:    "store-listextents",
			Aliases: []string{"lsx"},
			Usage:   "store-listextents <store_uuid>",
			Action: func(c *cli.Context) {
				admin.StoreListExtents(c)
			},
		},
	}...)

	app.Run(os.Args)
}

func getCommand(commands []cli.Command, name string) *cli.Command {
	for _, command := range commands {
		if command.Name == name {
			return &command
		}
	}
	return &cli.Command{}
}
