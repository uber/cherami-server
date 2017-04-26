package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-server/storage/manyrocks"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/Sirupsen/logrus"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/uber-common/bark"
)

type arguments struct {
	extent     string
	baseDir    string
	start      storage.Key
	end        storage.Key
	num        int
	printVal   bool
	formatTime bool
}

var log = bark.NewLoggerFromLogrus(logrus.StandardLogger())

func main() {

	args := getArgs()

	if args == nil {
		return
	}

	db, err := manyrocks.OpenExtentDB(storage.ExtentUUID(args.extent), fmt.Sprintf("%s/%s", args.baseDir, args.extent), log)

	if err != nil {
		fmt.Printf("error opening db (%s): %v\n", args.baseDir, err)
		return
	}

	defer db.CloseExtentDB()

	var num int

	addr, key, err := db.SeekCeiling(args.start)

	if err != nil {
		fmt.Printf("db.SeekCeiling(%x) error: %v\n", args.start, err)
		return
	}

	var val storage.Value

	if args.printVal {

		_, val, _, _, err = db.Get(addr)

		if err != nil {
			fmt.Printf("db.Get(%x) errored: %v\n", addr, err)
			return
		}
	}

	for (addr != storage.EOX) && (key < args.end) && (num < args.num) {

		// fmt.Printf("key = %d %x %v %p\n", key, key, key, key)

		if isSealExtentKey(key) {

			sealSeqNum := deconstructSealExtentKey(key)

			if sealSeqNum == seqNumUnspecifiedSeal {
				fmt.Printf("0x%016v => SEALED (seqnum: unspecified)\n", key)
			} else {
				fmt.Printf("0x%016v => SEALED (seqnum: %d)\n", key, sealSeqNum)
			}

		} else {

			ts, seqnum := deconstructKey(key)

			if args.printVal {

				msg, errd := deserializeMessage(val)

				enq := msg.GetEnqueueTimeUtc()

				var enqTime string

				var payload string

				if errd != nil { // corrupt message?
					payload = fmt.Sprintf("ERROR deserializing data: %v (val=%v)", errd, val)
				} else {

					if args.formatTime {
						enqTime = time.Unix(0, enq).Format(time.RFC3339Nano)
					} else {
						enqTime = strconv.FormatInt(enq, 16)
					}

					payload = fmt.Sprintf("seq=%d enq=%v data=%d bytes",
						msg.GetSequenceNumber(), enqTime, len(msg.GetPayload().GetData()))
					// payload = msg.String()
				}

				var vTime string

				if args.formatTime {
					vTime = time.Unix(0, ts).Format(time.RFC3339Nano)
				} else {
					vTime = strconv.FormatInt(ts, 16)
				}

				fmt.Printf("0x%016v => #%d ts=%v payload:[%v]\n", key, seqnum, vTime, payload)

			} else {

				fmt.Printf("0x%016v => #%d ts=%v\n", key, seqnum, ts)
			}
		}

		num++

		if args.printVal {

			key, val, addr, _, err = db.Get(addr)

			if err != nil {
				fmt.Printf("db.Get(%x) errored: %v\n", addr, err)
				break
			}

		} else {

			addr, key, err = db.Next(addr)

			if err != nil {
				fmt.Printf("db.Next(%x) errored: %v\n", addr, err)
				break
			}
		}

	}

	fmt.Printf("summary: dumped %d keys in range [%v, %v)\n", num, args.start, args.end)
	return
}

func getArgs() (args *arguments) {

	args = &arguments{}

	flag.StringVar(&args.extent, "x", "", "extent")
	flag.StringVar(&args.baseDir, "base", ".", "base dir")

	var start, end, num string

	flag.StringVar(&start, "s", "-1", "start range")
	flag.StringVar(&end, "e", "-1", "end range")
	flag.StringVar(&num, "n", "-1", "number of values")

	flag.BoolVar(&args.printVal, "v", false, "deserialize payload")
	flag.BoolVar(&args.formatTime, "t", false, "format time")

	flag.Parse()

	switch {
	case args.extent == "":
		cwd, _ := os.Getwd()

		args.extent = filepath.Base(cwd)
		args.baseDir = filepath.Dir(cwd)

	case args.baseDir == "":
		args.baseDir, _ = os.Getwd()
	}

	switch i, err := strconv.ParseInt(start, 0, 64); {
	case err != nil:
		fmt.Printf("error parsing start arg (%s): %v\n", start, err)
		return nil
	case i < 0:
		args.start = 0
	default:
		args.start = storage.Key(i)
	}

	switch i, err := strconv.ParseInt(end, 0, 64); {
	case err != nil:
		fmt.Printf("error parsing end arg (%s): %v\n", end, err)
		return nil
	case i < 0:
		args.end = storage.Key(math.MaxInt64)
	default:
		args.end = storage.Key(i)
	}

	switch i, err := strconv.ParseInt(num, 0, 64); {
	case err != nil:
		fmt.Printf("error parsing num arg (%s): %v\n", num, err)
		return nil
	case i < 0:
		args.num = math.MaxInt64
	default:
		args.num = int(i)
	}

	// fmt.Printf("args=%v\n", args)

	return args
}

// -- decode message/address -- //
const (
	seqNumBits = 26

	invalidKey = math.MaxInt64

	seqNumBitmask    = (int64(1) << seqNumBits) - 1
	timestampBitmask = math.MaxInt64 &^ seqNumBitmask
	seqNumMax        = int64(math.MaxInt64-2) & seqNumBitmask

	seqNumUnspecifiedSeal = int64(math.MaxInt64 - 1)
)

func deconstructKey(key storage.Key) (visibilityTime int64, seqNum int64) {
	return int64(int64(key) & timestampBitmask), int64(int64(key) & seqNumBitmask)
}

func deconstructSealExtentKey(key storage.Key) (seqNum int64) {

	seqNum = int64(key) & seqNumBitmask

	// we use the special seqnum ('MaxInt64 - 1') when the extent has been sealed
	// at an "unspecified" seqnum; check for this case, and return appropriate value
	if seqNum == (seqNumBitmask - 1) {
		seqNum = seqNumUnspecifiedSeal
	}

	return
}

func isSealExtentKey(key storage.Key) bool {
	return key != storage.InvalidKey && (int64(key)&timestampBitmask) == timestampBitmask
}

func deserializeMessage(data []byte) (*store.AppendMessage, error) {
	msg := &store.AppendMessage{}
	deserializer := thrift.NewTDeserializer()
	if err := deserializer.Read(msg, data); err != nil {
		return nil, err
	}

	return msg, nil
}
