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

package replicator

import (
	"sync"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

type (
	inConnection struct {
		extUUID string
		stream  storeStream.BStoreOpenReadStreamInCall
		msgCh   <-chan *store.ReadMessageContent

		logger     bark.Logger
		m3Client   metrics.Client
		metricsTag int

		closeChannel         chan struct{} // channel to indicate the connection should be closed
		creditsCh            chan int32    // channel to pass credits from readCreditsStream to writeMsgsStream
		creditFlowExpiration time.Time     // credit expiration is used to close the stream if we don't receive any credit for some period of time

		lk     sync.Mutex
		opened bool
		closed bool
	}
)

const (
	creditFlowTimeout = 10 * time.Minute

	flushTimeout = 50 * time.Millisecond
)

func newInConnection(extUUID string, stream storeStream.BStoreOpenReadStreamInCall, msgCh <-chan *store.ReadMessageContent, logger bark.Logger, m3Client metrics.Client, metricsTag int) *inConnection {
	conn := &inConnection{
		extUUID:              extUUID,
		stream:               stream,
		msgCh:                msgCh,
		logger:               logger.WithField(common.TagExt, extUUID).WithField(`scope`, "inConnection"),
		m3Client:             m3Client,
		metricsTag:           metricsTag,
		closeChannel:         make(chan struct{}),
		creditsCh:            make(chan int32, 5),
		creditFlowExpiration: time.Now().Add(creditFlowTimeout),
	}

	return conn
}

func (conn *inConnection) open() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if !conn.opened {
		go conn.writeMsgsStream()
		go conn.readCreditsStream()

		conn.opened = true
	}
	conn.logger.Info("in connection opened")
}

func (conn *inConnection) close() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if !conn.closed {
		close(conn.closeChannel)
		conn.closed = true
	}
	conn.logger.Info("in connection closed")
}

func (conn *inConnection) readCreditsStream() {
	for {
		select {
		case <-conn.closeChannel:
			return
		default:
			msg, err := conn.stream.Read()
			if err != nil {
				conn.logger.WithField(common.TagErr, err).Info("read credit failed")
				go conn.close()
				return
			}

			conn.m3Client.AddCounter(conn.metricsTag, metrics.ReplicatorInConnCreditsReceived, int64(msg.GetCredits()))

			// send this to writeMsgsPump which keeps track of the local credits
			// Make this non-blocking because writeMsgsStream could be closed before this
			select {
			case conn.creditsCh <- msg.GetCredits():
			default:
				conn.logger.
					WithField(`channelLen`, len(conn.creditsCh)).
					WithField(`credits`, msg.GetCredits()).
					Warn(`Dropped credits because of blocked channel`)
			}
		}
	}
}

func (conn *inConnection) writeMsgsStream() {
	defer conn.stream.Done()

	flushTicker := time.NewTicker(flushTimeout)
	defer flushTicker.Stop()

	var localCredits int32
	for {
		if localCredits == 0 {
			select {
			case credit := <-conn.creditsCh:
				conn.extentCreditExpiration()
				localCredits += credit
			case <-time.After(creditFlowTimeout):
				conn.logger.Warn("credit flow timeout")
				if conn.isCreditFlowExpired() {
					conn.logger.Warn("credit flow expired")
					go conn.close()
				}
			case <-conn.closeChannel:
				return
			}
		} else {
			select {
			case msg := <-conn.msgCh:
				if err := conn.stream.Write(msg); err != nil {
					conn.logger.Error("write msg failed")
					go conn.close()
					return
				}

				if msg.GetType() == store.ReadMessageContentType_SEALED {
					conn.logger.Info(`sealed msg read`)
				}

				conn.m3Client.IncCounter(conn.metricsTag, metrics.ReplicatorInConnMsgWritten)
				localCredits--
			case credit := <-conn.creditsCh:
				conn.extentCreditExpiration()
				localCredits += credit
			case <-flushTicker.C:
				if err := conn.stream.Flush(); err != nil {
					conn.logger.Error(`flush msg failed`)
					go conn.close()
				}
			case <-conn.closeChannel:
				return
			}
		}
	}
}

func (conn *inConnection) isCreditFlowExpired() (ret bool) {
	ret = conn.creditFlowExpiration.Before(time.Now())
	return
}

func (conn *inConnection) extentCreditExpiration() {
	conn.creditFlowExpiration = time.Now().Add(creditFlowTimeout)
}
