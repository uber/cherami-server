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

package metrics

import (
	"fmt"
	"time"

	gometrics "github.com/rcrowley/go-metrics"
	"github.com/uber-common/bark"
)

const (
	delayToFirstLogDump  = time.Second * 15 // PLACEHOLDER
	delayBetweenLogDumps = time.Minute * 1  // PLACEHOLDER
	delayBetweenExports  = time.Second * 10
)

// goMetricsExporter allows an rcrowley/go-metrics registry to be periodically exported to the Cherami metrics system
type goMetricsExporter struct {
	registry            gometrics.Registry // Registry to be exported
	metricNameToMetric  map[string]int     // maps a metric name to the metric symbol, e.g. "request-rate" -> OutputhostCGKafkaRequestRate
	metricNameToLastVal map[string]int64
	c                   Client
	scope               int
	l                   bark.Logger
	lastLogDump         time.Time
	closeCh             <-chan struct{}
}

// NewGoMetricsExporter starts the exporter loop to export go-metrics to the Cherami metrics system and returns the go-metrics
// registry that should be used
func NewGoMetricsExporter(
	c Client,
	scope int,
	metricNameToMetric map[string]int,
	l bark.Logger,
	closeCh <-chan struct{},
) gometrics.Registry {
	registry := gometrics.NewRegistry()
	r := &goMetricsExporter{
		c:                   c,
		scope:               scope,
		registry:            registry,
		metricNameToMetric:  metricNameToMetric,
		l:                   l,
		lastLogDump:         time.Now().Add(-1 * delayBetweenLogDumps).Add(delayToFirstLogDump),
		closeCh:             closeCh,
		metricNameToLastVal: make(map[string]int64),
	}
	go r.run()
	return registry
}

func (r *goMetricsExporter) run() {
	t := time.NewTicker(delayBetweenExports)
	defer t.Stop()

exportLoop:
	for {
		r.export()

		select {
		case <-t.C:
		case <-r.closeCh:
			break exportLoop
		}
	}
}

func (r *goMetricsExporter) export() {
	r.registry.Each(func(name string, i interface{}) {
		if metricID, ok := r.metricNameToMetric[name]; ok {
			switch metric := i.(type) {
			case gometrics.Histogram:
				// The timer metric type most closely approximates the histogram type. Milliseconds is the default unit, so
				// we scale our values as such. Extract the values from the registry, since we aggregate on our own.
				for _, v := range metric.Snapshot().Sample().Values() {
					if v == 0 { // Zero values are sometimes emitted, due to an issue with go-metrics
						continue
					}
					r.c.RecordTimer(r.scope, metricID, time.Duration(v)*time.Millisecond)
				}

				metric.Clear()
			case gometrics.Meter:
				// Nominally, the meter is a rate-type metric, but we can extract the underlying count and let our reporter aggregate
				// Last value is needed because our Cherami counter is incremental only
				count := metric.Snapshot().Count()
				lastVal := r.metricNameToLastVal[name]
				r.metricNameToLastVal[name] = count
				r.c.AddCounter(r.scope, metricID, count-lastVal)
			default:
				r.l.WithField(`type`, fmt.Sprintf("%T", i)).Error("unable to record metric")
			}
		} else {
			if time.Since(r.lastLogDump) >= delayBetweenLogDumps {
				r.lastLogDump = time.Now()
				switch metric := i.(type) {
				case gometrics.Histogram:
					s := metric.Snapshot()
					r.l.WithFields(bark.Fields{
						`max`:  s.Max(),
						`mean`: s.Mean(),
						`min`:  s.Min(),
						`sum`:  s.Sum(),
						`p99`:  s.Percentile(99),
						`p95`:  s.Percentile(95),
						`name`: name,
					}).Info(`unexported histogram metric`)
					metric.Clear()
				case gometrics.Meter:
					s := metric.Snapshot()
					r.l.WithFields(bark.Fields{
						`avg1Min`:  s.Rate1(),
						`avg5Min`:  s.Rate5(),
						`avg15Min`: s.Rate15(),
						`count`:    s.Count(),
						`name`:     name,
					}).Info(`unexported meter metric`)
				default:
					r.l.WithField(`type`, fmt.Sprintf("%T", i)).Error("unable to record unexported metric")
				}
			}
		}
	})
}
