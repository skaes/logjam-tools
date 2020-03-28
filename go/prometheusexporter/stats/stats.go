package stats

import (
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	//	promclient "github.com/prometheus/client_model/go"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/util"
)

// Stats collects prometheus exporter statistics. The various compoments of the
// exporter update the stats using atomic.SwapUint64 on its members.
var Stats struct {
	Processed            uint64 // number of ZeroMQ messages processed
	ProcessedBytes       uint64 // msg bytes processed
	Dropped              uint64 // number of messages dropped
	Missed               uint64 // number if messages dropped by the zeroMQ SUB socket
	Observed             uint64 // number of observed metrics
	Ignored              uint64 // number of invalid messages
	Raw                  int64  // number of messages not yet decompressed and parsed
	Invisible            int64  // number of messages not yet observed by the prometheus collectors
	EmptyMetricsResponse uint64 // number of erroneously empty metrics responses
	UnknownStreams       uint64 // number of messages with unknown streams
}

var (
	registry              *prometheus.Registry
	unknownStreamsChannel chan string
	unknownStreamsMap     map[string]uint64
)

func init() {
	unknownStreamsChannel = make(chan string, 10000)
	unknownStreamsMap = make(map[string]uint64)
}

// RegisterUnknownStream register an unknown stream with the stats collector, so that it
// will be printed only once per time interval.
func RegisterUnknownStream(streamName string) {
	atomic.AddUint64(&Stats.UnknownStreams, 1)
	unknownStreamsChannel <- streamName
}

// RequestHandler handles /metrics route of promethus exporter
var RequestHandler http.Handler

var promStats struct {
	Processed            prometheus.Counter
	ProcessedBytes       prometheus.Counter
	Dropped              prometheus.Counter
	Missed               prometheus.Counter
	Observed             prometheus.Counter
	Ignored              prometheus.Counter
	Raw                  prometheus.Gauge
	Invisible            prometheus.Gauge
	EmptyMetricsResponse prometheus.Counter
	UnknownStreams       prometheus.Counter
}

func initializePromStats() {
	registry = prometheus.NewRegistry()
	promStats.Processed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_received_total",
		Help: "How many logjam messages have been received by this exporter",
	})
	promStats.ProcessedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_received_bytes_total",
		Help: "How many bytes of logjam messages have been received by this exporter",
	})
	promStats.Dropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_dropped_total",
		Help: "How many logjam messages were dropped by this exporter",
	})
	promStats.Missed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_missed_total",
		Help: "How many logjam messages have been missed by this exporter",
	})
	promStats.Observed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_observed_total",
		Help: "How many logjam messages have been observed by this exporter",
	})
	promStats.Ignored = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_ignored_total",
		Help: "How many logjam messages were ignored by this exporter",
	})
	promStats.Raw = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logjam:exporter:msgs_raw",
		Help: "How many logjam messages are waiting to be parsed/decompressed by this exporter",
	})
	promStats.Invisible = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logjam:exporter:msgs_invisible",
		Help: "How many logjam messages are waiting to be recorded by this exporter",
	})
	promStats.EmptyMetricsResponse = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:empty_metrics_response",
		Help: "How many times a metrics request erroneously produced an empty response",
	})
	promStats.UnknownStreams = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logjam:exporter:msgs_unknown_streams_total",
		Help: "How many logjam messages were ignored by this exporter because their stream name was unknown",
	})
	registry.MustRegister(promStats.Processed)
	registry.MustRegister(promStats.ProcessedBytes)
	registry.MustRegister(promStats.Dropped)
	registry.MustRegister(promStats.Missed)
	registry.MustRegister(promStats.Observed)
	registry.MustRegister(promStats.Ignored)
	registry.MustRegister(promStats.Raw)
	registry.MustRegister(promStats.Invisible)
	registry.MustRegister(promStats.EmptyMetricsResponse)
	registry.MustRegister(promStats.UnknownStreams)
	RequestHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

const (
	heartbeatInterval            = 5
	unknownStreamsReportInterval = 60
)

// ReporterAndWatchdog reports exporter stats and aborts the exporter if no message has
// bee processed for some time. This works because logjam devices send heartbeats. The
// exporter starts it as a go routine.
func ReporterAndWatchdog(abortAfter uint, verbose bool) {
	initializePromStats()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	initialCredit := uint64(abortAfter / heartbeatInterval)
	credit := initialCredit
	ticks := 0
	processedSinceLastHeartbeat := uint64(0)
	for !util.Interrupted() {
		select {
		case stream := <-unknownStreamsChannel:
			unknownStreamsMap[stream]++
			if verbose {
				log.Warn("unknown stream: %s", stream)
			}
		case <-ticker.C:
			ticks++
			_observed := atomic.SwapUint64(&Stats.Observed, 0)
			_processed := atomic.SwapUint64(&Stats.Processed, 0)
			_processedBytes := atomic.SwapUint64(&Stats.ProcessedBytes, 0)
			_dropped := atomic.SwapUint64(&Stats.Dropped, 0)
			_missed := atomic.SwapUint64(&Stats.Missed, 0)
			_ignored := atomic.SwapUint64(&Stats.Ignored, 0)
			_raw := atomic.LoadInt64(&Stats.Raw)
			_invisible := atomic.LoadInt64(&Stats.Invisible)
			_emptyMetrics := atomic.SwapUint64(&Stats.EmptyMetricsResponse, 0)
			_unknownStreams := atomic.SwapUint64(&Stats.UnknownStreams, 0)

			promStats.Observed.Add(float64(_observed))
			promStats.Processed.Add(float64(_processed))
			promStats.ProcessedBytes.Add(float64(_processedBytes))
			promStats.Ignored.Add(float64(_ignored))
			promStats.Dropped.Add(float64(_dropped))
			promStats.Missed.Add(float64(_missed))
			promStats.Raw.Set(float64(_raw))
			promStats.Invisible.Set(float64(_invisible))
			promStats.UnknownStreams.Add(float64(_unknownStreams))

			log.Info("processed: %d, bytes: %d, ignored: %d, observed %d, dropped: %d, missed: %d, raw: %d, invisible: %d, empty: %d, unknown streams: %d",
				_processed, _processedBytes, _ignored, _observed, _dropped, _missed, _raw, _invisible, _emptyMetrics, _unknownStreams)

			processedSinceLastHeartbeat += _processed
			if ticks%heartbeatInterval == 0 {
				if processedSinceLastHeartbeat > 0 {
					credit = initialCredit
				} else {
					credit--
					if credit == 0 {
						log.Error("no credit left. aborting process.")
						os.Exit(1)
					} else if credit < initialCredit-1 {
						log.Info("credit left: %d", credit)
					}
				}
				processedSinceLastHeartbeat = 0
			}
			if ticks%unknownStreamsReportInterval == 0 {
				for k, v := range unknownStreamsMap {
					log.Info("unknown stream: %s (%d messages)", k, v)
				}
				unknownStreamsMap = make(map[string]uint64)
			}
		}
	}
}
