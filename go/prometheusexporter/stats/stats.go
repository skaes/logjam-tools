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

// Metrics is a collection on exporter statistics variables.
type Metrics struct {
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

// Stats collects prometheus exporter statistics. The various compoments of the
// exporter update the stats using atomic.SwapUint64 on its members.
var Stats Metrics

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
	Processed                  prometheus.Counter
	ProcessedBytes             prometheus.Counter
	Dropped                    prometheus.Counter
	Missed                     prometheus.Counter
	Observed                   prometheus.Counter
	Ignored                    prometheus.Counter
	Raw                        prometheus.Gauge
	Invisible                  prometheus.Gauge
	EmptyMetricsResponse       prometheus.Counter
	UnknownStreams             prometheus.Counter
	ScrapeDurationHistogramVec *prometheus.HistogramVec
	ScrapeDurationSummaryVec   *prometheus.HistogramVec
}

// ObserveScrapeDuration is called from the collectors to record scrape times.
func ObserveScrapeDuration(app, env string, d time.Duration) {
	labels := map[string]string{"app": app, "env": env}
	promStats.ScrapeDurationHistogramVec.With(labels).Observe(d.Seconds())
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
	promStats.ScrapeDurationHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logjam:exporter:scrape_duration_distribution_seconds",
			Help:    "logjam exporter scrape duration distribution by application and environment",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50},
		},
		[]string{"app", "env"},
	)
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
	registry.MustRegister(promStats.ScrapeDurationHistogramVec)
	RequestHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

const (
	heartbeatInterval            = 5
	unknownStreamsReportInterval = 60
)

func swapAndObserveMetrics() *Metrics {
	var m Metrics
	m.Observed = atomic.SwapUint64(&Stats.Observed, 0)
	m.Processed = atomic.SwapUint64(&Stats.Processed, 0)
	m.ProcessedBytes = atomic.SwapUint64(&Stats.ProcessedBytes, 0)
	m.Dropped = atomic.SwapUint64(&Stats.Dropped, 0)
	m.Missed = atomic.SwapUint64(&Stats.Missed, 0)
	m.Ignored = atomic.SwapUint64(&Stats.Ignored, 0)
	m.EmptyMetricsResponse = atomic.SwapUint64(&Stats.EmptyMetricsResponse, 0)
	m.UnknownStreams = atomic.SwapUint64(&Stats.UnknownStreams, 0)
	// gauges
	m.Raw = atomic.LoadInt64(&Stats.Raw)
	m.Invisible = atomic.LoadInt64(&Stats.Invisible)

	promStats.Observed.Add(float64(m.Observed))
	promStats.Processed.Add(float64(m.Processed))
	promStats.ProcessedBytes.Add(float64(m.ProcessedBytes))
	promStats.Dropped.Add(float64(m.Dropped))
	promStats.Missed.Add(float64(m.Missed))
	promStats.Ignored.Add(float64(m.Ignored))
	promStats.EmptyMetricsResponse.Add(float64(m.EmptyMetricsResponse))
	promStats.UnknownStreams.Add(float64(m.UnknownStreams))
	// gauges
	promStats.Raw.Set(float64(m.Raw))
	promStats.Invisible.Set(float64(m.Invisible))

	return &m
}

func (a *Metrics) mergeMetrics(b *Metrics) {
	a.Observed += b.Observed
	a.Processed += b.Processed
	a.ProcessedBytes += b.ProcessedBytes
	a.Dropped += b.Dropped
	a.Missed += b.Missed
	a.Ignored += b.Ignored
	a.EmptyMetricsResponse += b.EmptyMetricsResponse
	a.UnknownStreams += b.UnknownStreams
	// gauges
	a.Raw = b.Raw
	a.Invisible = b.Invisible
}

// ReporterAndWatchdog reports exporter stats and aborts the exporter if no message has
// bee processed for some time. This works because logjam devices send heartbeats. The
// exporter starts it as a go routine.
func ReporterAndWatchdog(abortAfter uint, verbose bool, statsReportInterval int) {
	initializePromStats()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	initialCredit := uint64(abortAfter / heartbeatInterval)
	credit := initialCredit
	ticks := 0
	processedSinceLastHeartbeat := uint64(0)
	stats := Metrics{}
	for !util.Interrupted() {
		select {
		case stream := <-unknownStreamsChannel:
			unknownStreamsMap[stream]++
			if verbose {
				log.Warn("unknown stream: %s", stream)
			}
		case <-ticker.C:
			m := swapAndObserveMetrics()
			stats.mergeMetrics(m)

			ticks++
			if ticks%statsReportInterval == 0 {
				log.Info("processed: %d, bytes: %d, ignored: %d, observed %d, dropped: %d, missed: %d, raw: %d, invisible: %d, empty: %d, unknown: %d",
					stats.Processed, stats.ProcessedBytes, stats.Ignored, stats.Observed, stats.Dropped, stats.Missed, stats.Raw, stats.Invisible, stats.EmptyMetricsResponse, stats.UnknownStreams)
				// reset metrics
				stats = Metrics{}
			}
			processedSinceLastHeartbeat += m.Processed
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
