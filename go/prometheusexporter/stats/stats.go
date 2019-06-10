package stats

import (
	"net/http"
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
	Processed      uint64 // number of ZeroMQ messages processed
	ProcessedBytes uint64 // msg bytes processed
	Dropped        uint64 // number of messages dropped
	Missed         uint64 // number if messages dropped by the zeroMQ SUB socket
	Observed       uint64 // number of observed metrics
	Ignored        uint64 // number of invalid messages
	Raw            int64  // number of messages not yet decompressed and parsed
	Invisible      int64  // number of messages not yet observed by the prometheus collectors
}

var registry *prometheus.Registry

// RequestHandler handles /metrics route of promethus exporter
var RequestHandler http.Handler

var promStats struct {
	Processed      prometheus.Counter
	ProcessedBytes prometheus.Counter
	Dropped        prometheus.Counter
	Missed         prometheus.Counter
	Observed       prometheus.Counter
	Ignored        prometheus.Counter
	Raw            prometheus.Gauge
	Invisible      prometheus.Gauge
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
	registry.MustRegister(promStats.Processed)
	registry.MustRegister(promStats.ProcessedBytes)
	registry.MustRegister(promStats.Dropped)
	registry.MustRegister(promStats.Missed)
	registry.MustRegister(promStats.Observed)
	registry.MustRegister(promStats.Ignored)
	registry.MustRegister(promStats.Raw)
	registry.MustRegister(promStats.Invisible)
	RequestHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

// Reporter reports exporter stats. The export starts it as a go routine.
func Reporter() {
	initializePromStats()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if util.Interrupted() {
			break
		}
		_observed := atomic.SwapUint64(&Stats.Observed, 0)
		_processed := atomic.SwapUint64(&Stats.Processed, 0)
		_processedBytes := atomic.SwapUint64(&Stats.ProcessedBytes, 0)
		_dropped := atomic.SwapUint64(&Stats.Dropped, 0)
		_missed := atomic.SwapUint64(&Stats.Missed, 0)
		_ignored := atomic.SwapUint64(&Stats.Ignored, 0)
		_raw := atomic.LoadInt64(&Stats.Raw)
		_invisible := atomic.LoadInt64(&Stats.Invisible)

		promStats.Observed.Add(float64(_observed))
		promStats.Processed.Add(float64(_processed))
		promStats.ProcessedBytes.Add(float64(_processedBytes))
		promStats.Ignored.Add(float64(_ignored))
		promStats.Dropped.Add(float64(_dropped))
		promStats.Missed.Add(float64(_missed))
		promStats.Raw.Set(float64(_raw))
		promStats.Invisible.Set(float64(_invisible))

		log.Info("processed: %d, bytes: %d, ignored: %d, observed %d, dropped: %d, missed: %d, raw: %d, invisible: %d",
			_processed, _processedBytes, _ignored, _observed, _dropped, _missed, _raw, _invisible)
	}
}
