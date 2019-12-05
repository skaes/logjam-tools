package mobile

import (
	"net/http"

	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/util"
)

// Metrics holds different metrics from mobile requests
type Metrics struct {
	RequestHandler http.Handler
	Registry       *prometheus.Registry

	payloadsChannel chan Payload

	AppFirstDraw  prometheus.Histogram
	AppResumeTime prometheus.Histogram
	AppHangTime   prometheus.Histogram
}

type Metric struct {
	Value     int
	Timestamp string
}

type Gauge struct {
	Name    string
	Metrics []Metric
}

type Bucket struct {
	Start_value int
	End_value   int
	Count       int
}

type Histogram struct {
	Name    string
	Begin   string
	End     string
	Buckets []Bucket
}

type Metadata struct {
	Os      string
	Device  string
	Version string
}

type Payload struct {
	Meta       Metadata
	Histograms []Histogram
	Gauges     []Gauge
}

// New Returns a new instance of mobile Metrics
func New() Metrics {
	r := prometheus.NewRegistry()
	m := Metrics{
		Registry:        r,
		RequestHandler:  promhttp.HandlerFor(r, promhttp.HandlerOpts{}),
		payloadsChannel: make(chan Payload, 10000),
		AppFirstDraw: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "application_time_firstdraw_ms",
				Help:    "A histogram of the different amounts of time taken to launch the app.",
				Buckets: prometheus.LinearBuckets(100, 100, 30)}),

		AppResumeTime: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "application_resume_time_ms",
				Help:    "A histogram of the different amounts of time taken to resume the app from the background.",
				Buckets: append(prometheus.LinearBuckets(100, 100, 9), prometheus.ExponentialBuckets(1000, 10, 3)...)},
		),

		AppHangTime: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "application_hang_time_ms",
				Help:    "How often is the main / UI thread blocked, such that the app is unresponsive to user input.",
				Buckets: append(prometheus.LinearBuckets(50, 50, 19), prometheus.LinearBuckets(1000, 250, 12)...)},
		),
	}

	r.MustRegister(m.AppFirstDraw)
	r.MustRegister(m.AppResumeTime)
	r.MustRegister(m.AppHangTime)

	go m.observer()

	return m
}

// ProcessMessage extracts mobile metrics from logjam payload and exposes it to Prometheus
func (m Metrics) ProcessMessage(routingKey string, data map[string]interface{}) {
	payload, err := m.parseData(data)
	if err != nil {
		log.Error("Error parsing mobile payload: %s", err)
		m.payloadsChannel <- payload
	}
}

func (m Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.RequestHandler.ServeHTTP(w, r)
}

func (m Metrics) parseData(data map[string]interface{}) (Payload, error) {
	var payload Payload
	if err := mapstructure.Decode(data, &payload); err != nil {
		return Payload{}, err
	}
	return payload, nil
}

func (m Metrics) observer() {
	for !util.Interrupted() {
		p := <-m.payloadsChannel
		for _, h := range p.Histograms {
			m.record(h)
		}
	}
}

func (m Metrics) record(h Histogram) {
	switch h.Name {
	case "application_time_firstdraw_ms":
		observe(m.AppFirstDraw, h.Buckets)
	case "application_resume_time_ms":
		observe(m.AppResumeTime, h.Buckets)
	case "application_hang_time_ms":
		observe(m.AppHangTime, h.Buckets)
	}
}

func observe(h prometheus.Histogram, buckets []Bucket) {
	for _, b := range buckets {
		for i := 0; i < b.Count; i++ {
			h.Observe(float64(b.End_value))
		}
	}
}
