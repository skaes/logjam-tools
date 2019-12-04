package mobile

import (
	"fmt"
	"net/http"

	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var metricLabels []string = []string{"app", "env", "instance"}

// Metrics holds different metrics from mobile requests
type Metrics struct {
	RequestHandler http.Handler
	Registry       *prometheus.Registry

	AppFirstDraw  *prometheus.HistogramVec
	AppResumeTime *prometheus.HistogramVec
	AppHangTime   *prometheus.HistogramVec
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
	BucketRange
	Count int
}

type BucketRange struct {
	StartValue int `json:"start_value"`
	EndValue   int `json:"end_value"`
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
	m := Metrics{}
	m.initMetrics()
	m.Registry = prometheus.NewRegistry()
	m.RequestHandler = promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
	return m
}

func (m Metrics) initMetrics() {
	m.AppFirstDraw = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "application_time_firstdraw_ms",
		Help:    "A histogram of the different amounts of time taken to launch the app.",
		Buckets: prometheus.LinearBuckets(100, 100, 30)},
		metricLabels,
	)

	m.AppResumeTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "application_resume_time_ms",
		Help:    "A histogram of the different amounts of time taken to resume the app from the background.",
		Buckets: append(prometheus.LinearBuckets(100, 100, 9), prometheus.ExponentialBuckets(1000, 10, 3)...)},
		metricLabels,
	)

	m.AppHangTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "application_hang_time_ms",
		Help:    "How often is the main / UI thread blocked, such that the app is unresponsive to user input.",
		Buckets: append(prometheus.LinearBuckets(50, 50, 19), prometheus.LinearBuckets(1000, 250, 12)...)},
		metricLabels,
	)
}

// ProcessMessage extracts mobile metrics from logjam payload and exposes it to Prometheus
func (m Metrics) ProcessMessage(routingKey string, data map[string]interface{}) {
	payload := parseData(data)
	for _, p := range payload.Histograms {
		fmt.Println("histo: ", p.Name)
	}
}

func parseData(data map[string]interface{}) Payload {
	var payload Payload
	if err := mapstructure.Decode(data, &payload); err != nil {
		fmt.Println("Error unmarshalling", err)
		return Payload{}
	}
	return payload
}
