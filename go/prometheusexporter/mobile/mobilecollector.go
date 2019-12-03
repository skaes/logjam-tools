package mobile

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds different metrics from mobile requests
type Metrics struct {
	RequestHandler http.Handler
	Registry       *prometheus.Registry

	AppFirstDraw  *prometheus.HistogramVec
	AppResumeTime *prometheus.HistogramVec
	AppHangTime   *prometheus.HistogramVec
}

var metricLabels []string = []string{"app", "env", "instance"}

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

func (m Metrics) ProcessMessage(routingKey string, data map[string]interface{}) {
	
}
