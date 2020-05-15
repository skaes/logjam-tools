package collector

import (
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promclient "github.com/prometheus/client_model/go"
	"github.com/skaes/logjam-tools/go/frontendmetrics"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/prometheusexporter/stats"
	"github.com/skaes/logjam-tools/go/util"
)

var logLevelNames = []string{
	"Debug",   // 0
	"Info",    // 1
	"Warn",    // 2
	"Error",   // 3
	"Fatal",   // 4
	"Unknown", // 5
}

const (
	logLevelInfo    = 1
	logLevelUnknown = 5
)

const (
	logMetric  = 1
	pageMetric = 2
	ajaxMetric = 3
)

type dcPair struct {
	name     string
	withDots string
}

type metric struct {
	kind           uint8              // log, page or ajax
	props          map[string]string  // stored as labels
	value          float64            // time value, in seconds
	timeMetrics    map[string]float64 // other time metrics
	counterMetrics map[string]float64 // counter metrics
	maxLogLevel    int                // log level
}

type Options struct {
	Verbose     bool            // Verbose logging.
	Debug       bool            // Extra verbose logging.
	Datacenters string          // Konown datacenters, comma separated.
	DefaultDC   string          // Use this DC name if none can be derived.
	CleanAfter  uint            // Remove actions with stale data after this many minutes.
	Resources   *util.Resources // Resources to extract from incoming messages.
}

type Collector struct {
	Name                       string
	opts                       Options
	stream                     *util.Stream
	app                        string
	env                        string
	mutex                      sync.RWMutex
	httpRequestSummaryVec      *prometheus.SummaryVec
	httpRequestsTotalVec       *prometheus.CounterVec
	jobExecutionSummaryVec     *prometheus.SummaryVec
	jobExecutionsTotalVec      *prometheus.CounterVec
	httpRequestHistogramVec    *prometheus.HistogramVec
	jobExecutionHistogramVec   *prometheus.HistogramVec
	pageHistogramVec           *prometheus.HistogramVec
	ajaxHistogramVec           *prometheus.HistogramVec
	requestMetricsSummaryMap   map[string]*prometheus.SummaryVec
	requestMetricsHistogramMap map[string]*prometheus.HistogramVec
	requestMetricsCounterMap   map[string]*prometheus.CounterVec
	registry                   *prometheus.Registry
	metricsChannel             chan *metric
	RequestHandler             http.Handler
	actionRegistry             chan string
	knownActions               map[string]time.Time
	knownActionsSize           int32
	stopped                    uint32
	datacenters                []dcPair
}

var defaultBuckets = []float64{0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100}

// NoCollector is a placeholder and not a real collector
var NoCollector Collector = Collector{}

// IsCollector is true only for real collectors
func (c *Collector) IsCollector() bool {
	return c != &NoCollector
}

// Lock contention for the collector state is between four go
// routines: message parser, observer, action registry updater and
// streams updater, where the streams updater is the only one needing
// a write lock on the stream and the histogram vectors. Thus a
// RWMutex seems appropriate.

func (c *Collector) Stream() *util.Stream {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stream
}

func (c *Collector) appEnv() string {
	return c.app + "-" + c.env
}

// requestType is only called when the caller already has a read lock on the stream.
func (c *Collector) requestType(action string) string {
	for _, p := range c.stream.APIRequests {
		if strings.HasPrefix(action, p) {
			return "api"
		}
	}
	return "web"
}

func (c *Collector) hasKnownActions() bool {
	return atomic.LoadInt32(&c.knownActionsSize) > 0
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.RequestHandler.ServeHTTP(w, r)
	contentLength := w.Header()["Content-Length"][0]
	if contentLength == "0" && c.hasKnownActions() {
		atomic.AddUint64(&stats.Stats.EmptyMetricsResponse, 1)
		log.Error("prometheus erroneously served empty response for stream %s", c.appEnv())
	}
}

func New(appEnv string, stream *util.Stream, opts Options) *Collector {
	app, env := util.ParseStreamName(appEnv)
	c := Collector{
		opts:                       opts,
		app:                        app,
		env:                        env,
		stream:                     stream,
		registry:                   prometheus.NewRegistry(),
		actionRegistry:             make(chan string, 10000),
		knownActions:               make(map[string]time.Time),
		knownActionsSize:           0,
		metricsChannel:             make(chan *metric, 10000),
		datacenters:                make([]dcPair, 0),
		requestMetricsSummaryMap:   make(map[string]*prometheus.SummaryVec),
		requestMetricsHistogramMap: make(map[string]*prometheus.HistogramVec),
		requestMetricsCounterMap:   make(map[string]*prometheus.CounterVec),
	}
	for _, dc := range strings.Split(opts.Datacenters, ",") {
		if dc != "" {
			c.datacenters = append(c.datacenters, dcPair{name: dc, withDots: "." + dc + "."})
		}
	}
	c.Update(stream)
	c.RequestHandler = promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
	go c.actionRegistryHandler()
	go c.observer()
	return &c
}

func (c *Collector) registerHttpRequestSummaryVec() {
	c.httpRequestSummaryVec = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "logjam:action:http_response_time_summary_seconds",
			Help:       "logjam http response time summary by action",
			Objectives: map[float64]float64{},
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "type", "code", "method", "instance", "cluster", "dc"},
	)
	c.registry.MustRegister(c.httpRequestSummaryVec)
}

func (c *Collector) registerRequestMetricsSummaryVec(metric string) {
	vec := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "logjam:action:" + metric + "_summary_seconds",
			Help:       "logjam " + metric + " summary by action",
			Objectives: map[float64]float64{},
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "type", "instance", "cluster", "dc"},
	)
	c.requestMetricsSummaryMap[metric] = vec
	c.registry.MustRegister(vec)
}

func (c *Collector) registerRequestMetricsCounterVec(metric string) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logjam:action:" + metric + "_total",
			Help: "logjam " + metric + " total by action",
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "type", "instance", "cluster", "dc"},
	)
	c.requestMetricsCounterMap[metric] = vec
	c.registry.MustRegister(vec)
}

func (c *Collector) registerHttpRequestsTotalVec() {
	c.httpRequestsTotalVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logjam:action:http_requests_total",
			Help: "logjam http requests total by action",
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "type", "level", "instance", "cluster", "dc"},
	)
	c.registry.MustRegister(c.httpRequestsTotalVec)
}

func (c *Collector) registerJobExecutionSummaryVec() {
	c.jobExecutionSummaryVec = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "logjam:action:job_execution_time_summary_seconds",
			Help:       "logjam job execution time summary by action",
			Objectives: map[float64]float64{},
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "code", "instance", "cluster", "dc"},
	)
	c.registry.MustRegister(c.jobExecutionSummaryVec)
}

func (c *Collector) registerJobExecutionsTotalVec() {
	c.jobExecutionsTotalVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logjam:action:job_executions_total",
			Help: "logjam job executions total by action",
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "level", "instance", "cluster", "dc"},
	)
	c.registry.MustRegister(c.jobExecutionsTotalVec)
}

func (c *Collector) registerHttpRequestHistogramVec(stream *util.Stream) {
	c.httpRequestHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logjam:action:http_response_time_distribution_seconds",
			Help:    "logjam http response time distribution by action",
			Buckets: stream.HttpBuckets,
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "type", "method", "instance"},
	)
	c.registry.MustRegister(c.httpRequestHistogramVec)
}

func (c *Collector) registerRequestMetricsHistogramVec(metric string) {
	vec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logjam:action:" + metric + "_distribution_seconds",
			Help:    "logjam " + metric + " distribution by action",
			Buckets: defaultBuckets,
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "type", "instance"},
	)
	c.requestMetricsHistogramMap[metric] = vec
	c.registry.MustRegister(vec)
}

func (c *Collector) registerJobExecutionHistogramVec(stream *util.Stream) {
	c.jobExecutionHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logjam:action:job_execution_time_distribution_seconds",
			Help:    "logjam background job execution time distribution by action",
			Buckets: stream.JobsBuckets,
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "instance"},
	)
	c.registry.MustRegister(c.jobExecutionHistogramVec)
}

func (c *Collector) registerPageHistogramVec(stream *util.Stream) {
	c.pageHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logjam:action:page_time_distribution_seconds",
			Help:    "logjam page loading time distribution by action",
			Buckets: stream.PageBuckets,
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "instance"},
	)
	c.registry.MustRegister(c.pageHistogramVec)
}

func (c *Collector) registerAjaxHistogramVec(stream *util.Stream) {
	c.ajaxHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logjam:action:ajax_time_distribution_seconds",
			Help:    "logjam ajax response time distribution by action",
			Buckets: stream.AjaxBuckets,
		},
		// instance always set to the empty string
		[]string{"app", "env", "action", "instance"},
	)
	c.registry.MustRegister(c.ajaxHistogramVec)
}

func (c *Collector) Update(stream *util.Stream) {
	locked := false
	if c.httpRequestSummaryVec == nil {
		c.registerHttpRequestSummaryVec()
	}
	for _, m := range c.opts.Resources.TimeResources {
		if m == "total_time" {
			continue
		}
		if c.requestMetricsSummaryMap[m] == nil {
			c.registerRequestMetricsSummaryVec(m)
		}
		if c.requestMetricsHistogramMap[m] == nil {
			c.registerRequestMetricsHistogramVec(m)
		}
	}
	for _, m := range c.opts.Resources.CallResources {
		if c.requestMetricsCounterMap[m] == nil {
			c.registerRequestMetricsCounterVec(m)
		}
	}
	if c.httpRequestsTotalVec == nil {
		c.registerHttpRequestsTotalVec()
	}
	if c.jobExecutionSummaryVec == nil {
		c.registerJobExecutionSummaryVec()
	}
	if c.jobExecutionsTotalVec == nil {
		c.registerJobExecutionsTotalVec()
	}
	if c.httpRequestHistogramVec != nil && !c.stream.SameHttpBuckets(stream) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		locked = true
		c.registry.Unregister(c.httpRequestHistogramVec)
		c.httpRequestHistogramVec = nil
	}
	if c.httpRequestHistogramVec == nil {
		c.registerHttpRequestHistogramVec(stream)
	}
	if c.jobExecutionHistogramVec != nil && !c.stream.SameJobsBuckets(stream) {
		if !locked {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			locked = true
		}
		c.registry.Unregister(c.jobExecutionHistogramVec)
		c.jobExecutionHistogramVec = nil
	}
	if c.jobExecutionHistogramVec == nil {
		c.registerJobExecutionHistogramVec(stream)
	}
	if c.pageHistogramVec != nil && !c.stream.SamePageBuckets(stream) {
		if !locked {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			locked = true
		}
		c.registry.Unregister(c.pageHistogramVec)
		c.pageHistogramVec = nil
	}
	if c.pageHistogramVec == nil {
		c.registerPageHistogramVec(stream)
	}
	if c.ajaxHistogramVec != nil && !c.stream.SameAjaxBuckets(stream) {
		if !locked {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			locked = true
		}
		c.registry.Unregister(c.ajaxHistogramVec)
		c.ajaxHistogramVec = nil
	}
	if c.ajaxHistogramVec == nil {
		c.registerAjaxHistogramVec(stream)
	}
	if (!c.stream.SameAPIRequests(stream) || c.stream.IgnoredRequestURI != stream.IgnoredRequestURI) && !locked {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		locked = true
	}
	if locked {
		c.stream = stream
	}
}

func (c *Collector) observeMetrics(m *metric) {
	atomic.AddInt64(&stats.Stats.Invisible, 1)
	c.metricsChannel <- m
}

func (c *Collector) observer() {
	for !util.Interrupted() && atomic.LoadUint32(&c.stopped) == 0 {
		m := <-c.metricsChannel
		switch m.kind {
		case logMetric:
			c.recordLogMetrics(m)
		case pageMetric:
			c.recordPageMetrics(m)
		case ajaxMetric:
			c.recordAjaxMetrics(m)
		}
		atomic.AddInt64(&stats.Stats.Invisible, -1)
		atomic.AddUint64(&stats.Stats.Observed, 1)
	}
}

func hasLabel(pairs []*promclient.LabelPair, label string, value string) bool {
	for _, p := range pairs {
		if p.GetName() == label && p.GetValue() == value {
			return true
		}
	}
	return false
}

func labelsFromLabelPairs(pairs []*promclient.LabelPair) prometheus.Labels {
	labels := make(prometheus.Labels)
	for _, p := range pairs {
		labels[p.GetName()] = p.GetValue()
	}
	return labels
}

var logjamMetricsNameMatcher = regexp.MustCompile(`^logjam:action:(.*)_(?:(distribution|summary)_seconds|(total))$`)

func extractLogjamMetricFromName(name string) (string, string) {
	matches := logjamMetricsNameMatcher.FindStringSubmatch(name)
	if len(matches) != 4 {
		return "", ""
	}
	resource, kind, total := matches[1], matches[2], matches[3]
	if total == "" {
		return resource, kind
	} else {
		return resource, total
	}
}

func (c *Collector) deleteLabels(name string, labels prometheus.Labels) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	deleted := false
	switch name {
	case "logjam:action:http_response_time_summary_seconds":
		deleted = c.httpRequestSummaryVec.Delete(labels)
	case "logjam:action:http_requests_total":
		deleted = c.httpRequestsTotalVec.Delete(labels)
	case "logjam:action:job_execution_time_summary_seconds":
		deleted = c.jobExecutionSummaryVec.Delete(labels)
	case "logjam:action:job_executions_total":
		deleted = c.jobExecutionsTotalVec.Delete(labels)
	case "logjam:action:http_response_time_distribution_seconds":
		deleted = c.httpRequestHistogramVec.Delete(labels)
	case "logjam:action:job_execution_time_distribution_seconds":
		deleted = c.jobExecutionHistogramVec.Delete(labels)
	case "logjam:action:page_time_distribution_seconds":
		deleted = c.pageHistogramVec.Delete(labels)
	case "logjam:action:ajax_time_distribution_seconds":
		deleted = c.ajaxHistogramVec.Delete(labels)
	default:
		metric, kind := extractLogjamMetricFromName(name)
		if metric != "" {
			switch kind {
			case "summary":
				deleted = c.requestMetricsSummaryMap[metric].Delete(labels)
			case "distribution":
				deleted = c.requestMetricsHistogramMap[metric].Delete(labels)
			case "total":
				deleted = c.requestMetricsCounterMap[metric].Delete(labels)
			}
		}
	}
	return deleted
}

func (c *Collector) removeAction(a string) bool {
	if c.opts.Verbose {
		log.Info("removing action: %s", a)
	}
	delete(c.knownActions, a)
	atomic.StoreInt32(&c.knownActionsSize, int32(len(c.knownActions)))
	mfs, err := c.registry.Gather()
	if err != nil {
		log.Error("could not gather metric families for deletion: %s", err)
		return false
	}
	numProcessed := 0
	numDeleted := 0
	for _, mf := range mfs {
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			pairs := m.GetLabel()
			if hasLabel(pairs, "action", a) {
				numProcessed++
				labels := labelsFromLabelPairs(pairs)
				if c.deleteLabels(name, labels) {
					numDeleted++
				} else {
					log.Error("Could not delete labels: %v", labels)
				}
			}
		}
	}
	return numProcessed > 0 && numProcessed == numDeleted
}

func (c *Collector) actionRegistryHandler() {
	// cleaning every minute, until we are interrupted or the stream was shut down
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for !util.Interrupted() && atomic.LoadUint32(&c.stopped) == 0 {
		select {
		case action := <-c.actionRegistry:
			c.knownActions[action] = time.Now()
			atomic.StoreInt32(&c.knownActionsSize, int32(len(c.knownActions)))
		case <-ticker.C:
			threshold := time.Now().Add(-1 * time.Duration(c.opts.CleanAfter) * time.Minute)
			for a, v := range c.knownActions {
				if v.Before(threshold) {
					c.removeAction(a)
				}
			}
		}
	}
}

func (c *Collector) Shutdown() {
	atomic.AddUint32(&c.stopped, 1)
}

func (c *Collector) fixDatacenter(m map[string]string, instance string) {
	if dc := m["dc"]; dc == "unknown" || dc == "" {
		fixed := false
		for _, d := range c.datacenters {
			if strings.Contains(instance, d.withDots) {
				fixed = true
				m["dc"] = d.name
				// log.Info("Fixed datacenter: %s ==> %s\n", dc, d.name)
				break
			}
		}
		if c.opts.Verbose && !fixed {
			m["dc"] = c.opts.DefaultDC
			log.Warn("Could not fix datacenter: %s, application: %s, instance: %s", dc, m["app"], instance)
		}
	}
}

func (c *Collector) recordLogMetrics(m *metric) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	p := m.props
	metric := p["metric"]
	instance := p["instance"]
	p["instance"] = ""
	action := p["action"]
	method := p["method"]
	delete(p, "metric")
	c.fixDatacenter(p, instance)
	switch metric {
	case "http":
		p["type"] = c.requestType(action)
		c.httpRequestSummaryVec.With(p).Observe(m.value)
		delete(p, "code")
		delete(p, "method")
		incrementTotalsForLogLevels(c.httpRequestsTotalVec, p, m.maxLogLevel)
		delete(p, "level")
		for k, v := range m.timeMetrics {
			if vec := c.requestMetricsSummaryMap[k]; vec != nil {
				vec.With(p).Observe(v)
			}
		}
		for k, v := range m.counterMetrics {
			if vec := c.requestMetricsCounterMap[k]; vec != nil {
				vec.With(p).Add(v)
			}
		}
		p["method"] = method
		delete(p, "cluster")
		delete(p, "dc")
		c.httpRequestHistogramVec.With(p).Observe(m.value)
		delete(p, "method")
		for k, v := range m.timeMetrics {
			if vec := c.requestMetricsHistogramMap[k]; vec != nil {
				vec.With(p).Observe(v)
			}
		}
		c.actionRegistry <- action
	case "job":
		c.jobExecutionSummaryVec.With(p).Observe(m.value)
		delete(p, "code")
		incrementTotalsForLogLevels(c.jobExecutionsTotalVec, p, m.maxLogLevel)
		delete(p, "level")
		p["type"] = "job"
		for k, v := range m.counterMetrics {
			if vec := c.requestMetricsCounterMap[k]; vec != nil {
				vec.With(p).Add(v)
			}
		}
		for k, v := range m.timeMetrics {
			if vec := c.requestMetricsSummaryMap[k]; vec != nil {
				vec.With(p).Observe(v)
			}
		}
		delete(p, "type")
		delete(p, "cluster")
		delete(p, "dc")
		c.jobExecutionHistogramVec.With(p).Observe(m.value)
		p["type"] = "job"
		for k, v := range m.timeMetrics {
			if vec := c.requestMetricsHistogramMap[k]; vec != nil {
				vec.With(p).Observe(v)
			}
		}
		delete(p, "type")
		c.actionRegistry <- action
	}
}

func incrementTotalsForLogLevels(counter *prometheus.CounterVec, labels map[string]string, maxLogLevel int) {
	for i := 0; i <= logLevelUnknown; i++ {
		labels["level"] = strconv.Itoa(i)
		var incrementBy float64
		if i == maxLogLevel {
			incrementBy = 1
		}
		counter.With(labels).Add(incrementBy)
	}
	delete(labels, "level")
}

func (c *Collector) recordPageMetrics(m *metric) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	p := m.props
	p["instance"] = ""
	action := p["action"]
	c.pageHistogramVec.With(p).Observe(m.value)
	c.actionRegistry <- action
}

func (c *Collector) recordAjaxMetrics(m *metric) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	p := m.props
	p["instance"] = ""
	action := p["action"]
	c.ajaxHistogramVec.With(p).Observe(m.value)
	c.actionRegistry <- action
}

func (c *Collector) ProcessMessage(routingKey string, data map[string]interface{}) {
	if c.opts.Debug {
		s := spew.Sdump(routingKey, data)
		log.Info("processMessage\n%s", s)
	}
	var m *metric
	switch {
	case strings.HasPrefix(routingKey, "logs"):
		m = c.processLogMessage(routingKey, data)
	case strings.HasPrefix(routingKey, "frontend.page"):
		m = c.processPageMessage(routingKey, data)
	case strings.HasPrefix(routingKey, "frontend.ajax"):
		m = c.processAjaxMessage(routingKey, data)
	}
	if c.opts.Debug {
		s := spew.Sdump(m)
		log.Info("observing metric\n%s", s)
	}
	if m != nil {
		c.observeMetrics(m)
	}
}

func (c *Collector) processLogMessage(routingKey string, data map[string]interface{}) *metric {
	p := make(map[string]string)
	p["app"] = c.app
	p["env"] = c.env
	ignoreMessage := extractBool(data, "logjam_ignore_message")
	if ignoreMessage {
		if c.opts.Verbose {
			log.Info("ignoring request because logjam_ignore_message was set to true")
		}
		return nil
	}
	info := extractMap(data, "request_info")
	method := ""
	if info != nil {
		method = strings.ToUpper(extractString(info, "method", ""))
		uri := extractString(info, "url", "")
		if uri != "" {
			u, err := url.Parse(uri)
			if err == nil && strings.HasPrefix(u.Path, c.Stream().IgnoredRequestURI) {
				atomic.AddUint64(&stats.Stats.Ignored, 1)
				if c.opts.Verbose {
					log.Info("ignoring request because of url match: %s", u.String())
				}
				return nil
			}
		}
	}
	if method != "" {
		p["metric"] = "http"
		p["method"] = method
	} else {
		p["metric"] = "job"
	}
	p["action"] = extractAction(data)
	p["code"] = extractString(data, "code", "500")
	p["instance"] = extractString(data, "host", "unknown")
	p["cluster"] = extractString(data, "cluster", "unknown")
	p["dc"] = extractString(data, "datacenter", c.opts.DefaultDC)
	totalTime, timeMetrics, counterMetrics := c.opts.Resources.ExtractResources(data)
	level := extractMaxLogLevel(data)
	if level > logLevelUnknown {
		level = logLevelUnknown
	}
	return &metric{kind: logMetric, props: p, value: totalTime, timeMetrics: timeMetrics, counterMetrics: counterMetrics, maxLogLevel: level}
}

func (c *Collector) processPageMessage(routingKey string, data map[string]interface{}) *metric {
	rts := extractString(data, "rts", "")
	p := make(map[string]string)
	p["app"] = c.app
	p["env"] = c.env
	p["action"] = extractAction(data)
	timings, err := frontendmetrics.ExtractPageTimings(rts)
	if err != nil || timings.PageTime > frontendmetrics.OutlierThresholdMs {
		atomic.AddUint64(&stats.Stats.Dropped, 1)
		if c.opts.Verbose {
			ua := extractString(data, "user_agent", "unknown")
			if err != nil {
				log.Error("could not extract page_time for %s [%s] from %s: %s, user agent: %s", c.appEnv(), p["action"], rts, err, ua)
			} else {
				log.Info("page_time outlier for %s [%s] from %s, user agent: %s, %f", c.appEnv(), p["action"], rts, ua, float64(timings.PageTime)/1000)
			}
		}
		return nil
	}
	// page_time is measured in milliseconds, but prometheus wants seconds
	return &metric{kind: pageMetric, props: p, value: float64(timings.PageTime) / 1000}
}

func (c *Collector) processAjaxMessage(routingKey string, data map[string]interface{}) *metric {
	rts := extractString(data, "rts", "")
	p := make(map[string]string)
	p["app"] = c.app
	p["env"] = c.env
	p["action"] = extractAction(data)
	ajaxTime, err := frontendmetrics.ExtractAjaxTime(rts)
	if err != nil || ajaxTime > frontendmetrics.OutlierThresholdMs {
		atomic.AddUint64(&stats.Stats.Dropped, 1)
		if c.opts.Verbose {
			ua := extractString(data, "user_agent", "unknown")
			if err != nil {
				log.Error("could not extract ajax_time for %s [%s] from %s: %s, user agent: %s", c.appEnv(), p["action"], rts, err, ua)
			} else {
				log.Info("ajax_time outlier for %s [%s] from %s, user agent: %s, %f", c.appEnv(), p["action"], rts, ua, float64(ajaxTime)/1000)
			}
		}
		return nil
	}
	// ajax_time is measured in milliseconds, but prometheus wants seconds
	return &metric{kind: ajaxMetric, props: p, value: float64(ajaxTime) / 1000}
}

func extractString(request map[string]interface{}, key string, defaultValue string) string {
	value := request[key]
	if value == nil {
		return defaultValue
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.Itoa(int(v))
	default:
		return defaultValue
	}
}

func extractBool(request map[string]interface{}, key string) bool {
	value := request[key]
	if value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return bool(v)
	case string:
		return v == "true"
	case int:
		return int(v) > 0
	default:
		return false
	}
}

func extractAction(request map[string]interface{}) string {
	action := extractString(request, "action", "")
	if action == "" {
		action = extractString(request, "logjam_action", "")
		if action == "" {
			action = "Unknown#unknown_method"
		}
	}
	if strings.Index(action, "#") == -1 {
		action += "#unknown_method"
	} else if strings.Index(action, "#") == len(action)-1 {
		action += "unknown_method"
	}
	return action
}

func extractMap(request map[string]interface{}, key string) map[string]interface{} {
	value := request[key]
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case map[string]interface{}:
		return v
	default:
		return nil
	}

}

func convertInt(value interface{}, defaultValue int) int {
	switch v := value.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	default:
		return defaultValue
	}
}

func extractMaxLogLevel(request map[string]interface{}) int {
	value := request["severity"]
	if value != nil {
		return convertInt(value, logLevelInfo)
	}
	lines, ok := request["lines"]
	if !ok {
		return logLevelInfo
	}
	switch lines := lines.(type) {
	case []interface{}:
		maxLevel := 0
		for _, line := range lines {
			switch line := line.(type) {
			case []interface{}:
				if len(line) > 0 {
					level := convertInt(line[0], logLevelInfo)
					if level > maxLevel {
						maxLevel = level
					}
				}
			}
		}
		return maxLevel
	}
	return logLevelInfo
}
