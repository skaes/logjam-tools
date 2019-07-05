package collector

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
	kind  uint8             // log, page or ajax
	props map[string]string // stored as labels
	value float64           // time value, in seconds
}

type Options struct {
	Verbose     bool   // Verbose logging.
	Debug       bool   // Extra verbose logging.
	Datacenters string // Konown datacenters, comma separated.
	DefaultDC   string // Use this DC name if none can be derived.
	CleanAfter  uint   // Remove actions with stale data after this many minutes.
}

type Collector struct {
	opts                     Options
	Name                     string
	app                      string
	env                      string
	apiRequests              []string
	ignoredRequestURI        string
	httpRequestSummaryVec    *prometheus.SummaryVec
	jobExecutionSummaryVec   *prometheus.SummaryVec
	httpRequestHistogramVec  *prometheus.HistogramVec
	jobExecutionHistogramVec *prometheus.HistogramVec
	pageHistogramVec         *prometheus.HistogramVec
	ajaxHistogramVec         *prometheus.HistogramVec
	registry                 *prometheus.Registry
	metricsChannel           chan *metric
	RequestHandler           http.Handler
	actionRegistry           chan string
	knownActions             map[string]time.Time
	stopped                  uint32
	datacenters              []dcPair
}

func (c *Collector) appEnv() string {
	return c.app + "-" + c.env
}

func (c *Collector) requestType(action string) string {
	for _, p := range c.apiRequests {
		if strings.HasPrefix(action, p) {
			return "api"
		}
	}
	return "web"
}

func New(appEnv string, stream util.Stream, opts Options) *Collector {
	app, env := util.ParseStreamName(appEnv)
	c := Collector{
		opts: opts,
		app:  app,
		env:  env,
		httpRequestSummaryVec: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "logjam:action:http_response_time_summary_seconds",
				Help:       "logjam http response time summary by action",
				Objectives: map[float64]float64{},
			},
			// instance always set to the empty string
			[]string{"app", "env", "action", "type", "code", "method", "instance", "cluster", "dc"},
		),
		jobExecutionSummaryVec: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "logjam:action:job_execution_time_summary_seconds",
				Help:       "logjam job execution time summary by action",
				Objectives: map[float64]float64{},
			},
			// instance always set to the empty string
			[]string{"app", "env", "action", "code", "instance", "cluster", "dc"},
		),
		httpRequestHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "logjam:action:http_response_time_distribution_seconds",
				Help:    "logjam http response time distribution by action",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100},
			},
			// instance always set to the empty string
			[]string{"app", "env", "action", "type", "method", "instance"},
		),
		jobExecutionHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "logjam:action:job_execution_time_distribution_seconds",
				Help:    "logjam background job execution time distribution by action",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100},
			},
			// instance always set to the empty string
			[]string{"app", "env", "action", "instance"},
		),
		pageHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "logjam:action:page_time_distribution_seconds",
				Help:    "logjam page loading time distribution by action",
				Buckets: []float64{.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250},
			},
			// instance always set to the empty string
			[]string{"app", "env", "action", "instance"},
		),
		ajaxHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "logjam:action:ajax_time_distribution_seconds",
				Help:    "logjam ajax response time distribution by action",
				Buckets: []float64{.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250},
			},
			// instance always set to the empty string
			[]string{"app", "env", "action", "instance"},
		),
		registry:          prometheus.NewRegistry(),
		actionRegistry:    make(chan string, 10000),
		apiRequests:       stream.APIRequests,
		ignoredRequestURI: stream.IgnoredRequestURI,
		knownActions:      make(map[string]time.Time),
		metricsChannel:    make(chan *metric, 10000),
		datacenters:       make([]dcPair, 0),
	}
	for _, dc := range strings.Split(opts.Datacenters, ",") {
		if dc != "" {
			c.datacenters = append(c.datacenters, dcPair{name: dc, withDots: "." + dc + "."})
		}
	}
	c.registry.MustRegister(c.httpRequestHistogramVec)
	c.registry.MustRegister(c.jobExecutionHistogramVec)
	c.registry.MustRegister(c.httpRequestSummaryVec)
	c.registry.MustRegister(c.jobExecutionSummaryVec)
	c.registry.MustRegister(c.pageHistogramVec)
	c.registry.MustRegister(c.ajaxHistogramVec)
	c.RequestHandler = promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
	go c.actionRegistryHandler()
	go c.observer()
	return &c
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

func (c *Collector) removeAction(a string) bool {
	if c.opts.Verbose {
		log.Info("removing action: %s", a)
	}
	delete(c.knownActions, a)
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
				deleted := false
				switch name {
				case "logjam:action:http_response_time_summary_seconds":
					deleted = c.httpRequestSummaryVec.Delete(labels)
				case "logjam:action:job_execution_time_summary_seconds":
					deleted = c.jobExecutionSummaryVec.Delete(labels)
				case "logjam:action:http_response_time_distribution_seconds":
					deleted = c.httpRequestHistogramVec.Delete(labels)
				case "logjam:action:job_execution_time_distribution_seconds":
					deleted = c.jobExecutionHistogramVec.Delete(labels)
				case "logjam:action:page_time_distribution_seconds":
					deleted = c.pageHistogramVec.Delete(labels)
				case "logjam:action:ajax_time_distribution_seconds":
					deleted = c.ajaxHistogramVec.Delete(labels)
				}
				if deleted {
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
	p := m.props
	metric := p["metric"]
	instance := p["instance"]
	p["instance"] = ""
	action := p["action"]
	delete(p, "metric")
	c.fixDatacenter(p, instance)
	switch metric {
	case "http":
		p["type"] = c.requestType(action)
		c.httpRequestSummaryVec.With(p).Observe(m.value)
		delete(p, "code")
		delete(p, "cluster")
		delete(p, "dc")
		c.httpRequestHistogramVec.With(p).Observe(m.value)
		c.actionRegistry <- action
	case "job":
		c.jobExecutionSummaryVec.With(p).Observe(m.value)
		delete(p, "code")
		delete(p, "cluster")
		delete(p, "dc")
		c.jobExecutionHistogramVec.With(p).Observe(m.value)
		c.actionRegistry <- action
	}
}

func (c *Collector) recordPageMetrics(m *metric) {
	p := m.props
	p["instance"] = ""
	action := p["action"]
	c.pageHistogramVec.With(p).Observe(m.value)
	c.actionRegistry <- action
}

func (c *Collector) recordAjaxMetrics(m *metric) {
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
	if strings.HasPrefix(routingKey, "logs") {
		m = c.processLogMessage(routingKey, data)
	}
	if strings.HasPrefix(routingKey, "frontend.page") {
		m = c.processPageMessage(routingKey, data)
	}
	if strings.HasPrefix(routingKey, "frontend.ajax") {
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
	ignore_message := extractBool(data, "logjam_ignore_message")
	if ignore_message {
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
			if err == nil && strings.HasPrefix(u.Path, c.ignoredRequestURI) {
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
	valstr := extractString(data, "total_time", "0")
	val, err := strconv.ParseFloat(valstr, 64)
	if err != nil {
		atomic.AddUint64(&stats.Stats.Dropped, 1)
		if c.opts.Verbose {
			log.Error("could not parse total_time(%s): %s", err)
		}
		return nil
	}
	// val is measured in milliseconds, but prometheus wants seconds
	return &metric{kind: logMetric, props: p, value: val / 1000}
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
