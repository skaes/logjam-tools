package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	// "runtime"
	// "runtime/pprof"

	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	zmq "github.com/pebbe/zmq4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promclient "github.com/prometheus/client_model/go"
	"github.com/skaes/logjam-tools/go/frontendmetrics"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/util"
	"golang.org/x/text/runes"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Devices     string `short:"d" long:"devices" default:"127.0.0.1:9606" description:"comma separated device specs (host:port pairs)"`
	Env         string `short:"e" long:"env" description:"logjam environments to process"`
	Port        string `short:"p" long:"port" default:"8081" description:"port to expose metrics on"`
	StreamURL   string `short:"s" long:"stream-url" default:"" description:"Logjam endpoint for retrieving stream definitions"`
	Datacenters string `short:"D" long:"datacenters" description:"List of known datacenters, comma separated. Will be used to determine label value if not available on incoming data."`
	CleanAfter  uint   `short:"c" long:"clean-after" default:"5" description:"Minutes to wait before cleaning old time series"`
	Parsers     uint   `short:"P" long:"parsers" default:"4" description:"Number of message parsers to run in parallel"`
}

type dcPair struct {
	name     string
	withDots string
}

var (
	verbose     = false
	interrupted uint32
	deviceSpecs []string
	processed   int64
	dropped     int64
	missed      int64
	observed    int64
	ignored     int64
	collectors  = make(map[string]*collector)
	mutex       sync.Mutex
	datacenters = make([]dcPair, 0)
)

func addCollector(appEnv string, stream stream) {
	mutex.Lock()
	defer mutex.Unlock()
	_, found := collectors[appEnv]
	if !found {
		log.Info("adding stream: %s : %+v", appEnv, stream)
		collectors[appEnv] = newCollector(appEnv, stream)
	}
}

func getCollector(appEnv string) *collector {
	mutex.Lock()
	defer mutex.Unlock()
	return collectors[appEnv]
}

const (
	Log  = 1
	Page = 2
	Ajax = 3
)

type metric struct {
	kind  uint8
	props map[string]string
	value float64
}

type collector struct {
	name                     string
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
	requestHandler           http.Handler
	actionRegistry           chan string
	knownActions             map[string]time.Time
	stopped                  uint32
}

func (c *collector) appEnv() string {
	return c.app + "-" + c.env
}

func (c *collector) requestType(action string) string {
	for _, p := range c.apiRequests {
		if strings.HasPrefix(action, p) {
			return "api"
		}
	}
	return "web"
}

func newCollector(appEnv string, stream stream) *collector {
	app, env := util.ParseStreamName(appEnv)
	c := collector{
		app: app,
		env: env,
		httpRequestSummaryVec: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "http_request_latency_seconds",
				Help:       "http request latency summary",
				Objectives: map[float64]float64{},
			},
			// instance always set to the empty string
			[]string{"application", "environment", "action", "type", "code", "http_method", "instance", "cluster", "datacenter"},
		),
		jobExecutionSummaryVec: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "job_execution_latency_seconds",
				Help:       "job execution latency summary",
				Objectives: map[float64]float64{},
			},
			// instance always set to the empty string
			[]string{"application", "environment", "action", "code", "instance", "cluster", "datacenter"},
		),
		httpRequestHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "http response time distribution",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
			},
			// instance always set to the empty string
			[]string{"application", "environment", "action", "type", "http_method", "instance"},
		),
		jobExecutionHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "job_execution_duration_seconds",
				Help:    "background job execution time distribution",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
			},
			// instance always set to the empty string
			[]string{"application", "environment", "action", "instance"},
		),
		pageHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "logjam_page_time_duration_seconds",
				Help:    "page loading time distribution",
				Buckets: []float64{.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100, 250},
			},
			// instance always set to the empty string
			[]string{"application", "environment", "action", "instance"},
		),
		ajaxHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "logjam_ajax_time_duration_seconds",
				Help:    "ajax response time distribution",
				Buckets: []float64{.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100, 250},
			},
			// instance always set to the empty string
			[]string{"application", "environment", "action", "instance"},
		),
		registry:          prometheus.NewRegistry(),
		actionRegistry:    make(chan string, 10000),
		apiRequests:       stream.APIRequests,
		ignoredRequestURI: stream.IgnoredRequestURI,
		knownActions:      make(map[string]time.Time),
		metricsChannel:    make(chan *metric, 10000),
	}
	c.registry.MustRegister(c.httpRequestHistogramVec)
	c.registry.MustRegister(c.jobExecutionHistogramVec)
	c.registry.MustRegister(c.httpRequestSummaryVec)
	c.registry.MustRegister(c.jobExecutionSummaryVec)
	c.registry.MustRegister(c.pageHistogramVec)
	c.registry.MustRegister(c.ajaxHistogramVec)
	c.requestHandler = promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
	go c.actionRegistryHandler()
	go c.observer()
	return &c
}

func (c *collector) observeMetrics(m *metric) {
	c.metricsChannel <- m
}

func (c *collector) observer() {
	for atomic.LoadUint32(&interrupted)+atomic.LoadUint32(&c.stopped) == 0 {
		m := <-c.metricsChannel
		// log.Info("ae: %s", c.appEnv())
		switch m.kind {
		case Log:
			c.recordLogMetrics(m)
		case Page:
			c.recordPageMetrics(m)
		case Ajax:
			c.recordAjaxMetrics(m)
		}
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

func (c *collector) removeAction(a string) bool {
	log.Info("removing action: %s", a)
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
				case "http_request_latency_seconds":
					deleted = c.httpRequestSummaryVec.Delete(labels)
				case "job_execution_latency_seconds":
					deleted = c.jobExecutionSummaryVec.Delete(labels)
				case "http_request_duration_seconds":
					deleted = c.httpRequestHistogramVec.Delete(labels)
				case "job_execution_duration_seconds":
					deleted = c.jobExecutionHistogramVec.Delete(labels)
				case "logjam_page_time_duration_seconds":
					deleted = c.pageHistogramVec.Delete(labels)
				case "logjam_ajax_time_duration_seconds":
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

func (c *collector) actionRegistryHandler() {
	// cleaning every minute, until we are interrupted or the stream was shut down
	ticker := time.NewTicker(1 * time.Minute)
	for atomic.LoadUint32(&interrupted)+atomic.LoadUint32(&c.stopped) == 0 {
		select {
		case action := <-c.actionRegistry:
			c.knownActions[action] = time.Now()
		case <-ticker.C:
			threshold := time.Now().Add(-1 * time.Duration(opts.CleanAfter) * time.Minute)
			for a, v := range c.knownActions {
				if v.Before(threshold) {
					c.removeAction(a)
				}
			}
		}
	}
}

func (c *collector) shutdown() {
	atomic.AddUint32(&c.stopped, 1)
}

func fixDatacenter(m map[string]string, instance string) {
	if dc := m["datacenter"]; dc == "unknown" || dc == "" {
		fixed := false
		for _, d := range datacenters {
			if strings.Contains(instance, d.withDots) {
				fixed = true
				m["datacenter"] = d.name
				// log.Info("Fixed datacenter: %s ==> %s\n", dc, d.name)
				break
			}
		}
		if verbose && !fixed {
			log.Warn("Could not fix datacenter: %s, application: %s, instance: %s", dc, m["application"], instance)
		}
	}
}

func (c *collector) recordLogMetrics(m *metric) {
	p := m.props
	metric := p["metric"]
	instance := p["instance"]
	p["instance"] = ""
	action := p["action"]
	delete(p, "metric")
	fixDatacenter(p, instance)
	switch metric {
	case "http":
		p["type"] = c.requestType(action)
		c.httpRequestSummaryVec.With(p).Observe(m.value)
		delete(p, "code")
		delete(p, "cluster")
		delete(p, "datacenter")
		c.httpRequestHistogramVec.With(p).Observe(m.value)
		c.actionRegistry <- action
	case "job":
		c.jobExecutionSummaryVec.With(p).Observe(m.value)
		delete(p, "code")
		delete(p, "cluster")
		delete(p, "datacenter")
		c.jobExecutionHistogramVec.With(p).Observe(m.value)
		c.actionRegistry <- action
	}
	atomic.AddInt64(&observed, 1)
}

func (c *collector) recordPageMetrics(m *metric) {
	p := m.props
	p["instance"] = ""
	action := p["action"]
	c.pageHistogramVec.With(p).Observe(m.value)
	c.actionRegistry <- action
	atomic.AddInt64(&observed, 1)
}

func (c *collector) recordAjaxMetrics(m *metric) {
	p := m.props
	p["instance"] = ""
	action := p["action"]
	c.ajaxHistogramVec.With(p).Observe(m.value)
	c.actionRegistry <- action
	atomic.AddInt64(&observed, 1)
}

func (c *collector) processMessage(routingKey string, data map[string]interface{}) {
	if strings.HasPrefix(routingKey, "logs") {
		c.processLogMessage(routingKey, data)
		return
	}
	if strings.HasPrefix(routingKey, "frontend.page") {
		c.processPageMessage(routingKey, data)
		return
	}
	if strings.HasPrefix(routingKey, "frontend.ajax") {
		c.processAjaxMessage(routingKey, data)
	}
}

func (c *collector) processLogMessage(routingKey string, data map[string]interface{}) {
	p := make(map[string]string)
	p["application"] = c.app
	p["environment"] = c.env
	info := extractMap(data, "request_info")
	method := ""
	if info != nil {
		method = strings.ToUpper(extractString(info, "method", ""))
		uri := extractString(info, "url", "")
		if uri != "" {
			u, err := url.Parse(uri)
			if err == nil && strings.HasPrefix(u.Path, c.ignoredRequestURI) {
				atomic.AddInt64(&ignored, 1)
				if verbose {
					log.Info("ignoring request: %s", u.String())
				}
				return
			}
		}
	}
	if method != "" {
		p["metric"] = "http"
		p["http_method"] = method
	} else {
		p["metric"] = "job"
	}
	p["action"] = extractAction(data)
	p["code"] = extractString(data, "code", "500")
	p["instance"] = extractString(data, "host", "unknown")
	p["cluster"] = extractString(data, "cluster", "unknown")
	p["datacenter"] = extractString(data, "datacenter", "unknown")
	valstr := extractString(data, "total_time", "0")
	val, err := strconv.ParseFloat(valstr, 64)
	if err != nil {
		atomic.AddInt64(&dropped, 1)
		if verbose {
			log.Error("could not parse total_time(%s): %s", err)
		}
		return
	}
	c.observeMetrics(&metric{kind: Log, props: p, value: val})
}

func (c *collector) processPageMessage(routingKey string, data map[string]interface{}) {
	rts := extractString(data, "rts", "")
	p := make(map[string]string)
	p["application"] = c.app
	p["environment"] = c.env
	p["action"] = extractAction(data)
	timings, err := frontendmetrics.ExtractPageTimings(rts)
	if err != nil || timings.PageTime > frontendmetrics.OutlierThresholdMs {
		atomic.AddInt64(&dropped, 1)
		if verbose {
			ua := extractString(data, "user_agent", "unknown")
			if err != nil {
				log.Error("could not extract page_time for %s [%s] from %s: %s, user agent: %s", c.appEnv(), p["action"], rts, err, ua)
			} else {
				log.Info("page_time outlier for %s [%s] from %s, user agent: %s, %f", c.appEnv(), p["action"], rts, ua, float64(timings.PageTime)/10000)
			}
		}
		return
	}
	c.observeMetrics(&metric{kind: Page, props: p, value: float64(timings.PageTime)})
}

func (c *collector) processAjaxMessage(routingKey string, data map[string]interface{}) {
	rts := extractString(data, "rts", "")
	p := make(map[string]string)
	p["application"] = c.app
	p["environment"] = c.env
	p["action"] = extractAction(data)
	ajaxTime, err := frontendmetrics.ExtractAjaxTime(rts)
	if err != nil || ajaxTime > frontendmetrics.OutlierThresholdMs {
		atomic.AddInt64(&dropped, 1)
		if verbose {
			ua := extractString(data, "user_agent", "unknown")
			if err != nil {
				log.Error("could not extract ajax_time for %s [%s] from %s: %s, user agent: %s", c.appEnv(), p["action"], rts, err, ua)
			} else {
				log.Info("ajax_time outlier for %s [%s] from %s, user agent: %s, %f", c.appEnv(), p["action"], rts, ua, float64(ajaxTime)/10000)
			}
		}
		return
	}
	c.observeMetrics(&metric{kind: Ajax, props: p, value: float64(ajaxTime)})
}

func initialize() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		log.Error("%s: passing arguments is not supported. please use options instead.", args[0])
		os.Exit(1)
	}
	for _, dc := range strings.Split(opts.Datacenters, ",") {
		if dc != "" {
			datacenters = append(datacenters, dcPair{name: dc, withDots: "." + dc + "."})
		}
	}
	deviceSpecs = make([]string, 0)
	for _, s := range strings.Split(opts.Devices, ",") {
		if s != "" {
			deviceSpecs = append(deviceSpecs, fmt.Sprintf("tcp://%s", s))
		}
	}
	verbose = opts.Verbose
	if opts.StreamURL != "" {
		u, err := url.Parse(opts.StreamURL)
		if err != nil {
			log.Error("could not parse stream url: %s", err)
			os.Exit(1)
		}
		u.Path = path.Join(u.Path, "admin/streams")
		url, env := u.String(), opts.Env
		streams := retrieveStreams(url, env)
		updateStreams(streams, env)
		go streamsUpdater(url, env)
	}
	log.Info("device-specs: %s", strings.Join(deviceSpecs, ","))
	log.Info("datacenters: %+v", datacenters)
}

type stream struct {
	App                 string   `json:"app"`
	Env                 string   `json:"env"`
	IgnoredRequestURI   string   `json:"ignored_request_uri"`
	BackendOnlyRequests string   `json:"backend_only_requests"`
	APIRequests         []string `json:"api_requests"`
}

func (s *stream) AppEnv() string {
	return s.App + "+" + s.Env
}

func streamsUpdater(url, env string) {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		if atomic.LoadUint32(&interrupted) != 0 {
			break
		}
		streams := retrieveStreams(url, env)
		updateStreams(streams, env)
	}
}

func retrieveStreams(url, env string) map[string]stream {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Error("could not create http request: %s", err)
		return nil
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Error("could not retrieve stream: %s", err)
		return nil
	}
	if res.StatusCode != 200 {
		log.Error("unexpected response: %d", res.Status)
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Error("could not read response body: %s", err)
		return nil
	}
	defer res.Body.Close()
	var streams map[string]stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		log.Error("could not parse stream: %s", err)
		return nil
	}
	return streams
}

func updateStreams(streams map[string]stream, env string) {
	if streams == nil {
		return
	}
	log.Info("updating streams")
	suffix := "-" + env
	for s, r := range streams {
		if env == "" || strings.HasSuffix(s, suffix) {
			addCollector(s, r)
		}
	}
	// delete streams which disappeaerd
	mutex.Lock()
	defer mutex.Unlock()
	for s, c := range collectors {
		_, found := streams[s]
		if !found {
			log.Info("removing stream: %s", s)
			delete(collectors, s)
			// make sure to stop go routines associated with the collector
			c.shutdown()
		}
	}
}

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		atomic.AddUint32(&interrupted, 1)
		signal.Stop(c)
	}()
}

// report number of incoming zmq messages every second
func statsReporter() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		if atomic.LoadUint32(&interrupted) != 0 {
			break
		}
		_observed := atomic.SwapInt64(&observed, 0)
		_processed := atomic.SwapInt64(&processed, 0)
		_dropped := atomic.SwapInt64(&dropped, 0)
		_missed := atomic.SwapInt64(&missed, 0)
		_ignored := atomic.SwapInt64(&ignored, 0)
		log.Info("processed: %d, ignored: %d, observed %d, dropped: %d, missed: %d",
			_processed, _ignored, _observed, _dropped, _missed)
	}
}

// web server
func webServer() {
	r := mux.NewRouter()
	r.HandleFunc("/metrics/{application}/{environment}", serveAppMetrics)
	r.HandleFunc("/_system/alive", serveAliveness)
	log.Info("starting http server on port %s", opts.Port)
	spec := ":" + opts.Port
	srv := &graceful.Server{
		Timeout: 10 * time.Second,
		Server: &http.Server{
			Addr:    spec,
			Handler: r,
		},
	}
	err := srv.ListenAndServe()
	if err != nil {
		log.Error("Cannot listen and serve: %s", err)
	}
}

func serveAliveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("ok"))
}

func serveAppMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	app := vars["application"]
	env := vars["environment"]
	c := getCollector(app + "-" + env)
	if c == nil {
		http.NotFound(w, r)
	} else {
		c.requestHandler.ServeHTTP(w, r)
	}
}

//*******************************************************************************

func main() {
	log.Info("%s starting", os.Args[0])
	initialize()

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	installSignalHandler()
	go statsReporter()
	go streamParser()
	webServer()
	log.Info("%s shutting down", os.Args[0])
}

//*******************************************************************************

func streamParser() {
	parser, _ := zmq.NewSocket(zmq.SUB)
	parser.SetLinger(100)
	parser.SetRcvhwm(1000000)
	parser.SetSubscribe("")
	for _, s := range deviceSpecs {
		log.Info("connection sub socket to %s", s)
		err := parser.Connect(s)
		if err != nil {
			log.Error("could not connect: %s", s)
		}
	}
	defer parser.Close()

	poller := zmq.NewPoller()
	poller.Add(parser, zmq.POLLIN)

	sequenceNumbers := make(map[uint32]uint64, 0)

	log.Info("starting %d parsers", opts.Parsers)
	for i := uint(1); i <= opts.Parsers; i++ {
		go decodeAndUnmarshal()
	}

	for {
		if atomic.LoadUint32(&interrupted) != 0 {
			break
		}
		sockets, _ := poller.Poll(1 * time.Second)
		for _, socket := range sockets {
			s := socket.Socket
			msg, err := s.RecvMessageBytes(0)
			if err != nil {
				log.Error("recv message error: %s", err)
				continue
			}
			atomic.AddInt64(&processed, 1)
			if n := len(msg); n != 4 {
				log.Error("invalid message length %s: %s", n, string(msg[0]))
				if atomic.AddInt64(&dropped, 1) == 1 {
					log.Error("got invalid message: %v", msg)
				}
				continue
			}
			info := util.UnpackInfo(msg[3])
			if info == nil {
				log.Error("could not decode meta info: %#x", msg[3])
			}
			lastNumber, ok := sequenceNumbers[info.DeviceNumber]
			if !ok {
				lastNumber = 0
			}
			d, n := info.DeviceNumber, info.SequenceNumber
			sequenceNumbers[d] = n
			if n != lastNumber+1 && n > lastNumber && lastNumber != 0 {
				gap := int64(n - lastNumber + 1)
				if atomic.AddInt64(&missed, gap) == gap {
					log.Error("detected message gap for device %d: missed %d messages", d, gap)
				}
			}
			decoderChannel <- &decodeAndUnmarshalTask{msg: msg, meta: info}
		}
	}
}

var decoderChannel = make(chan *decodeAndUnmarshalTask, 1000000)

type decodeAndUnmarshalTask struct {
	msg  [][]byte
	meta *util.MetaInfo
}

func decodeAndUnmarshal() {
	for {
		if atomic.LoadUint32(&interrupted) != 0 {
			break
		}
		select {
		case task := <-decoderChannel:
			info, msg := task.meta, task.msg
			jsonBody, err := util.Decompress(msg[2], info.CompressionMethod)
			if err != nil {
				log.Error("could not decompress json body: %s", err)
				continue
			}
			if !utf8.Valid(jsonBody) {
				jsonBody = runes.ReplaceIllFormed().Bytes(jsonBody)
				log.Error("replaced invalid utf8 in json body: %s", jsonBody)
			}
			data := make(map[string]interface{})
			if err := json.Unmarshal(jsonBody, &data); err != nil {
				log.Error("invalid json body: %s", err)
				continue
			}
			appEnv := string(msg[0])
			routingKey := string(msg[1])
			if appEnv == "heartbeat" {
				if verbose {
					log.Info("received heartbeat from %s", routingKey)
				}
				continue
			}
			c := getCollector(appEnv)
			if c == nil {
				log.Error("could not retrieve collector for %s", appEnv)
				continue
			}
			c.processMessage(routingKey, data)
		case <-time.After(1 * time.Second):
			// make sure we shut down timely even if no messages arrive
		}
	}
}

func extractString(request map[string]interface{}, key string, defaultValue string) string {
	value := request[key]
	if value == nil {
		return defaultValue
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return defaultValue
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
