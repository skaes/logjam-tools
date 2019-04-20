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
	"github.com/skaes/logjam-tools/go/util"
	"golang.org/x/text/runes"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Devices     string `short:"d" long:"importer" default:"127.0.0.1:9606" description:"comma separated device specs (host:port pairs)"`
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
		logInfo("adding stream: %s : %+v", appEnv, stream)
		collectors[appEnv] = newCollector(appEnv, stream)
	}
}

func getCollector(appEnv string) *collector {
	mutex.Lock()
	defer mutex.Unlock()
	return collectors[appEnv]
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
	registry                 *prometheus.Registry
	metricsChannel           chan map[string]string
	requestHandler           http.Handler
	actionRegistry           chan string
	knownActions             map[string]time.Time
	stopped                  uint32
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
		registry:          prometheus.NewRegistry(),
		actionRegistry:    make(chan string, 10000),
		apiRequests:       stream.APIRequests,
		ignoredRequestURI: stream.IgnoredRequestURI,
		knownActions:      make(map[string]time.Time),
		metricsChannel:    make(chan map[string]string, 10000),
	}
	c.registry.MustRegister(c.httpRequestHistogramVec)
	c.registry.MustRegister(c.jobExecutionHistogramVec)
	c.registry.MustRegister(c.httpRequestSummaryVec)
	c.registry.MustRegister(c.jobExecutionSummaryVec)
	c.requestHandler = promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
	go c.actionRegistryHandler()
	go c.observer()
	return &c
}

func (c *collector) observeMetrics(m map[string]string) {
	c.metricsChannel <- m
}

func (c *collector) observer() {
	for atomic.LoadUint32(&interrupted)+atomic.LoadUint32(&c.stopped) == 0 {
		m := <-c.metricsChannel
		c.recordMetrics(m)
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
	logInfo("removing action: %s", a)
	delete(c.knownActions, a)
	mfs, err := c.registry.Gather()
	if err != nil {
		logError("could not gather metric families for deletion: %s", err)
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
				}
				if deleted {
					numDeleted++
				} else {
					logError("Could not delete labels: %v", labels)
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
				// fmt.Printf("Fixed datacenter: %s ==> %s\n", dc, d.name)
				break
			}
		}
		if verbose && !fixed {
			logWarn("Could not fix datacenter: %s, application: %s, instance: %s", dc, m["application"], instance)
		}
	}
}

func (c *collector) recordMetrics(m map[string]string) {
	metric := m["metric"]
	instance := m["instance"]
	m["instance"] = ""
	action := m["action"]
	value, err := strconv.ParseFloat(m["value"], 64)
	if err != nil {
		logError("could not parse float: %s", err)
		return
	}
	delete(m, "metric")
	delete(m, "value")
	fixDatacenter(m, instance)
	switch metric {
	case "http":
		m["type"] = c.requestType(action)
		c.httpRequestSummaryVec.With(m).Observe(value)
		delete(m, "code")
		delete(m, "cluster")
		delete(m, "datacenter")
		c.httpRequestHistogramVec.With(m).Observe(value)
		c.actionRegistry <- action
	case "job":
		c.jobExecutionSummaryVec.With(m).Observe(value)
		delete(m, "code")
		delete(m, "cluster")
		delete(m, "datacenter")
		c.jobExecutionHistogramVec.With(m).Observe(value)
		c.actionRegistry <- action
	}
	atomic.AddInt64(&observed, 1)
}

func initialize() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Println(err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		logError("%s: passing arguments is not supported. please use options instead.", args[0])
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
			logError("could not parse stream url: %s", err)
			os.Exit(1)
		}
		u.Path = path.Join(u.Path, "admin/streams")
		url, env := u.String(), opts.Env
		streams := retrieveStreams(url, env)
		updateStreams(streams, env)
		go streamsUpdater(url, env)
	}
	logInfo("device-specs: %s", strings.Join(deviceSpecs, ","))
	logInfo("datacenters: %+v", datacenters)
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
		logError("could not create http request: %s", err)
		return nil
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		logError("could not retrieve stream: %s", err)
		return nil
	}
	if res.StatusCode != 200 {
		logError("unexpected response: %d", res.Status)
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logError("could not read response body: %s", err)
		return nil
	}
	defer res.Body.Close()
	var streams map[string]stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		logError("could not parse stream: %s", err)
		return nil
	}
	return streams
}

func updateStreams(streams map[string]stream, env string) {
	if streams == nil {
		return
	}
	logInfo("updating streams")
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
			logInfo("removing stream: %s", s)
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

func logInfo(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("INFO %s\n", format)
	fmt.Printf(finalFormat, args...)
}

func logError(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("ERROR %s\n", format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

func logWarn(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("WARN %s\n", format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
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
		logInfo("processed: %d, ignored: %d, observed %d, dropped: %d, missed: %d",
			_processed, _ignored, _observed, _dropped, _missed)
	}
}

// web server
func webServer() {
	r := mux.NewRouter()
	r.HandleFunc("/metrics/{application}/{environment}", serveAppMetrics)
	r.HandleFunc("/_system/alive", serveAliveness)
	logInfo("starting http server on port %s", opts.Port)
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
		logError("Cannot listen and serve: %s", err)
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
	logInfo("%s starting", os.Args[0])
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
	logInfo("%s shutting down", os.Args[0])
}

//*******************************************************************************

func streamParser() {
	parser, _ := zmq.NewSocket(zmq.SUB)
	parser.SetLinger(100)
	parser.SetRcvhwm(1000000)
	parser.SetSubscribe("")
	for _, s := range deviceSpecs {
		logInfo("connection sub socket to %s", s)
		err := parser.Connect(s)
		if err != nil {
			logError("could not connect: %s", s)
		}
	}
	defer parser.Close()

	poller := zmq.NewPoller()
	poller.Add(parser, zmq.POLLIN)

	sequenceNumbers := make(map[uint32]uint64, 0)

	logInfo("starting %d parsers", opts.Parsers)
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
				logError("recv message error: %s", err)
				continue
			}
			atomic.AddInt64(&processed, 1)
			if n := len(msg); n != 4 {
				logError("invalid message length %s: %s", n, string(msg[0]))
				if atomic.AddInt64(&dropped, 1) == 1 {
					logError("got invalid message: %v", msg)
				}
				continue
			}
			info := util.UnpackInfo(msg[3])
			if info == nil {
				logError("could not decode meta info: %#x", msg[3])
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
					logError("detected message gap for device %d: missed %d messages", d, gap)
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
				logError("could not decompress json body: %s", err)
				continue
			}
			if !utf8.Valid(jsonBody) {
				jsonBody = runes.ReplaceIllFormed().Bytes(jsonBody)
				logError("replaced invalid utf8 in json body: %s", jsonBody)
			}
			data := make(map[string]interface{})
			if err := json.Unmarshal(jsonBody, &data); err != nil {
				logError("invalid json body: %s", err)
				continue
			}
			appEnv := string(msg[0])
			routingKey := string(msg[1])
			processMessage(appEnv, routingKey, data)
		case <-time.After(1 * time.Second):
			// make sure we shut down timely even if no messages arrive
		}
	}
}

func processMessage(appEnv string, routingKey string, data map[string]interface{}) {
	if strings.Index(routingKey, "logs.") == -1 {
		return
		// TODO: process frontend timings
	}
	c := getCollector(appEnv)
	if c == nil {
		logError("could not retrieve collector for %s", appEnv)
		return
	}
	m := make(map[string]string)
	info := extractMap(data, "request_info")
	method := ""
	if info != nil {
		method = strings.ToUpper(extractString(info, "method", ""))
		uri := extractString(info, "url", "")
		if uri != "" {
			u, err := url.Parse(uri)
			if err == nil && strings.HasPrefix(u.Path, c.ignoredRequestURI) {
				atomic.AddInt64(&ignored, 1)
				// logInfo("ignoring request: %s", u.String())
				return
			}
		}
	}
	if method != "" {
		m["metric"] = "http"
		m["http_method"] = method
	} else {
		m["metric"] = "job"
	}
	m["action"] = extractAction(data)
	m["value"] = extractString(data, "total_time", "0")
	m["code"] = extractString(data, "code", "500")
	m["instance"] = extractString(data, "host", "unknown")
	m["cluster"] = extractString(data, "cluster", "unknown")
	m["datacenter"] = extractString(data, "datacenter", "unknown")
	m["application"] = c.app
	m["environment"] = c.env
	c.observeMetrics(m)
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
