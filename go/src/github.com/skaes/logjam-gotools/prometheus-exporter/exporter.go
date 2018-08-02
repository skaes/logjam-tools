package main

import (
	"encoding/binary"
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
	// "runtime"
	// "runtime/pprof"

	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	zmq "github.com/pebbe/zmq4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promclient "github.com/prometheus/client_model/go"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Importer    string `short:"i" long:"importer" default:"127.0.0.1:9612" description:"importer host:port pair"`
	Env         string `short:"e" long:"env" description:"logjam environments to process"`
	Port        string `short:"p" long:"port" default:"8081" description:"port to expose metrics on"`
	StreamURL   string `short:"s" long:"stream-url" default:"" description:"Logjam endpoint for retrieving stream definitions"`
	Datacenters string `short:"d" long:"datacenters" description:"List of known datacenters, comma separated. Will be used to determine label value if not available on incoming data."`
	CleanAfter  uint   `short:"c" long:"clean-after" default:"60" description:"Minutes to wait before cleaning old instances"`
}

type dcPair struct {
	name     string
	withDots string
}

var (
	verbose      = false
	interrupted  = false
	importerSpec string
	processed    int64
	dropped      int64
	missed       int64
	collectors   = make(map[string]*collector)
	mutex        sync.Mutex
	datacenters  = make([]dcPair, 0)
)

func createCollector(appEnv string, apiRequests []string) {
	c := newCollector(apiRequests)
	mutex.Lock()
	defer mutex.Unlock()
	collectors[appEnv] = c
}

func getCollector(appEnv string) *collector {
	mutex.Lock()
	defer mutex.Unlock()
	return collectors[appEnv]
}

type instanceInfo struct {
	name string
	kind string
}

type collector struct {
	httpRequestHistogramVec  *prometheus.HistogramVec
	jobExecutionHistogramVec *prometheus.HistogramVec
	registry                 *prometheus.Registry
	instanceRegistry         chan instanceInfo
	metricsChannel           chan map[string]string
	requestHandler           http.Handler
	apiRequests              []string
	knownInstances           map[instanceInfo]time.Time
}

func (c *collector) requestType(action string) string {
	for _, p := range c.apiRequests {
		if strings.HasPrefix(action, p) {
			return "api"
		}
	}
	return "web"
}

func newCollector(apiRequests []string) *collector {
	c := collector{
		httpRequestHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "http response time distribution",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
			},
			[]string{"application", "environment", "type", "code", "http_method", "instance", "cluster", "datacenter"},
		),
		jobExecutionHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "job_execution_duration_seconds",
				Help:    "background job execution time distribution",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
			},
			[]string{"application", "environment", "code", "instance", "cluster", "datacenter"},
		),
		registry:         prometheus.NewRegistry(),
		instanceRegistry: make(chan instanceInfo, 10000),
		apiRequests:      apiRequests,
		knownInstances:   make(map[instanceInfo]time.Time),
		metricsChannel:   make(chan map[string]string, 10000),
	}
	c.registry.MustRegister(c.httpRequestHistogramVec)
	c.registry.MustRegister(c.jobExecutionHistogramVec)
	c.requestHandler = promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
	go c.instanceRegistryHandler()
	go c.observer()
	return &c
}

func (c *collector) observeMetrics(m map[string]string) {
	c.metricsChannel <- m
}

func (c *collector) observer() {
	for !interrupted {
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

func (c *collector) removeInstance(i instanceInfo) bool {
	logInfo("removing instance: %s", i.name)
	delete(c.knownInstances, i)
	mfs, err := c.registry.Gather()
	if err != nil {
		logError("could not gather metric families for deletion: %s", err)
		return false
	}
	numProcessed := 0
	numDeleted := 0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			pairs := m.GetLabel()
			if hasLabel(pairs, "instance", i.name) {
				numProcessed++
				labels := labelsFromLabelPairs(pairs)
				deleted := false
				if i.kind == "http" {
					deleted = c.httpRequestHistogramVec.Delete(labels)
				} else {
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

func (c *collector) instanceRegistryHandler() {
	// cleaning every minute, until we are interrupted
	ticker := time.NewTicker(1 * time.Minute)
	for !interrupted {
		select {
		case instance := <-c.instanceRegistry:
			c.knownInstances[instance] = time.Now()
		case <-ticker.C:
			threshold := time.Now().Add(-1 * time.Duration(opts.CleanAfter) * time.Minute)
			for i, v := range c.knownInstances {
				if v.Before(threshold) {
					c.removeInstance(i)
				}
			}
		}
	}
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
	action := m["action"]
	value, err := strconv.ParseFloat(m["value"], 64)
	if err != nil {
		logError("could not parse float: %s", err)
		return
	}
	delete(m, "metric")
	delete(m, "value")
	delete(m, "action")
	fixDatacenter(m, instance)
	switch metric {
	case "http":
		m["type"] = c.requestType(action)
		c.httpRequestHistogramVec.With(m).Observe(value)
		c.instanceRegistry <- instanceInfo{name: instance, kind: metric}
	case "job":
		c.jobExecutionHistogramVec.With(m).Observe(value)
		c.instanceRegistry <- instanceInfo{name: instance, kind: metric}
	}
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
	if opts.StreamURL != "" {
		u, err := url.Parse(opts.StreamURL)
		if err != nil {
			logError("could not parse stream url: %s", err)
			os.Exit(1)
		}
		u.Path = path.Join(u.Path, "admin/streams")
		retrieveStreams(u.String(), opts.Env)
	}
	for _, dc := range strings.Split(opts.Datacenters, ",") {
		if dc != "" {
			datacenters = append(datacenters, dcPair{name: dc, withDots: "." + dc + "."})
		}
	}
	fmt.Printf("datacenters: %+v\n", datacenters)
}

type stream struct {
	APIRequests []string `json:"api_requests"`
}

func retrieveStreams(url, env string) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		logError("could not create http request: %s", err)
		return
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		logError("could not retrieve stream: %s", err)
		return
	}
	if res.StatusCode != 200 {
		logError("unexpected response: %d", res.Status)
		return
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logError("could not read response body: %s", err)
		return
	}
	defer res.Body.Close()
	var streams map[string]stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		logError("could not parse stream: %s", err)
		return
	}
	suffix := "-" + env
	for s, r := range streams {
		if env == "" || strings.HasSuffix(s, suffix) {
			logInfo("adding stream: %s : %v", s, r.APIRequests)
			createCollector(s, r.APIRequests)
		}
	}
}

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		interrupted = true
		signal.Stop(c)
	}()
}

func logInfo(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("LJI[%d] %s\n", os.Getpid(), format)
	fmt.Printf(finalFormat, args...)
}

func logError(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("LJE[%d] %s\n", os.Getpid(), format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

func logWarn(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("LJW[%d] %s\n", os.Getpid(), format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

//*****************************************************************

func setupSocket() *zmq.Socket {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	subscriber.SetLinger(100)
	subscriber.SetRcvhwm(1000000)
	subscriber.SetSubscribe(opts.Env)
	subscriber.Connect(importerSpec)
	return subscriber
}

// run zmq event loop
func zmqMsgHandler() {
	subscriber := setupSocket()
	defer subscriber.Close()

	sequenceNumbers := make(map[string]uint64)

	poller := zmq.NewPoller()
	poller.Add(subscriber, zmq.POLLIN)

	for !interrupted {
		sockets, _ := poller.Poll(1 * time.Second)
		for _, socket := range sockets {
			s := socket.Socket
			msg, _ := s.RecvMessage(0)
			atomic.AddInt64(&processed, 1)
			if len(msg) != 3 {
				if atomic.AddInt64(&dropped, 1) == 1 {
					logError("got invalid message: %v", msg)
				}
				continue
			}
			var env, data, seq = msg[0], msg[1], msg[2]
			n := binary.BigEndian.Uint64([]byte(seq))
			lastNumber, ok := sequenceNumbers[env]
			if !ok {
				lastNumber = 0
			}
			sequenceNumbers[env] = n
			if n != lastNumber+1 && n > lastNumber && lastNumber != 0 {
				gap := int64(n - lastNumber + 1)
				if atomic.AddInt64(&missed, gap) == gap {
					logError("detected message gap for env %s: missed %d messages", env, gap)
				}
			}
			metrics, err := parseMetrics(data)
			if err != nil {
				logError("%s", err)
				continue
			}
			appEnv := metrics["application"] + "-" + metrics["environment"]
			c := getCollector(appEnv)
			if c == nil {
				logError("could not retrieve collector for %s", appEnv)
				continue
			}
			c.observeMetrics(metrics)
		}
	}
}

func parseMetrics(data string) (map[string]string, error) {
	// fmt.Printf("data: %s\n", data)
	m := make(map[string]string)
	for _, l := range strings.Split(data, "\n") {
		if l == "" {
			continue
		}
		pos := strings.Index(l, ":")
		if pos == -1 {
			return m, fmt.Errorf("line without separator: %s", l)
		}
		m[l[:pos]] = l[pos+1:]
	}
	// fmt.Printf("map: %v\n", m)
	return m, nil
}

// report number of incoming zmq messages every second
func statsReporter() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		if interrupted {
			break
		}
		_processed := atomic.SwapInt64(&processed, 0)
		_dropped := atomic.SwapInt64(&dropped, 0)
		_missed := atomic.SwapInt64(&missed, 0)
		logInfo("processed: %d, dropped: %d, missed: %d", _processed, _dropped, _missed)
	}
}

// web server

func webServer() {
	r := mux.NewRouter()
	r.HandleFunc("/metrics/{application}/{environment}", serveAppMetrics)
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
	initialize()
	logInfo("%s starting", os.Args[0])
	importerSpec = fmt.Sprintf("tcp://%s", opts.Importer)
	verbose = opts.Verbose
	logInfo("importer-spec: %s", importerSpec)
	logInfo("env: %s", opts.Env)

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	installSignalHandler()
	go zmqMsgHandler()
	go statsReporter()
	webServer()
	logInfo("%s shutting down", os.Args[0])
}
