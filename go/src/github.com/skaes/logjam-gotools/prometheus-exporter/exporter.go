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
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose   bool   `short:"v" long:"verbose" description:"be verbose"`
	Importer  string `short:"i" long:"importer" default:"127.0.0.1:9612" description:"importer host:port pair"`
	Env       string `short:"e" long:"env" description:"logjam environments to process"`
	Port      string `short:"p" long:"port" default:"8081" description:"port to expose metrics on"`
	StreamURL string `short:"s" long:"stream-url" default:"" description:"Logjam endpoint for retrieveing stream definitions"`
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
)

func getCollector(appEnv string, create bool) *collector {
	mutex.Lock()
	defer mutex.Unlock()
	c := collectors[appEnv]
	if c == nil && create {
		c = newCollector()
		collectors[appEnv] = c
	}
	return c
}

type collector struct {
	httpRequestHistogramVec  *prometheus.HistogramVec
	jobExecutionHistogramVec *prometheus.HistogramVec
	registry                 *prometheus.Registry
	instanceRegistry         chan string
	requestHandler           http.Handler
}

func newCollector() *collector {
	c := collector{
		httpRequestHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "http response time distribution",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
			},
			[]string{"application", "environment", "action", "code", "http_method", "instance", "cluster", "datacenter"},
		),
		jobExecutionHistogramVec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "job_execution_duration_seconds",
				Help:    "background job execution time distribution",
				Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
			},
			[]string{"application", "environment", "action", "code", "instance", "cluster", "datacenter"},
		),
		registry:         prometheus.NewRegistry(),
		instanceRegistry: make(chan string, 10000),
	}
	c.registry.MustRegister(c.httpRequestHistogramVec)
	c.registry.MustRegister(c.jobExecutionHistogramVec)
	c.requestHandler = promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
	go c.instanceRegistryHandler()
	return &c
}

func (c *collector) instanceRegistryHandler() {
	// map from instance names to last seen timestamps
	knownInstances := make(map[string]time.Time)
	// cleaning every minute
	ticker := time.NewTicker(1 * time.Minute)
	// until we are interrupted
	for !interrupted {
		select {
		case instance := <-c.instanceRegistry:
			knownInstances[instance] = time.Now()
		case <-ticker.C:
			threshold := time.Now().Add(-1 * time.Hour)
			for i, v := range knownInstances {
				if v.Before(threshold) {
					logInfo("removing instance: %s", i)
					delete(knownInstances, i)
					labels := prometheus.Labels{"instance": i}
					c.httpRequestHistogramVec.Delete(labels)
					c.jobExecutionHistogramVec.Delete(labels)
				}
			}
		}
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
	var streams map[string]interface{}
	err = json.Unmarshal(body, &streams)
	if err != nil {
		logError("could not parse stream: %s", err)
		return
	}
	suffix := "-" + env
	for s := range streams {
		if env == "" || strings.HasSuffix(s, suffix) {
			logInfo("adding stream: %s", s)
			getCollector(s, true)
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
			recordMetrics(data)
		}
	}
}

func recordMetrics(data string) {
	// fmt.Printf("data: %s\n", data)
	m := make(map[string]string)
	for _, l := range strings.Split(data, "\n") {
		if l == "" {
			continue
		}
		pos := strings.Index(l, ":")
		if pos == -1 {
			logError("line without separator: %s", l)
			return
		}
		m[l[:pos]] = l[pos+1:]
	}
	// fmt.Printf("map: %v\n", m)
	metric := m["metric"]
	instance := m["instance"]
	value, err := strconv.ParseFloat(m["value"], 64)
	if err != nil {
		logError("could not parse float: %s", err)
		return
	}
	delete(m, "metric")
	delete(m, "value")
	// fmt.Printf("metric: %s, value: %f\n", metric, value)
	c := getCollector(m["application"]+"-"+m["environment"], true)
	switch metric {
	case "http":
		c.httpRequestHistogramVec.With(m).Observe(value)
		c.instanceRegistry <- instance
	case "job":
		c.jobExecutionHistogramVec.With(m).Observe(value)
		c.instanceRegistry <- instance
	}
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
	c := getCollector(app+"-"+env, false)
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
