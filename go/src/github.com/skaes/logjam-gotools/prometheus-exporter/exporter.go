package main

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	// "runtime"
	// "runtime/pprof"

	"github.com/jessevdk/go-flags"
	zmq "github.com/pebbe/zmq4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose  bool   `short:"v" long:"verbose" description:"be verbose"`
	Importer string `short:"i" long:"importer" default:"127.0.0.1:9612" description:"importer host:port pair"`
	Env      string `short:"e" long:"env" default:"production" description:"logjam environment to use"`
	Port     string `short:"p" long:"port" default:"8081" description:"port to expose metrics on"`
}

var (
	verbose                 = false
	interrupted             = false
	importerSpec            string
	processed               int64
	dropped                 int64
	missed                  int64
	httpRequestHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "http response time distribution",
			Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
		},
		[]string{"application", "action", "code", "http_method", "instance", "cluster", "datacenter"},
	)
	jobExecutionHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "job_execution_duration_seconds",
			Help:    "background job execution time distribution",
			Buckets: []float64{0.001, 0.0025, .005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25, 50, 100},
		},
		[]string{"application", "action", "code", "instance", "cluster", "datacenter"},
	)
	registry         = prometheus.NewRegistry()
	instanceRegistry = make(chan string, 10000)
)

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
	setupPrometheus()
}

func setupPrometheus() {
	registry.MustRegister(httpRequestHistogramVec)
	registry.MustRegister(jobExecutionHistogramVec)
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
	switch metric {
	case "http":
		httpRequestHistogramVec.With(m).Observe(value)
		instanceRegistry <- instance
	case "job":
		jobExecutionHistogramVec.With(m).Observe(value)
		instanceRegistry <- instance
	}
}

func instanceRegistryHandler() {
	// map from instance names to last seen timestamps
	knownInstances := make(map[string]time.Time)
	// cleaning every minute
	ticker := time.NewTicker(1 * time.Minute)
	// until we are interrupted
	for !interrupted {
		select {
		case instance := <-instanceRegistry:
			knownInstances[instance] = time.Now()
		case <-ticker.C:
			threshold := time.Now().Add(-1 * time.Hour)
			for i, v := range knownInstances {
				if v.Before(threshold) {
					logInfo("removing instance: %s", i)
					delete(knownInstances, i)
					labels := prometheus.Labels{"instance": i}
					httpRequestHistogramVec.Delete(labels)
					jobExecutionHistogramVec.Delete(labels)
				}
			}
		}
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
	mux := http.NewServeMux()
	promHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	handlerFunc := func(w http.ResponseWriter, r *http.Request) { promHandler.ServeHTTP(w, r) }
	mux.HandleFunc("/metrics", handlerFunc)
	logInfo("starting http server on port %s", opts.Port)
	spec := ":" + opts.Port
	srv := &graceful.Server{
		Timeout: 10 * time.Second,
		Server: &http.Server{
			Addr:    spec,
			Handler: mux,
		},
	}
	err := srv.ListenAndServe()
	if err != nil {
		logError("Cannot listen and serve: %s", err)
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
	go instanceRegistryHandler()
	webServer()
	logInfo("% shutting down", os.Args[0])
}
