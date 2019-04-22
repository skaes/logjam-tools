package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	// _ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jessevdk/go-flags"
	log "github.com/skaes/logjam-tools/go/logging"
	pub "github.com/skaes/logjam-tools/go/publisher"
	"github.com/skaes/logjam-tools/go/util"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Quiet       bool   `short:"q" long:"quiet" description:"be quiet"`
	BindIP      string `short:"b" long:"bind-ip" env:"LOGJAM_BIND_IP" default:"127.0.0.1" description:"ip address to bind to"`
	InputPort   int    `short:"p" long:"input-port" default:"9805" description:"port number of http input socket"`
	CertFile    string `short:"c" long:"cert-file" env:"LOGJAM_CERT_FILE" description:"certificate file to use"`
	KeyFile     string `short:"k" long:"key-file" env:"LOGJAM_KEY_FILE" description:"key file to use"`
	DeviceId    uint32 `short:"d" long:"device-id" description:"device id"`
	OutputPort  uint   `short:"P" long:"output-port" default:"9806" description:"port number of zeromq output socket"`
	SendHwm     int    `short:"S" long:"snd-hwm" env:"LOGJAM_SND_HWM" default:"100000" description:"high water mark for zeromq output socket"`
	IoThreads   int    `short:"i" long:"io-threads" default:"1" description:"number of zeromq io threads"`
	Compression string `short:"x" long:"compress" description:"compression method to use"`
	// DebugPort  int  `short:"D" long:"debug-port" default:"6060" description:"port number of http debug port (pprof)"`
}

var (
	verbose     bool
	quiet       bool
	outputSpec  string
	interrupted bool
	compression byte

	publisher *pub.Publisher

	// Statistics variables protected by a mutex.
	statsMutex        = &sync.Mutex{}
	processedCount    uint64
	processedBytes    uint64
	processedMaxBytes uint64
	httpFailures      uint64
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
		log.Error("%s: arguments are ingored, please use options instead.", args[0])
		os.Exit(1)
	}
	// Determine compression method.
	compression, err = util.ParseCompressionMethodName(opts.Compression)
	if err != nil {
		log.Error("%s: unsupported compression method: %s.", args[0], opts.Compression)
		os.Exit(1)
	}
}

// report number of processed requests every second
func statsReporter() {
	ticker := time.NewTicker(1 * time.Second)
	for !interrupted {
		<-ticker.C
		// obtain values and reset counters
		statsMutex.Lock()
		count := processedCount
		bytes := processedBytes
		maxBytes := processedMaxBytes
		failures := httpFailures
		processedCount = 0
		processedBytes = 0
		processedMaxBytes = 0
		httpFailures = 0
		statsMutex.Unlock()
		// report
		kb := float64(bytes) / 1024.0
		maxkb := float64(maxBytes) / 1024.0
		var avgkb float64
		if count > 0 {
			avgkb = kb / float64(count)
		}
		if !quiet {
			log.Info("processed %d, invalid %d, size: %.2f KB, avg: %.2f KB, max: %.2f", count, failures, kb, avgkb, maxkb)
		}
	}
}

var wg sync.WaitGroup

type (
	stringMap map[string]interface{}
	stringSet map[string]bool
)

func (sm stringMap) DeleteString(k string) string {
	v, _ := sm[k].(string)
	delete(sm, k)
	return v
}

// No thanks to https://github.com/golang/go/issues/19644, this is only an
// approximation of the actual number of bytes transferred.
func requestSize(r *http.Request) uint64 {
	size := uint64(len(r.URL.String()))
	for k, values := range r.Header {
		l := len(k)
		for _, v := range values {
			size += uint64(l + len(v) + 4) // k: v\r\n
		}
	}
	if r.ContentLength > 0 {
		size += uint64(r.ContentLength)
	}
	return size
}

func recordRequest(r *http.Request) {
	size := requestSize(r)
	statsMutex.Lock()
	processedCount++
	processedBytes += size
	if processedMaxBytes < size {
		processedMaxBytes = size
	}
	statsMutex.Unlock()
}

func recordFailure() {
	statsMutex.Lock()
	httpFailures++
	statsMutex.Unlock()
}

func serveEvents(w http.ResponseWriter, r *http.Request) {
	defer recordRequest(r)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "text/plain")
	if r.Method != "POST" {
		recordFailure()
		w.WriteHeader(400)
		io.WriteString(w, "Can only POST to this resource\n")
		return
	}
	ct := r.Header.Get("Content-Type")
	if ct != "application/json" {
		recordFailure()
		w.WriteHeader(415)
		io.WriteString(w, "Content-Type needs to be application/json\n")
		return
	}
	decoder := json.NewDecoder(r.Body)
	var data stringMap
	err := decoder.Decode(&data)
	if err != nil {
		recordFailure()
		w.WriteHeader(400)
		io.WriteString(w, "Request body is not valid JSON\n")
		return
	}
	app := data.DeleteString("app")
	env := data.DeleteString("env")
	if app == "" || env == "" {
		recordFailure()
		w.WriteHeader(400)
		io.WriteString(w, "Request body is missing proper app and env specs\n")
		return
	}
	appEnv := app + "-" + env
	routingKey := "events." + appEnv
	msgBody, err := json.Marshal(data)
	if err != nil {
		recordFailure()
		w.WriteHeader(500)
		return
	}
	publisher.Publish(appEnv, routingKey, msgBody)
	w.WriteHeader(202)
}

func serveAlive(w http.ResponseWriter, r *http.Request) {
	defer recordRequest(r)
	w.WriteHeader(200)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, "ALIVE\n")
}

func webServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/logjam/events", serveEvents)
	mux.HandleFunc("/alive.txt", serveAlive)
	log.Info("starting http server on port %d", opts.InputPort)
	spec := ":" + strconv.Itoa(opts.InputPort)
	srv := &graceful.Server{
		Timeout: 10 * time.Second,
		Server: &http.Server{
			Addr:    spec,
			Handler: mux,
		},
	}
	if opts.KeyFile != "" && opts.CertFile != "" {
		err := srv.ListenAndServeTLS(opts.CertFile, opts.KeyFile)
		if err != nil {
			log.Error("Cannot listen and serve TLS: %s", err)
		}
	} else if opts.KeyFile != "" {
		log.Error("cert-file given but no key-file!")
	} else if opts.CertFile != "" {
		log.Error("key-file given but no cert-file!")
	} else {
		err := srv.ListenAndServe()
		if err != nil {
			log.Error("Cannot listen and serve TLS: %s", err)
		}
	}
}

// func runProfiler(debugSpec string) {
// 	fmt.Println(http.ListenAndServe(debugSpec, nil))
// }

func main() {
	log.Info("%s starting", os.Args[0])
	initialize()
	outputSpec = fmt.Sprintf("tcp://%s:%d", opts.BindIP, opts.OutputPort)
	verbose = opts.Verbose
	quiet = opts.Quiet
	log.Info("device-id: %d", opts.DeviceId)
	log.Info("output-spec: %s", outputSpec)
	// debugSpec := fmt.Sprintf("localhost:%d", opts.DebugPort)
	// log.Info("debug-spec: %s", debugSpec)

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	util.InstallSignalHandler(&interrupted)
	go statsReporter()
	publisher = pub.New(&wg, &interrupted, pub.Opts{
		Compression: compression,
		DeviceId:    opts.DeviceId,
		OutputPort:  opts.OutputPort,
		OutputSpec:  outputSpec,
		SendHwm:     opts.SendHwm,
	})
	// go runProfiler(debugSpec)
	// Run web server in the foreground. It has its own signal handler.
	webServer()
	// Wait for publisher and stats reporter to finsh.
	if util.WaitForWaitGroupWithTimeout(&wg, 5*time.Second) {
		if !quiet {
			log.Info("shut down timed out")
		}
	} else {
		if !quiet {
			log.Info("shut down performed cleanly")
		}
	}
}
