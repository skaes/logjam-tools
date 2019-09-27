package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	InputPort   int    `short:"p" long:"input-port" default:"9705" description:"port number of http input socket"`
	CertFile    string `short:"c" long:"cert-file" env:"LOGJAM_CERT_FILE" description:"certificate file to use"`
	KeyFile     string `short:"k" long:"key-file" env:"LOGJAM_KEY_FILE" description:"key file to use"`
	DeviceId    uint32 `short:"d" long:"device-id" description:"device id"`
	OutputPort  uint   `short:"P" long:"output-port" default:"9706" description:"port number of zeromq output socket"`
	SendHwm     int    `short:"S" long:"snd-hwm" env:"LOGJAM_SND_HWM" default:"100000" description:"high water mark for zeromq output socket"`
	IoThreads   int    `short:"i" long:"io-threads" default:"1" description:"number of zeromq io threads"`
	Compression string `short:"x" long:"compress" description:"compression method to use"`
	// DebugPort  int  `short:"D" long:"debug-port" default:"6060" description:"port number of http debug port (pprof)"`
}

var (
	verbose     bool
	quiet       bool
	outputSpec  string
	compression byte

	publisher *pub.Publisher

	// Statistics variables protected by a mutex.
	statsMutex        = &sync.Mutex{}
	processedCount    uint64
	processedBytes    uint64
	processedMaxBytes uint64
	httpFailures      uint64

	// Zeromq PUB sockets are not thread safe, so we run the publisher in a
	// separate go routine.
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
	defer ticker.Stop()
	for !util.Interrupted() {
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

// URL params which neeed to be converted to integer for JSON
var integerKeyList = []string{"viewport_height", "viewport_width", "html_nodes", "script_nodes", "style_nodes", "v"}

// Lookup table for those params
var integerKeys stringSet

func init() {
	integerKeys = make(stringSet)
	for _, k := range integerKeyList {
		integerKeys[k] = true
	}
}

func parseValue(k string, v string) (interface{}, error) {
	if integerKeys[k] {
		return strconv.Atoi(v)
	}
	return v, nil
}

func parseQuery(r *http.Request) (stringMap, error) {
	sm := make(stringMap)
	for k, v := range r.URL.Query() {
		switch len(v) {
		case 1:
			v, err := parseValue(k, v[0])
			if err != nil {
				return sm, err
			}
			sm[k] = v
		case 0:
			sm[k] = ""
		default:
			return sm, fmt.Errorf("Parameter %s specified more than once", k)
		}
	}
	return sm, nil
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

func extractFrontendData(r *http.Request) (stringMap, *util.RequestId, error) {
	sm, err := parseQuery(r)
	if err != nil {
		return sm, nil, err
	}
	// add timestamps
	now := time.Now()
	sm["started_ms"] = now.UnixNano() / int64(time.Millisecond)
	sm["started_at"] = now.Format(time.RFC3339)

	// check protocol version
	if sm["v"] == nil {
		return sm, nil, errors.New("Missing protocol version number: v=1")
	}
	if sm["v"].(int) != 1 {
		return sm, nil, fmt.Errorf("Unsupported protocol version: v=%d", sm["v"].(int))
	}
	// check logjam_action
	if sm["logjam_action"] == nil {
		return sm, nil, errors.New("Missing field: logjam_action")
	}
	// check request_id
	if sm["logjam_request_id"] == nil {
		return sm, nil, errors.New("Missing field: logjam_request_id")
	}
	id := sm["logjam_request_id"].(string)
	// extract app and environment
	rid, err := util.ParseRequestId(id)
	if err != nil {
		return sm, nil, err
	}
	sm["user_agent"] = r.Header.Get("User-Agent")
	// log.Info("SM: %+v, RID: %+v", sm, rid)
	return sm, rid, nil
}

func sendFrontendData(rid *util.RequestId, msgType string, sm stringMap) error {
	appEnv := rid.AppEnv()
	routingKey := rid.RoutingKey("frontend", msgType)
	data, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	publisher.Publish(appEnv, routingKey, data)
	return nil
}

func writeErrorResponse(w http.ResponseWriter, txt string) {
	statsMutex.Lock()
	httpFailures++
	statsMutex.Unlock()
	http.Error(w, "400 RTFM", 400)
	fmt.Fprintln(w, txt)
}

func writeImageResponse(w http.ResponseWriter) {
	w.WriteHeader(200)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "image/gif")
	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("Content-Transfer-Encoding", "base64")
	io.WriteString(w, "R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==")
}

func serveFrontendRequest(w http.ResponseWriter, r *http.Request) {
	defer recordRequest(r)
	sm, rid, err := extractFrontendData(r)
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	msgType := extractFrontendMsgType(r)
	err = sendFrontendData(rid, msgType, sm)
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	writeImageResponse(w)
}

func extractFrontendMsgType(r *http.Request) string {
	if r.URL.Path == "/logjam/ajax" {
		return "ajax"
	}
	if r.URL.Path == "/logjam/page" {
		return "page"
	}
	return "unknown"
}

func serveAlive(w http.ResponseWriter, r *http.Request) {
	defer recordRequest(r)
	w.WriteHeader(200)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, "ALIVE\n")
}

func serveMobileMetrics(w http.ResponseWriter, r *http.Request) {
	defer recordRequest(r)
	bytes, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	appEnv := "mobile-production"
	routingKey := fmt.Sprintf("%s.%s", "frontend", "mobile")
	publisher.Publish(appEnv, routingKey, bytes)
	writeImageResponse(w)
}

func webServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/logjam/ajax", serveFrontendRequest)
	mux.HandleFunc("/logjam/page", serveFrontendRequest)
	mux.HandleFunc("/logjam/mobile", serveMobileMetrics)
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
	outputSpec = fmt.Sprintf("tcp://*:%d", opts.OutputPort)
	verbose = opts.Verbose
	quiet = opts.Quiet
	log.Info("device-id: %d", opts.DeviceId)
	log.Info("output-spec: %s", outputSpec)
	// debugSpec := fmt.Sprintf("localhost:%d", opts.DebugPort)
	// logInfo("debug-spec: %s", debugSpec)

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	util.InstallSignalHandler()
	go statsReporter()
	publisher = pub.New(&wg, pub.Opts{
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
