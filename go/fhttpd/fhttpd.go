package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	// _ "net/http/pprof"

	"github.com/jessevdk/go-flags"
	log "github.com/skaes/logjam-tools/go/logging"
	pub "github.com/skaes/logjam-tools/go/publisher"
	"github.com/skaes/logjam-tools/go/util"
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
	publisher.Publish(appEnv, routingKey, data, util.NoCompression)
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
	defer recordRequestStats(r)
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
	defer recordRequestStats(r)
	w.WriteHeader(200)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, "ALIVE\n")
}

func serveMobileMetrics(w http.ResponseWriter, r *http.Request) {
	defer recordRequestStats(r)
	bytes, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	appEnv := "mobile-production"
	routingKey := "mobile"
	publisher.Publish(appEnv, routingKey, bytes, util.NoCompression)
	writeImageResponse(w)
}

func setupWebServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/logjam/ajax", serveFrontendRequest)
	mux.HandleFunc("/logjam/page", serveFrontendRequest)
	mux.HandleFunc("/logjam/mobile", serveMobileMetrics)
	mux.HandleFunc("/alive.txt", serveAlive)
	spec := ":" + strconv.Itoa(opts.InputPort)
	return &http.Server{
		Addr:    spec,
		Handler: mux,
	}
}

func runWebServer(srv *http.Server) {
	log.Info("starting http server on %s", srv.Addr)
	if opts.KeyFile != "" && opts.CertFile != "" {
		err := srv.ListenAndServeTLS(opts.CertFile, opts.KeyFile)
		if err != nil && err != http.ErrServerClosed {
			log.Error("Cannot listen and serve TLS: %s", err)
		}
	} else if opts.KeyFile != "" {
		log.Error("cert-file given but no key-file!")
	} else if opts.CertFile != "" {
		log.Error("key-file given but no cert-file!")
	} else {
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Error("Cannot listen and serve: %s", err)
		}
	}
}

func shutdownWebServer(srv *http.Server, gracePeriod time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), gracePeriod)
	defer cancel()
	err := srv.Shutdown(ctx)
	if err != nil {
		log.Error("web server shutdown failed: %+v", err)
	} else {
		log.Info("web server shutdown successful")
	}
}

// func runProfiler(debugSpec string) {
// 	fmt.Println(http.ListenAndServe(debugSpec, nil))
// }

func waitForInterrupt() {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done
	log.Info("received interrupt")
}

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
	// go runProfiler(debugSpec)

	util.InstallSignalHandler()
	go statsReporter()
	publisher = pub.New(&wg, pub.Opts{
		Compression: compression,
		DeviceId:    opts.DeviceId,
		OutputPort:  opts.OutputPort,
		OutputSpec:  outputSpec,
		SendHwm:     opts.SendHwm,
	})

	srv := setupWebServer()
	go runWebServer(srv)
	waitForInterrupt()
	shutdownWebServer(srv, 5*time.Second)

	// Wait for publisher and stats reporter to finsh.
	if util.WaitForWaitGroupWithTimeout(&wg, 3*time.Second) {
		if !quiet {
			log.Info("publisher shut down timed out")
		}
	} else {
		if !quiet {
			log.Info("publisher shut down performed cleanly")
		}
	}
}
