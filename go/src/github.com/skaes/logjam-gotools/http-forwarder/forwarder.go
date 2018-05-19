package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ShowMax/go-fqdn"
	"github.com/golang/snappy"
	"github.com/jessevdk/go-flags"
	zmq "github.com/pebbe/zmq4"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Quiet       bool   `short:"q" long:"quiet" description:"be quiet"`
	BindIP      string `short:"b" long:"bind-ip" env:"LOGJAM_BIND_IP" default:"127.0.0.1" description:"ip address to bind to"`
	InputPort   int    `short:"p" long:"input-port" default:"9805" description:"port number of http input socket"`
	CertFile    string `short:"c" long:"cert-file" env:"LOGJAM_CERT_FILE" description:"certificate file to use"`
	KeyFile     string `short:"k" long:"key-file" env:"LOGJAM_KEY_FILE" description:"key file to use"`
	DeviceId    int    `short:"d" long:"device-id" description:"device id"`
	OutputPort  int    `short:"P" long:"output-port" default:"9806" description:"port number of zeromq output socket"`
	SendHwm     int    `short:"S" long:"snd-hwm" env:"LOGJAM_SND_HWM" default:"100000" description:"high water mark for zeromq output socket"`
	IoThreads   int    `short:"i" long:"io-threads" default:"1" description:"number of zeromq io threads"`
	Compression string `short:"x" long:"compress" description:"compression method to use"`
	Frontend    bool   `short:"f" long:"frontend" description:"whether to process frontend metrics or forward requests"`
	DebugPort   int    `short:"D" long:"debug-port" default:"6060" description:"port number of http debug port (pprof)"`
}

var (
	verbose     bool
	quiet       bool
	outputSpec  string
	interrupted bool
	compression byte

	// Statistics variables protected by a mutex.
	statsMutex        = &sync.Mutex{}
	processedCount    uint64
	processedBytes    uint64
	processedMaxBytes uint64

	// Zeromq PUB sockets are not thread safe, so we run the publisher in a
	// separate go routine.
	publisherChannel = make(chan *PubMsg, 1000)
)

type PubMsg struct {
	appEnv      string
	routingKey  string
	data        []byte
	compression byte
}

func init() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Println(err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		logError("%s: arguments are ingored, please use options instead.", args[0])
		os.Exit(1)
	}
	// Determine compression method.
	if opts.Compression == "snappy" {
		compression = SnappyCompression
	} else if opts.Compression == "zlib" {
		compression = ZlibCompression
	} else if opts.Compression != "" {
		logError("%s: unsupported compression method: %s.", args[0], opts.Compression)
		os.Exit(1)
	}
}

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, os.Interrupt)
	go func() {
		<-c
		interrupted = true
		signal.Stop(c)
	}()
}

func logInfo(format string, args ...interface{}) {
	final_format := "[I] " + format + "\n"
	fmt.Printf(final_format, args...)
}

func logError(format string, args ...interface{}) {
	final_format := "[E] " + format + "\n"
	fmt.Fprintf(os.Stderr, final_format, args...)
}

func logFatal(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	panic(msg)
}

func logWarn(format string, args ...interface{}) {
	final_format := fmt.Sprintf("LJF[%d] %s\n", os.Getpid(), format)
	fmt.Fprintf(os.Stderr, final_format, args...)
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
		processedCount = 0
		processedBytes = 0
		processedMaxBytes = 0
		statsMutex.Unlock()
		// report
		kb := float64(bytes) / 1024.0
		maxkb := float64(maxBytes) / 1024.0
		var avgkb float64
		if count > 0 {
			avgkb = kb / float64(count)
		}
		if !quiet {
			logInfo("processed %d requests, size: %.2f KB, avg: %.2f KB, max: %.2f", count, kb, avgkb, maxkb)
		}
	}
}

func sendMessage(socket *zmq.Socket, msg *PubMsg) {
	// logInfo("Sending logjam message: %+v", data)
	socket.SendBytes([]byte(msg.appEnv), zmq.SNDMORE)
	socket.SendBytes([]byte(msg.routingKey), zmq.SNDMORE)
	socket.SendBytes(msg.data, zmq.SNDMORE)
	meta := packInfo(nextSequenceNumber(), msg.compression)
	socket.SendBytes(meta, 0)
}

const HeartbeatInterval = 5

func pubSocketSpecForConnecting() string {
	return fmt.Sprintf("tcp://%s:%d", fqdn.Get(), opts.OutputPort)
}

func sendHeartbeat(socket *zmq.Socket) {
	socket.SendBytes([]byte("heartbeat"), zmq.SNDMORE)
	socket.SendBytes([]byte(pubSocketSpecForConnecting()), zmq.SNDMORE)
	socket.SendBytes([]byte("{}"), zmq.SNDMORE)
	meta := packInfo(nextSequenceNumber(), NoCompression)
	socket.SendBytes(meta, 0)
}

// publish Zer0MQ messages (needs to happen in a separate go routine)
func publisher(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	publisherSocket := setupPublisherSocket()
	ticker := time.NewTicker(100 * time.Millisecond)
	var ticks uint64 = 0
	for !interrupted {
		select {
		case msg := <-publisherChannel:
			sendMessage(publisherSocket, msg)
		case <-ticker.C:
			ticks++
			if ticks%(HeartbeatInterval*10) == 0 {
				sendHeartbeat(publisherSocket)
			}
		}
	}
	err := publisherSocket.Close()
	if err != nil {
		logError("Could not close publisher socker on shut down: %s", err)
	}
}

func setupPublisherSocket() *zmq.Socket {
	publisher, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		logFatal("Could not create publisher socket: %s", err)
	}
	publisher.SetLinger(1000)
	publisher.SetSndhwm(opts.SendHwm)
	publisher.Bind(outputSpec)
	return publisher
}

var sequenceNum uint64

func nextSequenceNumber() uint64 {
	sequenceNum += 1
	return sequenceNum
}

const (
	MetaInfoVersion = 1
	MetaInfoTag     = 0xcabd

	NoCompression     = 0
	ZlibCompression   = 1
	SnappyCompression = 2
)

func zclockTime() uint64 {
	return (uint64)(time.Now().UnixNano()) / 1000000
}

type MetaInfo struct {
	Tag               uint16
	CompressionMethod uint8
	Version           uint8
	DeviceNumber      uint32
	Timestamp         uint64
	SequenceNumber    uint64
}

func packInfo(seqNum uint64, compression byte) []byte {
	data := make([]byte, 24)
	binary.BigEndian.PutUint16(data, MetaInfoTag)
	data[2] = compression
	data[3] = MetaInfoVersion
	binary.BigEndian.PutUint32(data[4:8], uint32(opts.DeviceId))
	binary.BigEndian.PutUint64(data[8:16], zclockTime())
	binary.BigEndian.PutUint64(data[16:24], seqNum)
	return data
}

func unpackInfo(data []byte) *MetaInfo {
	if len(data) != 24 {
		return nil
	}
	info := &MetaInfo{
		Tag:               binary.BigEndian.Uint16(data[0:2]),
		CompressionMethod: data[2],
		Version:           data[3],
		DeviceNumber:      binary.BigEndian.Uint32(data[4:8]),
		Timestamp:         binary.BigEndian.Uint64(data[8:16]),
		SequenceNumber:    binary.BigEndian.Uint64(data[16:24]),
	}
	return info
}

var wg sync.WaitGroup

func waitForWaitGrouptWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}

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

// A parsed request id
type requestId struct {
	App string
	Env string
	Id  string
}

func parseRequestId(id string) (rid requestId, err error) {
	slices := strings.Split(id, "-")
	if len(slices) != 3 {
		err = fmt.Errorf("Wrong request id format: %s", id)
		return
	}
	rid.App = slices[0]
	rid.Env = slices[1]
	rid.Id = slices[2]
	return
}

func (rid *requestId) appEnv() string {
	return rid.App + "-" + rid.Env
}

func (rid *requestId) routingKey(prefix string, msgType string) string {
	return fmt.Sprintf("%s.%s.%s.%s", prefix, msgType, rid.App, rid.Env)
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

func extractFrontendData(r *http.Request) (stringMap, *requestId, error) {
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
	request_id := sm["logjam_request_id"].(string)
	// extract app and environment
	rid, err := parseRequestId(request_id)
	if err != nil {
		return sm, nil, err
	}
	// logInfo("SM: %+v, RID: %+v", sm, rid)
	return sm, &rid, nil
}

func sendFrontendData(rid *requestId, msgType string, sm stringMap) error {
	appEnv := rid.appEnv()
	routingKey := rid.routingKey("frontend", msgType)
	data, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	publish(appEnv, routingKey, data)
	return nil
}

func writeErrorResponse(w http.ResponseWriter, txt string) {
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

// publish msg for publisher routine, optionally compressing it
func publish(appEnv string, routingKey string, data []byte) {
	var usedCompression byte = NoCompression
	switch compression {
	case SnappyCompression:
		compressedData := snappy.Encode(nil, data)
		if len(compressedData) < len(data) {
			data = compressedData
			usedCompression = compression
		}
	case ZlibCompression:
		buf := bytes.Buffer{}
		w := zlib.NewWriter(&buf)
		_, err := w.Write(data)
		w.Close()
		if err == nil && buf.Len() < len(data) {
			data = buf.Bytes()
			usedCompression = compression
		}
	}
	publisherChannel <- &PubMsg{appEnv: appEnv, routingKey: routingKey, data: data, compression: usedCompression}
}

func serveEvents(w http.ResponseWriter, r *http.Request) {
	defer recordRequest(r)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "text/plain")
	if r.Method != "POST" {
		w.WriteHeader(400)
		io.WriteString(w, "Can only POST to this resource\n")
		return
	}
	ct := r.Header.Get("Content-Type")
	if ct != "application/json" {
		w.WriteHeader(415)
		io.WriteString(w, "Content-Type needs to be application/json\n")
		return
	}
	decoder := json.NewDecoder(r.Body)
	var data stringMap
	err := decoder.Decode(&data)
	if err != nil {
		w.WriteHeader(400)
		io.WriteString(w, "Request body is not valid JSON\n")
		return
	}
	app := data.DeleteString("app")
	env := data.DeleteString("env")
	if app == "" || env == "" {
		w.WriteHeader(400)
		io.WriteString(w, "Request body is missing proper app and env specs\n")
		return
	}
	appEnv := app + "-" + env
	routingKey := "events." + appEnv
	msgBody, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	publish(appEnv, routingKey, msgBody)
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
	if opts.Frontend {
		mux.HandleFunc("/logjam/ajax", serveFrontendRequest)
		mux.HandleFunc("/logjam/page", serveFrontendRequest)
	} else {
		mux.HandleFunc("/logjam/events", serveEvents)
	}
	mux.HandleFunc("/alive.txt", serveAlive)
	logInfo("starting http server on port %d", opts.InputPort)
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
			logError("Cannot list and serve TLS: %s", err)
		}
	} else if opts.KeyFile != "" {
		logError("cert-file given but no key-file!")
	} else if opts.CertFile != "" {
		logError("key-file given but no cert-file!")
	} else {
		err := srv.ListenAndServe()
		if err != nil {
			logError("Cannot list and serve TLS: %s", err)
		}
	}
}

func runProfiler(debugSpec string) {
	fmt.Println(http.ListenAndServe(debugSpec, nil))
}

func main() {
	logInfo("%s starting", os.Args[0])
	outputSpec = fmt.Sprintf("tcp://%s:%d", opts.BindIP, opts.OutputPort)
	verbose = opts.Verbose
	quiet = opts.Quiet
	logInfo("device-id: %d", opts.DeviceId)
	logInfo("output-spec: %s", outputSpec)
	debugSpec := fmt.Sprintf("localhost:%d", opts.DebugPort)
	logInfo("debug-spec: %s", debugSpec)

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	installSignalHandler()
	go statsReporter()
	go publisher(&wg)
	go runProfiler(debugSpec)
	// Run web server in the foreground. It has its own signal handler.
	webServer()
	// Wait for publisher and stats reporter to finsh.
	if waitForWaitGrouptWithTimeout(&wg, 5*time.Second) {
		if !quiet {
			logInfo("shut down timed out")
		}
	} else {
		if !quiet {
			logInfo("shut down performed cleanly")
		}
	}
}
