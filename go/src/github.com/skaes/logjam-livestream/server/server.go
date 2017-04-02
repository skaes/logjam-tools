package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	// "runtime/pprof"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jessevdk/go-flags"
	zmq "github.com/pebbe/zmq4"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	BindIp       string `short:"b" long:"bind-ip" default:"127.0.0.1" description:"ip address to bind to"`
	ImporterHost string `short:"i" long:"importer-host" default:"127.0.0.1" description:"importer host"`
	Verbose      bool   `short:"v" long:"verbose" description:"be verbose"`
}

var channelBlocked = errors.New("channel blocked")
var verbose = false

//**********************************************************************************
// symbol generator generator
//**********************************************************************************
func genSym(prefix string) func() string {
	var i uint64
	return func() string {
		i++
		return fmt.Sprintf("%s%d", prefix, i)
	}
}

// channel name generator
var nextChannelName = genSym("c-")

//**********************************************************************************
// Ring buffer of strings
//**********************************************************************************
const bufSize = 60

type StringRing struct {
	buf  [bufSize]string
	last int // points to the most recently added element
	size int // current size
}

func newStringRing() *StringRing {
	p := new(StringRing)
	p.last = -1
	return p
}

func (b *StringRing) Add(val string) {
	if b.size < bufSize {
		b.size += 1
	}
	b.last = (b.last + 1) % bufSize
	b.buf[b.last] = val
}

// returns oldest elements first
func (b *StringRing) ForEach(f func(int, string)) {
	if b.size == 0 {
		return
	}
	p := (b.last - b.size + 1) % bufSize
	if p < 0 {
		p = p + bufSize
	}
	for i := 0; i < b.size; i++ {
		f(i, b.buf[p])
		p = (p + 1) % bufSize
	}
}

func (b *StringRing) Send(c chan string) (err error) {
	b.ForEach(func(i int, s string) {
		select {
		case c <- s:
		default:
			if err == nil {
				err = channelBlocked
			}
		}
	})
	return err
}

//**********************************************************************************
// Buffer of float64s.
// Incidentally, it shares the bufSize constant of 60, which
// is used for seconds and minutes.
//**********************************************************************************

type Float64Ring struct {
	buf  [bufSize]float64
	last int // points to the most recently added element
	size int // current size
}

func newFloat64Ring() *Float64Ring {
	p := new(Float64Ring)
	p.last = -1
	return p
}

func (b *Float64Ring) Size() int {
	return b.size
}

func (b *Float64Ring) IsFull() bool {
	return b.size == bufSize
}

func (b *Float64Ring) Reset() {
	b.size = 0
	b.last = -1
}

func (b *Float64Ring) Add(val float64) {
	if b.size < bufSize {
		b.size += 1
	}
	b.last = (b.last + 1) % bufSize
	b.buf[b.last] = val
}

func (b *Float64Ring) Mean() (res float64) {
	if b.size == 0 {
		return
	}
	if b.size-1 == b.last {
		for _, v := range b.buf {
			res += v
		}
		res /= float64(b.size)
		return
	}
	p := (b.last - b.size + 1) % bufSize
	if p < 0 {
		p = p + bufSize
	}
	for i := 0; i < b.size; i++ {
		res += b.buf[p]
		p = (p + 1) % bufSize
	}
	res /= float64(b.size)
	return
}

//**********************************************************************************
//
//**********************************************************************************
type (
	ChannelSet map[string]chan string

	AppInfo struct {
		errors         StringRing
		metrics        StringRing
		lastMinute     Float64Ring
		lastHour       Float64Ring
		channels       ChannelSet
		lastAnomalyMsg string
	}

	AppEnvBufferMap map[string]*AppInfo
)

func (m *AppEnvBufferMap) Get(key string) *AppInfo {
	b := (*m)[key]
	if b == nil {
		b = &AppInfo{
			errors:     *newStringRing(),
			metrics:    *newStringRing(),
			lastMinute: *newFloat64Ring(),
			lastHour:   *newFloat64Ring(),
			channels:   ChannelSet{},
		}
		(*m)[key] = b
	}
	return b
}

func (ai *AppInfo) SendToWebSockets(data string) (err error) {
	for _, c := range ai.channels {
		select {
		case c <- data:
		default:
			err = channelBlocked
		}
	}
	return
}

func (ai *AppInfo) SendLastAnomalyMsg(c chan string) (err error) {
	if ai.lastAnomalyMsg == "" {
		return
	}
	select {
	case c <- ai.lastAnomalyMsg:
	default:
		err = channelBlocked
	}
	return
}

//**********************************************************************************
// main program
//**********************************************************************************

var (
	bind_spec      string
	importer_spec  string
	processed      int64
	ws_connections int64
	app_info       = make(AppEnvBufferMap)
	interrupted    = false
)

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
		logError("%s: passing arguments is obsolete. please use options instead.", args[0])
		os.Exit(1)
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
	final_format := fmt.Sprintf("LJI[%d] %s\n", os.Getpid(), format)
	fmt.Printf(final_format, args...)
}

func logError(format string, args ...interface{}) {
	final_format := fmt.Sprintf("LJE[%d] %s\n", os.Getpid(), format)
	fmt.Fprintf(os.Stderr, final_format, args...)
}

func logWarn(format string, args ...interface{}) {
	final_format := fmt.Sprintf("LJW[%d] %s\n", os.Getpid(), format)
	fmt.Fprintf(os.Stderr, final_format, args...)
}

const (
	perfMsg  = 1
	errorMsg = 2
)

type ZmqMsg struct {
	msgType int
	app_env string
	data    string
}

const (
	subscribeMsg   = 1
	unsubscribeMsg = 2
)

type WsMsg struct {
	msgType int
	name    string
	app_env string
	channel chan string
}

// The dispatcher listens for messages from three sources: translated
// zmq messages coming in from the zmq handler goroutine,
// subscribe/unsunscribe messages from individual web socket handler
// goroutines and timer ticks.
//
// Incoming zmq messages are forwarded to all web socket handlers
// subscribed to the particular message.

var (
	ws_channel  = make(chan *WsMsg, 10000)
	zmq_channel = make(chan *ZmqMsg, 10000)
)

func dispatcher() {
	ticker := time.NewTicker(1 * time.Second)
	for !interrupted {
		select {
		case msg := <-ws_channel:
			handleWebSocketMsg(msg)
		case msg := <-zmq_channel:
			handleZeromqMsg(msg)
		case <-ticker.C:
		}
	}
}

type MetricsData struct {
	TotalTime float64 `json:"total_time"`
}

func extractTotalTime(msg_data string) (res float64, err error) {
	var m MetricsData
	err = json.Unmarshal([]byte(msg_data), &m)
	if err == nil {
		res = m.TotalTime
	}
	return
}

type AnomalyData struct {
	Score     float64 `json:"score"`
	IsAnomaly bool    `json:"anomaly"`
}

func handleZeromqMsg(msg *ZmqMsg) {
	if verbose {
		logInfo("ZMQ msg: %v", *msg)
	}
	ai := app_info.Get(msg.app_env)
	switch msg.msgType {
	case perfMsg:
		ai.metrics.Add(msg.data)
		if tt, err := extractTotalTime(msg.data); err != nil {
			logError("could not extract total time: %v", err)
		} else {
			if ai.lastMinute.IsFull() {
				mean := ai.lastMinute.Mean()
				forecast := ai.lastHour.Mean()
				var ad AnomalyData
				if ai.lastHour.Size() > 0 {
					sum := forecast + mean
					if sum > 0 {
						ad.Score = math.Abs(forecast-mean) / (forecast + mean)
					}
				}
				ad.IsAnomaly = ad.Score > 0.24
				ai.lastHour.Add(mean)
				ai.lastMinute.Reset()
				if data, err := json.Marshal(ad); err != nil {
					logError("could not encode anomaly data as json: %v", err)
				} else {
					// logInfo("ANOMALY DATA: %s", string(data))
					ai.lastAnomalyMsg = string(data)
					ai.SendToWebSockets(ai.lastAnomalyMsg)
				}
			}
			ai.lastMinute.Add(tt)
		}
	case errorMsg:
		ai.errors.Add(msg.data)
	}
	ai.SendToWebSockets(msg.data)
	atomic.AddInt64(&processed, 1)
}

func handleWebSocketMsg(msg *WsMsg) {
	ai := app_info.Get(msg.app_env)
	switch msg.msgType {
	case subscribeMsg:
		logInfo("adding subscription to %s for %s", msg.app_env, msg.name)
		ai.channels[msg.name] = msg.channel
		if err := ai.metrics.Send(msg.channel); err != nil {
			logError("%v", err)
		}
		if err := ai.errors.Send(msg.channel); err != nil {
			logError("%v", err)
		}
		if err := ai.SendLastAnomalyMsg(msg.channel); err != nil {
			logError("%v", err)
		}
	case unsubscribeMsg:
		logInfo("removing subscription to %s for %s", msg.app_env, msg.name)
		delete(ai.channels, msg.name)
		close(msg.channel)
	}
}

//*****************************************************************

func setupSocket() *zmq.Socket {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	subscriber.SetLinger(100)
	subscriber.SetRcvhwm(1000)
	subscriber.SetSubscribe("")
	subscriber.Connect(importer_spec)
	return subscriber
}

// run zmq event loop
func zmqMsgHandler() {
	subscriber := setupSocket()
	defer subscriber.Close()

	poller := zmq.NewPoller()
	poller.Add(subscriber, zmq.POLLIN)

	for !interrupted {
		sockets, _ := poller.Poll(1 * time.Second)
		for _, socket := range sockets {
			s := socket.Socket
			msg, _ := s.RecvMessage(0)
			if len(msg) != 2 {
				logError("got invalid message: %v", msg)
				continue
			}
			var app_env, data = msg[0], msg[1]
			var msgType int
			if strings.Contains(data, "total_time") {
				msgType = perfMsg
			} else {
				msgType = errorMsg
			}
			zmq_channel <- &ZmqMsg{msgType: msgType, app_env: app_env, data: data}
		}
	}
}

// report number of incoming zmq messages every second
func statsReporter() {
	for !interrupted {
		time.Sleep(1 * time.Second)
		msg_count := atomic.SwapInt64(&processed, 0)
		conn_count := atomic.LoadInt64(&ws_connections)
		logInfo("processed: %d, ws connections: %d", msg_count, conn_count)
	}
}

//*******************************************************************************

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func wsReader(ws *websocket.Conn) {
	var dispatcher_input = make(chan string, 1000)
	// channel will be closed by dispatcher, to avoid sending on a closed channel

	var app_env string
	var channel_name string
	writerStarted := false

	for !interrupted {
		msgType, bytes, err := ws.ReadMessage()
		if err != nil || msgType != websocket.TextMessage {
			break
		}
		if !writerStarted {
			app_env = string(bytes[:])
			channel_name = nextChannelName()
			logInfo("starting web socket writer for %s", app_env)
			ws_channel <- &WsMsg{msgType: subscribeMsg, app_env: app_env, name: channel_name, channel: dispatcher_input}
			go wsWriter(app_env, ws, dispatcher_input)
			writerStarted = true
		}
	}
	ws_channel <- &WsMsg{msgType: unsubscribeMsg, app_env: app_env, name: channel_name, channel: dispatcher_input}
}

func wsWriter(app_env string, ws *websocket.Conn, input_from_dispatcher chan string) {
	defer ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1000, ""))
	for !interrupted {
		select {
		case data, ok := <-input_from_dispatcher:
			if !ok {
				logInfo("closed socket for %s?", app_env)
				return
			}
			ws.WriteMessage(websocket.TextMessage, []byte(data))
		case <-time.After(100 * time.Millisecond):
			// give the outer loop a chance to detect interrupts
		}
	}
}

func serveWs(w http.ResponseWriter, r *http.Request) {
	logInfo("received web socket request")
	atomic.AddInt64(&ws_connections, 1)
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		if _, ok := err.(websocket.HandshakeError); !ok {
			log.Println(err)
		}
		return
	}
	defer ws.Close()
	defer (func() {
		atomic.AddInt64(&ws_connections, -1)
	})()
	wsReader(ws)
}

func clientHandler() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveWs)
	web_socket_port := 8080
	if runtime.GOOS == "darwin" {
		web_socket_port = 9608
	}
	logInfo("starting web socket server on port %d", web_socket_port)
	web_socket_spec := ":" + strconv.Itoa(web_socket_port)
	graceful.Run(web_socket_spec, 10*time.Second, mux)
}

//*******************************************************************************

func main() {
	logInfo("%s starting", os.Args[0])
	bind_spec = fmt.Sprintf("tcp://%s:9611", opts.BindIp)
	importer_spec = fmt.Sprintf("tcp://%s:9607", opts.ImporterHost)
	verbose = opts.Verbose
	logInfo("bind-spec:     %s", bind_spec)
	logInfo("importer-spec: %s", importer_spec)

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	installSignalHandler()
	go statsReporter()
	go zmqMsgHandler()
	go dispatcher()
	clientHandler()

	logInfo("%s shutting down", os.Args[0])
}
