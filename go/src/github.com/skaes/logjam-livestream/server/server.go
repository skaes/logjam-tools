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
	AnomalyHost  string `short:"a" long:"anomaly-host" default:"127.0.0.1" description:"anomaly host"`
	BindIp       string `short:"b" long:"bind-ip" default:"127.0.0.1" description:"ip address to bind to"`
	ImporterHost string `short:"i" long:"importer-host" default:"127.0.0.1" description:"importer host"`
}

var channelBlocked = errors.New("channel blocked")

func genSym(prefix string) func() string {
	var i uint64
	return func() string {
		i++
		return fmt.Sprintf("%s%d", prefix, i)
	}
}

var nextChannelName = genSym("c-")

const bufSize = 60

type StringBuffer struct {
	buf  [bufSize]string
	last int // points to the most recently added element
	size int // current size
}

func newStringBuffer() *StringBuffer {
	p := new(StringBuffer)
	p.last = -1
	return p
}

func (b *StringBuffer) Add(val string) {
	if b.size < bufSize {
		b.size += 1
	}
	b.last = (b.last + 1) % bufSize
	b.buf[b.last] = val
}

// returns oldest elements first
func (b *StringBuffer) ForEach(f func(int, string)) {
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

func (b *StringBuffer) Send(c chan string) (err error) {
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

type AppEnvBufferMap map[string]*StringBuffer

func (m *AppEnvBufferMap) Get(key string) *StringBuffer {
	b := (*m)[key]
	if b == nil {
		b = newStringBuffer()
		(*m)[key] = b
	}
	return b
}

func (m *AppEnvBufferMap) Add(key, val string) {
	m.Get(key).Add(val)
}

func (m *AppEnvBufferMap) Send(key string, c chan string) error {
	return m.Get(key).Send(c)
}

type (
	ChannelSet map[string]chan string
	ChannelMap map[string]*ChannelSet
)

func (m *ChannelMap) Add(key string, name string, val chan string) {
	s := (*m)[key]
	if s == nil {
		s = &ChannelSet{}
		(*m)[key] = s
	}
	(*s)[name] = val
}

func (m *ChannelMap) Remove(key string, name string, val chan string) {
	s := (*m)[key]
	if s == nil {
		return
	}
	delete(*s, name)
}

var (
	perf_buffers  = make(AppEnvBufferMap)
	error_buffers = make(AppEnvBufferMap)
	channel_map   = make(ChannelMap)
	// @anomaly_scores = Hash.new(0)
	bind_spec      string
	anomaly_spec   string
	importer_spec  string
	processed      int64
	ws_connections int64
	interrupted    bool = false
)

func init() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		os.Exit(1)
	}
	if len(args) > 0 {
		opts.BindIp = args[0]
	}
	if len(args) > 1 {
		opts.AnomalyHost = args[1]
	}
	if len(args) > 2 {
		opts.ImporterHost = args[2]
	}
	bind_spec = fmt.Sprintf("tcp://%s:9611", opts.BindIp)
	anomaly_spec = fmt.Sprintf("tcp://%s:9610", opts.AnomalyHost)
	importer_spec = fmt.Sprintf("tcp://%s:9607", opts.ImporterHost)
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
	perfMsg    = 1
	errorMsg   = 2
	anomalyMsg = 3
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

func handleZeromqMsg(msg *ZmqMsg) {
	switch msg.msgType {
	case perfMsg:
		perf_buffers.Add(msg.app_env, msg.data)
	case errorMsg:
		error_buffers.Add(msg.app_env, msg.data)
	case anomalyMsg:
	}
	sendToWebSockets(msg)
	atomic.AddInt64(&processed, 1)
}

func sendToWebSockets(msg *ZmqMsg) (err error) {
	channels := channel_map[msg.app_env]
	if channels != nil {
		for _, c := range *channels {
			select {
			case c <- msg.data:
			default:
				err = channelBlocked
			}
		}
	}
	return err
}

func handleWebSocketMsg(msg *WsMsg) {
	switch msg.msgType {
	case subscribeMsg:
		logInfo("adding subscription to %s for %s", msg.app_env, msg.name)
		channel_map.Add(msg.app_env, msg.name, msg.channel)
		if err := perf_buffers.Send(msg.app_env, msg.channel); err != nil {
			logError("%v", err)
		}
		if err := error_buffers.Send(msg.app_env, msg.channel); err != nil {
			logError("%v", err)
		}
	case unsubscribeMsg:
		logInfo("removing subscription to %s for %s", msg.app_env, msg.name)
		channel_map.Remove(msg.app_env, msg.name, msg.channel)
		close(msg.channel)
	}
}

//*****************************************************************

func setupSockets() (*zmq.Socket, *zmq.Socket) {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	subscriber.SetLinger(100)
	subscriber.SetRcvhwm(1000)
	subscriber.SetSubscribe("")
	subscriber.Connect(importer_spec)

	anomalies, _ := zmq.NewSocket(zmq.SUB)
	anomalies.SetLinger(100)
	anomalies.SetRcvhwm(1000)
	anomalies.Connect(anomaly_spec)
	anomalies.SetSubscribe("")
	return subscriber, anomalies
}

// run zmq event loop
func zmqMsgHandler() {
	subscriber, anomalies := setupSockets()
	defer subscriber.Close()
	defer anomalies.Close()

	poller := zmq.NewPoller()
	poller.Add(subscriber, zmq.POLLIN)
	poller.Add(anomalies, zmq.POLLIN)

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
			switch s {
			case subscriber:
				if strings.Contains(data, "total_time") {
					msgType = perfMsg
				} else {
					msgType = errorMsg
				}
			case anomalies:
				msgType = anomalyMsg
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
