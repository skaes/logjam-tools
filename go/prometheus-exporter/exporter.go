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
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/promcollector"
	"github.com/skaes/logjam-tools/go/util"
	"golang.org/x/text/runes"
	"gopkg.in/tylerb/graceful.v1"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Devices     string `short:"d" long:"devices" default:"127.0.0.1:9606" description:"comma separated device specs (host:port pairs)"`
	Env         string `short:"e" long:"env" description:"logjam environments to process"`
	Port        string `short:"p" long:"port" default:"8081" description:"port to expose metrics on"`
	StreamURL   string `short:"s" long:"stream-url" default:"" description:"Logjam endpoint for retrieving stream definitions"`
	Datacenters string `short:"D" long:"datacenters" description:"List of known datacenters, comma separated. Will be used to determine label value if not available on incoming data."`
	CleanAfter  uint   `short:"c" long:"clean-after" default:"5" description:"Minutes to wait before cleaning old time series"`
	Parsers     uint   `short:"P" long:"parsers" default:"4" description:"Number of message parsers to run in parallel"`
}

var (
	verbose     = false
	interrupted uint32
	deviceSpecs []string
	processed   uint64
	dropped     uint64
	missed      uint64
	observed    uint64
	ignored     uint64
	collectors  = make(map[string]*promcollector.Collector)
	mutex       sync.Mutex
)

func addCollector(appEnv string, stream util.Stream) {
	mutex.Lock()
	defer mutex.Unlock()
	_, found := collectors[appEnv]
	if !found {
		log.Info("adding stream: %s : %+v", appEnv, stream)
		collectors[appEnv] = promcollector.New(appEnv, stream, promcollector.Options{
			Interrupted: &interrupted,
			Observed:    &observed,
			Ignored:     &ignored,
			Dropped:     &dropped,
			Verbose:     verbose,
			Datacenters: opts.Datacenters,
			CleanAfter:  opts.CleanAfter,
		})
	}
}

func getCollector(appEnv string) *promcollector.Collector {
	mutex.Lock()
	defer mutex.Unlock()
	return collectors[appEnv]
}

func initialize() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		log.Error("%s: passing arguments is not supported. please use options instead.", args[0])
		os.Exit(1)
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
			log.Error("could not parse stream url: %s", err)
			os.Exit(1)
		}
		u.Path = path.Join(u.Path, "admin/streams")
		url, env := u.String(), opts.Env
		streams := retrieveStreams(url, env)
		updateStreams(streams, env)
		go streamsUpdater(url, env)
	}
	log.Info("device-specs: %s", strings.Join(deviceSpecs, ","))
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

func retrieveStreams(url, env string) map[string]util.Stream {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Error("could not create http request: %s", err)
		return nil
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Error("could not retrieve stream: %s", err)
		return nil
	}
	if res.StatusCode != 200 {
		log.Error("unexpected response: %d", res.Status)
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Error("could not read response body: %s", err)
		return nil
	}
	defer res.Body.Close()
	var streams map[string]util.Stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		log.Error("could not parse stream: %s", err)
		return nil
	}
	return streams
}

func updateStreams(streams map[string]util.Stream, env string) {
	if streams == nil {
		return
	}
	log.Info("updating streams")
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
			log.Info("removing stream: %s", s)
			delete(collectors, s)
			// make sure to stop go routines associated with the collector
			c.Shutdown()
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

// report number of incoming zmq messages every second
func statsReporter() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		if atomic.LoadUint32(&interrupted) != 0 {
			break
		}
		_observed := atomic.SwapUint64(&observed, 0)
		_processed := atomic.SwapUint64(&processed, 0)
		_dropped := atomic.SwapUint64(&dropped, 0)
		_missed := atomic.SwapUint64(&missed, 0)
		_ignored := atomic.SwapUint64(&ignored, 0)
		log.Info("processed: %d, ignored: %d, observed %d, dropped: %d, missed: %d",
			_processed, _ignored, _observed, _dropped, _missed)
	}
}

// web server
func webServer() {
	r := mux.NewRouter()
	r.HandleFunc("/metrics/{application}/{environment}", serveAppMetrics)
	r.HandleFunc("/_system/alive", serveAliveness)
	log.Info("starting http server on port %s", opts.Port)
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
		log.Error("Cannot listen and serve: %s", err)
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
		c.RequestHandler.ServeHTTP(w, r)
	}
}

//*******************************************************************************

func main() {
	log.Info("%s starting", os.Args[0])
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
	log.Info("%s shutting down", os.Args[0])
}

//*******************************************************************************

func streamParser() {
	parser, _ := zmq.NewSocket(zmq.SUB)
	parser.SetLinger(100)
	parser.SetRcvhwm(1000000)
	parser.SetSubscribe("")
	for _, s := range deviceSpecs {
		log.Info("connection sub socket to %s", s)
		err := parser.Connect(s)
		if err != nil {
			log.Error("could not connect: %s", s)
		}
	}
	defer parser.Close()

	poller := zmq.NewPoller()
	poller.Add(parser, zmq.POLLIN)

	sequenceNumbers := make(map[uint32]uint64, 0)

	log.Info("starting %d parsers", opts.Parsers)
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
				log.Error("recv message error: %s", err)
				continue
			}
			atomic.AddUint64(&processed, 1)
			if n := len(msg); n != 4 {
				log.Error("invalid message length %s: %s", n, string(msg[0]))
				if atomic.AddUint64(&dropped, 1) == 1 {
					log.Error("got invalid message: %v", msg)
				}
				continue
			}
			info := util.UnpackInfo(msg[3])
			if info == nil {
				log.Error("could not decode meta info: %#x", msg[3])
			}
			lastNumber, ok := sequenceNumbers[info.DeviceNumber]
			if !ok {
				lastNumber = 0
			}
			d, n := info.DeviceNumber, info.SequenceNumber
			sequenceNumbers[d] = n
			if n != lastNumber+1 && n > lastNumber && lastNumber != 0 {
				gap := uint64(n - lastNumber + 1)
				if atomic.AddUint64(&missed, gap) == gap {
					log.Error("detected message gap for device %d: missed %d messages", d, gap)
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
				log.Error("could not decompress json body: %s", err)
				continue
			}
			if !utf8.Valid(jsonBody) {
				jsonBody = runes.ReplaceIllFormed().Bytes(jsonBody)
				log.Error("replaced invalid utf8 in json body: %s", jsonBody)
			}
			data := make(map[string]interface{})
			if err := json.Unmarshal(jsonBody, &data); err != nil {
				log.Error("invalid json body: %s", err)
				continue
			}
			appEnv := string(msg[0])
			routingKey := string(msg[1])
			if appEnv == "heartbeat" {
				if verbose {
					log.Info("received heartbeat from %s", routingKey)
				}
				continue
			}
			c := getCollector(appEnv)
			if c == nil {
				log.Error("could not retrieve collector for %s", appEnv)
				continue
			}
			c.ProcessMessage(routingKey, data)
		case <-time.After(1 * time.Second):
			// make sure we shut down timely even if no messages arrive
		}
	}
}
