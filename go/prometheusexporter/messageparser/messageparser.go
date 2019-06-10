package messageparser

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	zmq "github.com/pebbe/zmq4"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/prometheusexporter/collectormanager"
	"github.com/skaes/logjam-tools/go/prometheusexporter/stats"

	"github.com/skaes/logjam-tools/go/util"
	"golang.org/x/text/runes"
)

// Options for MessageParser
type Options struct {
	Verbose bool   // Verbose logging.
	Debug   bool   // Extra verbose logging.
	Parsers uint   // Number of message decoder go routines to run.
	Devices string // Logjam devices to connect to.
}

// MessageParser state
type MessageParser struct {
	opts           Options
	deviceSpecs    []string
	socket         *zmq.Socket
	decoderChannel chan *decodeAndUnmarshalTask
}

type decodeAndUnmarshalTask struct {
	msg  [][]byte
	meta *util.MetaInfo
}

// New creates a new message parser
func New(options Options) *MessageParser {
	p := MessageParser{opts: options}
	p.decoderChannel = make(chan *decodeAndUnmarshalTask, 1000000)
	p.deviceSpecs = make([]string, 0)
	for _, s := range strings.Split(p.opts.Devices, ",") {
		if s != "" {
			p.deviceSpecs = append(p.deviceSpecs, fmt.Sprintf("tcp://%s", s))
		}
	}
	log.Info("device-specs: %s", strings.Join(p.deviceSpecs, ","))
	return &p
}

func (p *MessageParser) createSocket() *zmq.Socket {
	socket, _ := zmq.NewSocket(zmq.SUB)
	socket.SetLinger(100)
	socket.SetRcvhwm(1000000)
	socket.SetSubscribe("")
	for _, s := range p.deviceSpecs {
		log.Info("connection sub socket to %s", s)
		err := socket.Connect(s)
		if err != nil {
			log.Error("could not connect: %s", s)
		}
	}
	return socket
}

// Run creates zmq socket, connects it to the devices, starts the
// message parser go routines and reads messages from the socket and
// forwards them to the parsers.
func (p *MessageParser) Run() {
	p.socket = p.createSocket()
	defer p.socket.Close()

	poller := zmq.NewPoller()
	poller.Add(p.socket, zmq.POLLIN)

	sequenceNumbers := make(map[uint32]uint64, 0)

	log.Info("starting %d parsers", p.opts.Parsers)
	for i := uint(1); i <= p.opts.Parsers; i++ {
		go p.decodeAndUnmarshal()
	}

	for !util.Interrupted() {
		sockets, _ := poller.Poll(1 * time.Second)
		for _, socket := range sockets {
			s := socket.Socket
			msg, err := s.RecvMessageBytes(0)
			if err != nil {
				log.Error("recv message error: %s", err)
				continue
			}
			atomic.AddUint64(&stats.Stats.Processed, 1)
			rawSize := 0
			for _, d := range msg {
				rawSize += len(d)
			}
			atomic.AddUint64(&stats.Stats.ProcessedBytes, uint64(rawSize))
			if n := len(msg); n != 4 {
				log.Error("invalid message length %s: %s", n, string(msg[0]))
				atomic.AddUint64(&stats.Stats.Dropped, 1)
				log.Error("got invalid message: %v", msg)
				continue
			}
			info := util.UnpackInfo(msg[3])
			if info == nil {
				atomic.AddUint64(&stats.Stats.Dropped, 1)
				log.Error("could not decode meta info: %#x", msg[3])
				continue
			}
			lastNumber, ok := sequenceNumbers[info.DeviceNumber]
			if !ok {
				lastNumber = 0
			}
			d, n := info.DeviceNumber, info.SequenceNumber
			sequenceNumbers[d] = n
			if n != lastNumber+1 && n > lastNumber && lastNumber != 0 {
				gap := uint64(n - lastNumber + 1)
				if atomic.AddUint64(&stats.Stats.Missed, gap) == gap {
					log.Error("detected message gap for device %d: missed %d messages", d, gap)
				}
			}
			atomic.AddInt64(&stats.Stats.Raw, 1)
			p.decoderChannel <- &decodeAndUnmarshalTask{msg: msg, meta: info}
		}
	}
}

func (p *MessageParser) decodeAndUnmarshal() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for !util.Interrupted() {
		select {
		case task := <-p.decoderChannel:
			atomic.AddInt64(&stats.Stats.Raw, -1)
			info, msg := task.meta, task.msg
			jsonBody, err := util.Decompress(msg[2], info.CompressionMethod)
			if err != nil {
				log.Error("could not decompress json body: %s", err)
				continue
			}
			if !utf8.Valid(jsonBody) {
				jsonBody = runes.ReplaceIllFormed().Bytes(jsonBody)
				log.Warn("replaced invalid utf8 in json body: %s", jsonBody)
			}
			if p.opts.Debug {
				log.Info("received msg body: %s", jsonBody)
			}
			data := make(map[string]interface{})
			if err := json.Unmarshal(jsonBody, &data); err != nil {
				log.Error("invalid json body: %s", err)
				continue
			}
			appEnv := string(msg[0])
			routingKey := string(msg[1])
			if appEnv == "heartbeat" {
				if p.opts.Verbose {
					log.Info("received heartbeat from %s", routingKey)
				}
				continue
			}
			c := collectormanager.GetCollector(appEnv)
			if c == nil {
				log.Error("could not retrieve collector for %s", appEnv)
				continue
			}
			c.ProcessMessage(routingKey, data)
		case <-ticker.C:
			// make sure we shut down timely even if no messages arrive
		}
	}
}
