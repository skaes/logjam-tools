package publisher

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"sync"
	"time"

	"github.com/ShowMax/go-fqdn"
	"github.com/golang/snappy"
	zmq "github.com/pebbe/zmq4"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/util"
)

const (
	HeartbeatInterval = 5
)

type Opts struct {
	Compression        byte
	DeviceId           uint32
	OutputPort         uint
	OutputSpec         string
	SendHwm            int
	SuppressHeartbeats bool // only used for testing
}

type Publisher struct {
	opts             Opts
	publisherChannel chan *pubMsg
	sequenceNum      uint64
	publisherSocket  *zmq.Socket
}

func New(wg *sync.WaitGroup, opts Opts) *Publisher {
	p := Publisher{opts: opts}
	p.publisherChannel = make(chan *pubMsg, 10000)
	p.publisherSocket = p.setupPublisherSocket()
	go p.publish(wg)
	return &p
}

type pubMsg struct {
	appEnv      string
	routingKey  string
	data        []byte
	compression byte
}

func (p *Publisher) nextSequenceNumber() uint64 {
	p.sequenceNum++
	return p.sequenceNum
}

func (p *Publisher) sendMessage(socket *zmq.Socket, msg *pubMsg) {
	// log.Info("Sending logjam message: %+v", data)
	socket.SendBytes([]byte(msg.appEnv), zmq.SNDMORE)
	socket.SendBytes([]byte(msg.routingKey), zmq.SNDMORE)
	socket.SendBytes(msg.data, zmq.SNDMORE)
	meta := util.PackInfo(p.nextSequenceNumber(), p.opts.DeviceId, msg.compression)
	socket.SendBytes(meta, 0)
}

func (p *Publisher) pubSocketSpecForConnecting() string {
	return fmt.Sprintf("tcp://%s:%d", fqdn.Get(), p.opts.OutputPort)
}

func (p *Publisher) sendHeartbeat(socket *zmq.Socket) {
	if p.opts.SuppressHeartbeats {
		return
	}
	socket.SendBytes([]byte("heartbeat"), zmq.SNDMORE)
	socket.SendBytes([]byte(p.pubSocketSpecForConnecting()), zmq.SNDMORE)
	socket.SendBytes([]byte("{}"), zmq.SNDMORE)
	meta := util.PackInfo(p.nextSequenceNumber(), p.opts.DeviceId, util.NoCompression)
	socket.SendBytes(meta, 0)
}

func (p *Publisher) publish(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var ticks uint64
	for !util.Interrupted() {
		select {
		case msg := <-p.publisherChannel:
			p.sendMessage(p.publisherSocket, msg)
		case <-ticker.C:
			ticks++
			if ticks%(HeartbeatInterval*10) == 0 {
				p.sendHeartbeat(p.publisherSocket)
			}
		}
	}
	err := p.publisherSocket.Close()
	if err != nil {
		log.Error("Could not close publisher socket on shut down: %s", err)
	}
}

func (p *Publisher) setupPublisherSocket() *zmq.Socket {
	publisher, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatal("Could not create publisher socket: %s", err)
	}
	publisher.SetLinger(1000)
	publisher.SetSndhwm(p.opts.SendHwm)
	publisher.Bind(p.opts.OutputSpec)
	return publisher
}

// Publish sends a message to publish msg for publisher go routine, optionally compressing it
func (p *Publisher) Publish(appEnv string, routingKey string, data []byte) {
	var usedCompression byte = util.NoCompression
	switch p.opts.Compression {
	case util.SnappyCompression:
		compressedData := snappy.Encode(nil, data)
		if len(compressedData) < len(data) {
			data = compressedData
			usedCompression = util.SnappyCompression
		}
	case util.ZlibCompression:
		buf := bytes.Buffer{}
		w := zlib.NewWriter(&buf)
		_, err := w.Write(data)
		w.Close()
		if err == nil && buf.Len() < len(data) {
			data = buf.Bytes()
			usedCompression = util.ZlibCompression
		}
	}
	p.publisherChannel <- &pubMsg{appEnv: appEnv, routingKey: routingKey, data: data, compression: usedCompression}
}
