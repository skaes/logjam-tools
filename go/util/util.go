package util

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
	log "github.com/skaes/logjam-tools/go/logging"
)

const (
	MetaInfoVersion = 1
	MetaInfoTag     = 0xcabd

	NoCompression     = 0
	ZlibCompression   = 1
	SnappyCompression = 2
	LZ4Compression    = 3
)

func CurrentTime() uint64 {
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

func PackInfo(seqNum uint64, deviceID uint32, compression byte) []byte {
	data := make([]byte, 24)
	binary.BigEndian.PutUint16(data, MetaInfoTag)
	data[2] = compression
	data[3] = MetaInfoVersion
	binary.BigEndian.PutUint32(data[4:8], deviceID)
	binary.BigEndian.PutUint64(data[8:16], CurrentTime())
	binary.BigEndian.PutUint64(data[16:24], seqNum)
	return data
}

func UnpackInfo(data []byte) *MetaInfo {
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

func Decompress(data []byte, method uint8) ([]byte, error) {
	switch method {
	case LZ4Compression:
		decompressedLen := binary.BigEndian.Uint32(data[:4])
		decompressed := make([]byte, decompressedLen)
		_, err := lz4.UncompressBlock(data[4:], decompressed)
		return decompressed, err
	case SnappyCompression:
		return snappy.Decode(nil, data)
	case ZlibCompression:
		buf := bytes.NewBuffer(data)
		reader, err := zlib.NewReader(buf)
		defer reader.Close()
		if err != nil {
			return nil, err
		}
		decompressed, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return decompressed, nil
	}
	return data, nil
}

func Compress(data []byte, method uint8) ([]byte, error) {
	switch method {
	case LZ4Compression:
		hashTable := make([]int, 64<<10)
		maxCompressedLen := lz4.CompressBlockBound(len(data))
		buf := make([]byte, maxCompressedLen+4)
		binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
		n, err := lz4.CompressBlock(data, buf[4:], hashTable)
		if n >= len(data) {
			return nil, errors.New("data is not compressible")
		}
		return buf[:n+4], err
	case SnappyCompression:
		return snappy.Encode(nil, data), nil
	case ZlibCompression:
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		w.Write(data)
		w.Close()
		return b.Bytes(), nil
	}
	return data, nil
}

func ParseStreamName(appEnv string) (app string, env string) {
	slices := strings.Split(appEnv, "-")
	n := len(slices)
	if n < 2 {
		return
	}
	app = strings.Join(slices[0:n-1], "-")
	env = slices[n-1]
	return
}

// RequestId is a strcut representation of a logjam request id, which
// has the form app-env-uuid, where app can contain hyphens but uuid
// can't.
type RequestId struct {
	App string
	Env string
	Id  string
}

// ParseRequestId parses the string representation of a logjam request
// id.
func ParseRequestId(id string) (rid *RequestId, err error) {
	slices := strings.Split(id, "-")
	n := len(slices)
	if n < 3 {
		err = fmt.Errorf("Wrong request id format: %s", id)
		return
	}
	rid = &RequestId{
		App: strings.Join(slices[0:n-2], "-"),
		Env: slices[n-2],
		Id:  slices[n-1],
	}
	return
}

// AppEnv returns the app-env string of a request id.
func (rid *RequestId) AppEnv() string {
	return rid.App + "-" + rid.Env
}

// RoutingKey fabricates a routing key for agive request id, given a
// prefix and a message type.
func (rid *RequestId) RoutingKey(prefix string, msgType string) string {
	return fmt.Sprintf("%s.%s.%s.%s", prefix, msgType, rid.App, rid.Env)
}

// WaitForWaitGroupWithTimeout waits for a wait group wg but times out.
func WaitForWaitGroupWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
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

var interrupted uint32

// InstallSignalHandler installs a signal handler for interrupts and TERM signal.
func InstallSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		atomic.StoreUint32(&interrupted, 1)
		signal.Stop(c)
	}()
}

// Interrupted returns whether an interrupt or TERM signal has been received
func Interrupted() bool {
	return atomic.LoadUint32(&interrupted) == 1
}

// ParseCompressionMethodName converts string to compression method
func ParseCompressionMethodName(name string) (method uint8, err error) {
	switch name {
	case "lz4":
		method = LZ4Compression
	case "snappy":
		method = SnappyCompression
	case "zlib":
		method = ZlibCompression
	case "":
		method = NoCompression
	default:
		err = fmt.Errorf("unknown compression method: %d", method)
	}
	return
}

type Stream struct {
	App                 string   `json:"app"`
	Env                 string   `json:"env"`
	IgnoredRequestURI   string   `json:"ignored_request_uri"`
	BackendOnlyRequests string   `json:"backend_only_requests"`
	APIRequests         []string `json:"api_requests"`
}

func (s *Stream) AppEnv() string {
	return s.App + "+" + s.Env
}

func RetrieveStreams(url, env string) map[string]Stream {
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
	var streams map[string]Stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		log.Error("could not parse stream: %s", err)
		return nil
	}
	return streams
}
