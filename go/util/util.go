package util

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io/ioutil"
	"strings"
	"time"

	"github.com/golang/snappy"
)

const (
	MetaInfoVersion = 1
	MetaInfoTag     = 0xcabd

	NoCompression     = 0
	ZlibCompression   = 1
	SnappyCompression = 2
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
	case SnappyCompression:
		return snappy.Decode(nil, data)
	case ZlibCompression:
		buf := bytes.NewBuffer(data)
		reader, err := zlib.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		decompressed, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return decompressed, nil
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
