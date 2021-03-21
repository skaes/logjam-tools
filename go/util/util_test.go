package util

import (
	"bytes"
	"reflect"
	"testing"
)

func TestPackingAndUnpacking(t *testing.T) {

}

func TestCompressionDecompression(t *testing.T) {
	data := []byte("111111111111111111111111111111111111111111111")
	for _, name := range []string{"", "lz4", "snappy", "zlib"} {
		method, err := ParseCompressionMethodName(name)
		if err != nil {
			t.Errorf("could not parse compression method: %s", name)
			continue
		}
		compressed, err := Compress(data, method)
		if err != nil {
			t.Errorf("could not compress data using method '%s': %s", name, err)
			continue
		}
		decompressed, err := Decompress(compressed, method)
		if err != nil {
			t.Errorf("could not decompress data using method '%s': %s", name, err)
			continue
		}
		if !bytes.Equal(data, decompressed) {
			t.Errorf("Compress(Decompress(data)) using method '%s' is not idempotent", name)
		}
	}
}

func TestWriteRead(t *testing.T) {
	msg := [][]byte{
		[]byte("1111"),
		[]byte("2222222"),
		[]byte("33"),
		[]byte("4"),
	}
	var buf bytes.Buffer
	err := WriteMsg(&buf, msg)
	if err != nil {
		t.Errorf("could not write msg: %s", err)
	}
	l, err := ReadMsg(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Errorf("could not read msg: %s", err)
	}
	if !reflect.DeepEqual(msg, l) {
		t.Errorf("msgs not properly decoded %v != %v", msg, l)
	}
}
