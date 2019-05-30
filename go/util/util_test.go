package util

import (
	"bytes"
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
