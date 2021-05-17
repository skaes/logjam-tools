package stats

import (
	"net/http"
	"sync"
	"time"

	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/util"
)

var (
	// Statistics variables protected by a mutex.
	statsMutex        = &sync.Mutex{}
	processedCount    uint64
	processedBytes    uint64
	processedMaxBytes uint64
	httpFailures      uint64
)

// report number of processed requests every second
func StatsReporter(quiet bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for !util.Interrupted() {
		<-ticker.C
		// obtain values and reset counters
		statsMutex.Lock()
		count := processedCount
		bytes := processedBytes
		maxBytes := processedMaxBytes
		failures := httpFailures
		processedCount = 0
		processedBytes = 0
		processedMaxBytes = 0
		httpFailures = 0
		statsMutex.Unlock()
		// report
		kb := float64(bytes) / 1024.0
		maxkb := float64(maxBytes) / 1024.0
		var avgkb float64
		if count > 0 {
			avgkb = kb / float64(count)
		}
		if !quiet {
			log.Info("processed %d, invalid %d, size: %.2f KB, avg: %.2f KB, max: %.2f", count, failures, kb, avgkb, maxkb)
		}
	}
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

func IncrementFailures() {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	httpFailures++
}

func RecordRequestStats(r *http.Request) {
	size := requestSize(r)
	statsMutex.Lock()
	processedCount++
	processedBytes += size
	if processedMaxBytes < size {
		processedMaxBytes = size
	}
	statsMutex.Unlock()
}
