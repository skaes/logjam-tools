package stats

import (
	"sync/atomic"
	"time"

	log "github.com/skaes/logjam-tools/go/logging"
)

// Stats collects prometheus exporter statistics. The various compoments of the
// exporter update the stats using atomic.SwapUint64 on its members.
var Stats struct {
	Processed uint64 // number of zeroMQ messages processed
	Dropped   uint64 // number of messages dropped
	Missed    uint64 // number if messages dropped by the zeroMQ SUB socket
	Observed  uint64 // number of observed metrics
	Ignored   uint64 // number of invalid messages
}

// Reporter reports exporter stats. The export starts it as a go routine.
func Reporter(interrupted *uint32) {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		if atomic.LoadUint32(interrupted) != 0 {
			break
		}
		_observed := atomic.SwapUint64(&Stats.Observed, 0)
		_processed := atomic.SwapUint64(&Stats.Processed, 0)
		_dropped := atomic.SwapUint64(&Stats.Dropped, 0)
		_missed := atomic.SwapUint64(&Stats.Missed, 0)
		_ignored := atomic.SwapUint64(&Stats.Ignored, 0)
		log.Info("processed: %d, ignored: %d, observed %d, dropped: %d, missed: %d",
			_processed, _ignored, _observed, _dropped, _missed)
	}
}
