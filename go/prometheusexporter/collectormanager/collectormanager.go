package collectormanager

import (
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/prometheusexporter/collector"
	"github.com/skaes/logjam-tools/go/prometheusexporter/mobile"
	"github.com/skaes/logjam-tools/go/util"
)

// MessageProcessor indicates that a type can process logjam messages
type MessageProcessor interface {
	ProcessMessage(routingKey string, data map[string]interface{})
}

var (
	collectors    = make(map[string]*collector.Collector)
	mutex         sync.Mutex
	opts          collector.Options
	mobileMetrics mobile.Metrics
)

func Initialize(logjamURL string, env string, options collector.Options) {
	opts = options
	mobileMetrics = mobile.New()
	if logjamURL == "" {
		return
	}
	u, err := url.Parse(logjamURL)
	if err != nil {
		log.Error("could not parse stream url: %s", err)
		os.Exit(1)
	}
	u.Path = path.Join(u.Path, "admin/streams")
	url := u.String()
	UpdateStreams(url, env)
	go StreamsUpdater(url, env)
}

func AddCollector(appEnv string, stream *util.Stream) {
	mutex.Lock()
	defer mutex.Unlock()
	c, found := collectors[appEnv]
	if found {
		if opts.Verbose {
			log.Info("updating collector: %s : %+v", appEnv, stream)
		}
		c.Update(stream)
		return
	}
	if opts.Verbose {
		log.Info("adding stream: %s : %+v", appEnv, stream)
	}
	collectors[appEnv] = collector.New(appEnv, stream, opts)
}

func GetMessageProcessor(appEnv string) MessageProcessor {
	if strings.HasPrefix(appEnv, "mobile") {
		return mobileMetrics
	}
	mutex.Lock()
	defer mutex.Unlock()
	return collectors[appEnv]
}

func GetCollector(appEnv string) *collector.Collector {
	mutex.Lock()
	defer mutex.Unlock()
	return collectors[appEnv]
}

func RemoveCollector(c *collector.Collector) {
	mutex.Lock()
	defer mutex.Unlock()
	delete(collectors, c.Name)
	// make sure to stop go routines associated with the collector
	c.Shutdown()
}

func StreamsUpdater(url, env string) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		if util.Interrupted() {
			break
		}
		UpdateStreams(url, env)
	}
}

func UpdateStreams(url string, env string) {
	streams := util.RetrieveStreams(url, env)
	if streams == nil {
		log.Error("could not retrieve streams from %s", url)
		return
	}
	log.Info("updating streams")
	suffix := "-" + env
	for s, r := range streams {
		if env == "" || strings.HasSuffix(s, suffix) {
			AddCollector(s, r)
		}
	}
	// delete streams which disappeared
	mutex.Lock()
	defer mutex.Unlock()
	for s, c := range collectors {
		_, found := streams[s]
		if !found {
			log.Info("removing stream: %s", s)
			RemoveCollector(c)
		}
	}
}
