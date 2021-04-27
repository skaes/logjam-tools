package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	// _ "net/http/pprof"

	"github.com/jessevdk/go-flags"
	log "github.com/skaes/logjam-tools/go/logging"
	pub "github.com/skaes/logjam-tools/go/publisher"
	"github.com/skaes/logjam-tools/go/util"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Quiet       bool   `short:"q" long:"quiet" description:"be quiet"`
	InputPort   int    `short:"p" long:"input-port" default:"9705" description:"port number of http input socket"`
	CertFile    string `short:"c" long:"cert-file" env:"LOGJAM_CERT_FILE" description:"certificate file to use"`
	KeyFile     string `short:"k" long:"key-file" env:"LOGJAM_KEY_FILE" description:"key file to use"`
	DeviceId    uint32 `short:"d" long:"device-id" description:"device id"`
	OutputPort  uint   `short:"P" long:"output-port" default:"9706" description:"port number of zeromq output socket"`
	SendHwm     int    `short:"S" long:"snd-hwm" env:"LOGJAM_SND_HWM" default:"100000" description:"high water mark for zeromq output socket"`
	IoThreads   int    `short:"i" long:"io-threads" default:"1" description:"number of zeromq io threads"`
	Compression string `short:"x" long:"compress" description:"compression method to use"`
	// DebugPort  int  `short:"D" long:"debug-port" default:"6060" description:"port number of http debug port (pprof)"`
}

var (
	verbose     bool
	quiet       bool
	outputSpec  string
	compression byte

	publisher *pub.Publisher

	// Zeromq PUB sockets are not thread safe, so we run the publisher in a
	// separate go routine.
)

func parseArgs() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Println(err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		log.Error("%s: arguments are ingored, please use options instead.", args[0])
		os.Exit(1)
	}
	// Determine compression method.
	// Determine compression method.
	compression, err = util.ParseCompressionMethodName(opts.Compression)
	if err != nil {
		log.Error("%s: unsupported compression method: %s.", args[0], opts.Compression)
		os.Exit(1)
	}
}

var wg sync.WaitGroup

func setupWebServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/logjam/ajax", serveFrontendRequest)
	mux.HandleFunc("/logjam/page", serveFrontendRequest)
	mux.HandleFunc("/logjam/mobile", serveMobileMetrics)
	mux.HandleFunc("/alive.txt", serveAlive)
	spec := ":" + strconv.Itoa(opts.InputPort)
	return &http.Server{
		Addr:    spec,
		Handler: mux,
	}
}

// func runProfiler(debugSpec string) {
// 	fmt.Println(http.ListenAndServe(debugSpec, nil))
// }

func main() {
	log.Info("%s starting", os.Args[0])
	parseArgs()
	outputSpec = fmt.Sprintf("tcp://*:%d", opts.OutputPort)
	verbose = opts.Verbose
	quiet = opts.Quiet
	log.Info("device-id: %d", opts.DeviceId)
	log.Info("output-spec: %s", outputSpec)
	// debugSpec := fmt.Sprintf("localhost:%d", opts.DebugPort)
	// logInfo("debug-spec: %s", debugSpec)

	// f, err := os.Create("profile.prof")
	// if err != nil {
	//     log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	// go runProfiler(debugSpec)

	util.InstallSignalHandler()
	go statsReporter()
	publisher = pub.New(&wg, pub.Opts{
		Compression: compression,
		DeviceId:    opts.DeviceId,
		OutputPort:  opts.OutputPort,
		OutputSpec:  outputSpec,
		SendHwm:     opts.SendHwm,
	})

	srv := setupWebServer()
	go runWebServer(srv)

	util.WaitForInterrupt()
	log.Info("received interrupt")

	shutdownWebServer(srv, 5*time.Second)

	// Wait for publisher and stats reporter to finsh.
	if util.WaitForWaitGroupWithTimeout(&wg, 3*time.Second) {
		if !quiet {
			log.Info("publisher shut down timed out")
		}
	} else {
		if !quiet {
			log.Info("publisher shut down performed cleanly")
		}
	}
}
