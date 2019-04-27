package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/jessevdk/go-flags"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/prometheusexporter/collector"
	"github.com/skaes/logjam-tools/go/prometheusexporter/collectormanager"
	"github.com/skaes/logjam-tools/go/prometheusexporter/messageparser"
	"github.com/skaes/logjam-tools/go/prometheusexporter/stats"
	"github.com/skaes/logjam-tools/go/prometheusexporter/webserver"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"Verbose logging."`
	StreamURL   string `short:"l" long:"logjam-url" default:"" description:"Logjam instance ti use for retrieving stream definitions."`
	Devices     string `short:"d" long:"devices" default:"127.0.0.1:9606" description:"Comma separated device specs (host:port pairs)."`
	Env         string `short:"e" long:"env" description:"Logjam environments to process."`
	Datacenters string `short:"D" long:"datacenters" description:"List of known datacenters, comma separated. Will be used to determine label value if not available on incoming data."`
	Parsers     uint   `short:"P" long:"parsers" default:"4" description:"Number of message parsers to run in parallel."`
	CleanAfter  uint   `short:"c" long:"clean-after" default:"5" description:"Minutes to wait before cleaning old time series."`
	Port        string `short:"p" long:"port" default:"8081" description:"Port to expose metrics on."`
}

var (
	interrupted uint32
)

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		atomic.AddUint32(&interrupted, 1)
		signal.Stop(c)
	}()
}

func parseArgs() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		log.Error("%s: passing arguments is not supported. please use options instead.", args[0])
		os.Exit(1)
	}
}

func main() {
	log.Info("%s starting", os.Args[0])
	parseArgs()

	installSignalHandler()

	collectorOptions := collector.Options{
		Interrupted: &interrupted,
		Verbose:     opts.Verbose,
		Datacenters: opts.Datacenters,
		CleanAfter:  opts.CleanAfter,
	}
	collectormanager.Initialize(opts.StreamURL, opts.Env, collectorOptions)

	go stats.Reporter(&interrupted)

	parserOptions := messageparser.Options{
		Interrupted: &interrupted,
		Verbose:     opts.Verbose,
		Parsers:     opts.Parsers,
		Devices:     opts.Devices,
	}
	go messageparser.New(parserOptions).Run()

	webserver.HandleHTTPRequests(opts.Port)

	log.Info("%s shutdown", os.Args[0])
}
