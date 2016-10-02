package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
)

var opts struct {
	App     string `short:"a" long:"app" default:"logjam" description:"Name of the application"`
	Env     string `short:"e" long:"env" default:"development" description:"Name of the environment"`
	Keys    string `short:"k" long:"keys" default:"all_pages,logjam" description:"List of logjam namespaces to generate data for"`
	Verbose bool   `short:"v" long:"verbose" description:"Be verbose"`
}

var (
	publisher *zmq.Socket
	anomalies *zmq.Socket
)

func abort(err error, s string) {
	fmt.Println(err)
	panic(s)
}

func setupSockets() {
	var err error
	if publisher, err = zmq.NewSocket(zmq.PUB); err != nil {
		abort(err, "could not create socket")
	}
	publisher.SetLinger(100)
	publisher.SetSndhwm(1000)
	publisher.Bind("tcp://127.0.0.1:9607")

	if anomalies, err = zmq.NewSocket(zmq.PUB); err != nil {
		abort(err, "could not create socket")
	}
	anomalies.SetLinger(100)
	anomalies.SetSndhwm(1000)
	anomalies.Bind("tcp://127.0.0.1:9610")
}

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		signal.Stop(c)
	}()
}

type PerfData struct {
	Count         int     `json:"count"`
	Total_time    float64 `json:"total_time"`
	Other_time    float64 `json:"other_time"`
	Gc_time       float64 `json:"gc_time"`
	Memcache_time float64 `json:"memcache_time"`
	Db_time       float64 `json:"db_time"`
	View_time     float64 `json:"view_time"`
}

var rnd = rand.New(rand.NewSource(0))

func generatePerfData() *PerfData {
	p := &PerfData{}
	p.Count = int(50 + 450*rnd.Float64())
	p.Gc_time = 100 * rnd.Float64()
	p.Memcache_time = 100 * rnd.Float64()
	p.Db_time = 100 * rnd.Float64()
	p.View_time = 100 * rnd.Float64()
	p.Other_time = 100 * rnd.Float64()
	p.Total_time = p.Other_time + p.Memcache_time + p.Db_time + p.View_time
	return p
}

type ErrorData struct {
	Severity    int    `json:"severity"`
	Time        string `json:"time"`
	Action      string `json:"action"`
	Description string `json:"description"`
	Request_Id  string `json:"request_id"`
}

var error_count = 0

func generateErrorData() []ErrorData {
	error_count++
	p := ErrorData{}
	p.Time = time.Now().Format(time.RFC3339)
	p.Severity = 2 + rnd.Intn(2)
	p.Action = "ApplicationController#index"
	p.Description = "Something broke " + strconv.Itoa(error_count)
	if uid, err := uuid.NewV4(); err != nil {
		abort(err, "could not generate uuid")
	} else {
		p.Request_Id = strings.Join(strings.Split(uid.String(), "-"), "")
	}
	return []ErrorData{p}
}

type AnomalyData struct {
	Anomaly bool    `json:"anomaly"`
	Score   float32 `json:"score"`
}

func generateAnomalyData() AnomalyData {
	p := AnomalyData{Score: rnd.Float32()}
	p.Anomaly = p.Score > 0.5
	return p
}

func sendData(socket *zmq.Socket, key string, data interface{}, kind string) {
	bs, err := json.Marshal(data)
	if err != nil {
		abort(err, "could not marshal data")
	}
	encoded_data := string(bs)
	msg_key := fmt.Sprintf("%s-%s,%s", opts.App, opts.Env, key)
	if _, err := socket.Send(msg_key, zmq.DONTWAIT|zmq.SNDMORE); err != nil {
		abort(err, "could not send message part 1")
	}
	if _, err := socket.Send(encoded_data, zmq.DONTWAIT); err != nil {
		abort(err, "could not send message part 2")
	}
	if opts.Verbose {
		fmt.Printf("KEY: %s\n%s: %s\n", key, kind, encoded_data)
	}
}

func main() {
	_, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		os.Exit(1)
	}
	setupSockets()
	defer publisher.Close()
	defer anomalies.Close()

	keys := strings.Split(opts.Keys, ",")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	interrupted := false

	ticker := time.NewTicker(1 * time.Second)

	for tick := 0; !interrupted; tick++ {
		select {
		case <-c:
			fmt.Println("shutting down")
			interrupted = true
		case <-ticker.C:
			for _, key := range keys {
				sendData(publisher, key, generatePerfData(), "DATA")
			}
			if tick%2 == 0 {
				for _, key := range keys {
					sendData(publisher, key, generateErrorData(), "ERRORS")
				}
			}
			if tick%5 == 0 {
				for _, key := range keys {
					sendData(anomalies, key, generateAnomalyData(), "ANOMALY")
				}
			}
		}
	}
}
