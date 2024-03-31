package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	zmq "github.com/pebbe/zmq4"
	pub "github.com/skaes/logjam-tools/go/publisher"
	"github.com/skaes/logjam-tools/go/util"
	. "github.com/smartystreets/goconvey/convey"
)

func TestForwarder(t *testing.T) {
	server := httptest.NewServer(setupHandler())
	defer server.Close()

	opts.BindIP = "127.0.0.1"
	opts.DeviceId = 4711
	outputSpec = "inproc://publisher"
	publisher = pub.New(&wg, pub.Opts{
		Compression:        compression,
		DeviceId:           opts.DeviceId,
		OutputPort:         opts.OutputPort,
		OutputSpec:         outputSpec,
		SendHwm:            opts.SendHwm,
		SuppressHeartbeats: true,
	})

	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		t.Fatal("could not create listening socket")
	}
	socket.SetSubscribe("")
	err = socket.Connect(outputSpec)
	if err != nil {
		t.Fatalf("could not connect to publisher: %s", err)
	}
	defer func() {
		socket.Close()
	}()

	Convey("liveness handler", t, func() {
		req, err := http.NewRequest("GET", server.URL+"/alive.txt", nil)
		res, err := server.Client().Do(req)

		So(err, ShouldBeNil)
		So(res.StatusCode, ShouldEqual, 200)
	})

	Convey("events handler", t, func() {
		event := map[string]interface{}{"app": "murks", "env": "schrott", "label": "none"}
		body, err := json.Marshal(event)
		So(err, ShouldBeNil)

		Convey("happy path", func() {

			happyPath := func(body []byte, compress bool) {
				req, err := http.NewRequest("POST", server.URL+"/logjam/events/app/env", bytes.NewReader(body))
				So(err, ShouldBeNil)
				req.Header.Set("Content-Type", "application/json")

				if compress {
					body, err := util.Compress(body, util.SnappyCompression)
					So(err, ShouldBeNil)
					req, err = http.NewRequest("POST", server.URL+"/logjam/events/app/env", bytes.NewReader(body))
					So(err, ShouldBeNil)
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Content-Encoding", "snappy")
				}

				res, err := server.Client().Do(req)

				So(err, ShouldBeNil)
				So(res.StatusCode, ShouldEqual, 202)
				body, err = io.ReadAll(res.Body)
				So(err, ShouldBeNil)
				So(string(body), ShouldEqual, "")

				msg, err := socket.RecvMessage(0)
				So(err, ShouldBeNil)
				So(msg, ShouldHaveLength, 4)

				So(msg[0], ShouldEqual, "app-env")
				So(msg[1], ShouldEqual, "events.app-env")
				meta := util.UnpackInfo([]byte(msg[3]))
				payload, err := util.Decompress([]byte(msg[2]), meta.CompressionMethod)
				So(err, ShouldBeNil)

				var output map[string]interface{}
				err = json.Unmarshal([]byte(payload), &output)
				So(err, ShouldBeNil)
				So(output, ShouldResemble, map[string]interface{}{"label": "none"})
			}

			Convey("uncompressed", func() { happyPath(body, false) })
			Convey("compressed", func() { happyPath(body, true) })
		})

		Convey("wrong content type", func() {
			req, err := http.NewRequest("POST", server.URL+"/logjam/events/app/env", bytes.NewReader(body))
			req.Header.Set("Content-Type", "text/plain")
			res, err := server.Client().Do(req)

			So(err, ShouldBeNil)
			So(res.StatusCode, ShouldEqual, 415)
			io.Copy(io.Discard, res.Body)
		})
	})

	Convey("deprecated events handler", t, func() {
		event := map[string]interface{}{"app": "app", "env": "env", "label": "none"}
		body, err := json.Marshal(event)
		So(err, ShouldBeNil)

		Convey("happy path", func() {

			happyPath := func(body []byte, compress bool) {
				req, err := http.NewRequest("POST", server.URL+"/logjam/events", bytes.NewReader(body))
				So(err, ShouldBeNil)
				req.Header.Set("Content-Type", "application/json")

				if compress {
					body, err := util.Compress(body, util.SnappyCompression)
					So(err, ShouldBeNil)
					req, err = http.NewRequest("POST", server.URL+"/logjam/events", bytes.NewReader(body))
					So(err, ShouldBeNil)
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Content-Encoding", "snappy")
				}

				res, err := server.Client().Do(req)

				So(err, ShouldBeNil)
				So(res.StatusCode, ShouldEqual, 202)
				body, err = io.ReadAll(res.Body)
				So(err, ShouldBeNil)
				So(string(body), ShouldEqual, "")

				msg, err := socket.RecvMessage(0)
				So(err, ShouldBeNil)
				So(msg, ShouldHaveLength, 4)

				So(msg[0], ShouldEqual, "app-env")
				So(msg[1], ShouldEqual, "events.app-env")
				meta := util.UnpackInfo([]byte(msg[3]))
				payload, err := util.Decompress([]byte(msg[2]), meta.CompressionMethod)
				So(err, ShouldBeNil)

				var output map[string]interface{}
				err = json.Unmarshal([]byte(payload), &output)
				So(err, ShouldBeNil)
				So(output, ShouldResemble, map[string]interface{}{"label": "none"})
			}

			Convey("uncompressed", func() { happyPath(body, false) })
			Convey("compressed", func() { happyPath(body, true) })
		})

		Convey("wrong content type", func() {
			req, err := http.NewRequest("POST", server.URL+"/logjam/events", bytes.NewReader(body))
			req.Header.Set("Content-Type", "text/plain")
			res, err := server.Client().Do(req)

			So(err, ShouldBeNil)
			So(res.StatusCode, ShouldEqual, 415)
			io.Copy(io.Discard, res.Body)
		})

		Convey("missing app", func() {
			event := map[string]interface{}{"env": "env"}
			body, err := json.Marshal(event)
			So(err, ShouldBeNil)

			req, err := http.NewRequest("POST", server.URL+"/logjam/events", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			res, err := server.Client().Do(req)

			So(err, ShouldBeNil)
			So(res.StatusCode, ShouldEqual, 400)
			io.Copy(io.Discard, res.Body)
		})

		Convey("missing env", func() {
			event := map[string]interface{}{"app": "app"}
			body, err := json.Marshal(event)
			So(err, ShouldBeNil)

			req, err := http.NewRequest("POST", server.URL+"/logjam/events", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			res, err := server.Client().Do(req)

			So(err, ShouldBeNil)
			So(res.StatusCode, ShouldEqual, 400)
			io.Copy(io.Discard, res.Body)
		})
	})

	Convey("logs handler", t, func() {
		event := map[string]interface{}{"code": 200}
		body, err := json.Marshal(event)
		So(err, ShouldBeNil)

		Convey("happy path", func() {

			happyPath := func(body []byte, compress bool) {
				req, err := http.NewRequest("POST", server.URL+"/logjam/logs/app/env", bytes.NewReader(body))
				So(err, ShouldBeNil)
				req.Header.Set("Content-Type", "application/json")

				if compress {
					body, err := util.Compress(body, util.SnappyCompression)
					So(err, ShouldBeNil)
					req, err = http.NewRequest("POST", server.URL+"/logjam/logs/app/env", bytes.NewReader(body))
					So(err, ShouldBeNil)
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Content-Encoding", "snappy")
				}

				res, err := server.Client().Do(req)

				So(err, ShouldBeNil)
				So(res.StatusCode, ShouldEqual, 202)
				body, err = io.ReadAll(res.Body)
				So(err, ShouldBeNil)
				So(string(body), ShouldEqual, "")

				msg, err := socket.RecvMessage(0)
				So(err, ShouldBeNil)
				So(msg, ShouldHaveLength, 4)

				So(msg[0], ShouldEqual, "app-env")
				So(msg[1], ShouldEqual, "logs.app-env")
				meta := util.UnpackInfo([]byte(msg[3]))

				payload, err := util.Decompress([]byte(msg[2]), meta.CompressionMethod)
				So(err, ShouldBeNil)

				var output map[string]interface{}
				err = json.Unmarshal([]byte(payload), &output)
				So(err, ShouldBeNil)
				So(output, ShouldResemble, map[string]interface{}{"code": float64(200)})
			}

			Convey("uncompressed", func() { happyPath(body, false) })
			Convey("compressed", func() { happyPath(body, true) })
		})

		Convey("wrong content type", func() {
			req, err := http.NewRequest("POST", server.URL+"/logjam/logs/app/env", bytes.NewReader(body))
			req.Header.Set("Content-Type", "text/plain")
			res, err := server.Client().Do(req)

			So(err, ShouldBeNil)
			So(res.StatusCode, ShouldEqual, 415)
			io.Copy(io.Discard, res.Body)
		})
	})

}
