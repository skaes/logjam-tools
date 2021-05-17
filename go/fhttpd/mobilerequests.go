package main

import (
	"io/ioutil"
	"net/http"

	"github.com/skaes/logjam-tools/go/fhttpd/stats"
	"github.com/skaes/logjam-tools/go/util"
)

func serveMobileMetrics(w http.ResponseWriter, r *http.Request) {
	defer stats.RecordRequestStats(r)
	bytes, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	appEnv := "mobile-production"
	routingKey := "mobile"
	publisher.Publish(appEnv, routingKey, bytes, util.NoCompression)
	writeImageResponse(w)
}
