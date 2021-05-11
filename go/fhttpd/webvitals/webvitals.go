package webvitals

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-playground/form"
	"github.com/skaes/logjam-tools/go/fhttpd/stats"
	format "github.com/skaes/logjam-tools/go/formats/webvitals"
	log "github.com/skaes/logjam-tools/go/logging"
	pub "github.com/skaes/logjam-tools/go/publisher"
	"github.com/skaes/logjam-tools/go/util"
)

var decoder *form.Decoder

func init() {
	decoder = form.NewDecoder()
}

func Serve(publisher pub.Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer stats.RecordRequestStats(r)
		webVitals, rid, err := extractWebVitals(r)
		if err != nil {
			log.Error("failed to extract webvitals: %v", err)
			writeErrorResponse(w)
			return
		}

		err = publishWebVitals(publisher, rid, webVitals)
		if err != nil {
			log.Error("failed to publish webvitals: %v", err)
			writeErrorResponse(w)
			return
		}

		writeSuccessResponse(w)
	}
}

func writeErrorResponse(w http.ResponseWriter) {
	w.WriteHeader(400)
	w.Write([]byte("Read the docs"))
}

func writeSuccessResponse(w http.ResponseWriter) {
	w.Write([]byte("OK"))
}

var nowFunc = time.Now

func extractWebVitals(r *http.Request) (*format.WebVitals, *util.RequestId, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, nil, err
	}
	webVitals := &format.WebVitals{}
	err = decoder.Decode(webVitals, r.Form)
	if err != nil {
		return nil, nil, err
	}

	requestId, err := util.ParseRequestId(webVitals.LogjamRequestId)
	if err != nil {
		return nil, nil, err
	}

	now := nowFunc()
	webVitals.StartedMs = now.UnixNano() / int64(time.Millisecond)
	webVitals.StartedAt = now.Format(time.RFC3339)

	return webVitals, requestId, nil
}

func publishWebVitals(publisher pub.Publisher, rid *util.RequestId, payload *format.WebVitals) error {
	appEnv := rid.AppEnv()
	routingKey := rid.RoutingKey(format.RoutingKeyPrefix, format.RoutingKeyMsgType)
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	publisher.Publish(appEnv, routingKey, data, util.NoCompression)
	return nil
}
