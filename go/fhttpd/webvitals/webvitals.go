package webvitals

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/skaes/logjam-tools/go/fhttpd/stats"
	format "github.com/skaes/logjam-tools/go/formats/webvitals"
	pub "github.com/skaes/logjam-tools/go/publisher"
	"github.com/skaes/logjam-tools/go/util"
)

func Serve(publisher pub.Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer stats.RecordRequestStats(r)
		webVitals, rid, err := extractWebVitals(r)
		if err != nil {
			writeErrorResponse(w, err)
			return
		}

		err = publishWebVitals(publisher, rid, webVitals)
		if err != nil {
			writeErrorResponse(w, err)
			return
		}

		writeSuccessResponse(w)
	}
}

func writeErrorResponse(w http.ResponseWriter, err error) {
	// TODO: do something with the error
	w.WriteHeader(400)
	w.Write([]byte("Read the docs"))
}

func writeSuccessResponse(w http.ResponseWriter) {
	w.Write([]byte("OK"))
}

type RequestPayload struct {
	LogjamRequestId string          `json:"logjam_request_id"`
	LogjamAction    string          `json:"logjam_action"`
	Metrics         []format.Metric `json:"metrics"`
}

var nowFunc = time.Now

func extractWebVitals(r *http.Request) (*format.WebVitals, *util.RequestId, error) {
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, nil, err
	}
	payload := &RequestPayload{}
	err = json.Unmarshal(bytes, payload)
	if err != nil {
		return nil, nil, err
	}

	requestId, err := util.ParseRequestId(payload.LogjamRequestId)
	if err != nil {
		return nil, nil, err
	}

	now := nowFunc()
	webVitals := &format.WebVitals{
		StartedMs:       now.UnixNano() / int64(time.Millisecond),
		StartedAt:       now.Format(time.RFC3339),
		LogjamRequestId: payload.LogjamRequestId,
		LogjamAction:    payload.LogjamAction,
		Metrics:         payload.Metrics,
	}

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
