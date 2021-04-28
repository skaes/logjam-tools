package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/skaes/logjam-tools/go/util"
)

func serveFrontendRequest(w http.ResponseWriter, r *http.Request) {
	defer recordRequestStats(r)
	sm, rid, err := extractFrontendData(r)
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	msgType := extractFrontendMsgType(r)
	err = sendFrontendData(rid, msgType, sm)
	if err != nil {
		writeErrorResponse(w, err.Error())
		return
	}
	writeImageResponse(w)
}

var nowFunc = time.Now

func extractFrontendData(r *http.Request) (stringMap, *util.RequestId, error) {
	sm, err := parseQuery(r)
	if err != nil {
		return sm, nil, err
	}
	// add timestamps
	now := nowFunc()
	sm["started_ms"] = now.UnixNano() / int64(time.Millisecond)
	sm["started_at"] = now.Format(time.RFC3339)

	// check protocol version
	if sm["v"] == nil {
		return sm, nil, errors.New("Missing protocol version number: v=1")
	}
	if sm["v"].(int) != 1 {
		return sm, nil, fmt.Errorf("Unsupported protocol version: v=%d", sm["v"].(int))
	}
	// check logjam_action
	if sm["logjam_action"] == nil {
		return sm, nil, errors.New("Missing field: logjam_action")
	}
	// check request_id
	if sm["logjam_request_id"] == nil {
		return sm, nil, errors.New("Missing field: logjam_request_id")
	}
	id := sm["logjam_request_id"].(string)
	// extract app and environment
	rid, err := util.ParseRequestId(id)
	if err != nil {
		return sm, nil, err
	}
	sm["user_agent"] = r.Header.Get("User-Agent")
	// log.Info("SM: %+v, RID: %+v", sm, rid)
	return sm, rid, nil
}

func extractFrontendMsgType(r *http.Request) string {
	if r.URL.Path == "/logjam/ajax" {
		return "ajax"
	}
	if r.URL.Path == "/logjam/page" {
		return "page"
	}
	return "unknown"
}

func sendFrontendData(rid *util.RequestId, msgType string, sm stringMap) error {
	appEnv := rid.AppEnv()
	routingKey := rid.RoutingKey("frontend", msgType)
	data, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	publisher.Publish(appEnv, routingKey, data, util.NoCompression)
	return nil
}
