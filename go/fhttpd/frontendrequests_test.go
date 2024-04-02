package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/skaes/logjam-tools/go/util"
	"github.com/stretchr/testify/assert"

	dummypub "github.com/skaes/logjam-tools/go/publisher/testhelper"
)

func TestSendFrontendData(t *testing.T) {
	publisher = dummypub.NewDummyPublisher()
	rid, err := util.ParseRequestId("some-app-preview-55ff333eee")
	assert.NoError(t, err, "we passed a valid request id, it should not fail")
	msgType := "unknown"
	sm := stringMap{
		"foo": "bar",
	}
	marshaledSM, _ := json.Marshal(sm)

	err = sendFrontendData(rid, msgType, sm)
	assert.NoError(t, err)
	assert.Equal(t, []dummypub.DummyMessage{
		{
			AppEnv:         "some-app-preview",
			RoutingKey:     "frontend.unknown.some-app.preview",
			Data:           marshaledSM,
			CompressedWith: 0,
		},
	}, publisher.(*dummypub.DummyPublisher).PublishedMessages)
}

func TestExtractFrontendMsgType(t *testing.T) {
	t.Run("Recognizes ajax", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/ajax")
		req := &http.Request{
			URL: uri,
		}
		msgType := extractFrontendMsgType(req)
		assert.Equal(t, "ajax", msgType)
	})
	t.Run("Recognizes page", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/page")
		req := &http.Request{
			URL: uri,
		}
		msgType := extractFrontendMsgType(req)
		assert.Equal(t, "page", msgType)
	})
	t.Run("Defaults to unknown", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/something")
		req := &http.Request{
			URL: uri,
		}
		msgType := extractFrontendMsgType(req)
		assert.Equal(t, "unknown", msgType)
	})
}

func TestExtractFrontendData(t *testing.T) {
	now := time.Now()
	nowFunc = func() time.Time { return now }

	action := "myActions#call"
	requestId := "some-app-preview-55ff333eee"
	userAgent := "testing/testing logjam-tools"

	t.Run("SuccessfulExtraction", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/page")
		query := uri.Query()
		query.Set("v", "1")
		query.Set("logjam_action", action)
		query.Set("logjam_request_id", requestId)
		uri.RawQuery = query.Encode()
		headers := http.Header{}
		headers.Add("user-agent", userAgent)
		req := &http.Request{
			Method: "GET",
			URL:    uri,
			Header: headers,
		}

		payload, logjamRequestId, err := extractFrontendData(req)
		assert.NoError(t, err)

		assert.NotNil(t, logjamRequestId)

		expectedPayload := stringMap{
			"started_ms":        now.UnixNano() / int64(time.Millisecond),
			"started_at":        now.Format(time.RFC3339),
			"v":                 1,
			"logjam_action":     action,
			"logjam_request_id": requestId,
			"user_agent":        userAgent,
		}
		assert.Equal(t, expectedPayload, payload)
	})

	t.Run("MissingProtocolVersion", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/page")
		query := uri.Query()
		query.Set("logjam_action", action)
		query.Set("logjam_request_id", requestId)
		uri.RawQuery = query.Encode()
		headers := http.Header{}
		headers.Add("user-agent", userAgent)
		req := &http.Request{
			Method: "GET",
			URL:    uri,
			Header: headers,
		}

		_, logjamRequestId, err := extractFrontendData(req)
		assert.EqualError(t, err, "missing protocol version number: v=1")
		assert.Nil(t, logjamRequestId)
	})

	t.Run("UnsupportedProtocolVersion", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/page")
		query := uri.Query()
		query.Set("v", "2")
		query.Set("logjam_action", action)
		query.Set("logjam_request_id", requestId)
		uri.RawQuery = query.Encode()
		headers := http.Header{}
		headers.Add("user-agent", userAgent)
		req := &http.Request{
			Method: "GET",
			URL:    uri,
			Header: headers,
		}

		_, logjamRequestId, err := extractFrontendData(req)
		assert.EqualError(t, err, "unsupported protocol version: v=2")
		assert.Nil(t, logjamRequestId)
	})

	t.Run("MissingLogjamAction", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/page")
		query := uri.Query()
		query.Set("v", "1")
		query.Set("logjam_request_id", requestId)
		uri.RawQuery = query.Encode()
		headers := http.Header{}
		headers.Add("user-agent", userAgent)
		req := &http.Request{
			Method: "GET",
			URL:    uri,
			Header: headers,
		}

		_, logjamRequestId, err := extractFrontendData(req)
		assert.EqualError(t, err, "missing field: logjam_action")
		assert.Nil(t, logjamRequestId)
	})

	t.Run("MissingLogjamRequestId", func(t *testing.T) {
		uri, _ := url.Parse("https://logjam.example.com/logjam/page")
		query := uri.Query()
		query.Set("v", "1")
		query.Set("logjam_action", action)
		uri.RawQuery = query.Encode()
		headers := http.Header{}
		headers.Add("user-agent", userAgent)
		req := &http.Request{
			Method: "GET",
			URL:    uri,
			Header: headers,
		}

		_, logjamRequestId, err := extractFrontendData(req)
		assert.EqualError(t, err, "missing field: logjam_request_id")
		assert.Nil(t, logjamRequestId)
	})
}
