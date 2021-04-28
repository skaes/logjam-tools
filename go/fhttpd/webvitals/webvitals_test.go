package webvitals

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	format "github.com/skaes/logjam-tools/go/formats/webvitals"
	dummypub "github.com/skaes/logjam-tools/go/publisher/testhelper"
	"github.com/skaes/logjam-tools/go/util"
	"github.com/stretchr/testify/assert"
)

func TestPublishWebVitals(t *testing.T) {
	publisher := dummypub.NewDummyPublisher()
	rid := "some-app-preview-55ff333eee"
	requestId, err := util.ParseRequestId(rid)
	assert.NoError(t, err, "we passed a valid request id, it should not fail")

	fid := 0.24
	webVitals := &format.WebVitals{
		LogjamAction:    "someAction#call",
		LogjamRequestId: rid,
		Metrics: []format.Metric{
			{
				Id:  "1",
				FID: &fid,
			},
		},
	}

	marshaled, _ := json.Marshal(webVitals)

	err = publishWebVitals(publisher, requestId, webVitals)
	assert.NoError(t, err)
	assert.Equal(t, []dummypub.DummyMessage{
		{
			AppEnv:         "some-app-preview",
			RoutingKey:     "frontend.webvitals.some-app.preview",
			Data:           marshaled,
			CompressedWith: 0,
		},
	}, publisher.PublishedMessages)
}

func TestExtractWebVitals(t *testing.T) {
	now := time.Now()
	nowFunc = func() time.Time { return now }

	action := "myActions#call"
	rid := "some-app-preview-55ff333eee"

	uri, _ := url.Parse("https://logjam.example.com/logjam/webvitals")

	fid := 0.24
	expectedWebVitals := &format.WebVitals{
		StartedMs:       now.UnixNano() / int64(time.Millisecond),
		StartedAt:       now.Format(time.RFC3339),
		LogjamRequestId: rid,
		LogjamAction:    action,
		Metrics: []format.Metric{
			{
				Id:  "1",
				FID: &fid,
			},
		},
	}
	payload := &RequestPayload{
		LogjamRequestId: rid,
		LogjamAction:    action,
		Metrics:         expectedWebVitals.Metrics,
	}

	marshaled, err := json.Marshal(payload)
	assert.NoError(t, err)

	body := bytes.NewReader(marshaled)
	req := httptest.NewRequest("POST", uri.String(), body)

	webVitals, requestId, err := extractWebVitals(req)
	assert.NoError(t, err)
	assert.Equal(t, rid, requestId.String())
	assert.Equal(t, expectedWebVitals, webVitals)
}
