package webvitals

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWebVitalsMarshaling(t *testing.T) {
	rid := "some-app-preview-55ff333eee"
	fid := 0.24
	webVitals := &WebVitals{
		LogjamAction:    "someAction#call",
		LogjamRequestId: rid,
		Metrics: []Metric{
			{
				Id:  "1",
				FID: &fid,
			},
		},
	}

	result := &WebVitals{}

	marshaled, err := json.Marshal(webVitals)
	assert.NoError(t, err)

	err = json.Unmarshal(marshaled, result)
	assert.NoError(t, err)

	assert.Equal(t, webVitals, result)

	assert.Nil(t, result.Metrics[0].LCP)
	assert.Nil(t, result.Metrics[0].CLS)
}
