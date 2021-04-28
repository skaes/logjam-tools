package webvitals

const RoutingKeyPrefix = "frontend"
const RoutingKeyMsgType = "webvitals"

// WebVitals is the representation of WebVitals metrics
// in the Payload of a Logjam Message.
type WebVitals struct {
	StartedMs       int64    `json:"started_ms"`
	StartedAt       string   `json:"started_at"`
	LogjamRequestId string   `json:"logjam_request_id"`
	LogjamAction    string   `json:"logjam_action"`
	Metrics         []Metric `json:"metrics"`
}

type Metric struct {
	Id  string   `json:"id"`
	LCP *float64 `json:"lcp,omitempty"`
	FID *float64 `json:"fid,omitempty"`
	CLS *float64 `json:"cls,omitempty"`
}
