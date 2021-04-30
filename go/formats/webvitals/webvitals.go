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

// Metric represents a single measurement of a single metric
// Only one of LCP, FID or CLS should be set
type Metric struct {
	Id string `json:"id" form:"id"`

	// LCP (Largest Contentful Paint) is a latency measurement in milliseconds
	LCP *float64 `json:"lcp,omitempty" form:"lcp"`

	// FID (First Input Delay) is a latency measurement in milliseconds
	FID *float64 `json:"fid,omitempty" form:"fid"`

	// CLS (Cumulative Layout Shift) is a score represented as a float between 0 and 1
	CLS *float64 `json:"cls,omitempty" form:"cls"`
}
