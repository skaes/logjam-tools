package webvitals

const RoutingKeyPrefix = "frontend"
const RoutingKeyMsgType = "webvitals"

// WebVitals is the representation of WebVitals metrics
// in the Payload of a Logjam Message.
type WebVitals struct {
	// StartedMs is filled in server side by us and not a field that should be
	// supplied via the WebVitals endpoint.
	StartedMs int64 `json:"started_ms" form:"-" mapstructure:"started_ms"`
	// StartedAt is filled in server side by us (As an RFC3339 timestamp) and not
	// a field that should be supplied via the WebVitals endpoint.
	StartedAt string `json:"started_at" form:"-" mapstructure:"started_at"`

	UserAgent string `json:"user_agent" form:"-" mapstructure:"user_agent"`

	LogjamRequestId string   `json:"logjam_request_id" form:"logjam_request_id" mapstructure:"logjam_request_id"`
	LogjamAction    string   `json:"logjam_action" form:"logjam_action" mapstructure:"logjam_action"`
	Metrics         []Metric `json:"metrics" form:"metrics"`
}

// Metric represents a single measurement of a single metric
// Only one of LCP, FID or CLS should be set
type Metric struct {
	Id string `json:"id" form:"id" mapstructure:"id"`

	// LCP (Largest Contentful Paint) is a latency measurement in milliseconds
	LCP *float64 `json:"lcp,omitempty" form:"lcp" mapstructure:"lcp"`

	// FID (First Input Delay) is a latency measurement in milliseconds
	FID *float64 `json:"fid,omitempty" form:"fid" mapstructure:"fid"`

	// CLS (Cumulative Layout Shift) is a score represented as a float between 0 and 1
	CLS *float64 `json:"cls,omitempty" form:"cls" mapstructure:"cls"`
}
