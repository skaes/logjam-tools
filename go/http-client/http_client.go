package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

// HttpClient provides a simple interface to send events to a
// logjam-http-forwarder endpoint.
type HttpClient struct {
	url    string
	client *http.Client
}

// NewHttpClient creates a new HttpClient instance.
func NewHttpClient(uri string, timeout time.Duration) (*HttpClient, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "/logjam/events")
	hc := HttpClient{url: u.String()}
	hc.client = &http.Client{Timeout: timeout}
	return &hc, nil
}

// LogjamEvent encapsulates all parameters of a logjam event notification.
type LogjamEvent struct {
	App       string `json:"app"`
	Env       string `json:"env"`
	Label     string `json:"label"`
	StartedAt string `json:"started_at"`
	Host      string `json:"host"`
	UUID      string `json:"uuid"`
	Cluster   string `json:"cluster"`
}

// SendEvent sends a logjam event notification to a logjam-http-forwarder
// endpoint.
func (hc *HttpClient) SendEvent(event *LogjamEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(body)
	response, err := hc.client.Post(hc.url, "application/json", reader)
	if err != nil {
		return err
	}
	buf, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode >= 202 && response.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("Unexpected response from logjam endpoint: status: %d, msg: %s ", response.StatusCode, string(buf))
}
