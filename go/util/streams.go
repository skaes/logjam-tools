package util

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	log "github.com/skaes/logjam-tools/go/logging"
)

type Stream struct {
	App                 string    `json:"app"`
	Env                 string    `json:"env"`
	IgnoredRequestURI   string    `json:"ignored_request_uri"`
	BackendOnlyRequests string    `json:"backend_only_requests"`
	APIRequests         []string  `json:"api_requests"`
	HttpBuckets         []float64 `json:"http_buckets"`
	JobsBuckets         []float64 `json:"jobs_buckets"`
	PageBuckets         []float64 `json:"page_buckets"`
	AjaxBuckets         []float64 `json:"ajax_buckets"`
}

func (s *Stream) AppEnv() string {
	return s.App + "-" + s.Env
}

func sameBuckets(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i += 1 {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i += 1 {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s1 *Stream) SameHttpBuckets(s2 *Stream) bool {
	return sameBuckets(s1.HttpBuckets, s2.HttpBuckets)
}

func (s1 *Stream) SameJobsBuckets(s2 *Stream) bool {
	return sameBuckets(s1.JobsBuckets, s2.JobsBuckets)
}

func (s1 *Stream) SamePageBuckets(s2 *Stream) bool {
	return sameBuckets(s1.PageBuckets, s2.PageBuckets)
}

func (s1 *Stream) SameAjaxBuckets(s2 *Stream) bool {
	return sameBuckets(s1.AjaxBuckets, s2.AjaxBuckets)
}

func (s1 *Stream) SameAPIRequests(s2 *Stream) bool {
	return sameStrings(s1.APIRequests, s2.APIRequests)
}

func RetrieveStreams(url, env string) map[string]*Stream {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Error("could not create http request: %s", err)
		return nil
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Error("could not retrieve stream: %s", err)
		return nil
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Error("unexpected response: %d", res.Status)
		ioutil.ReadAll(res.Body)
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Error("could not read response body: %s", err)
		return nil
	}
	var streams map[string]*Stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		log.Error("could not parse stream: %s", err)
		return nil
	}
	return streams
}
