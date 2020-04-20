package util

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	// "github.com/davecgh/go-spew/spew"
	log "github.com/skaes/logjam-tools/go/logging"
)

type Resources struct {
	TimeResources        []string `json:"time_resources"`
	CallResources        []string `json:"call_resources"`
	MemoryResources      []string `json:"memory_resources"`
	HeapResources        []string `json:"heap_resources"`
	TimeAndCallResources []string
}

func (r *Resources) Initialize() {
	m, n := len(r.TimeResources), len(r.CallResources)
	r.TimeAndCallResources = make([]string, m+n, m+n)
	for i, resource := range r.TimeResources {
		r.TimeAndCallResources[i] = resource
	}
	for i, resource := range r.CallResources {
		r.TimeAndCallResources[i+m] = resource
	}
}

func RetrieveResources(url, env string) *Resources {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Error("could not create http request: %s", err)
		return nil
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Error("could not retrieve resources: %s", err)
		return nil
	}
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
	defer res.Body.Close()
	var resources Resources
	err = json.Unmarshal(body, &resources)
	if err != nil {
		log.Error("could not parse resources: %s", err)
		return nil
	}
	return &resources
}

func convertFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		log.Error("ignored unknown metric type: %v", v)
		return 0
	}
}

func (rs *Resources) ExtractResources(data map[string]interface{}) (float64, map[string]float64) {
	var totalTime float64
	res := make(map[string]float64)
	for _, r := range rs.TimeResources {
		if v, found := data[r]; found {
			m := convertFloat64(v)
			if r == "total_time" {
				totalTime = m / 1000
			} else if m > 0 {
				res[r] = m / 1000
			}
		}
	}
	for _, r := range rs.CallResources {
		if v, found := data[r]; found {
			m := convertFloat64(v)
			if m > 0 {
				res[r] = m
			}
		}
	}
	return totalTime, res
}
