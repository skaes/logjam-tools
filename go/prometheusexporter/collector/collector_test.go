package collector

import (
	"github.com/skaes/logjam-tools/go/util"
	"testing"
)

func TestDeletingLabels(t *testing.T) {
	s := util.Stream{
		App:                 "a",
		Env:                 "b",
		IgnoredRequestURI:   "/_",
		BackendOnlyRequests: "",
		APIRequests:         []string{},
	}
	options := Options{
		Datacenters: "a,b",
		CleanAfter:  60,
	}
	c := New(s.AppEnv(), &s, options)
	metrics1 := &metric{
		kind: logMetric,
		props: map[string]string{
			"app":     "a",
			"env":     "b",
			"metric":  "http",
			"code":    "200",
			"method":  "GET",
			"cluster": "c",
			"dc":      "d",
			"action":  "murks",
		},
		value: 5.7,
	}
	metrics2 := &metric{
		kind: logMetric,
		props: map[string]string{
			"app":     "a",
			"env":     "b",
			"metric":  "http",
			"code":    "200",
			"method":  "GET",
			"cluster": "d",
			"dc":      "e",
			"action":  "marks",
		},
		value: 7.7,
	}
	metrics3 := &metric{
		kind: logMetric,
		props: map[string]string{
			"app":     "a",
			"env":     "b",
			"metric":  "job",
			"code":    "200",
			"cluster": "d",
			"dc":      "e",
			"action":  "marks",
		},
		value: 3.1,
	}
	metrics4 := &metric{
		kind: logMetric,
		props: map[string]string{
			"app":     "a",
			"env":     "b",
			"metric":  "job",
			"code":    "200",
			"cluster": "d",
			"dc":      "e",
			"action":  "marks"},
		value: 4.4,
	}
	c.recordLogMetrics(metrics1)
	c.recordLogMetrics(metrics2)
	c.recordLogMetrics(metrics3)
	c.recordLogMetrics(metrics4)
	if !c.removeAction("marks") {
		t.Errorf("could not remove action: %s", "marks")
	}
	if c.removeAction("schnippi") {
		t.Errorf("could remove non existing action : %s", "schnippi")
	}
}
