package main

import (
	"testing"
)

func TestDeletingLabels(t *testing.T) {
	s := stream{
		App:                 "a",
		Env:                 "b",
		IgnoredRequestURI:   "/_",
		BackendOnlyRequests: "",
		APIRequests:         []string{},
	}
	c := newCollector(s.AppEnv(), s)
	metrics1 := map[string]string{
		"application": "a",
		"environment": "b",
		"metric":      "http",
		"code":        "200",
		"http_method": "GET",
		"cluster":     "c",
		"datacenter":  "d",
		"action":      "murks",
		"value":       "5.7",
	}
	metrics2 := map[string]string{
		"application": "a",
		"environment": "b",
		"metric":      "http",
		"code":        "200",
		"http_method": "GET",
		"cluster":     "d",
		"datacenter":  "e",
		"action":      "marks",
		"value":       "7.7",
	}
	metrics3 := map[string]string{
		"application": "a",
		"environment": "b",
		"metric":      "job",
		"code":        "200",
		"cluster":     "d",
		"datacenter":  "e",
		"action":      "marks",
		"value":       "3.1",
	}
	metrics4 := map[string]string{
		"application": "a",
		"environment": "b",
		"metric":      "job",
		"code":        "200",
		"cluster":     "d",
		"datacenter":  "e",
		"action":      "marks",
		"value":       "4.4",
	}
	c.recordMetrics(metrics1)
	c.recordMetrics(metrics2)
	c.recordMetrics(metrics3)
	c.recordMetrics(metrics4)
	if !c.removeAction("marks") {
		t.Errorf("could not remove action: %s", "marks")
	}
	if c.removeAction("schnippi") {
		t.Errorf("could remove non existing action : %s", "schnippi")
	}
}
