package main

import (
	"testing"
)

func TestDeletingLabels(t *testing.T) {
	c := newCollector([]string{})
	metrics1 := map[string]string{
		"application": "a",
		"environment": "b",
		"metric":      "http",
		"code":        "200",
		"http_method": "GET",
		"instance":    "i",
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
		"instance":    "k",
		"cluster":     "d",
		"datacenter":  "e",
		"action":      "marks",
		"value":       "7.7",
	}
	c.recordMetrics(metrics1)
	c.recordMetrics(metrics2)
	if !c.removeInstance(instanceInfo{name: "i", kind: "http"}) {
		t.Errorf("could not remove instance: %s", "i")
	}
	if c.removeInstance(instanceInfo{name: "j", kind: "http"}) {
		t.Errorf("could remove non existent instance : %s", "j")
	}
}
