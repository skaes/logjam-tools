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
	metrics3 := map[string]string{
		"application": "a",
		"environment": "b",
		"metric":      "job",
		"code":        "200",
		"instance":    "i",
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
		"instance":    "k",
		"cluster":     "d",
		"datacenter":  "e",
		"action":      "marks",
		"value":       "4.4",
	}
	c.recordMetrics(metrics1)
	c.recordMetrics(metrics2)
	c.recordMetrics(metrics3)
	c.recordMetrics(metrics4)
	if !c.removeInstance("i") {
		t.Errorf("could not remove instance: %s", "i")
	}
	if c.removeInstance("j") {
		t.Errorf("could remove non existent instance : %s", "j")
	}
}
