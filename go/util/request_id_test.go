package util

import (
	"testing"
)

func TestParseRequestId(t *testing.T) {
	var rid RequestId
	var r string
	var err error
	r = ""
	rid, err = ParseRequestId(r)
	if err == nil {
		t.Errorf("missed to detect invalid request id: %s", r)
	}
	r = "a"
	rid, err = ParseRequestId(r)
	if err == nil {
		t.Errorf("missed to detect invalid request id: %s", r)
	}
	r = "a-b"
	rid, err = ParseRequestId(r)
	if err == nil {
		t.Errorf("missed to detect invalid request id: %s", r)
	}
	r = "a-e-r"
	rid, err = ParseRequestId(r)
	if err != nil || rid.App != "a" || rid.Env != "e" || rid.Id != "r" {
		t.Errorf("could not parse request id: %s", r)
	}
	r = "a-b-e-r"
	rid, err = ParseRequestId(r)
	if err != nil || rid.App != "a-b" || rid.Env != "e" || rid.Id != "r" {
		t.Errorf("could not parse request id: %s", r)
	}
}
