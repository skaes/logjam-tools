package main

import (
	"testing"
)

func TestParseRequestId(t *testing.T) {
	var rid requestId
	var r string
	var err error
	r = ""
	rid, err = parseRequestId(r)
	if err == nil {
		t.Errorf("missed to detect invalid request id: %s", r)
	}
	r = "a"
	rid, err = parseRequestId(r)
	if err == nil {
		t.Errorf("missed to detect invalid request id: %s", r)
	}
	r = "a-b"
	rid, err = parseRequestId(r)
	if err == nil {
		t.Errorf("missed to detect invalid request id: %s", r)
	}
	r = "a-e-r"
	rid, err = parseRequestId(r)
	if err != nil || rid.App != "a" || rid.Env != "e" || rid.Id != "r" {
		t.Errorf("could not parse request id: %s", r)
	}
}
