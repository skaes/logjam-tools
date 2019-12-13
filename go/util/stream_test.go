package util

import (
	"testing"
)

func TestParsingStreamNames(t *testing.T) {
	var appEnv string
	app, env := ParseStreamName(appEnv)
	if app != "" || env != "" {
		t.Errorf("failed to parse appEnv: %s ==> %s,%s", appEnv, app, env)
	}
	appEnv = "x"
	app, env = ParseStreamName(appEnv)
	if app != "" || env != "" {
		t.Errorf("failed to parse appEnv: %s ==> %s,%s", appEnv, app, env)
	}
	appEnv = "a-b"
	app, env = ParseStreamName(appEnv)
	if app != "a" || env != "b" {
		t.Errorf("failed to parse appEnv: %s ==> %s,%s", appEnv, app, env)
	}
	appEnv = "a-b-c"
	app, env = ParseStreamName(appEnv)
	if app != "a-b" || env != "c" {
		t.Errorf("failed to parse appEnv: %s ==> %s,%s", appEnv, app, env)
	}
}
