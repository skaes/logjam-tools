package httpclient

import (
	"fmt"
	"testing"
	"time"
)

func TestSendingEvent(t *testing.T) {
	c, err := NewHttpClient("http://127.0.0.1:9805", 100*time.Millisecond)
	if err != nil {
		t.Errorf("unexpected error creating http client: %+v", err)
	}
	fmt.Println("sending messages")
	for i := 0; i < 10; i += 1 {
		tm := time.Now().Format(time.RFC3339)
		event := LogjamEvent{App: "X", Env: "T", Label: "halli hallo", StartedAt: tm, Host: "foo", UUID: "dc9076e92fda4019bd2c900a8284b9c4", Cluster: "murks"}
		if err := c.SendEvent(&event); err != nil {
			t.Errorf("unexpected error sending event %d to logjam: %+v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
