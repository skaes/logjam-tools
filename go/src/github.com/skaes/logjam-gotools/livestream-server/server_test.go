package main

import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func (b *StringRing) dumpBuffer() {
	fmt.Println("===============")
	fmt.Printf("b.last: %d\n", b.last)
	fmt.Printf("b.size: %d\n", b.size)
	fmt.Printf("b.buf:  %v\n", b.buf)
	fmt.Println("===============")
}

func (b *StringRing) content() []string {
	result := make([]string, 0, b.size)
	b.ForEach(func(_ int, s string) {
		result = append(result, s)
	})
	return result
}

func (b *StringRing) hasContent(a []string) bool {
	// fmt.Printf("checking for: %v\n", a)
	if b.size != len(a) {
		fmt.Printf("wrong length: %d, %d\n", b.size, len(a))
		return false
	}
	c := b.content()
	// fmt.Printf("content is: %v\n", c)
	for i, x := range a {
		if x != c[i] {
			// fmt.Printf("unequal elements: %s,%s\n", x, c[i])
			return false
		}
	}
	return true
}

func makeSlice(a, b int) []string {
	res := []string{}
	for i := a; i <= b; i++ {
		res = append(res, strconv.Itoa(i))
	}
	return res
}

func TestStringRing(t *testing.T) {
	b := newStringRing()
	// b.dumpBuffer()
	if !b.hasContent([]string{}) {
		t.Error("Empty buffer is broken: ", b.content())
	}

	b.Add("0")
	// b.dumpBuffer()
	if !b.hasContent([]string{"0"}) {
		t.Error("Adding 1 element doesn't work: ", b.content())
	}

	b.Add("1")
	// b.dumpBuffer()
	if !b.hasContent([]string{"0", "1"}) {
		t.Error("Adding 2 elements doesn't work: ", b.content())
	}

	b = newStringRing()
	results := []string{}
	for i := 0; i < 61; i++ {
		b.Add(strconv.Itoa(i))
	}

	results = makeSlice(1, 60)
	// b.dumpBuffer()
	if !b.hasContent(results) {
		t.Error("Adding 61 elements is broken: ", b.content())
	}

}

func TestFloat64Ring(t *testing.T) {
	b := newFloat64Ring()
	// b.dumpBuffer()
	if m := b.Mean(); m != 0.0 {
		t.Error("Empty float 64 buffer should have a mean of zero: ", m)
	}
	b.Add(1.1)
	if m := b.Mean(); math.Abs(m-1.1) > 1e-12 {
		t.Error("Float 64 buffer containing a single 1.1, should have a mean of 1.1: ", m)
	}
	b.Add(1.1)
	if m := b.Mean(); math.Abs(m-1.1) > 1e-12 {
		t.Error("Float 64 buffer containing two 1.1s, should have a mean of 1.1: ", m)
	}
	b.Add(0.1)
	b.Add(0.2)
	b.Add(0.5)
	if m := b.Mean(); math.Abs(m-0.6) > 1e-12 {
		t.Errorf("Float 64 buffer containing {1.1 1.1 0.1 0.2 0.3} should have a mean of 0.6, but is %v", m)
	}
	if b.Size() != 5 {
		t.Errorf("Float 64 buffer should have size 5 but it's %d", b.Size())
	}
	for i := 0; i < 55; i++ {
		b.Add(0.2)
	}
	if b.Size() != 60 {
		t.Errorf("Float 64 buffer should have size 60 but it's %d", b.Size())
	}
	if !b.IsFull() {
		t.Errorf("Float 64 buffer should be full but is not")
	}
}

func waitSig(t *testing.T, c <-chan os.Signal, sig os.Signal) {
	select {
	case s := <-c:
		if s != sig {
			t.Fatalf("signal was %v, want %v", s, sig)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for %v", sig)
	}
}

func TestSignalsAreDeliveredToMultipleListeners(t *testing.T) {
	c1 := make(chan os.Signal, 1)
	c2 := make(chan os.Signal, 1)
	signal.Notify(c1, syscall.SIGWINCH)
	signal.Notify(c2, syscall.SIGWINCH)
	defer signal.Stop(c1)
	defer signal.Stop(c2)
	syscall.Kill(syscall.Getpid(), syscall.SIGWINCH)
	waitSig(t, c1, syscall.SIGWINCH)
	waitSig(t, c2, syscall.SIGWINCH)
}
