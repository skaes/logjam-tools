package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func (b *StringBuffer) dumpBuffer() {
	fmt.Println("===============")
	fmt.Printf("b.last: %d\n", b.last)
	fmt.Printf("b.size: %d\n", b.size)
	fmt.Printf("b.buf:  %v\n", b.buf)
	fmt.Println("===============")
}

func (b *StringBuffer) content() []string {
	result := make([]string, 0, b.size)
	b.ForEach(func(_ int, s string) {
		result = append(result, s)
	})
	return result
}

func (b *StringBuffer) hasContent(a []string) bool {
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

func TestStringBuffer(t *testing.T) {
	b := newStringBuffer()
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

	b = newStringBuffer()
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
