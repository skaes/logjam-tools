package frontendmetrics

import (
	"fmt"
	"strconv"
	"strings"
)

// Timings collects relevant metrics from the browser performance API
type Timings struct {
	NavigationTime int64 `json:"navigation_time"`
	ConnectTime    int64 `json:"connect_time"`
	RequestTime    int64 `json:"request_time"`
	ResponseTime   int64 `json:"response_time"`
	ProcessingTime int64 `json:"processing_time"`
	LoadTime       int64 `json:"load_time"`
	PageTime       int64 `json:"page_time"`
	DomInteractive int64 `json:"dom_interactive"`
}

const (
	numTimings      = 16
	navigationStart = 0
	// unloadEventStart           = -1
	// unloadEventEnd             = -1
	// redirectStart              = -1
	// redirectEnd                = -1
	fetchStart        = 1
	domainLookupStart = 2
	domainLookupEnd   = 3
	connectStart      = 4
	connectEnd        = 5
	// secureConnectionStart      = -1
	requestStart               = 6
	responseStart              = 7
	responseEnd                = 8
	domLoading                 = 9
	domInteractive             = 10
	domContentLoadedEventStart = 11
	domContentLoadedEventEnd   = 12
	domComplete                = 13
	loadEventStart             = 14
	loadEventEnd               = 15
)

// OutlierThresholdMs defines which frontend messages should be
// ignored because there reported response time is wildly out of what
// makes sense.
const OutlierThresholdMs = 60000

func extract(rts string, n int) ([]int64, error) {
	elems := strings.Split(rts, ",")
	l := len(elems)
	if l != n {
		return nil, fmt.Errorf("rts has not the correct length: got %d, expected %d", l, n)
	}
	vals := make([]int64, n)
	for i, s := range elems {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("could not convert rts element %s to int64", s)
		}
		vals[i] = v
	}
	return vals, nil
}

func makeRelative(a []int64, base int64) {
	for i, v := range a {
		if v > 0 {
			a[i] -= base
		}
	}
}

func allZero(a []int64) bool {
	for _, v := range a {
		if v != 0 {
			return false
		}
	}
	return true
}

func sortedAscending(a []int64, n int) bool {
	for i := 1; i < n; i++ {
		if a[i] < a[i-1] {
			return false
		}
	}
	return true
}

func logTimings(t []int64) {
	fmt.Printf("navigationStart:            %d\n", t[0])
	fmt.Printf("fetchStart:                 %d\n", t[1])
	fmt.Printf("domainLookupStart:          %d\n", t[2])
	fmt.Printf("domainLookupEnd:            %d\n", t[3])
	fmt.Printf("connectStart:               %d\n", t[4])
	fmt.Printf("connectEnd:                 %d\n", t[5])
	fmt.Printf("requestStart:               %d\n", t[6])
	fmt.Printf("responseStart:              %d\n", t[7])
	fmt.Printf("responseEnd:                %d\n", t[8])
	fmt.Printf("domLoading:                 %d\n", t[9])
	fmt.Printf("domInteractive:             %d\n", t[10])
	fmt.Printf("domContentLoadedEventStart: %d\n", t[11])
	fmt.Printf("domContentLoadedEventEnd:   %d\n", t[12])
	fmt.Printf("domComplete:                %d\n", t[13])
	fmt.Printf("loadEventStart:             %d\n", t[14])
	fmt.Printf("loadEventEnd:               %d\n", t[15])
}

// ExtractPageTimings builds a Timings object from the rts parameters received from the browser.
func ExtractPageTimings(rts string) (*Timings, error) {
	timings, err := extract(rts, numTimings)
	if err != nil {
		return nil, err
	}
	base := timings[navigationStart]
	if base == 0 {
		base = timings[fetchStart]
		timings[navigationStart] = base
	}
	makeRelative(timings, base)
	// logTimings(timings)
	utimes := []int64{
		timings[navigationStart],
		timings[requestStart],
		timings[responseStart],
		timings[responseEnd],
		timings[domComplete],
		timings[loadEventEnd],
		timings[domInteractive],
	}
	// TODO: log timings to file
	if utimes[0] < 0 {
		return nil, fmt.Errorf("navigationStart is less than zero: %d", utimes[0])
	}
	if utimes[6] <= 0 {
		return nil, fmt.Errorf("domIntercative is negative or zero: %d", utimes[6])
	}
	if !sortedAscending(utimes, 5) {
		return nil, fmt.Errorf("values are not properly sorted: %v", utimes[0:6])
	}
	return &Timings{
		NavigationTime: timings[fetchStart],
		ConnectTime:    timings[requestStart] - timings[fetchStart],
		RequestTime:    timings[responseStart] - timings[requestStart],
		ResponseTime:   timings[responseEnd] - timings[responseStart],
		ProcessingTime: timings[domComplete] - timings[responseEnd],
		LoadTime:       timings[loadEventEnd] - timings[domComplete],
		PageTime:       timings[loadEventEnd],
		DomInteractive: timings[domInteractive],
	}, nil
}

// ExtractAjaxTime returns the ajax response time.
func ExtractAjaxTime(rts string) (int64, error) {
	timings, err := extract(rts, 2)
	if err != nil {
		return 0, err
	}
	start, end := timings[0], timings[1]
	if start < 0 || start > end {
		return 0, fmt.Errorf("ajax start is negative or ajax end is before ajax start")
	}
	return end - start, nil
}
