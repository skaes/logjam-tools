package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/skaes/logjam-tools/go/fhttpd/stats"
	log "github.com/skaes/logjam-tools/go/logging"
)

func runWebServer(srv *http.Server) {
	log.Info("starting http server on %s", srv.Addr)
	if opts.KeyFile != "" && opts.CertFile != "" {
		err := srv.ListenAndServeTLS(opts.CertFile, opts.KeyFile)
		if err != nil && err != http.ErrServerClosed {
			log.Error("Cannot listen and serve TLS: %s", err)
		}
	} else if opts.KeyFile != "" {
		log.Error("cert-file given but no key-file!")
	} else if opts.CertFile != "" {
		log.Error("key-file given but no cert-file!")
	} else {
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Error("Cannot listen and serve: %s", err)
		}
	}
}

func shutdownWebServer(srv *http.Server, gracePeriod time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), gracePeriod)
	defer cancel()
	err := srv.Shutdown(ctx)
	if err != nil {
		log.Error("web server shutdown failed: %+v", err)
	} else {
		log.Info("web server shutdown successful")
	}
}

type (
	stringMap map[string]interface{}
	stringSet map[string]bool
)

func (sm stringMap) DeleteString(k string) string {
	v, _ := sm[k].(string)
	delete(sm, k)
	return v
}

// URL params which neeed to be converted to integer for JSON
var integerKeyList = []string{"viewport_height", "viewport_width", "html_nodes", "script_nodes", "style_nodes", "v"}

// Lookup table for those params
var integerKeys stringSet

func init() {
	integerKeys = make(stringSet)
	for _, k := range integerKeyList {
		integerKeys[k] = true
	}
}

func parseValue(k string, v string) (interface{}, error) {
	if integerKeys[k] {
		return strconv.Atoi(v)
	}
	return v, nil
}

func parseQuery(r *http.Request) (stringMap, error) {
	sm := make(stringMap)
	for k, v := range r.URL.Query() {
		switch len(v) {
		case 1:
			v, err := parseValue(k, v[0])
			if err != nil {
				return sm, err
			}
			sm[k] = v
		case 0:
			sm[k] = ""
		default:
			return sm, fmt.Errorf("Parameter %s specified more than once", k)
		}
	}
	return sm, nil
}

func writeErrorResponse(w http.ResponseWriter, txt string) {
	stats.IncrementFailures()
	http.Error(w, "400 RTFM", 400)
	fmt.Fprintln(w, txt)
}

func writeImageResponse(w http.ResponseWriter) {
	w.WriteHeader(200)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "image/gif")
	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("Content-Transfer-Encoding", "base64")
	io.WriteString(w, "R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==")
}

func serveAlive(w http.ResponseWriter, r *http.Request) {
	defer stats.RecordRequestStats(r)
	w.WriteHeader(200)
	w.Header().Set("Cache-Control", "private")
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, "ALIVE\n")
}
