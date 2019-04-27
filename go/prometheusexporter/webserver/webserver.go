package webserver

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/prometheusexporter/collectormanager"
	"gopkg.in/tylerb/graceful.v1"
)

// HandleHTTPRequests starts a webserver for exposing prometheus metrics.
func HandleHTTPRequests(port string) {
	r := mux.NewRouter()
	r.HandleFunc("/metrics/{application}/{environment}", serveAppMetrics)
	r.HandleFunc("/_system/alive", serveAliveness)
	log.Info("starting http server on port %s", port)
	spec := ":" + port
	srv := &graceful.Server{
		Timeout: 10 * time.Second,
		Server: &http.Server{
			Addr:    spec,
			Handler: r,
		},
	}
	err := srv.ListenAndServe()
	if err != nil {
		log.Error("Cannot listen and serve: %s", err)
	}
}

func serveAliveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("ok"))
}

func serveAppMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	app := vars["application"]
	env := vars["environment"]
	c := collectormanager.GetCollector(app + "-" + env)
	if c == nil {
		http.NotFound(w, r)
	} else {
		c.RequestHandler.ServeHTTP(w, r)
	}
}
