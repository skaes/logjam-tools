package webserver

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	log "github.com/skaes/logjam-tools/go/logging"
	"github.com/skaes/logjam-tools/go/prometheusexporter/collectormanager"
	"github.com/skaes/logjam-tools/go/prometheusexporter/stats"
	"gopkg.in/tylerb/graceful.v1"
)

type serveMetrics func(w http.ResponseWriter, r *http.Request)

// HandleHTTPRequests starts a webserver for exposing prometheus metrics.
func HandleHTTPRequests(port string) {
	r := mux.NewRouter()
	r.HandleFunc("/metrics/{application}/{environment}", serveAppMetrics)
	r.HandleFunc("/metrics/exceptions/{application}/{environment}", serveExceptionsMetrics)
	r.HandleFunc("/metrics", serveExporterMetrics)
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
	h := getReqHandler(r)
	serve(w, r, h, h.ServeAppMetrics)
}

func serveExceptionsMetrics(w http.ResponseWriter, r *http.Request) {
	h := getReqHandler(r)
	serve(w, r, h, h.ServeExceptionsMetrics)
}

func serve(w http.ResponseWriter, r *http.Request, h collectormanager.MetricsRequestHandler, servefn serveMetrics) {
	if !h.IsCollector() {
		http.NotFound(w, r)
		return
	}
	t := time.Now()
	app, env := getAppEnv(r)
	defer func() { stats.ObserveScrapeDuration(app, env, time.Since(t)) }()
	servefn(w, r)
}

func getAppEnv(r *http.Request) (app string, env string) {
	vars := mux.Vars(r)
	app = vars["application"]
	env = vars["environment"]
	return
}

func getReqHandler(r *http.Request) collectormanager.MetricsRequestHandler {
	app, env := getAppEnv(r)
	return collectormanager.GetRequestHandler(app + "-" + env)
}

func serveExporterMetrics(w http.ResponseWriter, r *http.Request) {
	stats.RequestHandler.ServeHTTP(w, r)
}
