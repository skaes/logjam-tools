prefix ?= /usr/local

PKG_CONFIG_PATH ?= $(prefix)/lib/pkgconfig
export PKG_CONFIG_PATH

OPTDIR_LDFLAGS ?= -Wl,-rpath -Wl,$(prefix)/lib
CGO_LDFLAGS = $(OPTDIR_LDFLAGS)
export CGO_LDFLAGS

INSTALL_PROGRAM ?= install
bindir ?= $(prefix)/bin

GO_NOINSTALL_TARGETS = logjam-livestream-generator
GO_INSTALL_TARGETS = logjam-livestream logjam-http-forwarder logjam-fhttpd logjam-prometheus-exporter logjam-mongodb-backup logjam-mongodb-restore logjam-rename-callers
GO_TARGETS = $(GO_INSTALL_TARGETS) $(GO_NOINSTALL_TARGETS)

all: $(GO_TARGETS)

clean:
	rm -f $(GO_TARGETS)

install: $(GO_INSTALL_TARGETS)
	$(INSTALL_PROGRAM) $^ $(bindir)

uninstall:
	cd $(bindir) && rm -f $(GO_INSTALL_TARGETS)

logjam-livestream: livestream-server/server.go
	go build -o $@ $<

logjam-livestream-generator: livestream-generator/generator.go
	go build -o $@ $<

logjam-http-forwarder: http-forwarder/forwarder.go util/util.go
	go build -o $@ $<

logjam-fhttpd: fhttpd/* util/util.go
	go build -o $@ github.com/skaes/logjam-tools/go/fhttpd

EXPORTER_PKGS = prometheusexporter/messageparser/messageparser.go prometheusexporter/collector/collector.go prometheusexporter/collectormanager/collectormanager.go prometheusexporter/webserver/webserver.go prometheusexporter/stats/stats.go
EXPORTER_DEPS = util/util.go util/streams.go util/resources.go logging/logging.go frontendmetrics/frontendmetrics.go

logjam-prometheus-exporter: prometheusexporter/exporter.go $(EXPORTER_DEPS) $(EXPORTER_PKGS)
	go build -o $@ $<

logjam-mongodb-backup: backup/backup.go
	go build -o $@ $<

logjam-mongodb-restore: restore/restore.go
	go build -o $@ $<

logjam-rename-callers: rename-callers/rename-callers.go
	go build -o $@ $<

GO_TESTS = promcollector-test livestream-test util-test frontendmetrics-test fhttpd-test formats-test
.PHONY: $(GO_TESTS)
check: $(GO_TESTS)

http-client-test:
	go test ./http-client

promcollector-test:
	go test ./prometheusexporter/...

livestream-test: logjam-livestream
	go test ./livestream-server

util-test:
	go test ./util

frontendmetrics-test: frontendmetrics/*.go
	go test ./frontendmetrics

fhttpd-test: fhttpd/*
	go test ./fhttpd/.../.

formats-test: formats/*
	go test ./formats/*
