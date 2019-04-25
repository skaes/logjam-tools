AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

GO_ENV = CGO_LDFLAGS='$(OPTDIR_LDFLAGS)' PKG_CONFIG_PATH=$(PKG_CONFIG_PATH)
GO_SRC = .
GO_NOINSTALL_TARGETS = logjam-livestream-generator
GO_INSTALL_TARGETS = logjam-livestream logjam-http-forwarder logjam-fhttpd logjam-prometheus-exporter logjam-mongodb-backup logjam-mongodb-restore
GO_TARGETS = $(GO_INSTALL_TARGETS) $(GO_NOINSTALL_TARGETS)

# Extend generated Makefile with rules for compiling the go part. See
# https://www.gnu.org/software/automake/manual/html_node/Extending.html for why
# it's all-local, check-local, clean-local, etc.

all-local: $(GO_TARGETS)

clean-local:
	rm -f $(GO_TARGETS)

install-exec-local: $(GO_INSTALL_TARGETS)
	$(INSTALL_PROGRAM) $^ $(bindir)

uninstall-local:
	cd $(bindir) && rm -f $(GO_INSTALL_TARGETS)

logjam-livestream: livestream-server/server.go
	$(GO_ENV) go build -i -o $@ $<

logjam-livestream-generator: livestream-generator/generator.go
	$(GO_ENV) go build -i -o $@ $<

logjam-http-forwarder: http-forwarder/forwarder.go util/util.go
	$(GO_ENV) go build -i -o $@ $<

logjam-fhttpd: fhttpd/fhttpd.go util/util.go
	$(GO_ENV) go build -i -o $@ $<

logjam-prometheus-exporter: prometheus-exporter/exporter.go util/util.go frontendmetrics/frontendmetrics.go
	$(GO_ENV) go build -i -o $@ $<

logjam-mongodb-backup: backup/backup.go
	$(GO_ENV) go build -i -o $@ $<

logjam-mongodb-restore: restore/restore.go
	$(GO_ENV) go build -i -o $@ $<

GO_TESTS = promcollector-test livestream-test util-test frontendmetrics-test
.PHONY: $(GO_TESTS)
check-local: $(GO_TESTS)

http-client-test:
	$(GO_ENV) go test ./http-client

promcollector-test:
	$(GO_ENV) go test ./promcollector

livestream-test: logjam-livestream
	$(GO_ENV) go test ./livestream-server

util-test:
	$(GO_ENV) go test ./util

frontendmetrics-test: frontendmetrics/*.go
	$(GO_ENV) go test ./frontendmetrics
