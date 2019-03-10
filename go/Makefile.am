AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

GO_ENV = CGO_LDFLAGS='$(OPTDIR_LDFLAGS)' PKG_CONFIG_PATH=$(PKG_CONFIG_PATH)
GO_SRC = .
GO_NOINSTALL_TARGETS = logjam-livestream-generator
GO_INSTALL_TARGETS = logjam-livestream logjam-http-forwarder logjam-prometheus-exporter logjam-mongodb-backup logjam-mongodb-restore
GO_TARGETS = $(GO_INSTALL_TARGETS) $(GO_NOINSTALL_TARGETS)
GO_TESTS = http-client-test prometheus-exporter-test

.godeps:
	$(GO_ENV) go mod tidy
	touch .godeps

# Extend generated Makefile with rules for compiling the go part. See
# https://www.gnu.org/software/automake/manual/html_node/Extending.html for why
# it's all-local, check-local, clean-local, etc.

all-local: $(GO_TARGETS)

clean-local:
	rm -f $(GO_TARGETS) .godeps

install-exec-local: $(GO_INSTALL_TARGETS)
	$(INSTALL_PROGRAM) $^ $(bindir)

uninstall-local:
	cd $(bindir) && rm -f $(GO_INSTALL_TARGETS)

logjam-livestream: livestream-server/server.go .godeps
	$(GO_ENV) go build -i -o $@ livestream-server/server.go

logjam-livestream-generator: livestream-generator/generator.go .godeps
	$(GO_ENV) go build -i -o $@ livestream-generator/generator.go

logjam-http-forwarder: http-forwarder/forwarder.go .godeps
	$(GO_ENV) go build -i -o $@ http-forwarder/forwarder.go

logjam-prometheus-exporter: prometheus-exporter/exporter.go .godeps
	$(GO_ENV) go build -i -o $@ prometheus-exporter/exporter.go

logjam-mongodb-backup: backup/backup.go .godeps
	$(GO_ENV) go build -i -o $@ backup/backup.go

logjam-mongodb-restore: restore/restore.go .godeps
	$(GO_ENV) go build -i -o $@ restore/restore.go

.PHONY: http-client-test prometheus-exporter-test livestream-test forwarde
check-local: livestream-test http-forwarder-test prometheus-exporter-test

http-client-test:
	$(GO_ENV) go test ./http-client

prometheus-exporter-test:
	$(GO_ENV) go test ./prometheus-exporter

livestream-test: logjam-livestream
	$(GO_ENV) go test ./livestream-server

http-forwarder-test: logjam-http-forwarder
	$(GO_ENV) go test ./http-forwarder
