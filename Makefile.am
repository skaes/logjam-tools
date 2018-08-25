AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS} -I m4

AM_CPPFLAGS = -I$(top_srcdir) $(OPTDIR_CPPFLAGS) $(DEPS_CFLAGS) -D_GNU_SOURCE
AM_CFLAGS = --std=gnu99 -Wall -fno-fast-math
AM_LDFLAGS = $(OPT_PTHREAD_FLAGS)
LDADD = $(OPTDIR_LDFLAGS) $(DEPS_LIBS) $(OPT_PROFILER_FLAGS) -lm

dist_bin_SCRIPTS = logjam-prometheus-config

bin_PROGRAMS = \
    logjam-device \
    logjam-importer \
    logjam-fhttpd \
    logjam-graylog-forwarder \
    logjam-dump \
    logjam-replay \
    logjam-pubsub-bridge \
    logjam-forwarder \
    logjam-logger \
    logjam-tail

noinst_PROGRAMS = \
    test_publisher \
    test_puller \
    test_subscriber \
    tester \
    checker

logjam_device_SOURCES = \
    config.h \
    src/logjam-device.c \
    src/logjam-util.c \
    src/logjam-util.h \
    src/rabbitmq-listener.c \
    src/rabbitmq-listener.h \
    src/message-compressor.c \
    src/message-compressor.h

logjam_importer_SOURCES = \
    config.h \
    src/importer-adder.c \
    src/importer-adder.h \
    src/importer-common.c \
    src/importer-common.h \
    src/importer-controller.c \
    src/importer-controller.h \
    src/importer-increments.c \
    src/importer-increments.h \
    src/importer-indexer.c \
    src/importer-indexer.h \
    src/importer-livestream.c \
    src/importer-livestream.h  \
    src/importer-mongoutils.c \
    src/importer-mongoutils.h \
    src/importer-parser.c \
    src/importer-parser.h \
    src/importer-processor.c \
    src/importer-processor.h \
    src/importer-requestwriter.c \
    src/importer-requestwriter.h \
    src/importer-resources.c \
    src/importer-resources.h \
    src/importer-statsupdater.c \
    src/importer-statsupdater.h \
    src/importer-streaminfo.c \
    src/importer-streaminfo.h \
    src/importer-subscriber.c \
    src/importer-subscriber.h \
    src/importer-tracker.c \
    src/importer-tracker.h \
    src/importer-watchdog.c \
    src/importer-watchdog.h \
    src/logjam-importer.c \
    src/logjam-util.c \
    src/logjam-util.h \
    src/statsd-client.c \
    src/statsd-client.h \
    src/zring.c \
    src/zring.h \
    src/device-tracker.c \
    src/device-tracker.h \
    src/prom-collector.c \
    src/prom-collector.h

logjam_fhttpd_SOURCES = \
    config.h \
    src/logjam-fhttpd.c \
    src/logjam-util.c \
    src/logjam-util.h

logjam_graylog_forwarder_SOURCES = \
    config.h \
    src/logjam-graylog-forwarder.c \
    src/graylog-forwarder-common.c \
    src/graylog-forwarder-common.h \
    src/graylog-forwarder-controller.c \
    src/graylog-forwarder-controller.h \
    src/graylog-forwarder-parser.c \
    src/graylog-forwarder-parser.h \
    src/graylog-forwarder-subscriber.c \
    src/graylog-forwarder-subscriber.h \
    src/graylog-forwarder-writer.c \
    src/graylog-forwarder-writer.h \
    src/logjam-util.c \
    src/logjam-util.h \
    src/gelf-message.c \
    src/gelf-message.h \
    src/logjam-message.c \
    src/logjam-message.h \
    src/str-builder.c \
    src/str-builder.h \
    src/device-tracker.c \
    src/device-tracker.h

logjam_dump_SOURCES = \
    config.h \
    src/logjam-dump.c \
    src/logjam-util.c \
    src/logjam-util.h \
    src/device-tracker.c \
    src/device-tracker.h

logjam_replay_SOURCES = \
    config.h \
    src/logjam-replay.c \
    src/logjam-util.c \
    src/logjam-util.h

logjam_pubsub_bridge_SOURCES = \
    config.h \
    src/logjam-pubsub-bridge.c \
    src/logjam-util.c \
    src/logjam-util.h \
    src/message-compressor.c \
    src/message-compressor.h \
    src/device-tracker.c \
    src/device-tracker.h

logjam_forwarder_SOURCES = \
    config.h \
    src/logjam-forwarder.c \
    src/logjam-util.c \
    src/logjam-util.h \
    src/device-tracker.c \
    src/device-tracker.h

logjam_logger_SOURCES = \
    config.h \
    src/logjam-logger.c \
    src/logjam-util.c \
    src/logjam-util.h

logjam_tail_SOURCES = \
    config.h \
    src/logjam-tail.c \
    src/logjam-util.c \
    src/logjam-util.h

tester_SOURCES = src/tester.c

test_publisher_SOURCES = src/test_publisher.c

test_subscriber_SOURCES = src/test_subscriber.c

test_puller_SOURCES = src/test_puller.c

dist_noinst_SCRIPTS = autogen.sh

checker_SOURCES = \
    src/checker.c \
    src/zring.c \
    src/zring.h \
    src/logjam-util.c \
    src/logjam-util.h


#local rules
#TEST_PUBLISHERS=1 2 3 4 5
TEST_PUBLISHERS=1
ULIMIT=20000

.PHONY: test run cov-build analyze check

test: tester
	for i in $(TEST_PUBLISHERS); do (ulimit -n $(ULIMIT); ./tester 200 100000&); done

run: logjam-device
	ulimit -n $(ULIMIT); ./logjam-device 12345 localhost

cov-build: clean
	cov-build --dir cov-int make

analyze: clean
	scan-build ./configure
	report=`scan-build make | egrep -e '^scan-build: Run'`; echo $$report;\
        scan-view `echo $$report | sed -e "s/scan-build: Run 'scan-view \(.*\)' to examine bug reports./\1/"`

check: checker
	./checker

.godeps:
	cd $(GO_SRC) && $(GO_ENV) go mod vendor
	touch .godeps

GO_ENV = CGO_LDFLAGS='$(OPTDIR_LDFLAGS)' PKG_CONFIG_PATH=$(PKG_CONFIG_PATH)
GO_SRC = go
GO_NOINSTALL_TARGETS = livestream-generator
GO_INSTALL_TARGETS = logjam-livestream logjam-http-forwarder logjam-prometheus-exporter
GO_TARGETS = $(GO_INSTALL_TARGETS) $(GO_NOINSTALL_TARGETS)
GO_TESTS = http-client-test prometheus-exporter-test

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

logjam-livestream: $(GO_SRC)/livestream-server/server.go .godeps
	cd $(GO_SRC) && $(GO_ENV) go build -i -o ../$@ livestream-server/server.go

livestream-generator: $(GO_SRC)/livestream-generator/generator.go .godeps
	cd $(GO_SRC) && $(GO_ENV) go build -i -o ../$@ livestream-generator/generator.go

logjam-http-forwarder: $(GO_SRC)/http-forwarder/forwarder.go .godeps
	cd $(GO_SRC) && $(GO_ENV) go build -i -o ../$@ http-forwarder/forwarder.go

logjam-prometheus-exporter: $(GO_SRC)/prometheus-exporter/exporter.go .godeps
	cd $(GO_SRC) && $(GO_ENV) go build -i -o ../$@ prometheus-exporter/exporter.go

.PHONY: http-client-test prometheus-exporter-test

http-client-test:
	cd $(GO_SRC) && $(GO_ENV) go test ./$(GO_SRC)/http-client

prometheus-exporter-test:
	$(GO_ENV) go test ./$(GO_SRC)/prometheus-exporter

check-local: livestream-test

livestream-test: logjam-livestream
	$(GO_ENV) go test ./$(GO_SRC)/livestream-server
