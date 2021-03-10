AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

AM_CPPFLAGS = -I$(top_srcdir) $(OPTDIR_CPPFLAGS) $(DEPS_CFLAGS) -D_GNU_SOURCE
AM_CFLAGS = --std=gnu99 -Wall -fno-fast-math
AM_CXXFLAGS = --std=c++11 -Wall -fno-fast-math
AM_LDFLAGS = $(OPT_PTHREAD_FLAGS)
LDADD = $(OPTDIR_LDFLAGS) $(DEPS_LIBS) $(OPT_PROFILER_FLAGS) -lm
PROMETHEUS_LIBS = $(libdir)/libprometheus-cpp-pull.a $(libdir)/libprometheus-cpp-push.a $(libdir)/libprometheus-cpp-core.a

bin_PROGRAMS = \
logjam-device \
    logjam-importer \
    logjam-graylog-forwarder \
    logjam-dump \
    logjam-debug \
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
    ../config.h \
    logjam-device.c \
    logjam-util.c \
    logjam-util.h \
    message-compressor.c \
    message-compressor.h \
    importer-watchdog.c \
    importer-watchdog.h \
    device-prometheus-client.cpp \
    device-prometheus-client.h

logjam_device_LDADD = $(PROMETHEUS_LIBS) $(LDADD)

logjam_importer_SOURCES = \
    ../config.h \
    importer-adder.c \
    importer-adder.h \
    importer-common.c \
    importer-common.h \
    importer-controller.c \
    importer-controller.h \
    importer-increments.c \
    importer-increments.h \
    importer-indexer.c \
    importer-indexer.h \
    importer-livestream.c \
    importer-livestream.h  \
    importer-mongoutils.c \
    importer-mongoutils.h \
    importer-parser.c \
    importer-parser.h \
    importer-processor.c \
    importer-processor.h \
    importer-requestwriter.c \
    importer-requestwriter.h \
    importer-resources.c \
    importer-resources.h \
    importer-statsupdater.c \
    importer-statsupdater.h \
    logjam-streaminfo.c \
    logjam-streaminfo.h \
    logjam-streaminfo-types.h \
    importer-subscriber.c \
    importer-subscriber.h \
    importer-tracker.c \
    importer-tracker.h \
    importer-watchdog.c \
    importer-watchdog.h \
    logjam-importer.c \
    logjam-util.c \
    logjam-util.h \
    zring.c \
    zring.h \
    device-tracker.c \
    device-tracker.h \
    importer-prometheus-client.cpp \
    importer-prometheus-client.h \
    unknown-streams-collector.c \
    unknown-streams-collector.h

logjam_importer_LDADD = $(PROMETHEUS_LIBS) $(LDADD)

logjam_graylog_forwarder_SOURCES = \
    ../config.h \
    logjam-graylog-forwarder.c \
    graylog-forwarder-common.c \
    graylog-forwarder-common.h \
    graylog-forwarder-controller.c \
    graylog-forwarder-controller.h \
    graylog-forwarder-parser.c \
    graylog-forwarder-parser.h \
    graylog-forwarder-subscriber.c \
    graylog-forwarder-subscriber.h \
    graylog-forwarder-writer.c \
    graylog-forwarder-writer.h \
    logjam-util.c \
    logjam-util.h \
    logjam-streaminfo.c \
    logjam-streaminfo-types.h \
    logjam-streaminfo.h \
    gelf-message.c \
    gelf-message.h \
    logjam-message.c \
    logjam-message.h \
    str-builder.c \
    str-builder.h \
    device-tracker.c \
    device-tracker.h \
    importer-watchdog.c \
    importer-watchdog.h\
    graylog-forwarder-prometheus-client.cpp \
    graylog-forwarder-prometheus-client.h

logjam_graylog_forwarder_LDADD = $(PROMETHEUS_LIBS) $(LDADD)

logjam_dump_SOURCES = \
    ../config.h \
    logjam-dump.c \
    logjam-util.c \
    logjam-util.h \
    device-tracker.c \
    device-tracker.h \
    importer-watchdog.c \
    importer-watchdog.h

logjam_debug_SOURCES = \
    ../config.h \
    logjam-debug.c \
    logjam-util.c \
    logjam-util.h

logjam_replay_SOURCES = \
    ../config.h \
    logjam-replay.c \
    logjam-util.c \
    logjam-util.h

logjam_pubsub_bridge_SOURCES = \
    ../config.h \
    logjam-pubsub-bridge.c \
    logjam-util.c \
    logjam-util.h \
    message-compressor.c \
    message-compressor.h \
    device-tracker.c \
    device-tracker.h \
    importer-watchdog.c \
    importer-watchdog.h

logjam_forwarder_SOURCES = \
    ../config.h \
    logjam-forwarder.c \
    logjam-util.c \
    logjam-util.h \
    device-tracker.c \
    device-tracker.h \
    importer-watchdog.c \
    importer-watchdog.h

logjam_logger_SOURCES = \
    ../config.h \
    logjam-logger.c \
    logjam-util.c \
    logjam-util.h

logjam_tail_SOURCES = \
    ../config.h \
    logjam-tail.c \
    logjam-util.c \
    logjam-util.h

tester_SOURCES = tester.c

test_publisher_SOURCES = test_publisher.c

test_subscriber_SOURCES = test_subscriber.c

test_puller_SOURCES = test_puller.c

dist_noinst_SCRIPTS = autogen.sh

checker_SOURCES = \
    checker.c \
    zring.c \
    zring.h \
    logjam-util.c \
    logjam-util.h


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
