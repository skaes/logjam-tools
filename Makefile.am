AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

AM_CPPFLAGS = $(OPTDIR_CPPFLAGS) $(DEPS_CFLAGS) -D_GNU_SOURCE
AM_CFLAGS = --std=gnu99 -Wall -fno-fast-math
LDADD = $(OPTDIR_LDFLAGS) $(DEPS_LIBS) -lm

bin_PROGRAMS = logjam-device logjam-importer
noinst_PROGRAMS = tester test_subscriber test_publisher test_puller

logjam_device_SOURCES = src/logjam-device.c src/rabbitmq-listener.h src/rabbitmq-listener.c
logjam_importer_SOURCES = src/logjam-importer.c src/importer-processor.c src/importer-processor.h src/importer-common.h src/importer-common.c src/importer-parser.h src/importer-parser.c src/importer-livestream.h  src/importer-livestream.c src/importer-increments.c src/importer-increments.h src/importer-subscriber.c src/importer-subscriber.h src/importer-statsupdater.h src/importer-statsupdater.c src/importer-requestwriter.c src/importer-requestwriter.h src/importer-indexer.c src/importer-indexer.h src/importer-controller.c src/importer-controller.h src/importer-streaminfo.c src/importer-streaminfo.h src/importer-resources.c src/importer-resources.h src/importer-mongoutils.c src/importer-mongoutils.h src/importer-tracker.c src/importer-tracker.h
tester_SOURCES = src/tester.c
test_publisher_SOURCES = src/test_publisher.c
test_subscriber_SOURCES = src/test_subscriber.c
test_puller_SOURCES = src/test_puller.c

dist_noinst_SCRIPTS = autogen.sh

#local rules
#TEST_PUBLISHERS=1 2 3 4 5
TEST_PUBLISHERS=1
ULIMIT=20000

.PHONY: test run cov-build analyze

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
