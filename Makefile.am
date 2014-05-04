AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

AM_CPPFLAGS = $(OPTDIR_CPPFLAGS) $(DEPS_CFLAGS)
AM_CFLAGS = --std=gnu99 -Wall
LDADD = $(OPTDIR_LDFLAGS) $(DEPS_LIBS)

bin_PROGRAMS = logjam-device logjam-importer
noinst_PROGRAMS = tester test_subscriber test_publisher

logjam_device_SOURCES = src/logjam-device.c
logjam_importer_SOURCES = src/logjam-importer.c
tester_SOURCES = src/tester.c
test_publisher_SOURCES = src/test_publisher.c
test_subscriber_SOURCES = src/test_subscriber.c

dist_noinst_SCRIPTS = autogen.sh

#local rules
#TEST_PUBLISHERS=1 2 3 4 5
TEST_PUBLISHERS=1
ULIMIT=20000

test: tester
	for i in $(TEST_PUBLISHERS); do (ulimit -n $(ULIMIT); ./tester 200 100000&); done

run: logjam-device
	ulimit -n $(ULIMIT); ./logjam-device 12345 localhost
