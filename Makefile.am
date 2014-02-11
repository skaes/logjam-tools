AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

AM_CPPFLAGS = $(OPTDIR_CPPFLAGS) $(DEPS_CFLAGS)
AM_CFLAGS = -Wall
LDADD = $(OPTDIR_LDFLAGS) $(DEPS_LIBS)

bin_PROGRAMS = logjam-device logjam-proxy
check_PROGRAMS = tester test_subscriber test_publisher

logjam_device_SOURCES = src/logjam-device.c
logjam_proxy_SOURCES = src/logjam-proxy.c
tester_SOURCES = src/tester.c
test_publisher_SOURCES = src/test_publisher.c
test_subscriber_SOURCES = src/test_subscriber.c

dist_noinst_SCRIPTS = autogen.sh
