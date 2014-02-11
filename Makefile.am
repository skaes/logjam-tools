AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS}

LOGJAM_CPPFLAGS = -I/opt/local/include -I/usr/local/include
AM_CPPFLAGS = $(DEPS_CFLAGS) $(LOGJAM_CPPFLAGS)

LOGJAM_LIBS = -L/opt/local/lib -L/usr/local/lib -lzmq -lczmq -lrabbitmq
LDADD = $(DEPS_LIBS) $(LOGJAM_LIBS)

bin_PROGRAMS = logjam-device logjam-proxy
check_PROGRAMS = tester test_subscriber test_publisher

logjam_device_SOURCES = src/logjam-device.c
logjam_proxy_SOURCES = src/logjam-proxy.c
tester_SOURCES = src/tester.c
test_publisher_SOURCES = src/test_publisher.c
test_subscriber_SOURCES = src/test_subscriber.c

dist_noinst_SCRIPTS = autogen.sh
