.PHONY: all clean install uninstall test run

INCLUDE_PATHS=-I/opt/local/include -I/usr/local/include
LIB_PATHS=-L/usr/local/lib -L/opt/local/lib
OPTS=-O3 -ggdb
ZLIBS=-lzmq -lczmq

TEN=0 1 2 3 4 5 6 7 8
ULIMIT=20000

all: logjam-device tester

logjam-device: logjam-device.c
	gcc -o logjam-device $(INCLUDE_PATHS) $(OPTS) $(LIB_PATHS) $(ZLIBS) -lrabbitmq logjam-device.c

tester: tester.c
	gcc -o tester $(INCLUDE_PATHS) $(LIB_PATHS) $(OPTS) $(ZLIBS) tester.c

clean:
	rm -f logjam-device tester $(wildcard *.o)

install:
	cp logjam-device /usr/local/bin

uninstall:
	rm -f /usr/local/bin/logjam-device

test: tester
	for i in $(TEN); do (ulimit -n $(ULIMIT); ./tester 500 100000&); done

run: logjam-device
	ulimit -n $(ULIMIT); ./logjam-device 12345 5672
