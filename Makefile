.PHONY: clean install uninstall

INCLUDE_PATHS=-I/opt/local/include -I/usr/local/include
LIB_PATHS=-L/usr/local/lib -L/opt/local/lib
OPTS=-O3 -ggdb
ZLIBS=-lzmq -lczmq

all: logjam-device tester

logjam-device: logjam-device.o
	gcc -o logjam-device $(LIB_PATHS) $(ZLIBS) -lrabbitmq logjam-device.o

logjam-device.o: logjam-device.c
	gcc -c logjam-device.c $(INCLUDE_PATHS) $(OPTS)

tester: tester.c
	gcc -o tester $(INCLUDE_PATHS) $(LIB_PATHS) $(OPTS) $(ZLIBS) tester.c

clean:
	rm -f logjam-device tester $(wildcard *.o)

install:
	cp logjam-device /usr/local/bin

uninstall:
	rm -f /usr/local/bin/logjam-device


