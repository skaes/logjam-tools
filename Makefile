.PHONY: clean install uninstall

logjam-device: logjam-device.o
	gcc -o logjam-device -L/usr/local/lib -L/opt/local/lib -O3 -ggdb -lrabbitmq -lzmq -lczmq logjam-device.o

logjam-device.o: logjam-device.c
	gcc -c logjam-device.c -I/opt/local/include -I/usr/local/include

clean:
	rm logjam-device $(wildcard *.o)

install:
	cp logjam-device /usr/local/bin

uninstall:
	rm -f /usr/local/bin/logjam-device


