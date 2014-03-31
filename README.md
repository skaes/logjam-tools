# A ZeroMQ device for logjam

This might replace rabbitmq usage for logjam at some point.

For now, it's just a bridge to rabbitmq.

## Speed

On my iMac, one device can forward 20K messages per second (4K message size).

Which is about 20 times faster than my original attempt using ruby.

See also: (; http://www.youtube.com/watch?v=1S1fISh-pag ;)


## Dependencies

* librabbitmq
* libzmq
* libczmq
* libbson
* libmongoc
* json-c

# Installation

On OS X:

* Install zmq, czmq and rabbitmq-c ports (or use brew, if you like it better)

```
sudo port install zmq, czmq, rabbitmq-c
```

* Download and install json-c version 0.11 from https://s3.amazonaws.com/json-c_releases/releases/index.html
* Clone https://github.com/mongodb/libbson and follow build/install instructions
* Clone https://github.com/mongodb/mongo-c-driver and follow build/install instrictions

Then

```
git clone git://github.com/skaes/logjam-tools.git
cd logjam-tools
sh autogen.sh
make
sudo make install
```

The generated `./configure` script will try to use `pkg-config` to find the
required libraries. If `pkg-config` is not installed, it assumes the
headers and libraries are installed under `/usr/local` or
`/opt/local`. If they're somewhere else, you can specify
`--with-opt-dir=dir1:dir2:dir3` as argument to `sh autogen.sh` (or
`./configure`).

## Usage

For now, the device needs to be running on the same machine as rabbitmq

```logjam-device <device-port> <rabbitmq-port>```

## Testing on OS X

First, build the test programs:

```
make check
```

In order to run the tests on OS X you will need to increase two kernel paramters: number
of total files per system and number of open files per process. The easiest way to do this
is to create the file /etc/sysctl.conf and add the following lines and reboot.

```
kern.maxfiles=40960
kern.maxfilesperproc=30000
```

Alternativly, you can also set the parameters from a root shell:

```
sysctl -w kern.maxfiles=40960
sysctl -w kern.maxfilesperproc=30000
```

In addition, you must set file limits in the shell before starting the tester and logjam-device

```ulimit -n 30000```

Or run the tasks

```
make run
make test
```

in two separate shells.

## License

GPL v3. See LICENSE.txt.
