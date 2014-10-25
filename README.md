# Logjam Tools

A collection of programs and daemons build the server side
infrastructure for logjam (see https://github.com/skaes/logjam_app).

Currently only two daemons are provided:

## logjam-device

A daemon which offers a zeromq PULL socket endpoint for applications
to connect to and a zeromq SUB socket for forwarding. It optionally
subscribes to a RabbitMQ server to collect application messages from
there and republishes them on the PUB socket. You can place as many of
those devices as needed to to scale the logging infrastructure.

## logjam-importer

A daemon which is schduled to replace all of the ruy importer code
still in logjam. It's much less resource intensive than the ruby code
and a _lot_ faster, while still very stable: it has been in production
use since April 2014 and hasn't crashed once.

## Speed

On my iMac, one logjam-device can forward 20K messages per second (4K
message size). This is about 20 times faster than my original attempt
using ruby. The importer currently handles up to 30K app messages per
second on moderate hardware and scales with the number of cores.

![CPU Usage Graph](doc/logjam-importer-cpu-usage.png)

See also: (; http://www.youtube.com/watch?v=1S1fISh-pag ;)


## Dependencies

* librabbitmq (0.5.2)
* libzmq (4.0.5)
* libczmq (3.0.0rc1)
* libmongoc (git master, 1.0.2 has a memory leak)
* libbson (included in libmongoc as a submodule)
* json-c (0.11)

# Installation

* Download and install rabbitmq-c from https://github.com/alanxz/rabbitmq-c/releases/tag/v0.5.2
* Download and install zmq 4.0.5 from http://zeromq.org/intro:get-the-software
* Dowmload and install czmq 3.0.0rc1 from http://czmq.zeromq.org/page:get-the-software
* Download and install json-c version 0.11 from https://s3.amazonaws.com/json-c_releases/releases/index.html
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


## License

GPL v3. See LICENSE.txt.
