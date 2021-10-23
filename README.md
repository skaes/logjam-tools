# Logjam Tools

A collection of programs and daemons to build the server side
infrastructure for logjam (see https://github.com/skaes/logjam_app).

<a href="https://scan.coverity.com/projects/3357">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/3357/badge.svg"/>
</a>

Currently the following programs are provided:

## logjam-device

A daemon which offers a ZeroMQ PULL socket endpoint for applications
to connect to and a ZeroMQ SUB socket for forwarding. It can compress
the log stream on the fly to reduce network traffic, although
compressing it at the producer is preferable. You can run as many of
those devices as needed to scale the logging infrastructure.

## logjam-importer

A multithreaded daemon using CZMQ's actor framework which has replaced
all of the ruby importer code in logjam. It's much less resource
intensive than the ruby code and a _lot_ faster.

## logjam-fhttpd

A daemon which accepts frontend performance data via HTTP GET requests
and publishes it on a ZeroMQ PUB socket for the importer to pick up.
Additional information about the available endpoints
can be found in [Additional Logjam inputs](doc/additional-inputs/additional-inputs.md).

## logjam-http-forwarder

A daemon which offers a HTTP endpoint where it accepts logjam event
messages via POST requests and publishes them on a ZeroMQ PUB socket
for the importer to pick up.

## logjam-graylog-forwarder

A daemon which subscribes to PUB sockets of logjam-devices and
forwards GELF messages to a Graylog GELF socket endpoint.

## logjam-dump

A utility program to capture messages published by a logjam device or a logjam importer
process and log them to disk or to `stdout` in text format (JSON).

## logjam-debug

A utility program to capture messages sent to a logjam importer device print them to
stdout in JSON format.

## logjam-replay

A utility program to replay messages captured by logjam-dump. Useful in
determining maximum system throughput. Can mimics a logjam-device or a logjam
agent.

## logjam-pubsub-bridge

A utility program which subscribes to a logjam-device PUB socket,
decompresses and forwards messages to a PUSH socket. Only to be used
when writing the decompression logic is to complex for a consumer.

## logjam-forwarder

A utility program which subscribes to a logjam-device PUB socket, and
forwards messages to a DEALER socket. Can be used to forward messages
from one logjam installation to another.

## logjam-logger

A utility program which reads lines from stdin and publishes them on
a PUB socket, optionally using a given topic.

## logjam-tail

A utility program which connects to a logjam-logger PUB socket and
displays lines matching an optional list of topics on stdout.

## logjam-mongodb-backup

A utility program which backs up the logjam database.

## logjam-mongodb-restore

A utility program which restores a logjam database from a backup
produced by `logjam-mongodb-backup`.

## logjam-rename-callers

A utility program which updates caller and sender references after renaming an
application.

## Default ports

This is the list of ports to which various programs bind, along with
the ZeroMQ socket types behind the ports.

### 9604

A ZeroMQ BROKER socket for which implements the server side of the
logjam producer protocol (logjam-device, logjam-importer). See
[producer_protocol](specs/producer_protocol.md).

### 9605

A ZeroMQ PUSH socket to which logjam messages can be sent
(logjam-device, logjam-importer).

### 9606

A ZeroMQ PUB socket on which logjam messages are published
(logjam-device). Messages sent over this socket follow the logjam
consumer protocol. See
[consumer_protocol](specs/consumer_protocol.md).

### 9607

A ZeroMQ PUB socket on which live stream messages are published
(logjam-importer).

### 9608 (8080)

A websocket which listens for connects from logjam livestream clients
running in browsers (livestream server, currently in ruby, see
https://github.com/skaes/logjam_core).

### 9609

A ZeroMQ PUSH socket for graylog to connect to
(logjam-graylog-forwarder).

### 9610

A Prometheus metrics endpoint, used by logjam-importer.

### 9612

A ZeroMQ PUB socket on which the logjam importer publishes messages it received from
devices with stream names which are not known to the importer. These messages can be
captured in JSON format using `logjam-dump -q -T -h host:9612` for debugging purposes.

### 9651

ZeroMQ PUB socket offered by the importer for debugging messages sent to its router port.


### 9705

HTTP listening port for logjam-fhttpd

### 9706

ZMQ pub port for logjam-fhttpd

### 9708

HTTPS listening port for nginx SSL proxy for logjam-fhttpd port

### 9805

HTTP listening port for logjam-http-forwarder

### 9806

ZMQ pub port for logjam-http-forwarder



# Dependencies

logjam-tools depends on a number of libraries, some of which are patched versions of
official packages. These dependencies are managed in a separate project:
[github.com/skaes/logjam-libs](https://github.com/skaes/logjam-libs). Please follow the
instructions of this projects' README.


# Installation

## Ubuntu packages

Ubuntu packages are available from
[railsexpress.de](https://railsexpress.de/packages/ubuntu). The Travis pipeline will build
packages for all supported OS versions and upload them to this server, if the version
specified in [VERSION.txt](./VERSION.txt) is not on the server. Consequently, this version
needs to be incremented in order to release a new version.

The are two types of packages available: `logjam-tools` will install
in `/opt/logjam/`, `logjam-tools-usr-local` in `/usr/local`. The tools
package uses very recent and sometimes patched libraries. Installing
in `/usr/local` might cause problems with other applications. In this
case, use `-opt-logjam` packages. However, you will need to set some
environment variables to make use of the libraries provided by logjam:
Set `ZMQ_LIB_PATH` to `/opt/logjam/lib` in order to use `libzmq`from
`ffi-rzmq`, add `/opt/logjam/lib/pkgconfig` to `PKG_CONFIG_PATH` and
`/opt/logjam/bin` to `PATH`.

The final step is then `apt-get install logjam-tools`.

Currently, 20.04 LTS, 18.04 LTS and 16.04 LTS are supported.


## From source

Install dependencies as described [here](https://github.com/skaes/logjam-libs). Remember
your choice for the `--prefix` argument. Let's assume you stuck to the default
`/usr/local`.

* Install go (https://golang.org/doc/install) and set up PATH for it


Clone the repository:
```
git clone git://github.com/skaes/logjam-tools.git
cd logjam-tools
```

Configure your build pipeline:
```
sh autogen.sh --prefix=/usr/local
make
sudo make install
```

The generated `./configure` script will try to use `pkg-config` to find the
required libraries. If `pkg-config` is not installed, it assumes the
headers and libraries are installed under `/opt/logjam`, `/usr/local` or
`/opt/local`. If they're somewhere else, you can specify
`--with-opt-dir=dir1:dir2:dir3` as argument to `sh autogen.sh` (or
`./configure`).

`autogen.sh` accepts the usual configure arguments, such as
`--prefix`. Thus, if you have installed the libraries under
`/opt/logjam`, and want to install the logjam tools in the same place,
run `sh autogen.sh --prefix=/opt/logjam`

If you want to get rid of the installed software, run
```
sudo make uninstall
```

# Profiling with gperftools

Install Google perftools on your machine (https://code.google.com/p/gperftools/).

Set environment variable CPUPROFILE to the name of the profile data
file you want to use. Reconfigure and recompile everything:

```
CPUPROFILE=logjam.prof sh autogen.sh
make clean
make
```

Then invoke the command you want to profile. For example:

```
CPUPROFILE=logjam.prof ./logjam-device -c logjam.conf
pprof --web ./logjam-device logjam.prof
```

On Ubuntu, you will likely need to add `LD_PRELOAD=<path to libprofile.so>`
to make this work.

# License

GPL v3. See LICENSE.txt.
