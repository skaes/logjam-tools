# A ZeroMQ device for logjam

This might replace rabbitmq usage for logjam at some point.

For now, it's just a bridge to rabbitmq.

## Speed

On my iMac, one device can forward 20K messages per second (4K message size).


## Dependencies

* librabbitmq
* libzmq
* libczmq

# Installation

On OS X using MacPorts:

* Follow instructions https://github.com/alanxz/rabbitmq-c on how to install librabbitmq
* Install zmq and czmq ports (or use brew, if you like it better)

```
git clone git://github.com/skaes/logjam-device.git
cd logjam-zmq-device
make
sudo make install
```

## Usage

For now, the device needs to be running on the same machine as rabbitmq

```logjam-device <device-port> <rabbitmq-port>```


