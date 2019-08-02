# Logjam Consumer Protocol

The logjam consumer protocol describes the message flow between a
logjam-device and a consumer of its data (e.g. a logjam-importer).

#### Version: 1
#### Status: DRAFT
#### Editor: Stefan Kaes

## Terminology

Any program wishing to consume logjam data offered by a logjam-device
MUST open a SUB socket and connect it to the PUB socket offered by the
logjam-device and follow the rules outlined below. We'll call such a
program a consumer and the logjam-device the producer.

### Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in
[RFC 2119](https://tools.ietf.org/html/rfc2119).

## Message stream

The stream of messages exchanged between producer and consumer is
described by the pseudo ABNF below. The string "S:" is a meta
information intended to specify that the following output is produced
by the producer and "C:" is used to mark consumer output. ZeroMQ frame
delimiters are left out in order to simplify the presentation.

```
consumer-stream = *(S: data-msg)
```

### Data messages

A data message consists of four ZeroMQ message frames: one frame
carrying information about which program produced the message and in
which environment, one frame describing the topic of the message, the
body frame containing a (possibly compressed) JSON payload, and
finally a frame containing meta information, such as protocol version,
compression method used for the JSON body, when the messages was
produced and a message sequence number.

```
data-msg  = app-env topic json-body meta-info

app-env      = application "-" environment
application  = ALPHA *(ALPHA / "_" / "-")
environment  = ALPHA *(ALPHA / "_")

topic = logs *( ALPHA / "." )            ; normal log messages
topic /= javascript *( ALPHA / "." )     ; javascript errors
topic /= events *( ALPHA / "." )         ; logjam event
topic /= frontend.page                   ; frontend metric (page render)
topic /= frontend.ajax                   ; frontend metric (ajax call)

body = *OCTET                            ; JSON string, possibly compressed

meta-info = tag compression-method version device-number created-ms sequence-number

tag = %xCABD                             ; tag is used internally to detect programming errors

compression-method = no-compression / zlib-compression / snappy-compression / lz4-compression
no-compression     = %x0
zlib-compression   = %x1
snappy-compression = %x2
lz4-compression    = %x3

version            = %x1

device-number      = 2OCTET              ; uint16, network byte order
created-ms         = 8OCTET              ; uint64, network byte order
sequence-number    = 8OCTET              ; uint64, network byte order
```

Note: as of version 1, the format is identical to the format used in
the producer protocol.


## Constraints

* The consumer MUST use a SUB socket.

* The consumer MAY subscribe to a subset of the data stream offered by
  the producer.

* The server MUST use a PUB socket.

* The producer MUST number messages offered on the PUB
  socket. Numbering MUST start with 1 and start again at 1 when the
  number space for 64bit unsigned integers in the client's
  implementation language has been exhausted (this might mean
  switching before 2**64-1 has been reached).

* The field device-number SHOULD be either 0, or a small integer,
  which MUST be unique across all logjam producers.

* The producer SHOULD use the original created-ms of any message it
  forwards on behalf of a client, if the original value is
  non-zero. For messages which don't have a non-zero created-ms value,
  the producer MUST set the field created-ms to a value near the
  system time when sending the message. The expected format is
  milliseconds since the epoch. The producer SHOULD use real
  millisecond resolution, but it is acceptable for the producer to use
  a timer with second resolution to calculate this value.

## JSON payload requirements

See companion document [json_payload](json_payload.md)
