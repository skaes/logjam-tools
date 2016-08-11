# Logjam Producer Protocol

The logjam producer protocol describes the interaction between
multiple clients and a single server process.

#### Version: 1
#### Status: DRAFT
#### Editor: Stefan Kaes

## Terminology

Any program wishing to send logjam data MUST open either a DEALER or a
PUSH socket and connect to either a logjam-device or a logjam-importer
and follow the rules outlined below. We'll call such a program a
CLIENT and the other endpoint the SERVER.

### Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in
[RFC 2119](https://tools.ietf.org/html/rfc2119).



## Message stream

The stream of messages exchanged between client and server is
described by the pseudo ABNF below. The string "C:" is a meta
information intended to specify that the following output is produced
by the client and "S:" is used to mark server output. ZeroMQ frame
delimiters are left out in order to simplify the presentation.

```
stream = *(request-reply / async-data-msg)

request-reply  = C: request-msg S: reply-msg
async-data-msg = C: data-msg

request-msg = empty-message-frame data-msg
empty-message-frame = %s""

reply-msg    = accepted / bad-request
accepted     = %s"202 Accepted"
bad-request  = %s"400 Bad Request"
```

The client signals its desire to receive a response for a given
message by prepending an empty message frame to a data message.

The server accepts messages and either processes the payload
(logjam-importer) or publishes the message on a PUB socket (with a new
sequence number). For this reason, the app-env field is the first one
in the four frame sequence so that the incoming data can be forwarded
without reordering. The order of the remaining fields has historical
reasons.

### Data frames

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
application  = ALPHA *ALPHA
environment  = ALPHA *ALPHA

topic = logs *( ALPHA / "." )            ; normal log messages
topic /= javascript *( ALPHA / "." )     ; javascript errors
topic /= events *( ALPHA / "." )         ; logjam event
topic /= frontend.page                   ; frontend metric (page render)
topic /= frontend.ajax                   ; frontend metric (ajax call)

json-body = *OCTET                       ; JSON string, possibly compressed

meta-info = tag compression-method version device-number created-ms sequence-number

tag = %xCABD                             ; tag is used internally to detect programming errors

compression-method = no-compression / zlib-compression / snappy-compression
no-compression     = %x0
zlib-compression   = %x1
snappy-compression = %x2

version            = %x1

device-number      = 2OCTET              ; uint16, network byte order
created-ms         = 8OCTET              ; uint64, network byte order
sequence-number    = 8OCTET              ; uint64, network byte order
```

## Constraints

* The client MUST use either PUSH or a DEALER socket. If a push socket
  is used, the message stream is restricted to messages described by
  the async-data-msg rule above.

* The server MUST use offer a ROUTER socket for clients to connect to.

* The server MAY offer a PULL socket for clients to connect to.

* When the client sends a request-msg, the server MUST respond as soon
  as it starts processing the message.

* The server SHOULD send a bad-request response when it a receives a
  request when it detects an invalid or missing meta frame.

* The client SHOULD number messages starting with 1 and start again at 1
  when the number space for 64bit unsigned integers in the client's
  implementation language has been exhausted (this might mean switching
  before 2**64-1 has been reached).

* The field device-number is used by logjam internally, and MUST be
  set to 0, unless the message is sent from a logjam-device process.

* The client MUST set the field created-ms to a value near the
  client's system time when sending the message. The expected format
  is milliseconds since the epoch. The client SHOULD use real
  millisecond resolution, but it is acceptable for the client to use a
  timer with second resolution to calculate this value.

## JSON payload requirements

See companion document [json_payload](json_payload.md)
