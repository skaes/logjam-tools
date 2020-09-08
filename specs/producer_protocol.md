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

```abnf
stream = *(request-reply / ping-pong / async-data)

request-reply = C: request-msg S: reply-msg
ping-pong     = C: ping-msg S: pong-msg
async-data    = C: data-msg

request-msg   = empty-frame data-msg
empty-frame   = %s""

reply-msg     = accepted / bad-request
accepted      = %s"202 Accepted"
bad-request   = %s"400 Bad Request"

ping-msg      = empty-frame %s"ping" app-env json-body meta-info
pong-msg      = app-env %s"200 OK" fqdn
fqdn          = *( ALPHA / "." )

app-env      = application "-" environment
application  = ALPHA *(ALPHA / "_" / "-")
environment  = ALPHA *(ALPHA / "_")
```

The client signals its desire to receive a response for a given
message by prepending an empty message frame to a data message.

The server accepts messages and either processes the payload
(logjam-importer) or publishes the message on a PUB socket (with a new
sequence number). For this reason, the app-env field is the first one
in the four frame sequence so that the incoming data can be forwarded
without reordering. The order of the remaining fields has historical
reasons.

The ping message can be sent by the client to verify that it has a
proper connection to the server. The server answers directly with a
pong message. This dialog can be used by clients during start up, shut
down or any time in between and is especially useful if the ZeroMQ
client implementation does not support setting a linger period on
ZeroMQ sockets.


### Data frames

A data message consists of four ZeroMQ message frames: one frame
carrying information about which program produced the message and in
which environment, one frame describing the topic of the message, the
body frame containing a (possibly compressed) JSON payload, and
finally a frame containing meta information, such as protocol version,
compression method used for the JSON body, when the messages was
produced and a message sequence number.

```abnf
data-msg = app-env topic json-body meta-info

topic = logs *( "." ALPHA *ALPHA )         ; normal log messages
topic /= javascript *( "." ALPHA *ALPHA )  ; javascript errors
topic /= events *( "." ALPHA *ALPHA )      ; logjam event
topic /= frontend.page                     ; frontend metric (page render)
topic /= frontend.ajax                     ; frontend metric (ajax call)
topic /= mobile                            ; mobile metric

json-body = *OCTET                             ; JSON string, possibly compressed

meta-info = tag compression-method version device-number created-ms sequence-number

tag = %xCA %xBD                                ; used internally to detect programming errors

compression-method = no-compression / zlib-compression / snappy-compression / lz4-compression
no-compression     = %d0
zlib-compression   = %d1
snappy-compression = %d2
lz4-compression    = %d3

version            = %d1

device-number      = 4(OCTET)              ; uint32, network byte order
created-ms         = 8(OCTET)              ; uint64, network byte order
sequence-number    = 8(OCTET)              ; uint64, network byte order
```

## Constraints

* The client MUST use either DEALER or a PUSH socket. If a PUSH socket
  is used, the message stream is restricted to messages described by
  the async-data-msg rule above.

* The server MUST offer a ROUTER socket for clients to connect to.

* The server MAY offer a PULL socket for clients to connect to.

* When the client sends a request-msg, the server MUST respond as soon
  as it starts processing the message.

* The server SHOULD send a bad-request response when it a receives a
  malformed request: e.g. when message frames are missing, when the
  JSON body isn't parseable or when the meta info cannot be properly
  decoded.

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
