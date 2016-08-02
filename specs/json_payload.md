# Logjam Message Content

This document describes how the JSON payload in logjam messages needs
to be stuctured so that the logjam app can extract and display useful
information.

#### Version: 1
#### Status: DRAFT
#### Editor: Stefan Kaes

### Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in
[RFC 2119](https://tools.ietf.org/html/rfc2119).

The JSON body MUST be a hash. The contents of the hash depends on the
message topic, which can take on the following values (see
[producer_protocol](producer_protocol.md)).

```
topic = logs *( ALPHA / "." )            ; normal log messages
topic /= javascript *( ALPHA / "." )     ; javascript errors
topic /= events *( ALPHA / "." )         ; logjam event
topic /= frontend.page                   ; frontend metric (page render)
topic /= frontend.ajax                   ; frontend metric (ajax call)
```

### Topic logs...

This topic is used for performance data, log lines and various
aggregated information about an action which has been performed by the
given aplication in the given environment.

The following fields are mandatory:

| Field          | Format | Description |
|:---------------|:-------|:------------|
| action         | String    | identifies the action performed (usually a controller action) |
| started\_at    | String    | time when the request was started (see example) |
| started\_ms    | Integer   | request start time in milliseconds since the epoch (1.1.1970 00:00:00 UTC) |
| total\_time    | Float     | wall time used to perform the request (ms) |
| code           | Integer   | usually a http response code (but could be anything) |
| severity       | 0..5      | highest log level used (DEBUG,INFO,WARN,ERROR,FATAL,ANY); will be extracted from lines if not set, or 1 (INFO) by default |
| caller\_id     | String    | value of http request header X-Logjam-Caller-Id (if present) |
| caller\_action | String | value of http request header X-Logjam-Action (if present) |
| request_id     | String  |	version 1 UUID (32 characters, without hyphens) |

Note: the logjam interface benefits from using http response codes
even for non http requests.

All other fields are optional. Their meaning depends on the logjam
application handling the request message.  The lines field, if
present, is a JSON array representation of the log lines produced
during request handling. Each element is a triple of (log severity,
timestamp, logged info). Here's an example message:

```javascript
{
  action: "Logjam::LogjamController#index",
  request_id: "03a31d7b520a4b4d8c078bb5df1eef0d",
  caller_id: "",
  caller_action: "",
  host: "lalila.lilu.de",
  process_id: 15379,
  lines: [
    [
      1,
      "2016-08-01T03:33:03.537112",
      "Started GET "/" for 62.75.163.XXX at 2016-08-01 03:33:02 +0200"
    ],
    [
      1,
      "2016-08-01T03:33:03.541352",
      "Processing by Logjam::LogjamController#index as */*"
      ],
    [
      1,
      "2016-08-01T03:33:03.541459",
      "Parameters: {"page"=>"::"}"
    ],
    [
      1,
      "2016-08-01T03:33:03.560924",
      "Redirected to http://demo.logjam.io/2016/08/01?page="
    ],
    [
      1,
      "2016-08-01T03:33:03.561180",
      "Filter chain halted as :redirect_to_clean_url rendered or redirected"
    ],
    [
      1,
      "2016-08-01T03:33:03.562099",
      "Completed 302 Found in 1412.6ms (Mongo: 2.386ms(1) | GC: 0.000(0) | HP: 0(499800,5096,2556312,131446))"
    ]
  ],
  started_at: "2016-08-01T03:33:02+02:00",
  started_ms: 1470015182149,
  ip: "62.75.163.XXX",
  request_info: {
    method: "GET",
    url: "/",
    headers: {
      User-Agent: "curl/7.35.0",
      Host: "demo.logjam.io",
      Accept: "*/*",
      X-Starttime: "t=1470015182149451"
    }
  },
  code: 302,
  severity: 1,
  db_time: 2.385859,
  other_time: 23.6725409999999,
  total_time: 1412.57119962024,
  wait_time: 1386.51279962024,
  db_calls: 1,
  allocated_bytes: 2569592,
  allocated_memory: 2780792,
  allocated_objects: 5280,
  heap_size: 499800,
  live_data_set_size: 131446
}
```

### Topic javascript...

This topic is used for javascript error information. TODO: add spec.

### Topic events...

This topic is used for logjam events. TODO: add spec.

### Topic frontend.page

Used to send performance metrics of page render events. TODO: add
spec.

### Topic frontend.ajax

Used to send performance metrics of ajax calls performed in the
browser. TODO: add spec.
