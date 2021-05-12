# Additional Logjam inputs

Logjam also offers additional input options apart from the main logjam-device
for logging input.

Most of these are part of the (optional) *logjam-fhttpd* component.

## Frontend metrics

*logjam-fhttpd* provides the `/logjam/ajax` and `/logjam/page` endpoints for general frontend metrics requests.

* `/logjam/page` should be called (from the browser) after the page load is completed
* `/logjam/ajax` should be called after the completion of an AJAX call (e.g. to
  obtain additional information that should be displayed after some interaction
  on the page)

### HTTP Payload

As this endpoint is intended to receive metrics from within a browser it is likely
subject to [CORS] (Cross-Origin Resource Sharing).
Generally browsers limit the connection to other domains to certain types of request
based on whether the target server allows the connection.
This requires the server to maintain a list of *allowed origins*.
To keep the configuration footprint small the endpoint is providing
a safe *simple request* format.

For a request to be considered *simple* it needs to be limited to a small
selection of supported HTTP Verbs, HTTP request headers as well as a limited
selection of allowed content types.

This endpoint is intended to be used by adding the URL as an *image* into the
DOM of the HTML page. This will subsequently cause the browser to perform an
HTTP `GET` request. The payload is appended to the URL in the form of a *query
string* that contains form data.

Format of the payload:

| Field              | type/unit | mandatory/optional     |
|--------------------|-----------|------------------------|
| &v                 | int       | mandatory, must be `1` |
| &logjam_request_id | string    | mandatory              |
| &logjam_action     | string    | mandatory              |
| &viewport_height   | int       | optional               |
| &viewport_width    | int       | optional               |
| &html_nodes        | int       | optional               |
| &script_nodes      | int       | optional               |
| &style_nodes       | int       | optional               |

If the HTTP request was successfully processed it returns a valid HTTP response
(`200` status code) for a `image/gif` 1x1 pixel size image. In the case of an
issue with processing it returns a "Bad Request" HTTP response (`400` status
code) with a description of the error in the response body.


### Published Logjam message

| Routing field            | value                |
|--------------------------|----------------------|
| Routing key prefix       | `"frontend"`         |
| Routing key message type | `"ajax"` or `"page"` |

The data section of the message is JSON in the following format:

```json
{
  "v": 1,
  "logjam_request_id": "",
  "logjam_action": "",
  "viewport_height": 0,
  "viewport_width": 0,
  "html_nodes": 0,
  "script_nodes": 0,
  "style_nodes": 0,
  "user_agent": ""
  "started_ms": 0,
  "started_at": "0000-00-00T00:00:00+00:00",
}
```

The payload can contain additional fields as other form parameters in the query string
get automatically added to the payload as well.

#### Known processors

##### logjam-prometheus-exporter

Currently *logjam-prometheus-exporter* is processing these Logjam messages to provide
Prometheus metrics.

It supports the following incoming metrics and translates them into Prometheus metrics:

#### logjam-importer

## Mobile/native metrics

*logjam-fhttpd* provides the `/logjam/mobile` endpoint for metrics from mobile
clients (specifically native clients, e.g. Android or iOS).

### HTTP Payload

A valid request must be a HTTP request with an `application/json` content type.
No Logjam specific headers are required.

Currently defined fields:

```json
{
  "meta": {
    "os": "",
    "device": "",
    "version": "",
    "internal_build": false
  },
  "histograms": [
    {
      "name": "",
      "begin": "",
      "end": "",
      "buckets": [
        {
          "start_value": 0,
          "end_value": 0,
          "count": 0
        }
      ]
    }
  ],
  "gauges": [
    {
      "name": "",
      "metrics": [
        {
          "value": 0,
          "timestamp": ""
        }
      ]
    }
  ]
}
```


### Published Logjam message

| Routing field | value      |
|---------------|------------|
| Routing key   | `"mobile"` |

*Currently fhttpd forces a synthetic appenv for the mobile message: `mobile-production`*

The data section of the message is the body of the HTTP request as documented above.

#### Known processors

Currently *logjam-prometheus-exporter* is processing these Logjam messages to provide
Prometheus metrics.

It supports the following incoming metrics and translates them into Prometheus metrics:

| metric name in logjam message   | metric type | prometheus metric name                        |
|---------------------------------|-------------|-----------------------------------------------|
| `application_time_firstdraw_ms` | histogram   | `logjam:mobile:application_time_firstdraw_ms` |
| `application_resume_time_ms`    | histogram   | `logjam:mobile:application_resume_time_ms`    |
| `application_hang_time_ms`      | histogram   | `logjam:mobile:application_hang_time_ms`      |


The prometheus metrics have the `version` and `internalBuild` labels.

## WebVitals

*logjam-fhttpd* provides the `/logjam/webvitals` endpoint for metrics from browsers about [WebVitals].

[WebVitals]: https://web.dev/vitals/

### HTTP Payload

As this endpoint is intended to receive metrics from within a browser it is likely
subject to [CORS] (Cross-Origin Resource Sharing).
Generally browsers limit the connection to other domains to certain types of request
based on whether the target server allows the connection.
This requires the server to maintain a list of *allowed origins*.
To keep the configuration footprint small the endpoint is providing
a safe *simple request* format.

For a request to be considered *simple* it needs to be limited to a small
selection of supported HTTP Verbs, HTTP request headers as well as a limited
selection of allowed content types.

A request should be of the following format:

* A HTTP `POST` request
* The Content-Type must be `application/x-www-form-urlencoded` or `multipart/formdata`
* The payload should either be a *query string* (in form encoded format) or a
  form encoded text in the body of the request.
  
Format of the payload:

| Field              | type/unit               | mandatory/optional          |
|--------------------|-------------------------|-----------------------------|
| &logjam_request_id | string                  | mandatory                   |
| &logjam_action     | string                  | mandatory                   |
| &metrics[].id      | string                  | mandatory (within a metric) |
| &metrics[].fid     | float (in milliseconds) | optional (within a metric)  |
| &metrics[].lcp     | float (in milliseconds) | mandatory (within a metric) |
| &metrics[].cls     | float (score 0.0-1.0)   | mandatory (within a metric) |

#### Example (single metric)

Linebreaks are only added for readability.

```
logjam_action=someAction%23call
&logjam_request_id=some-app-preview-4ca101ebe46e4bdaaebe95df9cc7fe83
&metrics%5B%5D.id=241f3328-d781-4c77-8470-be48739ddfc3
&metrics%5B%5D.cls=0
&metrics%5B%5D.fid=891.2476906688746
&metrics%5B%5D.lcp=0
```

### Example (multiple metrics)

```
logjam_action=someAction%23call
&logjam_request_id=some-app-preview-4ca101ebe46e4bdaaebe95df9cc7fe83
&metrics%5B0%5D.id=241f3328-d781-4c77-8470-be48739ddfc3
&metrics%5B0%5D.cls=0
&metrics%5B0%5D.fid=891.2476906688746
&metrics%5B0%5D.lcp=0
&metrics%5B1%5D.id=241f3328-d781-4c77-8470-be48739ddfc3
&metrics%5B1%5D.lcp=1202.24333333334
&metrics%5B1%5D.fid=0
&metrics%5B1%5D.cls=0
```

[CORS]: https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS

### Published Logjam message

| Routing field            | value         |
|--------------------------|---------------|
| Routing key prefix       | `"frontend"`  |
| Routing key message type | `"webvitals"` |

The data section of the message is JSON in the following format:

```json
{
  "started_ms": 0,
  "started_at": "0000-00-00T00:00+00:00",
  "logjam_request_id": "some-app-preview-dddeeefff",
  "logjam_action_id": "someAction#call",
  "metrics": [
    {
      "id": "webvitals-id-000dddeee111222333222",
      "lcp": 0.0
    },
    {
      "id": "webvitals-id-000dddeee111222333444",
      "fid": 0.0
    },
    {
      "id": "webvitals-id-000dddeee1112223335555",
      "cls": 0.0
    }
  ]
}
```
