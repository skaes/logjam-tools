Logjam Request GELF Mapping
===========================

Logjam Request Message:
<https://confluence.xing.hh/confluence/display/ARCH/Logjam+Integration>

GELF Specification:
<http://www.graylog2.org/resources/gelf/specification>

Ruby Logger to Syslog Level Mapping:
<https://github.com/Graylog2/gelf-rb/blob/bb1f4a95549f44981ff4a4caee0c43d75585e4ae/lib/gelf/severity.rb#L21-L27>

Mapping
-------

```
GELF field            Logjam field
----------            ------------

Standard GELF fields:

version               1.1 (fixed value)
host                  host
short_message         action
full_message          all entries from "lines" array as "severity-as-word timestamp logged-info", concatenated by "\n"
timestamp             started_at as seconds since UNIX epoch, with optional decimal places for milliseconds
level                 highest log level used in "lines" array, mapped to syslog level as described in "Ruby Logger to Syslog Level Mapping" link above

Additional fields:

_app                  app-env -- first frame in logjam-device ZeroMQ message
_total_time           total_time
_code                 code
_caller_id            caller_id
_caller_action        caller_action
_request_id           request_id
_user_id              user_id
_ip                   ip
_process_id           process_id
_http_method          request_info["method"]
_http_url             request_info["url"]
_http_header_*        all entries from request_info["headers"] as separate fields (normalize header name/key using lowercase letters and underscores)
```

Example
-------

```json
{
  "version": "1.1",
  "host": "jobs-3.api.fra1.xing.com",
  "short_message": "Rest::Jobs::PostingsController#show",
  "full_message": "INFO 2013-05-24T09:22:33.789958 Started GET \"/rest/jobs/postings/2196851?fields=created_at%2Cfunction%2Clinks&rid=internal\" for 10.4.9.24 at 2013-05-24 09:22:33 +0200\nINFO 2013-05-24T09:22:33.792732 Processing by Rest::Jobs::PostingsController#show as JSON\nINFO 2013-05-24T09:22:33.799331 Completed 404 Not Found in 10.8ms (Views: 0.313ms | ActiveRecord: 1.057ms(1q,0h) | API: 0.000(0) | Dalli: 0.000ms(0r,0m,0w,0c) | GC: 0.403(0) | HP: 0(2498680,2558,114017,591464) | REST: 0.000(0))",
  "timestamp": 1369380153,
  "level": 6,
  "_app": "jobs-production",
  "_total_time": 10.83065,
  "_code": 404,
  "_caller_id": "riaktivities-production-b02e866ec44211e293a70050569ed893",
  "_caller_action": "Rest::Activities::Legacy::NewsFeedController#show",
  "_request_id": "b3cb8de4c44211e2b166000c29e07312",
  "_user_id": 0,
  "_ip": "10.4.9.24",
  "_process_id": 7057,
  "_http_method": "GET",
  "_http_url": "/rest/jobs/postings/2196851?fields=created_at%2Cfunction%2Clinks&rid=internal",
  "_http_header_user_agent": "XING AG - RestCake/0.10.6",
  "_http_header_accept_encoding": "deflate, gzip",
  "_http_header_accept": "application/json",
}
```
