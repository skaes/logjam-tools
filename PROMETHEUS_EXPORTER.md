# Exporting Logjam Metrics for Prometheus

logjam is a great tool for analyzing performance problems, but it lacks functionalities
for alerting and does not support building customized views (e.g. dashboards). Instead of
building all of this into logjam, it will be much less work to export logjam metrics to
Prometheus, which supports customized views through Grafana and alerting through both
Grafana and it's own [alert manager].

The following [metrics] are exported:

| Metric                                                       | Metric Type | Usage Pattern                          |
|--------------------------------------------------------------|-------------|----------------------------------------|
| 1. logjam:action:http\_response\_time\_distribution\_seconds | histogram   | used for both web and API requests     |
| 2. logjam:action:job\_execution\_time\_distribution\_seconds | histogram   | used for all kinds of background jobs  |
| 3. logjam:action:page\_time\_distribution\_seconds           | histogram   | used for page load times, RUM          |
| 4. logjam:action:ajax\_time\_distribution\_seconds           | histogram   | used for ajax requests, RUM            |
| 5. logjam:action:http\_response\_time\_summary\_seconds      | summary     | used for both web and API requests     |
| 6. logjam:action:job\_execution\_time\_summary\_seconds      | summary     | used for all kinds of background jobs  |
| 7. logjam:action:_metric\_name_\_distribution\_seconds       | histogram   | all secondary logjam time metric names |
| 8. logjam:action:_metric\_name_\_summary\_seconds            | summary     | all secondary logjam time metric names |
| 9. logjam:action:http\_requests\_total                       | counter     | web and API requests with log level    |
| 10. logjam:action:job\_executions\_total                     | counter     | job executions with log level          |
| 11. logjam:action:_metric\_name_\_total                      | counter     | all secondary logjam call metric names |
| 12. logjam:action:action\_calls\_total                       | counter     | count of all action calls              |


Secondary logjam metric names are things like `db_time`, `db_calls`, `memcache_time`,
`memcache_misses` etc. Of these a `_time` suffix signifies time metrics. You can inspect
which resources are available and what type they have by curling `/admin/resources` on
your logjam instance.

And the following [labels] will be used:

| Label      | Value                                    | Metrics it is used with |
|------------|------------------------------------------|-------------------------|
| 1. app     | application name in logjam               | 1-12                    |
| 2. env     | environment name in logjam               | 1-12                    |
| 3. action  | action name in logjam                    | 1-12                    |
| 4. type    | type of http request: web or api         | 1,5,9                   |
|            | type of request (web, api, job)          | 7,8,11                  |
|            | type of call (web, api, job, ajax, page) | 12                      |
| 5. code    | response code in logjam                  | 5,6                     |
| 6. method  | http request method                      | 1,5                     |
| 7. cluster | cluster name (e.g. Kubernetes)           | 5,6,9,10,11,12          |
| 8. dc      | datacenter name                          | 5,6,9,10,11,12          |
| 9. level   | log level (0-5)                          | 9,10                    |


Log level values are:

| Value | Ruby Constant |
|-------|---------------|
| "0"   | DEBUG         |
| "1"   | INFO          |
| "2"   | WARN          |
| "3"   | ERROR         |
| "4"   | FATAL         |
| "5"   | UNKNOWN       |

The importer will need to figure out whether an action maps to which metric in Prometheus,
but this is actually simple: it already knows whether it is processing an Ajax or a page
request, and the distinction between HTTP requests and job execution can be made by
looking whether the request\_info sub hash contains a HTTP method specification.

## TODO

1. figure out how to export Apdex values!
2. do we need counters for logged exceptions?

[metrics]: https://prometheus.io/docs/concepts/data_model/
[labels]: https://prometheus.io/docs/practices/naming/
[histogram]: https://prometheus.io/docs/concepts/metric_types/#histogram
[summary]: https://prometheus.io/docs/concepts/metric_types/#summary
[alert manager]: https://prometheus.io/docs/alerting/overview/
