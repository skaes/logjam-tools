# Exporting Logjam Metrics for Prometheus

logjam is a great tool for analyzing performance problems, but it lacks functionalities
for alerting and does not support building customized views (e.g. dashboards). Instead of
building all of this into logjam, it will be much less work to export logjam metrics to
Prometheus, which supports customized views through Grafana and alerting through both
Grafana and it's own [alert manager].

The following [metrics] are be exported:

| Metric                                                          | Metric Type | Usage Pattern                            |
|-----------------------------------------------------------------|-------------|------------------------------------------|
| 1. logjam:action:http\_response\_time\_distribution\_seconds    | histogram   | used for both web and API requests       |
| 2. logjam:action:job\_execution\_time\_distribution\_seconds    | histogram   | used for all kinds of background jobs    |
| 3. logjam:action:page\_time\_distribution\_seconds              | histogram   | used for page load times, RUM            |
| 4. logjam:action:ajax\_time\_distribution\_seconds              | histogram   | used for ajax requests, RUM              |
| 5. logjam:action:http\_response\_time\_summary\_seconds         | summary     | used for both web and API requests       |
| 6. logjam:action:job\_execution\_time\_summary\_seconds         | summary     | used for all kinds of background jobs    |
| 7. logjam:action:http\_response\_metrics\_distribution\_seconds | histogram   | same as 1, but for all secondary metrics |
| 8. logjam:action:job\_execution\_metrics\_distribution\_seconds | histogram   | same as 2, but for all secondary metrics |
| 9. logjam:action:http\_response\_metrics\_summary\_seconds      | summary     | same as 5, but for all secondary metrics |
| 10. logjam:action:job\_execution\_metrics\_summary\_seconds     | summary     | same as 5, but for all secondary metrics |

Secondary metrics are things like `db_time`, `db_calls`, `memcache_time`,
`memcache_misses` etc.


And the following [labels] will be used:

| Label      | Value                            | Metrics used with |
|------------|----------------------------------|-------------------|
| 1. app     | application name in logjam       | 1-10              |
| 2. env     | environment name in logjam       | 1-10              |
| 3. action  | action name in logjam            | 1-10              |
| 4. type    | type of http request: web or api | 1,5,7,9           |
| 4. code    | response code in logjam          | 5,6               |
| 5. method  | http request method              | 5,6               |
| 6. cluster | cluster name (e.g. Kubernetes)   | 5,6               |
| 7. dc      | datacenter name                  | 5,6               |
| 8. metric  | the name of the metric           | 7,8,9,10          |


The importer will need to figure out whether an action maps to which metric in Prometheus,
but this is actually simple: it already knows whether it is processing an Ajax or a page
request, and the distinction between HTTP requests and job execution can be made by
looking whether the request\_info sub hash contains a HTTP method specification.

## TODO

1. figure out how to export Apdex values!
2. do we need counters for logged errors?
2. do we need counters for logged exceptions?


[metrics]: https://prometheus.io/docs/concepts/data_model/
[labels]: https://prometheus.io/docs/practices/naming/
[histogram]: https://prometheus.io/docs/concepts/metric_types/#histogram
[summary]: https://prometheus.io/docs/concepts/metric_types/#summary
[alert manager]: https://prometheus.io/docs/alerting/overview/
