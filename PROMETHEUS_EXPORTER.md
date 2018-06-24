# Exporting Logjam Metrics for Prometheus

logjam is a great tool for analyzing performance problems, but it lacks functionalities
for alerting and does not support building customized views (e.g. dashboards). Instead of
building all of this into logjam, it will be much less work to export logjam metrics to
Prometheus, which supports customized views through Grafana and alerting through both
Grafana and it's own [alert manager].

For starters, the following [metrics] will be exported as [histograms]

| Metric | Usage Pattern |
|--------|---------------|
|1. http\_request\_duration\_seconds | used for both web and API requests |
|2. job\_execution\_duration_seconds | used for all kinds of background jobs |
|3. page\_request\_duration\_seconds | used for page load times, RUM |
|4. ajax\_request\_duration\_seconds | used for ajax requests, RUM |

And the following [labels] will be used:

| Label  | Value | Metrics used with |
|--------|-------|------|
|1. application             |application name in logjam      | 1,2,3,4
|2. action                  |action name in logjam           | 1,2,3,4
|3. code                    |response code in logjam         | 1,2
|4. http\_method            |http requests                   | 1
|5. http\_url\_normalized   |only for http requests          | 1
|6. host                    |host or container name          | 1,2
|7. cluster                 |cluster name (e.g. Kubernetes)  | 1,2
|8. datacenter              |datacenter name                 | 1,2

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
[histograms]: https://prometheus.io/docs/concepts/metric_types/#histogram
[alert manager]: https://prometheus.io/docs/alerting/overview/
