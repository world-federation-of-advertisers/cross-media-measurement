# Reporting CLI Tools

Command-line tools for Reporting API.

## ReportingSet Operations

A ReportingSet is a collection of EventGroups. It allows users to reuse the same 
set of EventGroups for different report without specify EventGroups over again.

### TLS flags

You'll need to specify the reporting server's API target using the
`--reporting-server-api-target` option.

You'll also need to specify a TLS client certificate and key using the
`--tls-cert-file` and `--tls-key-file` options, respectively. The issuer of this
certificate must be trusted by the reporting server, i.e. the issuer
certificate must be in that server's trusted certificate collection file.

### Create

Example:
```shell
reporting \
--tls-cert-file=secretfiles/mc_tls.pem \
--tls-key-file=secretfiles/mc_tls.key \
--cert-collection-file=secretfiles/reporting_root.pem \
--reporting-server-api-target=reporting.dev.halo-cmm.org:8443 \
create-reporting-set \
--measurement-consumer=measurementConsumers/777 \
--event-groups \
dataProviders/1/eventGroups/1 \
dataProviders/1/eventGroups/2 \
dataProviders/2/eventGroups/1 \
--filter="video_ad.age.value == 1" \
--display-name=test-reporting-set
```

### List

To retrieve the next page of reports, add the argument "--page-token", which is
provided from the previews ListReportingSets response.

Example:
```shell
reporting \
--tls-cert-file=secretfiles/mc_tls.pem \
--tls-key-file=secretfiles/mc_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--reporting-server-api-target=reporting.dev.halo-cmm.org:8443 \
list-reporting-sets \
--measurement-consumer=measurementConsumers/777
```

## Reports Operations

### Create

User specifies the type of time args by using either repeated interval params(
`--interval-start-time`, `--interval-end-time`) or periodic time args(
`--periodic-interval-start-time`, `--periodic-interval-increment` and
`--periodic-interval-count`)

The input of metric is in format of `.textproto`. See 
[metric.textproto](../../../../../../../../../../test/kotlin/org/wfanet/measurement/reporting/service/api/v1alpha/tools/metric1.textproto) 
for a complicated example. Use single quotes to wrap the content in terminal 
to input multiple lines as arg. Or write the content into a file 
(eg. metric1.textproto) and use `--metrics="$(cat metric1.textproto)"`.

Example:
```shell
reporting \
--tls-cert-file=secretfiles/mc_tls.pem \
--tls-key-file=secretfiles/mc_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--reporting-server-api-target=reporting.dev.halo-cmm.org:8443 \
reports \
create \
--parent=measurementConsumers/777 \
--event-group-key=$EVENT_GROUP_NAME_1 \
--event-group-value="video_ad.age.value == 1" \
--event-group-key=$EVENT_GROUP_NAME_2 \
--event-group-value="video_ad.age.value == 12" \
--interval-start-time=2017-01-15T01:30:15.01Z \
--interval-end-time=2018-10-27T23:19:12.99Z \
--interval-start-time=2019-01-19T09:48:35.57Z \
--interval-end-time=2022-06-13T11:57:54.21Z \
--metric='
reach { }
    set_operations {
      display_name: "operation1"
      set_operation {
        type: 1
        lhs {
          reporting_set: "measurementConsumers/1/reportingSets/1"
        }
        rhs {
          reporting_set: "measurementConsumers/1/reportingSets/2"
        }
      }
    }
'
```

### List

Example:
```shell
reporting \
--tls-cert-file=secretfiles/mc_tls.pem \
--tls-key-file=secretfiles/mc_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--reporting-server-api-target=reporting.dev.halo-cmm.org:8443 \
reports \
list \
--parent=measurementConsumers/777
```

### Get

Example:
```shell
reporting \
--tls-cert-file=secretfiles/mc_tls.pem \
--tls-key-file=secretfiles/mc_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--reporting-server-api-target=reporting.dev.halo-cmm.org:8443 \
reports \
get \
--parent=measurementConsumers/777
--name=measurementConsumers/777/reports/5
```