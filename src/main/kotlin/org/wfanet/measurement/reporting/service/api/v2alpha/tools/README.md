# Reporting CLI Tools

Command-line tools for Reporting API. Use the `help` subcommand for help with
any of the subcommands.

Note that instead of specifying arguments on the command-line, you can specify
an arguments file using `@` followed by the path. For example,

```shell
Reporting @/home/foo/args.txt
```

## Certificate Host

In the event that the host you specify to the `--reporting-server-api-target`
option doesn't match what's in the Subject Alternative Name (SAN) extension of
the server's certificate, you'll need to specify a host that does match using
the `--reporting-server-api-cert-host` option.

## Examples

### reporting-sets

#### create

```shell
Reporting \
  --tls-cert-file=src/main/k8s/testing/secretfiles/mc_tls.pem \
  --tls-key-file=src/main/k8s/testing/secretfiles/mc_tls.key \
  --cert-collection-file src/main/k8s/testing/secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  reporting-sets create \
  --parent=measurementConsumers/VCTqwV_vFXw \
  --cmms-event-group=dataProviders/FeQ5FqAQ5_0/eventGroups/OsICFAbXJ5c \
  --filter='video_ad.age.value == 1' --display-name='test-reporting-set' \
  --id=abc
```

```shell
Reporting \
  --tls-cert-file=src/main/k8s/testing/secretfiles/mc_tls.pem \
  --tls-key-file=src/main/k8s/testing/secretfiles/mc_tls.key \
  --cert-collection-file src/main/k8s/testing/secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  reporting-sets create \
  --parent=measurementConsumers/VCTqwV_vFXw \
  --set-expression='
      operation: UNION
      lhs {
        reporting_set: "measurementConsumers/VCTqwV_vFXw/reportingSets/abc"
      }
  ' \
  --filter='video_ad.age.value == 1' --display-name='test-reporting-set' \
  --id=abc
```

User specifies a primitive ReportingSet with one or more `--cmms-event-group`
and a composite ReportingSet with `--set-expression`.

The `--set-expression` option expects a
[`SetExpression`](../../../../../../../../../proto/wfa/measurement/reporting/v2alpha/reporting_set.proto)
protobuf message in text format. You can use shell quoting for a multiline
string, or use command substitution to read the message from a file e.g.
`--set-expression=$(cat set_expression.textproto)`.

#### list

```shell
Reporting \
  --tls-cert-file=src/main/k8s/testing/secretfiles/mc_tls.pem \
  --tls-key-file=src/main/k8s/testing/secretfiles/mc_tls.key \
  --cert-collection-file src/main/k8s/testing/secretfiles/reporting_root.pem \
  --reporting-server-api-target v2alpha.reporting.dev.halo-cmm.org:8443 \
  reporting-sets list --parent=measurementConsumers/VCTqwV_vFXw
```

To retrieve the next page of reports, use the `--page-token` option to specify
the token returned from the previous response.

### reports

#### create

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  reports create \
  --parent=measurementConsumers/VCTqwV_vFXw \
  --id=abcd \
  --request-id=abcd \
  --interval-start-time=2023-01-15T01:30:15.01Z \
  --interval-end-time=2023-06-27T23:19:12.99Z \
  --interval-start-time=2023-06-28T09:48:35.57Z \
  --interval-end-time=2023-12-13T11:57:54.21Z \
  --reporting-metric-entry='
      key: "measurementConsumers/VCTqwV_vFXw/reportingSets/abc"
      value {
        metric_calculation_specs: "measurementConsumers/Dipo47pr5to/metricCalculationSpecs/abc"
      }
  '
```

User specifies the type of time args by using either repeated interval params(
`--interval-start-time`, `--interval-end-time`) or reporting interval args(
`--reporting-interval-report-start-time`,
`-reporting-interval-report-start-utc-offset`/`--reporting-interval-report-start-time-zone`,
and `--reporting-interval-report-end`)

The `--reporting-metric-entry` option expects a
[`ReportingMetricEntry`](../../../../../../../../../proto/wfa/measurement/reporting/v2alpha/report.proto)
protobuf message in text format. You can use shell quoting for a multiline
string, or use command substitution to read the message from a file e.g.
`--reporting-metric-entry=$(cat reporting_metric_entry.textproto)`.

#### list

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  reports list --parent=measurementConsumers/VCTqwV_vFXw
```

#### get

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  reports get measurementConsumers/VCTqwV_vFXw/reports/abcd
```

### metric-calculation-specs

#### create

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  metric-calculation-specs create \
  --parent=measurementConsumers/VCTqwV_vFXw \
  --id=abcd \
  --display-name=display \
  --metric-spec='
    reach {
      privacy_params {
        epsilon: 0.0041
        delta: 1.0E-12
      }
    }
    vid_sampling_interval {
      width: 0.01
    }
  ' \
  --metric-spec='
    impression_count {
      privacy_params {
        epsilon: 0.0041
        delta: 1.0E-12
      }
    }
    vid_sampling_interval {
      width: 0.01
    }
  ' \
  --filter='person.gender == 1' \
  --grouping='person.gender == 1,person.gender == 2' \
  --grouping='person.age_group == 1,person.age_group == 2' \
  --day-of-the-week=2 \
  --day-window-count=5
```

The `--metric-spec` option expects a
[`MetricSpec`](../../../../../../../../../proto/wfa/measurement/reporting/v2alpha/metric.proto)
protobuf message in text format. You can use shell quoting for a multiline
string, or use command substitution to read the message from a file e.g.
`--metric-spec=$(cat metric_spec.textproto)`.

MetricFrequencySpec expects `--daily`, `--day-of-the-week`, or
`--day-of-the-month`. `--daily` is a boolean and `--day-of-the-week` is
represented by 1-7, with 1 being Monday.

TrailingWindow expects `--day-window-count`, `--week-window-count`, or
`--month-window-count`.

#### list

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  metric-calculation-specs list --parent=measurementConsumers/VCTqwV_vFXw
```

#### get

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  metric-calculation-specs get measurementConsumers/VCTqwV_vFXw/metricCalculationSpecs/abcd
```

### event-groups

#### list

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  event-groups list \
  --parent=measurementConsumers/VCTqwV_vFXw
```

### data-providers

#### get

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  dataProviders get \
    dataProviders/FeQ5FqAQ5_0
```

### event-group-metadata-descriptors

#### get

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  event-group-metadata-descriptors get \
    dataProviders/FeQ5FqAQ5_0/eventGroupMetadataDescriptors/clBPpgytI38
```

#### batch-get

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  event-group-metadata-descriptors batch-get \
    dataProviders/FeQ5FqAQ5_0/eventGroupMetadataDescriptors/clBPpgytI38 \
    dataProviders/FeQ5FqAQ5_0/eventGroupMetadataDescriptors/AnBj8RB1XFc
```
