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
protobuf message in text format. You can use shell quoting for a multiline string, or
use command substitution to read the message from a file e.g. `--set-expression=$(cat
set_expression.textproto)`.

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
protobuf message in text format. You can use shell quoting for a multiline string, or
use command substitution to read the message from a file e.g. `--reporting-metric-entry=$(cat
reporting_metric_entry.textproto)`.

#### create ui
```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v2alpha.reporting.dev.halo-cmm.org:8443 \
  reports create-ui-report\
  --parent=measurementConsumers/VCTqwV_vFXw \
  --id=abcd \
  --request-id=abcd \
  --display-name=TESTDISPLAYREPORT \
  --cmms-event-group=1 \
  --primitive-rs-display-name='EDP 1' \
  --primitive-rs-id=abc \
  --cmms-event-group=2 \
  --primitive-rs-display-name='EDP 2' \
  --primitive-rs-id=def \
  --cmms-event-group=3 \
  --primitive-rs-display-name='EDP 3' \
  --primitive-rs-id=ghi \
  --prim-metric-id='mcs-1',
  --prim-metric-display-name='primitive calc spec 1',
  --comp-metric-id='mcs-2',
  --comp-metric-display-name='primitive calc spec 2',
  --union-rs-id=union-rs-1,
  --union-rs-display-name='EDP Union',
  --reporting-interval-report-start-time='2000-01-01T00:00:00',
  --reporting-interval-report-end='2000-01-30',
  --daily-frequency=true,
```

There currently needs to be three groups of primitive reporting sets defined by the arg group: --cmms-event-group, --primitive-rs-id, and --primitive-rs-display-name. You can pass in multiple event groups, but be sure to specify them first. There is a parsing issue if they are specified later in the arg group. This is similar to creating a reporting set.

The union reporting set is created automatically from the primitives, but just require an id and name: --union-rs-id and --union-rs-display-name.

The unique reporting sets are created automatically from the primitives and union. They are not explicitly used in the UI and require no input. They are post-fixed with "unique."

The actual values of the metric calculation specs are hardcoded for user simplicity as the combinations have to be very specific. What the user will define are the ids and names for the specs. There are two different configurations of specs so they each need to be defined: --prim-metric-id and --prim-metric-display-name as well as --comp-metric-id and --comp-metric-display-name. These aren't use in the UI so they can be any valid value for creation purposes.

The time range of the report must be specified through the start time and end (date): --reporting-interval-report-start-time and --reporting-interval-report-end. These are formatted as the example above and are the same as in create report. We force incremental to simplify the input.

The user must specify the interval frequency: --daily-frequency, --day-of-the-week, or --day-of-the-month. This is used in combination with the interval time range.

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
protobuf message in text format. You can use shell quoting for a multiline string, or
use command substitution to read the message from a file e.g. `--metric-spec=$(cat
metric_spec.textproto)`.

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
