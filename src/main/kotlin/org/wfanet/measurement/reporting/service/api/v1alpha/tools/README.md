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
  --reporting-server-api-target v1alpha.reporting.dev.halo-cmm.org:8443 \
  reporting-sets create --parent=measurementConsumers/VCTqwV_vFXw \
  --event-group=measurementConsumers/VCTqwV_vFXw/dataProviders/1/eventGroups/1 \
  --event-group=measurementConsumers/VCTqwV_vFXw/dataProviders/1/eventGroups/2 \
  --event-group=measurementConsumers/VCTqwV_vFXw/dataProviders/2/eventGroups/1 \
  --filter='video_ad.age == 1' --display-name='test-reporting-set'
```

#### list

```shell
Reporting \
  --tls-cert-file=src/main/k8s/testing/secretfiles/mc_tls.pem \
  --tls-key-file=src/main/k8s/testing/secretfiles/mc_tls.key \
  --cert-collection-file src/main/k8s/testing/secretfiles/reporting_root.pem \
  --reporting-server-api-target v1alpha.reporting.dev.halo-cmm.org:8443 \
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
  --reporting-server-api-target=v1alpha.reporting.dev.halo-cmm.org:8443 \
  reports create \
  --idempotency-key="report001" \
  --parent=measurementConsumers/777 \
  --event-group-key=$EVENT_GROUP_NAME_1 \
  --event-group-value="video_ad.age == 1" \
  --event-group-key=$EVENT_GROUP_NAME_2 \
  --event-group-value="video_ad.age == 12" \
  --interval-start-time=2017-01-15T01:30:15.01Z \
  --interval-end-time=2018-10-27T23:19:12.99Z \
  --interval-start-time=2019-01-19T09:48:35.57Z \
  --interval-end-time=2022-06-13T11:57:54.21Z \
  --metric='
  reach { }
      set_operations {
        unique_name: "operation1"
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

User specifies the type of time args by using either repeated interval params(
`--interval-start-time`, `--interval-end-time`) or periodic time args(
`--periodic-interval-start-time`, `--periodic-interval-increment` and
`--periodic-interval-count`)

The `--metric` option expects a
[`Metric`](../../../../../../../../../proto/wfa/measurement/reporting/v1alpha/metric.proto)
protobuf message in text format. See
[`metric1.textproto`](../../../../../../../../../../test/kotlin/org/wfanet/measurement/reporting/service/api/v1alpha/tools/metric1.textproto)
for a complicated example. You can use shell quoting for a multiline string, or
use command substitution to read the message from a file e.g. `--metric=$(cat
metric1.textproto)`.

#### list

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v1alpha.reporting.dev.halo-cmm.org:8443 \
  reports list --parent=measurementConsumers/777
```

#### get

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v1alpha.reporting.dev.halo-cmm.org:8443 \
  reports get measurementConsumers/777/reports/5
```

### event-groups

#### list

```shell
Reporting \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --reporting-server-api-target=v1alpha.reporting.dev.halo-cmm.org:8443 \
  event-groups list \
  --parent=measurementConsumers/777/dataProviders/1
```
