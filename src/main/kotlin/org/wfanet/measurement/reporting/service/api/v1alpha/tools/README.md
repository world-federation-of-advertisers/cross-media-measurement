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

Example
```shell
reporting \
--tls-cert-file=secretfiles/mc_tls.pem \
--tls-key-file=secretfiles/mc_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--reporting-server-api-target=reporting.dev.halo-cmm.org:8443 \
list-reporting-sets \
--measurement-consumer=measurementConsumers/777
```

## Report Operations

### Create

### List

### Get