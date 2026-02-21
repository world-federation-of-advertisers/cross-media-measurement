# Internal Reporting API tools

## DetectInvalidReportingSets

Used to detect invalid ReportingSets (those with incorrect weighted subset
unions) across Reports. This requires access to both the public and internal
Reporting APIs.

### Accessing Internal Reporting API

The internal Reporting API is usually not accessible outside of the Kubernetes
cluster. This can be remedied using `kubectl port-forward` with your `kubectl`
pointing to the Reporting Kubernetes cluster. e.g.

```shell
kubectl port-forward --address=localhost services/postgres-internal-reporting-server 8443:8443
```

### Auth

The specified TLS credentials should be those used to access the internal API
server. This means a client certificate trusted by the internal API server, such
as one issued by the Reporting root certificate authority.

For the public API server, a bearer token must be provided using the
`--bearer-token` option which grants the ability to list Reports from the
specified parent Measurement Consumers.

### Continuing after interruption

The tool outputs a page token after processing a page of Reports. If the tool is
interrupted, it can pick up where it left off by passing the most recent token
using the `--page-token` option, removing any values from
`--measurement-consumer` that have already been processed.
