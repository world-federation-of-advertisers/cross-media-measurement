# Kingdom CLI Tools

Command-line tools for Kingdom operators.

## `CreateResource`

The `CreateResource` tool can be used to create resources by calling the
internal Kingdom API.

Running the tool with the `--help` option will provide more information on
command-line options. You can use the `help` subcommand for help on other
subcommands. For example, for help on the `data_provider` subcommand used for
creating a `DataProvider` resource, pass `help data_provider` to the command.

You'll need to specify the internal API target using the `--internal-api-target`
option. See the [Port Forwarding](#port-forwarding) section below.

You'll also need to specify a TLS client certificate and key using the
`--tls-cert-file` and `--tls-key-file` options, respectively. The issuer of this
certificate must be trusted by the Kingdom, i.e. the issuer certificate must be
in the Kingdom's trusted certificate collection file.

### Port Forwarding

The internal Kingdom API server is generally not accessible outside of the K8s
cluster. In order to call the API, you can forward its service port to your
local machine using `kubectl`.

Note: This assumes that you have `kubectl` installed and configured to point to
the Kingdom cluster.

Supposing you have the internal API server in a K8s deployment named
`gcp-kingdom-data-server-deployment` and the service port is `8443`, the command
would be

```shell
kubectl port-forward deployments/gcp-kingdom-data-server-deployment 8443:8443
```

You can then pass `--internal-api-target=localhost:8443` to the tool. You'll
most likely need to specify the hostname of the Kingdom's TLS certificate using
the `--internal-api-cert-host` option.
