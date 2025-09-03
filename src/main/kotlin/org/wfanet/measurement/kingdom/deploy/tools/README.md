# Kingdom CLI Tools

Command-line tools for Kingdom operators.

## `CreateResource`

The `CreateResource` tool can be used to create resources by calling the
internal Kingdom API.

Running the tool with the `help` command will provide more information on
command-line options. You can also pass another command name to the `help`
command for usage information for that particular command. For example, for help
on the `data-provider` command used for creating a `DataProvider` resource, pass
`help data-provider`.

You'll need to specify the internal API target using the `--internal-api-target`
option. See the [Port Forwarding](#port-forwarding) section below.

You'll also need to specify a TLS client certificate and key using the
`--tls-cert-file` and `--tls-key-file` options, respectively. The issuer of this
certificate must be trusted by the Kingdom internal server, i.e. the issuer
certificate must be in that server's trusted certificate collection file.

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

### Examples

This assumes that you have built the `CreateResource` target, which outputs to
`bazel-bin` by default. For brevity, the examples to not include the full path
to the executable.

*   Creating a `DataProvider` in the `dev` environment

    Assuming you have a serialized `EncryptionPublicKey` message containing
    [`edp1_enc_public.tink`](../../../../../../../k8s/testing/secretfiles/edp1_enc_public.tink)
    at `/tmp/edp1_enc_public.binpb`, and a signature of that using
    [`edp1_cs_private.der`](../../../../../../../k8s/testing/secretfiles/edp1_cs_private.der)
    at `/tmp/edp1_enc_public.sig`.

    ```shell
    CreateResource \
      --tls-cert-file=src/main/k8s/testing/secretfiles/kingdom_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/kingdom_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --internal-api-target=localhost:8443 --internal-api-cert-host=localhost \
      data-provider \
      --certificate-der-file=src/main/k8s/testing/secretfiles/edp1_cs_cert.der \
      --encryption-public-key-file=/tmp/edp1_enc_public.binpb \
      --encryption-public-key-signature-file=/tmp/edp1_enc_public.sig \
      --encryption-public-key-signature-algorithm=ECDSA_WITH_SHA256
    ```

*   Creating a `ModelProvider` in the `dev` environment

    ```shell
    CreateResource \
      --tls-cert-file=src/main/k8s/testing/secretfiles/kingdom_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/kingdom_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --internal-api-target=localhost:8443 --internal-api-cert-host=localhost \
      model-provider
    ```

*   Creating the initial `Certificate` for the `worker1` `Duchy` in the `dev`
    environment

    ```shell
    CreateResource \
      --tls-cert-file=src/main/k8s/testing/secretfiles/kingdom_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/kingdom_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --internal-api-target=localhost:8443 --internal-api-cert-host=localhost \
      duchy-certificate --duchy-id=worker1 \
      --cert-file=src/main/k8s/testing/secretfiles/worker1_cs_cert.der
    ```

## `ModelRepository`

Command-Line interface (CLI) tool to manager Model Repository artifacts.

There are three layers for this CLI. Firstly, the root command is
model-repository. Secondly, under the root command, there are command groups.
Each command group focus on one type of resource (e.g. model-providers,
model-lines). Thirdly, each command group has several leaf commands that
corresponds to one specific action on the resource (e.g. create, get, list).

Run the help subcommand for usage information. For help on each leaf command,
due to current limitations, a value needs to be provided for
--kingdom-public-api-target, (e.g. model-repository
--kingdom-public-api-target=x model-lines create help).

The examples assume that you have built the relevant target, which outputs to
bazel-bin by default. For brevity, the examples do not include the full path to
the executable.

### Authenticating to the Kingdom API Server

Arguments:

`--tls-cert-file`: TLS client certificate. The issuer of this certificate must
be trusted by the Kingdom server, i.e. the issuer certificate must be in that
server's trusted certificate collection file.

`--tls-key-file`: TLS client key.

`--kingdom-public-api-cert-host`: In the event that the host you specify to the
`--kingdom-public-api-target` option doesn't match what is in the Subject
Alternative Name (SAN) extension of the server's certificate, this option
specifies a host that does match using the `--kingdom-public-api-cert-host`
option.

`--kingdom-public-api-target`: specify the public API target.

To access ModelProvider, ModelSuite, ModelLine subcommands, authenticate use
ModelProvider certificates:

```shell
ModelRepository \
--tls-cert-file=secretfiles/mp1_tls.pem \
--tls-key-file=secretfiles/mp1_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
sub-command
```

To access Population subcommands, authenticate use DataProvider certificates:

```shell
ModelRepository \
--tls-cert-file=secretfiles/edp1_tls.pem \
--tls-key-file=secretfiles/edp1_tls.key \
--cert-collection-file=secretfiles/kingdom_root.pem \
--kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  sub-command
```

### Commands

### `model-providers`

*   Get a ModelProvider

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-providers get modelProviders/AAAAAAHs
    ```

*   List ModelProviders

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-providers list --page-size=50 --page-token=pageTokenFromPreviousListResponse
    ```

### `model-suites`

*   Get a ModelSuite

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-suites get modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs
    ```

*   List ModelSuites

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-suites list --parent=modelProviders/AAAAAAAAAHs --page-size=50 \
      --page-token=pageTokenFromPreviousListResponse
    ```

*   Create ModelSuite

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-suites create --parent=modelProviders/AAAAAAAAAHs --display-name=name \
      --description=testing
    ```

### `model-lines`

*   Create ModelLine

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-lines create --parent=modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs \
      --display-name=name --description=description \
      --active-start-time=2035-09-04T09:04:00Z --active-end-time=2035-09-12T11:05:00Z \
      --type=DEV \
      --holdback-model-line=modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelLines/AAAAAAAAAHs \
      --population=dataProviders/AAAAAAAAAHs/populations/AAAAAAAAAHs
    ```

*   Set ModelLine's active end time

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      model-lines set-active-end-time \
      --name=modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelLines/AAAAAAAAAHs \
      --active-end-time=2035-05-29T09:04:00Z
    ```

### `populations`

*   Get Population

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      populations get dataProviders/AAAAAAAAAHs/populations/AAAAAAAAAHs
    ```

*   List Populations

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      populations list --parent=dataProviders/AAAAAAAAAHs --page-size=50 \
      --page-token=pageTokenFromPreviousListResponse
    ```

*   Create Population

    ```shell
    ModelRepository \
      --tls-cert-file=src/main/k8s/testing/secretfiles/mp1_tls.pem \
      --tls-key-file=src/main/k8s/testing/secretfiles/mp1_tls.key \
      --cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
      --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      populations create --parent=modelProviders/AAAAAAAAAHs --description=testing \
      --event-message-descriptor-set=/path/to/event-descriptor-set.bin \
      --event-message-type-url='type.googleapis.com/halo_cmm.origin.uk.eventtemplate.v1.Event' \
      --population-spec=/path/to/population-spec.binpb
    ```
