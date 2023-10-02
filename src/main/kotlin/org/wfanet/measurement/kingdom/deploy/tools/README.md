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
