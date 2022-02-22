# Creating Resources

How to create resources within a Kingdom.

This guide assumes familiarity with Kubernetes and how your Kingdom cluster is
configured.

## The `CreateResource` tool

Resources can be created using the
[`CreateResource`](../../src/main/kotlin/org/wfanet/measurement/kingdom/deploy/tools/README.md)
tool.

If you do not have access to a pre-built version of the `CreateResource`
command-line tool, you can build it using Bazel. See [Building](../building.md)
for more information on system requirements.

```shell
bazel build //src/main/kotlin/org/wfanet/measurement/kingdom/deploy/tools:CreateResource
```

## mTLS Credentials

Some resources also act as principals for authentication. A subset of these
resource types use Transport Layer Security (TLS) client authentication, also
known as mutual TLS (mTLS). These include the `DataProvider`, `ModelProvider`,
and `Duchy` resource types.

### Update trusted certificate collection

After creating one of these resources, the trusted certificate collection for
the Kingdom must be updated. This is also true for other entities that
authenticate with the Kingdom using mTLS, such as Measurement Consumer reporting
tools.

If your Kingdom cluster is configured similarly to the
[`dev` environment](../../src/main/k8s/dev), then the trusted certificate
collection may be a file called `all_root_certs.pem` in a `certs-and-configs`
K8s secret. You'll need to update this file, regenerate the secret, and then
update the K8s resource in your Kingdom cluster.

### Update AKID to principal map

If you added a resource that uses mTLS, you will need to update the
configuration file that maps the client certificate's authority key identifier
(AKID) to the resource. If the resource is a `Duchy`, then you need to update
the `DuchyCertConfig`. Otherwise, you need to update the
`AuthorityKeyToPrincipalMap`.

If your Kingdom cluster is configured similarly to the
[`dev` environment](../../src/main/k8s/dev), then the two files are in different
locations:

*   The `DuchyCertConfig` is in the `duchy_cert_config.textproto` file in a
    `certs-and-configs` K8s Secret. You'll need to update this file, regenerate
    the secret, and then update the K8s resource in your Kingdom cluster.
*   The `AuthorityKeyToPrincipalMap` is in the
    `authority_key_identifier_to_principal_map.textproto` file in the
    `config-files` ConfigMap. You'll need to update the file within the
    ConfigMap, and restart the Kingdom deployments.
