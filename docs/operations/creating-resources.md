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
the
[`DuchyCertConfig`](../../src/main/proto/wfa/measurement/config/duchy_cert_config.proto).
Otherwise, you need to update the
[`AuthorityKeyToPrincipalMap`](../../src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto).

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

#### Config file format

The configuration files above are protocol buffer messages in
[text format](https://developers.google.com/protocol-buffers/docs/text-format-spec).
They each have a field of type `bytes` that contains the certificate AKID which
identifies that principal. The AKID is common represented as a series of bytes
in hexadecimal. Byte literals in protobuf text format can be represented by the
`\x` escape sequence followed by the two-digit hex value.

One way to obtain the AKID value from an X.509 certificate is to use the
`openssl` command-line tool:

```shell
openssl x509 -noout -text -in src/main/k8s/testing/secretfiles/edp1_root.pem
```

Example `authority_key_identifier_to_principal_map.textproto` file:

```prototext
# proto-file: wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
entries {
  authority_key_identifier: "\x90\xC1\xD3\xBD\xE6\x74\x01\x55\xA7\xEF\xE6\x64\x72\xA6\x68\x9C\x41\x5B\x77\x04"
  principal_resource_name: "dataProviders/OljiQHRz-E4"
}
entries {
  authority_key_identifier: "\xF6\xED\xD1\x90\x2E\xF2\x04\x06\xEB\x16\xC4\x40\xCF\x69\x43\x86\x16\xCC\xAE\x08"
  principal_resource_name: "dataProviders/Fegw_3Rz-2Y"
}
entries {
  authority_key_identifier: "\xC8\x03\x73\x90\x9E\xBF\x33\x46\xEA\x94\x44\xC4\xAC\x77\x4D\x47\x67\xA1\x81\x94"
  principal_resource_name: "dataProviders/aeULv4uMBDg"
}
entries {
  authority_key_identifier: "\x95\x42\x02\x4C\xED\x13\x36\xFD\x2E\xB3\xAB\x30\xFE\x2B\x9A\x06\xBE\x19\x17\x54"
  principal_resource_name: "dataProviders/d2QIG4uMA8s"
}
entries {
  authority_key_identifier: "\x84\xEA\x3D\xFE\xD6\x45\x43\x3F\x5C\xC6\xED\x86\xA2\x83\x3D\xF8\x0D\x5D\x6B\xB7"
  principal_resource_name: "dataProviders/IjDOL3Rz_PY"
}
entries {
  authority_key_identifier: "\xBB\x12\x20\xA8\xE6\x04\x95\xCF\xA8\x33\x42\x33\x27\xD2\x07\x69\xC2\xBF\x8A\x5A"
  principal_resource_name: "dataProviders/U8rTiHRz_b4"
}
```
