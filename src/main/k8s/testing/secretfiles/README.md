# Secret Files for K8s Correctness Test

For the K8s correctness test, we pre-generate all certificates and key pairs to
store inside a Kubernetes Secret, which is then mounted to a volume in each pod.

## Certificate Generation

These certificates were generated using OpenSSL. See the
`generate_root_certificate` and `generate_user_certificate`
[build defs in the common-jvm repo](https://github.com/world-federation-of-advertisers/common-jvm/blob/main/build/openssl/defs.bzl)
for examples on how to do this. The OpenSSL CLI also has some tools for
inspecting the certificates and signing keys.

Test certificates under version control should be valid for a long period (e.g.
10 years) to avoid breaking tests.

## Tink Keyset Generation

Use the
[`tinkey` CLI tool](https://github.com/google/tink/blob/master/docs/TINKEY.md).

### Example

Generate a private ECIES Keyset:

```shell
tinkey create-keyset --key-template ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM --out-format binary --out edp1_enc_private.tink
```

Extract a public Keyset from the private one:

```shell
tinkey create-public-keyset --in-format binary --out-format binary --in edp1_enc_private.tink --out edp1_enc_public.tink
```

## Create the Kubernetes Secret

To create a K8s secret from these testing files, run the
`//src/main/k8s/testing/secretfiles:apply_kustomization` Bazel target. This will
generate the trusted certificates file as well as the `kustomization.yaml` file.
