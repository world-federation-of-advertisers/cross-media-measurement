# Public API Tools

Command-line tools for Cross-Media Measurement System public API.

## `EncryptionPublicKeys`

Tool for dealing with `EncryptionPublicKey` messages. Run the `help` subcommand
for usage information.

### Examples

This assumes that you have built the `EncryptionPublicKeys` target, which
outputs to `bazel-bin` by default. For brevity, the examples to not include the
full path to the executable.

*   Serializing testing ModelProvider encryption key

    ```shell
    EncryptionPublicKeys serialize \
      --data=src/main/k8s/testing/secretfiles/mp1_enc_public.tink \
      --out=/tmp/mp1_enc_public.pb
    ```

*   Signing above serialized `EncryptionPublicKey`

    ```shell
    EncryptionPublicKeys sign \
      --certificate src/main/k8s/testing/secretfiles/mp1_cs_cert.der \
      --signing-key src/main/k8s/testing/secretfiles/mp1_cs_private.der \
      --in /tmp/mp1_enc_public.pb --out /tmp/mp1_enc_public_sig.sha256
    ```
