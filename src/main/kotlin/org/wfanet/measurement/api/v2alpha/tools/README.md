# Public API Tools

Command-line interface (CLI) tools for Cross-Media Measurement System public
API.

The examples assume that you have built the relevant target, which outputs to
`bazel-bin` by default. For brevity, the examples to not include the full path
to the executable.

## `EncryptionPublicKeys`

Tool for dealing with `EncryptionPublicKey` messages. Run the `help` subcommand
for usage information.

### Examples

*   Serialize the testing key for `DataProvider` "edp1".

    ```shell
    EncryptionPublicKeys serialize \
      --data=src/main/k8s/testing/secretfiles/edp1_enc_public.tink \
      --out=/tmp/edp1_enc_public.binpb
    ```

*   Signing above serialized `EncryptionPublicKey`

    ```shell
    EncryptionPublicKeys sign \
      --certificate src/main/k8s/testing/secretfiles/edp1_cs_cert.der \
      --signing-key src/main/k8s/testing/secretfiles/edp1_cs_private.der \
      --in /tmp/edp1_enc_public.binpb --out /tmp/edp1_enc_public_sig.bin
    ```

## `MeasurementSystem`

CLI for calling methods on the CMMS public API. Run the `help` subcommand for
usage information.

### TLS options

To specify the public API target, use the `--kingdom-public-api-target` option.

To specify a TLS client certificate and key, use the `--tls-cert-file` and
`--tls-key-file` options, respectively. The issuer of this certificate must be
trusted by the Kingdom server, i.e. the issuer certificate must be in that
server's trusted certificate collection file.

In the event that the host you specify to the `--kingdom-public-api-target`
option doesn't match what is in the Subject Alternative Name (SAN) extension of
the server's certificate, you'll need to specify a host that does match using
the `--kingdom-public-api-cert-host` option.

### Commands

#### `accounts`

*   `authenticate`

*   `activate`

#### `measurements`

The `measurements` command requires an API authentication key to be specified
using the `--api-key` option.

*   `create`

    To create a `Measurement`, provide values of the `Measurement` by arguments

    `Measurement` is a nested structure contains a list of `DataProviderEntry`,
    while `DataProviderEntry` contains a list of `EventGroupEntry`. To specify
    these value via CLI arguments, the inputs should be grouped.

    See the [Examples](#examples-1) section below.

*   `list`

    To list `Measurement`s of a `MeasurementConsumer`, the user specifies the
    name of the `MeasurementConsumer` with corresponding credential arguments.
    To navigate to next page, use `--page-size` and `page-token`.

*   `get`

    To get a certain `Measurement`, the user specifies the name of the
    `Measurement` and provides the private encryption key to decrypt the
    results.

#### `data-providers`

The `data-providers``command requires the DataProvider resource name to be
specified using the`--name` option.

*   `get`
*   `replace-required-duchies`
*   `update-capabilities`

#### `api-keys`

*   `create`

#### `certificates`

*   `create`

*   `revoke`

#### `public-keys`

*   `update`

### Examples

#### `accounts`

*   `authenticate`

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
    accounts \
    authenticate \
    --self-issued-openid-provider-key=secretfiles/account1_siop_private.tink
    ```

*   `activate`

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
    accounts \
    activate \
    accounts/KcuXSjfBx9E \
    --id-token=Sjf8Sdjd2V \
    --activation-token=vzmtXavLdk4
    ```

#### `measurements`

*   `create`

    Assuming you have a private key for `MeasurementConsumer` containing
    [`mc_cs_private.der`](../../../../../../../k8s/testing/secretfiles/mc_cs_private.der)
    at `secretfiles/mc_cs_private.pb`.

    Given that the `Measurement` contains 2 `DataProviderEntries` of
    `dataProviders/1` and `dataProviders/2`. `dataProviders/1` has two
    `EventGroups` `dataProviders/1/eventGroups/1` and
    `dataProviders/1/eventGroups/2` while `dataProviders/2` contains
    `dataProviders/2/eventGroups/1`. The order of options within a group does
    not matter.

    `Measurement` type of `ReachAndFrequency`:

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
    measurements \
    --api-key=nR5QPN7ptx \
    create \
    --measurement-consumer=measurementConsumers/777 \
    --reach-and-frequency \
    --rf-reach-privacy-epsilon=0.0033 \
    --rf-reach-privacy-delta=0.00001 \
    --rf-frequency-privacy-epsilon=0.115 \
    --rf-frequency-privacy-delta=0.00001 \
    --max-frequency=10 \
    --vid-sampling-start=0.16 \
    --vid-sampling-width=0.016667 \
    --private-key-der-file=secretfiles/mc_cs_private.der \
    --measurement-ref-id=9999 \
    --event-data-provider=dataProviders/1 \
    --event-group=dataProviders/1/eventGroups/1 \
    --event-filter="video_ad.age == 1" \
    --event-start-time=2022-05-22T01:00:00.000Z \
    --event-end-time=2022-05-24T05:00:00.000Z \
    --event-group=dataProviders/1/eventGroups/2 \
    --event-filter="video_ad.age == 2" \
    --event-start-time=2022-05-22T01:22:32.250Z \
    --event-end-time=2022-05-23T03:14:55.450Z \
    --event-data-provider=dataProviders/2 \
    --event-group=dataProviders/2/eventGroups/1 \
    --event-start-time=2022-04-22T01:19:42.336Z \
    --event-end-time=2022-05-22T01:56:12.257Z
    ```

    `Measurement` type of `Reach`:

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
    measurements \
    --api-key=nR5QPN7ptx \
    create \
    --measurement-consumer=measurementConsumers/777 \
    --reach \
    --reach-privacy-epsilon=0.0033 \
    --reach-privacy-delta=0.00001 \
    --vid-sampling-start=0.0 \
    --vid-sampling-width=0.5 \
    --private-key-der-file=secretfiles/mc_cs_private.der \
    --measurement-ref-id=7777 \
    --event-data-provider=dataProviders/1 \
    --event-group=dataProviders/1/eventGroups/1 \
    --event-filter="video_ad.age == 1" \
    --event-start-time=2022-05-22T01:00:00.000Z \
    --event-end-time=2022-05-24T05:00:00.000Z \
    --event-group=dataProviders/1/eventGroups/2 \
    --event-filter="video_ad.age == 2" \
    --event-start-time=2022-05-22T01:22:32.250Z \
    --event-end-time=2022-05-23T03:14:55.450Z \
    --event-data-provider=dataProviders/2 \
    --event-group=dataProviders/2/eventGroups/1 \
    --event-start-time=2022-04-22T01:19:42.336Z \
    --event-end-time=2022-05-22T01:56:12.257Z
    ```

*   `list`

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
    measurements \
    --api-key=nR5QPN7ptx \
    list \
    --measurement-consumer=measurementConsumers/777
    ```

*   `get`

    Given that the MeasurementConsumer has the encryption private key
    `mc_enc_private.tink`.

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
    measurements \
    --api-key=nR5QPN7ptx \
    get \
    --encryption-private-key-file=secretfiles/mc_enc_private.tink \
    measurementConsumers/777/measurements/100
    ```

#### `public-keys`

*   `update`

    Given an encryption public key `/tmp/edp1_enc_public.binpb` and a signature
    `/tmp/edp1_enc_public_sig.bin` that can be verified with
    `dataProviders/FeQ5FqAQ5_0/certificates/I0oaV1_vGAM`:

    ```shell
    MeasurementSystem \
    --tls-cert-file=secretfiles/edp1_tls.pem \
    --tls-key-file=secretfiles/edp1_tls.key \
    --cert-collection-file=secretfiles/kingdom_root.pem \
    --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
    public-keys update dataProviders/FeQ5FqAQ5_0/publicKey
    --certificate=dataProviders/FeQ5FqAQ5_0/certificates/I0oaV1_vGAM \
    --public-key=/tmp/edp1_enc_public.binpb \
    --public-key-signature=/tmp/edp1_enc_public_sig.bin
    ```

## `EventTemplateValidator`

Validates the event templates used in a given event message type. Use the
`--help` option for usage information.

The set of `FileDescriptorSet` files you provide must include your event message
and all of your template messages. These can be obtained from the protobuf
compiler (`protoc`) using the `--descriptor_set_out` option. If you are using
`protoc` indirectly via
[rules_proto](https://github.com/bazelbuild/rules_proto), the `proto_library`
and `proto_descriptor_set` rules can generate these for you.
