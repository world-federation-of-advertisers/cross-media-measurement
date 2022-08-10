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


## `Simple Report`

The Simple Report Tool can be used to create, list or get a `Measurement` by
calling the public Kingdom API.

### TLS flags

You'll need to specify the public API target using the 
`--kingdom-public-api-target` option.

You'll also need to specify a TLS client certificate and key using the
`--tls-cert-file` and `--tls-key-file` options, respectively. The issuer of this
certificate must be trusted by the Kingdom server, i.e. the issuer
certificate must be in that server's trusted certificate collection file.

### Create

To create a `Measurement`, the user needs to provide values of the `Measurement`
by arguments

`Measurement` is a nested structure contains a list of `DataProviderEntry`,
while `DataProviderEntry` contains a list of `EventGroupEntry`. To specify these
value via CLI arguments, the inputs should be grouped.

See the [Examples](##Examples) section below.

### List

To list `Measurement`s of a `MeasurementConsumer`, the user specifies the
name of the `MeasurementConsumer` with corresponding credential arguments.

See the [Examples](##Examples) section below.

### Get

To get a certain `Measurement`, the user specifies the name of the `Measurement` 
and provides the private encryption key to decrypt the results.

See the [Examples](##Examples) section below.

## Examples

This assumes that you have built the `SimpleReport` target, which outputs to
`bazel-bin` by default. For brevity, the examples to not include the full path
to the executable.

* Create

  Assuming you have a private key for `MeasurementConsumer` containing
  [`mc_cs_private.der`](../../../../../../../k8s/testing/secretfiles/mc_cs_private.der)
  at `secretfiles/mc_cs_private.pb`.

  Given that the `Measurement` contains 2 `DataProviderEntries` of `dataProviders/1`
  and `dataProviders/2`. `dataProviders/1` has two `EventGroups`
  `dataProviders/1/eventGroups/1` and `dataProviders/1/eventGroups/2` while
  `dataProviders/2` contains `dataProviders/2/eventGroups/1`. The order of
  options within a group does not matter.

  ```shell
  SimpleReport \
  --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
  --api-key=nR5QPN7ptx \
  create \
  --measurement-consumer=measurementConsumers/777 \
  --reach-and-frequency \
  --reach-privacy-epsilon=0.0033 \
  --reach-privacy-delta=0.0 \
  --frequency-privacy-epsilon=0.115 \
  --frequency-privacy-delta=0.0 \
  --vid-sampling-start=0.16 \
  --vid-sampling-width=0.016667 \
  --private-key-der-file=secretfiles/mc_cs_private.der \
  --measurement-ref-id=9999 \
  --data-provider=dataProviders/1 \
  --event-group=dataProviders/1/eventGroups/1 \
  --event-filter="video_ad.age.value == 1" \
  --event-start-time=2022-05-22T01:00:00.000Z \
  --event-end-time=2022-05-24T05:00:00.000Z \
  --event-group=dataProviders/1/eventGroups/2 \
  --event-filter="video_ad.age.value == 2" \
  --event-start-time=2022-05-22T01:22:32.250Z \
  --event-end-time=2022-05-23T03:14:55.450Z \
  --data-provider=dataProviders/2 \
  --event-group=dataProviders/2/eventGroups/1 \
  --event-start-time=2022-04-22T01:19:42.336Z \
  --event-end-time=2022-05-22T01:56:12.257Z
  ```

* List

  ```shell
  SimpleReport \
  --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
  --api-key=nR5QPN7ptx \
  list \
  --measurement-consumer=measurementConsumers/777
  ```

* Get

  Given that the MeasurementConsumer has the encryption private key
  `mc_enc_private.tink`.

  ```shell
  SimpleReport \
  --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443 \
  --api-key=nR5QPN7ptx \
  get \
  --measurement=measurementConsumers/777/measurements/100 \
  --encryption-private-key-file=secretfiles/mc_enc_private.tink
  ```

