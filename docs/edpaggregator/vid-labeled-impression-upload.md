# Upload already VID-labeled impressions

Use this guide when the publisher assigns a Virtual Person ID (VID) to every
impression before uploading it to the EDP Aggregator (EDPA).

> **Impression blobs are never JSON.** They contain binary
> `LabeledImpression` protobuf messages in Mesos RecordIO framing. Only campaign
> files and `BlobDetails` metadata sidecars may use JSON.

This is the alternative to EDPA-managed VID labeling:

| EDPA-managed labeling | Publisher-managed labeling |
| --- | --- |
| Input is PME-encrypted Parquet | Input is envelope-encrypted RecordIO |
| EDPA assigns VIDs | Every input record already has a VID |
| EDPA creates labeled metadata | The publisher creates a `BlobDetails` sidecar |

After the labeled data are registered, both paths use the same measurement
pipeline. See [How data moves through the EDP Aggregator](data-flow.md) for the
data-availability, ResultsFulfiller, and TrusTEE stages.

For the recommended Parquet path and the rest of publisher onboarding, return to
[Onboard a publisher to the EDP Aggregator](README.md).

## Before uploading

Obtain these values from the market operator:

* the publisher's Halo `DataProvider` resource name;
* the full `ModelLine` resource name;
* the storage bucket and labeled-impression prefix;
* the market's event protobuf type and descriptor;
* the campaign/entity-key convention; and
* the trusted ResultsFulfiller and, if enabled, TrusTEE workload identities.

Set up the publisher-owned KMS key and attestation policy using the
[Google Cloud KMS instructions](edp-onboarding.md#22-kms-setup--google-cloud) or
the [AWS KMS Setup Guide](aws-kms-setup.md).

## 1. Build the labeled records

The data file contains binary protobuf
`wfa.measurement.edpaggregator.LabeledImpression` messages framed with Mesos
RecordIO. Generate code from
[labeled_impression.proto](../../src/main/proto/wfa/measurement/edpaggregator/v1alpha/labeled_impression.proto)
rather than copying the table below as a schema.

Each RecordIO record contains exactly one bare serialized `LabeledImpression`:

```text
<record byte length as ASCII decimal>\n<exact protobuf bytes>
```

For example, a 123-byte message starts with the four bytes `123\n`, followed
immediately by the 123 bytes returned by protobuf `toByteArray()` or its
equivalent. Do not use protobuf's delimited form, such as Java
`writeDelimitedTo`: Mesos RecordIO already supplies the length prefix. There is
no separator after the protobuf bytes; the next record starts with its ASCII
length.

| Field | Requirement | Meaning |
| --- | --- | --- |
| `event_time` | Required | Timestamp of the impression |
| `vid` | Required | VID assigned by the publisher |
| `event` | Required | An `Any` containing the market's event message |
| `entity_keys` | Required for new writers | One or more publisher entity identifiers used to select and filter impressions |
| `event_group_reference_id` | Legacy | Older campaign reference; new writers use `entity_keys` |

An entity key contains:

* `entity_type`: a URL-safe type such as `campaign`, `creative`, or
  `placement`; and
* `entity_id`: the publisher's URL-safe identifier for that entity.

At least one entity key on each impression must match the `entity_key` of the
campaign registered with Halo. Otherwise, the file may process successfully
while measurements find no matching impressions.

## 2. Encrypt the RecordIO file

For each daily batch:

1. Generate a fresh Tink AEAD keyset to use as the data-encryption key (DEK).
   The reference writer uses the `AES128_GCM_HKDF_1MB` Streaming AEAD template,
   which avoids buffering an entire large blob.
2. Serialize each `LabeledImpression` as binary protobuf and write the records
   using Mesos RecordIO framing. You may split the batch into multiple blobs.
3. Encrypt each complete RecordIO stream with the matching Tink AEAD primitive.
   Use the blob's exact object key—the path within the bucket—as associated data.
4. Wrap the serialized DEK with the publisher's KMS key.

The associated data is the object key, not the full `gs://` URI. For example,
for `gs://example-bucket/publisher/impressions.enc.recordio`, use
`publisher/impressions.enc.recordio`. `BlobDetails.blob_uri` must resolve to
that same bucket and object key. Renaming or moving an encrypted blob without
re-encrypting it changes the associated data and makes decryption fail.

The publisher keeps the KMS key in its own cloud account. The wrapped DEK is
recorded in every metadata sidecar for a blob protected by that DEK; the
plaintext DEK is not uploaded.

The repository's
[reference writer](../../src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/testing/ImpressionWriter.kt)
shows the RecordIO framing, Tink envelope encryption, and metadata construction
used by EDPA tests.

## 3. Create the metadata sidecar

Create one `BlobDetails` message for each encrypted RecordIO blob. The
authoritative schema is
[blob_details.proto](../../src/main/proto/wfa/measurement/edpaggregator/v1alpha/blob_details.proto).

| Field | Requirement | Meaning |
| --- | --- | --- |
| `blob_uri` | Required | Full URI of the encrypted RecordIO blob |
| `encrypted_dek` | Required | Information needed to unwrap the DEK |
| `model_line` | Required | Full Halo `ModelLine` resource name |
| `interval` | Required | Time interval covered by the blob |
| `entity_keys` | Required for new writers | Entity IDs present in the data, grouped by entity type |
| `event_group_reference_id` | Legacy | Accepted for older blobs; new writers use `entity_keys` |

`BlobDetails.entity_keys` is an index of the values present in the data blob.
Every listed key must occur on at least one impression, and every impression
must carry at least one of the listed keys.

`BlobDetails.interval` must cover every impression in the blob. For a daily
batch, use a half-open UTC interval from `YYYY-MM-DDT00:00:00Z` through the next
date at `00:00:00Z`.

The nested `EncryptedDek` contains:

| Field | Meaning |
| --- | --- |
| `kek_uri` | URI of the publisher's KMS key |
| `ciphertext` | Wrapped DEK representation selected by `type_url` and `protobuf_format` |

Use one of the two supported `type_url` and `protobuf_format` pairs:

| `type_url` | `protobuf_format` |
| --- | --- |
| `type.googleapis.com/google.crypto.tink.Keyset` | `BINARY` |
| `type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey` | `JSON` |

For the `Keyset` and `BINARY` combination, `ciphertext` must contain the
serialized `google.crypto.tink.EncryptedKeyset` produced by
`TinkProtoKeysetFormat.serializeEncryptedKeyset`. It is **not** the raw
ciphertext returned by calling KMS encrypt on key bytes.

For the `EncryptionKey` and `JSON` combination, serialize the `EncryptionKey`
message containing an `AesGcmHkdfStreamingKey` or `AesGcmKey` as protobuf JSON.
Encrypt those JSON bytes with the KEK AEAD using empty associated data, and put
that ciphertext in `EncryptedDek.ciphertext`.

These formats describe only the wrapped DEK representation. The impression
records themselves are always binary `LabeledImpression` protobuf messages.

Serialize `BlobDetails` as binary protobuf or JSON. Its filename must contain
the case-insensitive substring `metadata` **and** end with the lowercase suffix
`.binpb` or `.json`, for example `metadata.binpb` or
`metadata-campaign-123.json`.

Do not upload a bare `EncryptedDek` as the sidecar. EDPA expects the complete
`BlobDetails` message.

## 4. Upload one date

The operator supplies the exact prefix. The canonical layout is:

```text
gs://<BUCKET>/<LABELED_PREFIX>/model-line/<MODEL_LINE_ID>/<YYYY-MM-DD>/
  impressions.enc.recordio
  metadata.binpb
  done
```

The encrypted data blob may use another directory tree within the same
operator-provided bucket if `BlobDetails.blob_uri` points to it. Cross-bucket
references are not currently supported. Co-locating the three objects is easier
to operate.

For each date:

1. Upload every encrypted RecordIO blob.
2. Upload a `BlobDetails` sidecar for every blob.
3. Verify that each sidecar has the correct blob URI, model line, interval,
   encrypted DEK, and entity-key index.
4. Upload an empty object named exactly `done` **last**.
5. Do not add, replace, or remove objects after `done` is published.

The model-line directory uses the ID at the end of the full `ModelLine`
resource name. It must agree with `BlobDetails.model_line`.

## 5. Validate before publishing `done`

Validate the encrypted blob and metadata yourself before uploading `done`. The
repository's `VerifySyntheticData` command also works for non-synthetic,
publisher-produced labeled impressions. From a repository checkout, run:

```bash
bazel run \
  //src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/tools:VerifySyntheticData \
  -- \
  --kms-type=GCP \
  --kek-uri='<EDP_KEY_URI>' \
  --metadata-uri='gs://<BUCKET>/<LABELED_PREFIX>/model-line/<MODEL_LINE_ID>/<YYYY-MM-DD>/metadata.binpb' \
  --event-message-type-url='type.googleapis.com/<MARKET_EVENT_MESSAGE>' \
  --event-message-descriptor-set='<FILE_DESCRIPTOR_SET>'
```

Run it with a publisher-controlled validation identity that has read access to
the upload objects and decrypt access to the KEK. Prefer keyless application
default credentials, such as service-account impersonation or workload identity,
over a downloaded service-account key. The publisher's ordinary upload identity
does not need decrypt access.

The command also supports AWS and GCP-to-AWS KMS. Run the same target with
`--help` for their required identity, role, and region flags.

A successful run reports `PASS` and the number of decrypted records. It rejects
non-positive VIDs; missing timestamps or events; unexpected event types;
unparseable event payloads; and empty entity-key fields. By exercising DEK
unwrapping, associated-data decryption, RecordIO parsing, and protobuf parsing,
it also exposes the wire-format failures described above. Do not publish `done`
until every metadata sidecar passes.

After self-validation, confirm all of the following with the operator:

1. EDPA discovered every metadata sidecar.
2. The publisher's attested ResultsFulfiller can unwrap the DEK and read the
   RecordIO file.
3. Halo published data availability for the expected date and model line.
4. A test measurement selects the expected impressions.
5. If TrusTEE is enabled, a TrusTEE test measurement completes successfully.

## Common problems

| Symptom | First check |
| --- | --- |
| Date is not registered | Path, model-line ID, and lowercase empty `done` object |
| Metadata is ignored | Filename contains `metadata`, ends in `.binpb` or `.json`, and is non-empty |
| Metadata parsing fails | Sidecar is `BlobDetails`, not a bare `EncryptedDek` |
| KMS permission denied before RecordIO parsing | For the `Keyset`/`BINARY` form, `ciphertext` is a serialized Tink `EncryptedKeyset`, not raw KMS ciphertext |
| Streaming decryption fails | The exact object key used as associated data matches `BlobDetails.blob_uri`; the object was not moved after encryption |
| RecordIO cannot be read | Each frame is ASCII byte length, newline, then bare protobuf bytes without a protobuf-delimited prefix |
| Measurement returns zero | Campaign entity key matches at least one key on each relevant impression and in `BlobDetails` |

For campaign registration and TrusTEE key setup, see the
[General EDP Onboarding Guide](edp-onboarding.md). For a stuck measurement, use
the [Report Debugging Guide](report-debugging-guide.md).

## For market operators

Market-specific onboarding material should link to this guide for the wire
format instead of copying it. Keep only market-owned values in the market guide:
bucket and prefix assignments, KEK and workload identities, event descriptors,
model lines, entity-key conventions, contacts, and timelines.
