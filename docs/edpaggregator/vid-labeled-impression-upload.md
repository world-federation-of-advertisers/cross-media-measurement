# Upload already VID-labeled impressions

Use this guide when the publisher assigns a Virtual Person ID (VID) to every
impression before uploading it to the EDP Aggregator (EDPA).

This is the alternative to EDPA-managed VID labeling:

| EDPA-managed labeling | Publisher-managed labeling |
| --- | --- |
| Input is PME-encrypted Parquet | Input is envelope-encrypted RecordIO |
| EDPA assigns VIDs | Every input record already has a VID |
| EDPA creates labeled metadata | The publisher creates a `BlobDetails` sidecar |

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
[General EDP Onboarding Guide](edp-onboarding.md) and, when applicable, the
[AWS KMS Setup Guide](aws-kms-setup.md).

## 1. Build the labeled records

The data file contains binary protobuf
`wfa.measurement.edpaggregator.LabeledImpression` messages framed with Mesos
RecordIO. Generate code from
[labeled_impression.proto](../../src/main/proto/wfa/measurement/edpaggregator/v1alpha/labeled_impression.proto)
rather than copying the table below as a schema.

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

1. Generate a fresh data-encryption key (DEK) suitable for Tink Streaming AEAD.
2. Serialize each `LabeledImpression` as binary protobuf and write the records
   using Mesos RecordIO framing. You may split the batch into multiple blobs.
3. Encrypt each RecordIO stream with the DEK.
4. Wrap the serialized DEK with the publisher's KMS key.

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
| `ciphertext` | Serialized DEK encrypted by `kek_uri` |

Use one of the two supported `type_url` and `protobuf_format` pairs:

| `type_url` | `protobuf_format` |
| --- | --- |
| `type.googleapis.com/google.crypto.tink.Keyset` | `BINARY` |
| `type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey` | `JSON` |

The format describes the wrapped key representation. The impression records
themselves are binary `LabeledImpression` protobuf messages.

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

## 5. Validate the upload

Confirm all of the following with the operator:

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
| KMS permission denied | KEK URI, federated identity, and attestation-policy conditions |
| RecordIO cannot be read | Binary protobuf records, framing, streaming encryption, and wrapped DEK all agree |
| Measurement returns zero | Campaign entity key matches at least one key on each relevant impression and in `BlobDetails` |

For campaign registration and TrusTEE key setup, see the
[General EDP Onboarding Guide](edp-onboarding.md). For a stuck measurement, use
the [Report Debugging Guide](report-debugging-guide.md).
