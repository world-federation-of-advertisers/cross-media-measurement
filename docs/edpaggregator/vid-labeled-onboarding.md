# Onboard with already VID-labeled impressions

Use this guide after completing the [common EDP onboarding steps](README.md),
when the EDP assigns a Virtual Person ID (VID) to every impression before
uploading it to the EDP Aggregator (EDPA).

> **Impression blobs are never JSON.** They contain binary
> `LabeledImpression` protobuf messages in Mesos RecordIO framing. Only campaign
> files and `BlobDetails` metadata sidecars may use JSON.

This is the alternative to EDPA-managed VID labeling:

| EDPA-managed labeling | EDP-managed labeling |
| --- | --- |
| Input is PME-encrypted Parquet | Input is envelope-encrypted RecordIO |
| EDPA assigns VIDs | Every input record already has a VID |
| EDPA creates labeled metadata | The EDP creates a `BlobDetails` sidecar |

After the labeled data are registered, both paths use the same measurement
pipeline. See [How data moves through the EDP Aggregator](data-flow.md) for the
data-availability, ResultsFulfiller, and TrusTEE stages.

For the recommended alternative, use
[Onboard with encrypted Parquet containing pre-labeled reference VIDs](parquet-onboarding.md).
In that path, the EDP uploads pre-labeled reference VIDs and EDPA assigns the
final VIDs.

## Contents

1. [Before uploading](#before-uploading)
2. [Build the labeled records](#1-build-the-labeled-records)
3. [Encrypt the RecordIO file](#2-encrypt-the-recordio-file)
4. [Create the metadata sidecar](#3-create-the-metadata-sidecar)
5. [Upload one date](#4-upload-one-date)
6. [Validate before publishing `done`](#5-validate-before-publishing-done)
7. [Common problems](#common-problems)

## Before uploading

Obtain these values from the market operator:

* the EDP's Halo `DataProvider` resource name;
* the full `ModelLine` resource name;
* the storage bucket and labeled-impression prefix;
* the market's event protobuf type and descriptor;
* the campaign/entity-key convention; and
* the trusted ResultsFulfiller and, if enabled, TrusTEE workload identities.

The common guide defines the EDP-owned KMS key, attestation policy, storage
access, campaign publication, and optional TrusTEE setup. Publish the campaign's
`EventGroup` before using its entity key in this guide.

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
| `vid` | Required | VID assigned by the EDP |
| `event` | Required | An `Any` containing the market's event message |
| `entity_keys` | Required for new writers | One or more EDP entity identifiers used to select and filter impressions |
| `event_group_reference_id` | Legacy | Older campaign reference; new writers use `entity_keys` |

An entity key contains:

* `entity_type`: a URL-safe type such as `campaign`, `creative`, or
  `placement`; and
* `entity_id`: the EDP's URL-safe identifier for that entity.

Here, URL-safe means the unreserved characters `A-Z`, `a-z`, `0-9`, `.`, `_`,
`~`, and `-`. Do not use spaces, `/`, `%`, or already URL-encoded values.

At least one entity key on each impression must match the `entity_key` of the
campaign registered with Halo. Otherwise, the file may process successfully
while measurements find no matching impressions.

## 2. Encrypt the RecordIO file

For each encrypted blob:

1. Generate a fresh Tink Streaming AEAD keyset to use as the data-encryption
   key (DEK).
   The reference writer uses the `AES128_GCM_HKDF_1MB` Streaming AEAD template,
   which avoids buffering an entire large blob.
2. Serialize each `LabeledImpression` as binary protobuf and write the records
   using Mesos RecordIO framing.
3. Encrypt each complete RecordIO stream with the matching Tink Streaming AEAD
   primitive. Use the blob's exact object key—the path within the bucket—as
   associated data.
4. Wrap the serialized DEK with the EDP's KMS key.

EDPA uses the exact object key parsed from `BlobDetails.blob_uri` as the
associated data; the bucket name and `gs://` scheme are not included. For
`gs://example-bucket/edp/impressions.enc.recordio`, the associated data is
`edp/impressions.enc.recordio`. The URI must point to the uploaded object
in the same operator-provided bucket, and its object key must exactly match the
key used during encryption. Renaming the object or moving it to another
directory changes its key, so it must be re-encrypted using the new key.

The EDP keeps the KMS key in its own cloud account. The wrapped DEK is
recorded in every metadata sidecar for a blob protected by that DEK; the
plaintext DEK is not uploaded.

For JVM export code, use the Maven artifact published to the repository's
[GitHub Packages registry](https://github.com/world-federation-of-advertisers/cross-media-measurement/packages)
for each Halo release:
`org.wfanet.measurement.edpaggregator:labeled-impression-writer:<HALO_VERSION>`.
Its
[`LabeledImpressionWriter`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/client/LabeledImpressionWriter.kt)
generates a fresh streaming DEK, applies the correct associated data, writes
Mesos RecordIO, derives the sidecar's entity-key index from the records, and
writes the binary `BlobDetails` sidecar only after the encrypted blob succeeds.
It uses create-only writes, removes objects created by a failed invocation, and
refuses to overwrite an existing blob or sidecar.
Use the artifact version that matches the Halo release selected with the
operator; it is the release tag without the leading `v`. The
[`LabeledImpressionWriterTest`](../../src/test/kotlin/org/wfanet/measurement/edpaggregator/client/LabeledImpressionWriterTest.kt)
shows the supported Cloud Storage and local-filesystem APIs.

### Blob size and campaign splitting

Ask the operator for the per-blob target before choosing a shard size. A useful
starting target to discuss is approximately **1 GB**. In some deployments, the
operator may approve blobs up to approximately **2 GB** after considering
ResultsFulfiller worker capacity. These are operational targets, not protocol
limits; the operator-provided target always takes precedence.

Prefer one campaign per blob for one UTC date. If one campaign's data exceeds
the approved target, split it across as many blobs as needed to stay within the
target. A campaign may therefore span multiple blobs, but avoid splitting a
smaller campaign without an operational reason. Every blob needs its own fresh
DEK and `BlobDetails` sidecar.

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

`BlobDetails.entity_keys` is the exact set of distinct `(entity_type,
entity_id)` pairs present in the data blob. Every listed pair must occur on at
least one impression, every pair on an impression must be listed, and every
impression must carry at least one entity key.

`BlobDetails.interval` must cover every impression in the blob. For a daily
batch, use a half-open UTC interval from `YYYY-MM-DDT00:00:00Z` through the next
date at `00:00:00Z`.

The nested `EncryptedDek` contains:

| Field | Meaning |
| --- | --- |
| `kek_uri` | URI of the EDP's KMS key |
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

Prefer placing each encrypted data blob in this date folder beside its
`BlobDetails` sidecar. `BlobDetails.blob_uri` may instead point to another
directory tree in the same operator-provided bucket. Cross-bucket references are
not currently supported.

For each date:

1. Upload every encrypted RecordIO blob.
2. Upload a `BlobDetails` sidecar for every blob.
3. Verify that each sidecar has the correct blob URI, model line, interval,
   encrypted DEK, and entity-key index.
4. Upload an empty object named exactly `done` **last**.
5. Do not add, replace, or remove objects after `done` is published.

The model-line directory uses the ID at the end of the full `ModelLine`
resource name. It must agree with `BlobDetails.model_line`.

When one campaign is split across multiple blobs, give every blob and sidecar a
unique name in the same date folder, for example
`campaign-123-part-000.enc.recordio` with
`metadata-campaign-123-part-000.binpb`. Upload all parts and sidecars before the
single `done` object for that date.

## 5. Validate before publishing `done`

Validate the encrypted blob and metadata yourself before uploading `done`.
Download `VerifyVidLabeledImpressions_deploy.jar` from the project's
[Halo releases](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases),
then run:

```bash
java -jar VerifyVidLabeledImpressions_deploy.jar \
  --kms-type=GCP \
  --kek-uri='<EDP_KEY_URI>' \
  --metadata-uri='gs://<BUCKET>/<LABELED_PREFIX>/model-line/<MODEL_LINE_ID>/<YYYY-MM-DD>/metadata.binpb' \
  --event-message-type-url='type.googleapis.com/<MARKET_EVENT_MESSAGE>' \
  --event-message-descriptor-set='<FILE_DESCRIPTOR_SET>'
```

Run it with an EDP-controlled validation identity that has read access to
the upload objects and decrypt access to the KEK. Prefer keyless application
default credentials, such as service-account impersonation or workload identity,
over a downloaded service-account key. The EDP's ordinary upload identity
does not need decrypt access.

The command also supports AWS and GCP-to-AWS KMS. Run the same command with
`--help` for their required identity, role, and region flags.

A successful run exits with status `0` and reports `PASS`, the number of
decrypted records, and impression counts for each distinct entity key. Legacy
sidecars also report event-group counts. Any failed blob makes the process exit
nonzero. The verifier rejects invalid model-line resource names; missing or
invalid sidecar intervals; event timestamps outside the sidecar's half-open
interval; non-positive VIDs; missing timestamps or events; impressions without
an entity key when the sidecar declares entity keys; empty entity-key fields;
non-URL-safe entity keys; and a sidecar entity-key index that differs from the
records. When the event type and descriptor flags shown above are supplied, it
also rejects unexpected event types and unparseable event payloads. By
exercising DEK unwrapping, associated-data decryption, RecordIO parsing, and
protobuf parsing, it also exposes the wire-format failures described above. Do
not publish `done` until every metadata sidecar passes.

After self-validation, confirm all of the following with the operator:

1. EDPA discovered every metadata sidecar.
2. The attested ResultsFulfiller authorized by the EDP can unwrap the DEK and
   read the RecordIO file.
3. Halo published data availability for the expected date and model line.
4. A test measurement selects the expected impressions.
5. If TrusTEE is enabled, a TrusTEE test measurement completes successfully.

## Common problems

| Symptom | First check |
| --- | --- |
| Date is not registered | Path, model-line ID, and lowercase empty `done` object |
| Metadata is not registered | Filename contains `metadata`, ends in lowercase `.binpb` or `.json`, and is non-empty |
| Metadata parsing fails | Sidecar is `BlobDetails`, not a bare `EncryptedDek` |
| KMS permission denied before RecordIO parsing | For the `Keyset`/`BINARY` form, `ciphertext` is a serialized Tink `EncryptedKeyset`, not raw KMS ciphertext |
| Streaming decryption fails | The exact object key parsed from `BlobDetails.blob_uri` matches the key used as associated data; re-encrypt after any rename or directory move |
| RecordIO cannot be read | Each frame is ASCII byte length, newline, then bare protobuf bytes without a protobuf-delimited prefix |
| Measurement returns zero | Campaign entity key matches at least one key on each relevant impression and in `BlobDetails` |

For campaign registration, KMS, and optional TrusTEE setup, see the
[common onboarding guide](README.md). For a stuck measurement, use the
[Report Debugging Guide](report-debugging-guide.md).
