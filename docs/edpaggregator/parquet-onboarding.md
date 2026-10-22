# Onboard with encrypted Parquet containing pre-labeled reference VIDs

Use this guide after completing the [common EDP onboarding steps](README.md).
It covers the recommended integration:

* the EDP or its data exporter uploads pre-labeled reference VIDs as encrypted
  Parquet;
* EDPA assigns Virtual Person IDs (VIDs) inside trusted hardware; and
* trusted fulfillment workloads use the labeled data for measurements.

> **Already assigning VIDs yourself?** Use
> [Onboard with already VID-labeled impressions](vid-labeled-onboarding.md).
> That path uses encrypted RecordIO files and metadata sidecars, not Parquet.

After onboarding, the EDP has two recurring responsibilities:

1. Publish campaign information when campaigns are added, changed, or removed.
2. Upload each day's encrypted Parquet files, followed by an empty `done`
   object.

The market operator runs the VID-labeling and measurement services. The EDP does
not deploy Halo software, run the VID model, or respond to individual measurement
requests.

## Contents

1. [Agree on the Parquet contract](#1-agree-on-the-parquet-contract)
2. [Upload daily impressions](#2-upload-daily-impressions)
3. [Validate the Parquet integration](#3-validate-the-parquet-integration)
4. [Operate the feed](#4-operate-the-feed)
5. [Troubleshooting](#5-troubleshooting)
6. [Related guides](#6-related-guides)

## Before you begin

Complete the [common onboarding guide](README.md) first. It covers registration,
storage access, campaign metadata, KMS, attestation, and optional TrusTEE setup.
For this path, confirm that the operator has also provided:

* the pre-labeled reference VID upload prefix;
* the active model lines;
* the fields and identity signals required by the market's VID model;
* the event fields used by measurement filters; and
* the campaign/entity-key mapping.

## 1. Agree on the Parquet contract

Complete the mapping before building the production export. Pre-labeled
reference VIDs are identifiers used as input to VID labeling; they are not the
final VIDs assigned by EDPA. The input consists of flat Parquet files with one
row per impression.

### If an advertiser or another data aggregator prepares the export

An advertiser or another data aggregator can produce the daily files. EDPA
does not require a live database connector, a Halo SDK in the source system,
or an API call for each measurement. The integration boundary is the two file
feeds in this guide.

The impression feed must contain the approved **impression-level rows**, not an
aggregated reach, frequency, or campaign result. Each exported row needs the
identity inputs, event fields, and campaign identifier agreed with the operator.
If the exporter's policy does not permit those rows to leave in plaintext, the
Parquet encryption step must run inside its controlled environment and emit
only PME-encrypted files.

A native Parquet export is sufficient only if it can also:

* add the `event_date` file metadata described below;
* apply Parquet Modular Encryption with a plaintext footer; and
* produce files near the agreed size target.

If the native export cannot do all three, add an exporter-controlled step,
inside its controlled environment when policy requires it, that reads the
approved rows, validates and shards them, adds the footer, applies PME with the
EDP's KMS key, and writes the encrypted objects to the operator-provided
prefix. Do not write plaintext impression files to the operator's bucket. Use
a separate, prefix-scoped upload identity and publish `done` only after every
encrypted object for the date is present.

Campaign information remains a separate feed. It may come from the same
exporter or from the EDP's campaign system, but its entity keys must
match the values in the impression export.

Column names belong to the EDP. The operator maps them into the market's
VID model and event schema. The names below are examples from the reference
writer, not required names.

| Concept | Reference column | Parquet type | Requirement |
| --- | --- | --- | --- |
| Unique impression ID | `event_id` | STRING | Required; mapped to `event_id.id` |
| Impression time | `event_time_usec` | INT64 | Required; Unix epoch microseconds in UTC |
| Person or device input | `person_id` | STRING | As required by the VID model |
| Demographic/model input | `person_gender`, `person_age_group`, … | Configured type | As required by the VID model |
| Campaign/entity IDs | `campaign_id`, `adset_id`, `ad_id`, `account_id`, `creative_id` | STRING | One column per entity type; at least one populated mapped entity key per impression |
| Measurement filter input | `video_completed_fraction`, … | Configured type | Required when a measurement filter reads it |

The reference writer calls the video completion column
`video_completed_fraction`. An EDP may use another name if the operator's
mapping points the event field to that column.

An impression can carry account, campaign, ad-set, ad, and creative keys together.
Map each column to its entity type independently so every key is preserved on the
labeled impression. Each column contains one ID of that type for that impression;
IDs must match the corresponding EventGroups.

Required mapped columns must exist with the agreed type. Optional entity-key
columns may be absent, null, or empty, allowing older files to omit newer hierarchy
levels. A populated entity-key column must be a string. A filter field that is not
mapped can remain at its event-schema default and cause impressions to fail that
filter, so agree on filter inputs explicitly.

The operator configures:

* how columns populate the VID model input;
* how columns populate the market's event schema;
* required and optional entity-key mappings;
* enum and age-bucket translations, where needed; and
* the active model lines and their population definitions.

### File dates and size

Each file must represent exactly one UTC calendar date. A date may contain any
number of files.

Ask the operator for the per-file target and keep each Parquet object at or
below it. The target should not exceed the operator-configured
`max_file_batch_size_bytes`, which depends on worker capacity. An oversized
file is dispatched alone on a best-effort basis and cannot be split across
labeling jobs.

Approximately **1 GB** is a useful starting target to discuss with the operator.
In some deployments, the operator may approve files up to approximately **2
GB** after confirming the configured limit and worker capacity. These are
operational targets, not protocol limits; the operator-provided target always
takes precedence. Several moderately sized files also allow more parallelism
than one very large file.

Prefer one campaign per file for one UTC date. This produces clear backfill and
correction boundaries. If one campaign's data exceeds the approved target,
split that campaign across as many files as needed to stay within it. A campaign
may therefore span multiple files, but avoid splitting a smaller campaign
without an operational reason.

### Plaintext footer

Every file's Parquet key-value metadata must contain:

| Key | Value |
| --- | --- |
| `event_date` | The file's UTC date in `YYYY-MM-DD` format |

The schema and footer remain plaintext so EDPA can register the file and select
the applicable model without decrypting the impression columns.

### Parquet Modular Encryption

Encrypt the data columns using Parquet Modular Encryption (PME), with the
EDP's KMS key URI as the uniform master key. Leave the footer plaintext:

```text
parquet.encryption.uniform.key      = <EDP_KEY_URI>
parquet.encryption.plaintext.footer = true
```

`<EDP_KEY_URI>` is a URI such as `gcp-kms://...` or `aws-kms://...`.
The exact library integration depends on the EDP's data platform. The
repository's [EDP aggregator load-test writers](../../src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/testing/)
include a reference implementation of the contract used by EDPA.

Do not create a metadata sidecar for pre-labeled reference VID Parquet. EDPA
registers the Parquet objects when it sees `done` and creates the encrypted
labeled output and its metadata later.

### Match impressions to campaigns

Halo calls a registered campaign an `EventGroup`. New integrations identify it
with an `entity_key`:

```text
entity type + entity ID
```

For example, a market may use entity type `campaign` and the EDP's
campaign ID. The Parquet mapping must produce the same entity type and ID on the
related impressions.

If the campaign and impressions use different identifiers, the files can process
successfully while measurements return no matching impressions.

Campaign publication is common to both impression paths. Complete
[Publish campaign information](README.md#5-publish-campaign-information) before
validating that the mapped entity keys select impressions.

## 2. Upload daily impressions

The operator provides the pre-labeled reference VID prefix. A typical date
folder is:

```text
gs://<BUCKET>/<PRE_LABELED_REFERENCE_VID_PREFIX>/<YYYY-MM-DD>/part-000.parquet
gs://<BUCKET>/<PRE_LABELED_REFERENCE_VID_PREFIX>/<YYYY-MM-DD>/part-001.parquet
gs://<BUCKET>/<PRE_LABELED_REFERENCE_VID_PREFIX>/<YYYY-MM-DD>/done
```

For each date:

1. Write every encrypted Parquet file into the assigned date directory.
2. Confirm that every intended object is present and each `event_date` footer
   matches the directory date.
3. Write an empty object named exactly `done` **last**.
4. Do not add, replace, or remove files after publishing `done`.

`done` is the publication boundary. EDPA takes a snapshot of the directory and
starts processing it.

EDPA then:

1. registers the pre-labeled reference VID files and selects the applicable
   model lines;
2. decrypts and labels impressions inside trusted hardware;
3. writes encrypted, VID-labeled impressions and metadata sidecars;
4. publishes labeled-output `done` objects; and
5. registers data availability with Halo.

For additive backfills and corrections, follow the
[pre-labeled reference VID backfill and correction guide](reference-vid-impression-upload-guide.md).
Do not overwrite bad data or rewrite `done` before coordinating a correction
with the operator.

## 3. Validate the Parquet integration

Test one small but representative date end to end. A successful upload alone is
not sufficient.

Confirm:

1. **Campaign registration:** the campaign is active in Halo and belongs to the
   correct advertiser.
2. **Pre-labeled reference VID upload:** EDPA registered every expected Parquet
   object and date.
3. **VID labeling:** processing completed for every expected model line.
4. **Data availability:** Halo shows the date as available.
5. **Measurement:** an end-to-end test measurement completes and includes the
   campaign. If TrusTEE is enabled, test that path as well.
6. **Key audit:** cloud audit logs show only the expected federated identity or
   role using the key through the approved attestation path.

If a test returns zero, first compare the campaign's entity key with one Parquet
row, confirm the measurement interval overlaps `event_date`, and check every
field used by the measurement filter.

## 4. Operate the feed

For each day:

1. Generate Parquet files with the agreed schema.
2. Encrypt their columns with PME.
3. Check the schema, `event_date` footer, date directory, and file sizes.
4. Upload every Parquet file.
5. Upload `done` last.
6. Confirm that labeling and data availability complete.

When campaigns change or are removed, publish the update or explicit deletion
tombstone and confirm registration before relying on it in a measurement.

Coordinate key-policy changes before the operator rolls out new workload
identities, projects, or signing keys. Add the new trust values first, verify the
new build, and remove the old values after rollout.

## 5. Troubleshooting

| Symptom | Likely cause | First check |
| --- | --- | --- |
| Nothing happens after upload | Wrong prefix or missing `done` | Compare the full URI with the operator-provided prefix; confirm `done` is lowercase and empty |
| Registration fails | Missing `event_date`, unreadable footer, or schema mismatch | Read the plaintext footer and schema |
| KMS returns permission denied | Missing permission or mismatched attestation claim | Compare the project, workload identity, stable status, and signature with the key policy |
| Labeling succeeds but measurement is zero | Entity keys, dates, or filter fields do not match | Compare one campaign, one Parquet row, and the measurement filter |
| Direct fulfillment works but TrusTEE fails | TrusTEE is not trusted or its re-encryption mapping is wrong | Check the TrusTEE workload identity, key permission, and operator mapping |
| Previously published data is wrong | A finalized upload needs correction | Stop modifying the folder and follow the reference upload guide with the operator |

For deeper investigation, use the
[Report Debugging Guide](report-debugging-guide.md).

## 6. Related guides

* [Common EDP onboarding](README.md) — registration, KMS, attestation, campaign
  metadata, and shared validation.
* [Pre-labeled reference VID backfill and correction guide](reference-vid-impression-upload-guide.md)
  — backfill or correct a finalized pre-labeled reference VID upload.
* [Data flow and trust boundaries](data-flow.md) — follow Parquet input through
  VID labeling, fulfillment, and TrusTEE.
* [Report Debugging Guide](report-debugging-guide.md) — investigate a stuck or
  failed measurement after onboarding.
