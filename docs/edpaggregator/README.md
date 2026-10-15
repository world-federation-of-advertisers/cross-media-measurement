# Onboard a publisher to the EDP Aggregator

This guide explains how a publisher joins Halo cross-media measurement using the
EDP Aggregator (EDPA). In Halo documentation, a publisher that supplies impression
data is called an **Event Data Provider (EDP)**.

This guide covers the recommended integration:

* the publisher uploads raw impressions as encrypted Parquet files;
* EDPA assigns Virtual Person IDs (VIDs) inside trusted hardware; and
* cross-publisher measurements use the TrusTEE protocol.

> **Already assigning VIDs yourself?** Use
> [Upload already VID-labeled impressions](vid-labeled-impression-upload.md).
> That path uses encrypted RecordIO files and metadata sidecars, not Parquet.

After onboarding, the publisher has two recurring responsibilities:

1. Publish campaign information when campaigns are added, changed, or removed.
2. Upload each day's encrypted Parquet files, followed by an empty `done`
   object.

The market operator runs the VID-labeling and measurement services. The publisher
does not deploy Halo software, run the VID model, or respond to individual
measurement requests.

## Contents

1. [How the integration works](#1-how-the-integration-works)
2. [Exchange onboarding information](#2-exchange-onboarding-information)
3. [Set up encryption and workload trust](#3-set-up-encryption-and-workload-trust)
4. [Agree on the Parquet contract](#4-agree-on-the-parquet-contract)
5. [Publish campaign information](#5-publish-campaign-information)
6. [Upload daily impressions](#6-upload-daily-impressions)
7. [Validate onboarding](#7-validate-onboarding)
8. [Operate the feed](#8-operate-the-feed)
9. [Troubleshooting](#9-troubleshooting)
10. [Other EDPA guides](#10-other-edpa-guides)

## 1. How the integration works

The publisher sends two independent feeds to one or more private Cloud Storage
locations supplied by the operator:

```text
Campaign information ──────────────► Halo registers the campaigns

Encrypted Parquet + done ──────────► EDPA assigns VIDs in trusted hardware
                                    ► EDPA writes encrypted labeled impressions
                                    ► Halo records that the date is available
                                    ► EDPA answers measurement requests
                                    ► TrusTEE combines publisher contributions
```

The impression columns remain encrypted in storage. They are decrypted only
inside Google Confidential Space, a trusted execution environment (TEE). Before
the publisher's cloud key manager grants access, the workload presents a
hardware-signed attestation describing the software and environment that are
running.

Storage access does not grant decryption access. The publisher keeps control of
the encryption key and can revoke future use by changing the key policy.

### What each organization owns

| The publisher owns | The market operator owns |
| --- | --- |
| The encryption key in the publisher's cloud account | The private buckets and assigned upload prefixes |
| The policy deciding which trusted workloads may use the key | Registration of the publisher in Halo |
| The raw-impression and campaign exports | Configuration of VID models and Parquet field mappings |
| The identity used to upload files | The trusted VID-labeling, fulfillment, and TrusTEE workloads |

The publisher never gives the operator a private encryption key or a downloaded
cloud credential.

### This guide assumes TrusTEE is enabled

TrusTEE support is a capability enabled during onboarding; it is not mandatory
for every possible Halo integration. This guide assumes the publisher and market
have chosen to enable it. After it is enabled, EDPA automatically fulfills
eligible TrusTEE requisitions rather than asking the publisher to approve each
request.

## 2. Exchange onboarding information

The operator gives the publisher:

* its Halo `DataProvider` resource name;
* the bucket URIs and exact prefixes for campaign and raw-impression uploads;
* the trusted workload projects, runtime identities, and official Halo
  image-signing fingerprint needed by the publisher's key policy;
* the event fields and identity signals required by the market's VID model;
* the event fields that measurement filters may use;
* the active versions of the VID model, called model lines; and
* the campaign identifier that connects an impression to campaign information.

The publisher gives the operator:

* the cloud identity that will upload files;
* its KMS key URI and cloud-specific federation settings;
* a sample Parquet schema;
* the mapping from Parquet columns to model, event, and campaign fields; and
* whether TrusTEE will reuse the primary key or use a second re-encryption key.

Onboarding is per market. Keys, models, identifiers, and storage locations may
differ between markets.

## 3. Set up encryption and workload trust

The publisher creates a symmetric key in its own Google Cloud or AWS account.
This is the key-encryption key (KEK), sometimes called the master key.

Use separate identities for the publisher's export and Halo's trusted workloads:

| Identity | Required key access | Why |
| --- | --- | --- |
| Publisher export | Encrypt | Writes PME-encrypted Parquet |
| VID-labeling workloads | Encrypt and decrypt | Read raw impressions and protect intermediate and labeled output |
| ResultsFulfiller | Decrypt, plus encrypt for TrusTEE | Reads labeled data and encrypts the TrusTEE contribution |
| TrusTEE workload | Decrypt | Reads the encrypted contribution inside its TEE |

Grant the publisher export decrypt access only if a publisher-operated validation
process needs it.

The publisher may reuse the primary key for TrusTEE or create a dedicated
re-encryption key. A second key provides independent rotation and artifact
isolation. It does not by itself separate IAM roles if the same federated
identity has encrypt and decrypt access to both keys. Follow the cloud-specific
placement rules: for example, a dedicated Google Cloud re-encryption key must be
on the same key ring as the primary key.

### Trust workload classes, not individual jobs

The publisher does not approve each queue item, VM, or measurement request.
Instead, its policy accepts approved classes of attested workloads.

The production policy should verify:

* genuine Confidential Space;
* a stable, non-debug Confidential Space image;
* a container image signed by the approved Halo signing key;
* an approved operator project; and
* the approved runtime service account or image identity, when exposed by the
  cloud's attestation path.

For this integration, the approved workloads are:

* VidLabeler and, for memoized model lines, SubpoolAssigner and VidRankBuilder;
* ResultsFulfiller; and
* the TrusTEE aggregation workload.

The operator supplies the exact identities enabled in that market. Do not
allowlist an unused workload merely because it exists in another deployment.

The signing key identifies an approved publisher of builds; it does not pin one
exact container build. A new build signed by that same key can satisfy the
signature condition, so the project and workload-identity conditions are also
important.

Do not grant the operator's human accounts, upload functions, file watchers, or
general-purpose VMs access to the key.

For cloud-specific setup, use:

* [Google Cloud KMS setup](edp-onboarding.md#22-kms-setup--google-cloud); or
* [AWS KMS setup](aws-kms-setup.md). Direct Confidential Space attestation is
  the preferred AWS path for a new integration.

For Google Cloud, follow the linked guide's
[EDPA-managed VID-labeling variant](edp-onboarding.md#edpa-managed-vid-labeling-variant).
For either cloud, allow only the VID-labeling, ResultsFulfiller, and TrusTEE
workload classes enabled for this market. Use exact runtime service accounts
when the attestation path exposes them. Direct AWS attestation instead uses the
approved workload projects together with the image-signature claims.

## 4. Agree on the Parquet contract

Complete the mapping before building the production export. Raw impressions do
not contain a VID. They are flat Parquet files with one row per impression.

Column names belong to the publisher. The operator maps them into the market's
VID model and event schema. The names below are examples from the reference
writer, not required names.

| Concept | Reference column | Parquet type | Requirement |
| --- | --- | --- | --- |
| Unique impression ID | `event_id` | STRING | Required; mapped to `event_id.id` |
| Impression time | `event_time_usec` | INT64 | Required; Unix epoch microseconds in UTC |
| Person or device input | `person_id` | STRING | As required by the VID model |
| Demographic/model input | `person_gender`, `person_age_group`, … | Configured type | As required by the VID model |
| Campaign/entity ID | `creative_id`, … | STRING | At least one entity-key mapping; required mappings must be non-empty |
| Measurement filter input | `video_completed_fraction`, … | Configured type | Required when a measurement filter reads it |

The reference writer calls the video completion column
`video_completed_fraction`. A publisher may use another name if the operator's
mapping points the event field to that column.

Every configured column must exist with the agreed type. A renamed, missing, or
retyped mapped column fails schema validation when EDPA opens the file. A filter
field that is not mapped can instead remain at its event-schema default and cause
impressions to fail that filter, so agree on filter inputs explicitly.

The operator configures:

* how columns populate the VID model input;
* how columns populate the market's event schema;
* required and optional entity-key mappings;
* enum and age-bucket translations, where needed; and
* the active model lines and their population definitions.

### File dates and size

Each file must represent exactly one UTC calendar date. A date may contain any
number of files.

Aim to keep each Parquet object at or below approximately **1 GB**. This is a
best-practice target, not a protocol limit. A file is the smallest unit the
pipeline can assign to a worker, so several moderately sized files allow more
parallelism than one very large file. The operator-configured
`max_file_batch_size_bytes` controls how files are packed into a labeling job
and may require a smaller target. Confirm the target with the operator because
one oversized file cannot be split across workers.

Splitting by campaign and date is a useful default because it produces natural
backfill and correction boundaries.

### Plaintext footer

Every file's Parquet key-value metadata must contain:

| Key | Value |
| --- | --- |
| `event_date` | The file's UTC date in `YYYY-MM-DD` format |

The schema and footer remain plaintext so EDPA can register the file and select
the applicable model without decrypting the impression columns.

### Parquet Modular Encryption

Encrypt the data columns using Parquet Modular Encryption (PME), with the
publisher's KMS key URI as the uniform master key. Leave the footer plaintext:

```text
parquet.encryption.uniform.key      = <EDP_KEY_URI>
parquet.encryption.plaintext.footer = true
```

`<EDP_KEY_URI>` is a URI such as `gcp-kms://...` or `aws-kms://...`.
The exact library integration depends on the publisher's data platform. The
repository's
[reference writer](../../src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/testing/RawImpressionsWriter.kt)
shows the contract used by EDPA.

Do not create a metadata sidecar for raw Parquet. EDPA registers the Parquet
objects when it sees `done` and creates the encrypted labeled output and its
metadata later.

### Match impressions to campaigns

Halo calls a registered campaign an `EventGroup`. New integrations identify it
with an `entity_key`:

```text
entity type + entity ID
```

For example, a market may use entity type `campaign` and the publisher's
campaign ID. The Parquet mapping must produce the same entity type and ID on the
related impressions.

If the campaign and impressions use different identifiers, the files can process
successfully while measurements return no matching impressions.

## 5. Publish campaign information

Publish campaign information before the first measurement, then again when a
campaign is added, changed, or removed. It is not part of every daily upload.

The operator provides a path similar to:

```text
gs://<BUCKET>/<EDP_PREFIX>/event-groups/<FILENAME>.json
gs://<BUCKET>/<EDP_PREFIX>/event-groups/<FILENAME>.binpb
```

Campaign information may be JSON or protobuf. JSON files contain an
`EventGroups` wrapper. Protobuf files contain RecordIO-framed `EventGroup`
messages. Filenames must end in the lowercase suffix `.json` or `.binpb`;
otherwise EDPA ignores them. The authoritative schema is
[event_group.proto](../../src/main/proto/wfa/measurement/edpaggregator/eventgroups/v1alpha/event_group.proto).

Each campaign includes:

* brand and campaign names;
* the interval for which impression data is available;
* media types such as video or display;
* the advertiser identity; and
* the entity key that connects it to Parquet rows.

Identify the advertiser by setting either:

1. `measurement_consumer` to the advertiser's Halo resource name; or
2. `client_account_reference_id` to the publisher's account ID after the
   operator links it to the advertiser. See the
   [Self-Serve Onboarding Guide](self-serve-onboarding.md).

Use `entity_key` for new integrations. `event_group_reference_id` is the
legacy alternative.

Omitting a campaign from a later file does not delete it. To remove a campaign,
upload an `EventGroup` tombstone with `state` set to `DELETED`, a complete
`entity_key`, and an explicit `measurement_consumer`. A
`client_account_reference_id` alone is not sufficient for a deletion.

## 6. Upload daily impressions

The operator provides the raw-impression prefix. A typical date folder is:

```text
gs://<BUCKET>/<RAW_IMPRESSION_PREFIX>/<YYYY-MM-DD>/part-000.parquet
gs://<BUCKET>/<RAW_IMPRESSION_PREFIX>/<YYYY-MM-DD>/part-001.parquet
gs://<BUCKET>/<RAW_IMPRESSION_PREFIX>/<YYYY-MM-DD>/done
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

1. registers the raw files and selects the applicable model lines;
2. decrypts and labels impressions inside trusted hardware;
3. writes encrypted, VID-labeled impressions and metadata sidecars;
4. publishes labeled-output `done` objects; and
5. registers data availability with Halo.

For additive backfills and corrections, follow the
[Reference VID impression upload guide](reference-vid-impression-upload-guide.md).
Do not overwrite bad data or rewrite `done` before coordinating a correction
with the operator.

## 7. Validate onboarding

Test one small but representative date end to end. A successful upload alone is
not sufficient.

Confirm:

1. **Campaign registration:** the campaign is active in Halo and belongs to the
   correct advertiser.
2. **Raw upload:** EDPA registered every expected Parquet object and date.
3. **VID labeling:** processing completed for every expected model line.
4. **Data availability:** Halo shows the date as available.
5. **Measurement:** a TrusTEE test measurement completes and includes the
   campaign.
6. **Key audit:** cloud audit logs show only the expected federated identity or
   role using the key through the approved attestation path.

If a test returns zero, first compare the campaign's entity key with one Parquet
row, confirm the measurement interval overlaps `event_date`, and check every
field used by the measurement filter.

## 8. Operate the feed

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

## 9. Troubleshooting

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

## 10. Other EDPA guides

### Publisher integration

| Guide | Purpose |
| --- | --- |
| [Upload already VID-labeled impressions](vid-labeled-impression-upload.md) | Upload encrypted RecordIO when the publisher assigns VIDs |
| [General EDP Onboarding Guide](edp-onboarding.md) | Full KMS, schema, upload, and optional TrusTEE reference |
| [AWS KMS Setup Guide](aws-kms-setup.md) | Configure either supported AWS KMS federation path |
| [Reference VID impression upload guide](reference-vid-impression-upload-guide.md) | Backfill or correct raw Parquet uploads |
| [Dashboard EDP Onboarding Guide](dashboard/onboarding-guide.md) | Obtain access to publisher dashboard data |

### Market operation

| Guide | Purpose |
| --- | --- |
| [Deployment Guide](deployment-guide.md) | Deploy and configure EDPA |
| [Metadata Operator Guide](metadata-operator-guide.md) | Operate requisition and impression bookkeeping |
| [Report Debugging Guide](report-debugging-guide.md) | Trace a report end to end |
| [Self-Serve Onboarding Guide](self-serve-onboarding.md) | Link publisher account IDs to advertisers |
| [High Overlap Data Shape](high-overlap-data-shape.md) | Understand the high-overlap synthetic dataset |
| [Dashboard Deployment Guide](dashboard/deployment-guide.md) | Deploy the reporting dashboard |

The guides use placeholders such as `<BUCKET>`, `<EDP_PREFIX>`, and
`dataProviders/<DATA_PROVIDER_ID>`. Replace them with values supplied by the
market operator.
