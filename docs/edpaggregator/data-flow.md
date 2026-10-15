# How data moves through the EDP Aggregator

This guide follows impression data from a partner's export through the EDP
Aggregator (EDPA) and TrusTEE to a Halo measurement result. It is for technical
and security teams that want to understand the system before building or
approving an integration.

For the files to produce and the onboarding steps to complete, start with
[Onboard a publisher to the EDP Aggregator](README.md). This guide uses that
recommended path: encrypted Parquet input, EDPA-managed Virtual Person ID (VID)
labeling, and TrusTEE fulfillment.

A publisher may produce the export itself, or an advertiser or another data
aggregator may prepare it on the publisher's behalf. Halo identifies the data as
belonging to a registered Event Data Provider (EDP), regardless of which system
creates the files.

## Contents

1. [The complete flow](#1-the-complete-flow)
2. [The two input feeds](#2-the-two-input-feeds)
3. [What happens after upload](#3-what-happens-after-upload)
4. [Where the data can be decrypted](#4-where-the-data-can-be-decrypted)
5. [What attestation and key policy enforce](#5-what-attestation-and-key-policy-enforce)
6. [Alternative: upload already VID-labeled data](#6-alternative-upload-already-vid-labeled-data)
7. [Related documentation](#7-related-documentation)

## 1. The complete flow

Campaign information and impressions are separate feeds. They meet when an
impression's entity key matches the entity key of a campaign registered in
Halo.

### Simplified Halo flow

```mermaid
flowchart LR
  P["Publisher or data exporter"]

  subgraph STORAGE["Private object storage<br/>Sensitive payloads are KMS-encrypted"]
    EG["Campaign metadata<br/>Protobuf or JSON<br/>(IAM-protected)"]
    RAW["Raw impressions<br/>KMS-encrypted Parquet<br/>(recommended)"]
    LABELED["Already VID-labeled impressions<br/>KMS-encrypted RecordIO<br/>+ plaintext BlobDetails sidecar"]
  end

  HALO["Halo processing with separate trust boundaries<br/><br/>Campaign and data-availability registration<br/>VID Labeling (Confidential Space TEE)<br/>ResultsFulfiller (Confidential Space TEE)<br/>TrusTEE (separate Confidential Space TEE)"]
  RESULT["Aggregated result<br/>with protocol-defined<br/>privacy controls"]
  RS["Reporting Server / Kingdom"]

  P --> EG --> HALO
  P --> RAW -->|"Primary path"| HALO
  P --> LABELED -->|"Optional already-labeled path"| HALO
  HALO --> RESULT --> RS

  classDef publisher fill:#EAF2FF,stroke:#3974D0,stroke-width:2px,color:#111827,font-weight:bold;
  classDef input fill:#FFF2DA,stroke:#D88B16,stroke-width:2px,color:#111827;
  classDef halo fill:#EEF0FF,stroke:#6667B0,stroke-width:3px,color:#111827,font-weight:bold;
  classDef result fill:#FFF7E8,stroke:#B55B00,stroke-width:2px,color:#8A4B08,font-weight:bold;
  classDef server fill:#E8F5EC,stroke:#2E8B57,stroke-width:2px,color:#111827,font-weight:bold;

  class P publisher;
  class EG,RAW,LABELED input;
  class HALO halo;
  class RESULT result;
  class RS server;
  style STORAGE fill:#F7FAFC,stroke:#17827B,stroke-width:2px;
```

This view intentionally hides queues, markers, and intermediate files. “Halo
processing” is not one TEE: VID Labeling, ResultsFulfiller, and TrusTEE are
separately attested workloads, and TrusTEE has a separate trust boundary.

The primary path is encrypted Parquet. The already-labeled path is included only
to show where it joins the flow; its format is documented separately.

### Detailed EDP Aggregator and TrusTEE flow

```mermaid
flowchart LR
  P["Publisher or data exporter"]
  OTHER["Other participating EDPs"]
  ADV["Advertiser / report consumer"]
  RS["Reporting Server / Kingdom"]

  subgraph EDPA["EDP Aggregator"]
    direction TB

    subgraph INPUTS["Private object storage: uploaded inputs<br/>Sensitive payloads are KMS-encrypted"]
      direction TB
      EG["Campaign metadata<br/>Protobuf or JSON<br/>(IAM-protected)"]
      RAW["Raw impressions<br/>KMS-encrypted Parquet<br/>+ plaintext done marker"]
    end

    VL["VID Labeling<br/>Confidential Space TEE<br/>Maps fields and assigns VID"]
    STORE["VID-labeled impression store<br/>Private object storage<br/>KMS-encrypted RecordIO<br/>+ plaintext BlobDetails and done marker"]
    RP["ResultsFulfiller<br/>Confidential Space TEE<br/>Filters impressions and builds vector"]

    RAW -->|"Encrypted Parquet columns"| VL
    VL -->|"Encrypted labeled impressions"| STORE
    STORE -->|"Qualifying impressions"| RP
  end

  subgraph TRUSTEE["TrusTEE: separate trust boundary"]
    direction BT
    FVSTORE["KMS-encrypted frequency-vector handoff"]
    TT["TrusTEE processing<br/>Confidential Space TEE<br/>Combines publisher vectors"]
    FVSTORE -->|"Encrypted vectors"| TT
  end

  P -->|"Uploads"| EG
  P -->|"Primary path"| RAW
  P -->|"Optional: already labeled<br/>+ BlobDetails sidecar"| STORE
  EG -->|"Registers campaigns"| RS
  STORE -->|"Publishes data availability"| RS
  ADV -->|"Requests report"| RS
  RS -->|"Issues requisition"| RP
  RP -->|"Encrypted frequency vector"| FVSTORE
  OTHER -->|"Encrypted frequency vectors"| FVSTORE
  TT -->|"Aggregated result with<br/>configured privacy controls"| RS
  RS -->|"Completed report"| ADV

  classDef publisher fill:#EAF2FF,stroke:#3974D0,stroke-width:2px,color:#111827,font-weight:bold;
  classDef metadata fill:#EAF2FF,stroke:#5A87CC,stroke-width:2px,color:#111827;
  classDef tee fill:#FFF2DA,stroke:#C87512,stroke-width:2px,color:#111827;
  classDef storage fill:#E6F7F5,stroke:#17827B,stroke-width:2px,color:#111827;
  classDef server fill:#E8F5EC,stroke:#2E8B57,stroke-width:2px,color:#111827,font-weight:bold;
  classDef consumer fill:#F3E8FF,stroke:#8B4AC2,stroke-width:2px,color:#111827,font-weight:bold;

  class P,OTHER publisher;
  class EG metadata;
  class VL,RP,TT tee;
  class RAW,STORE,FVSTORE storage;
  class RS server;
  class ADV consumer;
  style INPUTS fill:#F7FAFC,stroke:#17827B,stroke-width:2px;
  style EDPA fill:#F7FAFC,stroke:#667085,stroke-width:2px;
  style TRUSTEE fill:#FFF9F0,stroke:#C87512,stroke-width:3px;
```

The detailed view keeps the same two input options as the source architecture:
raw Parquet passes through VID Labeling, while already-labeled RecordIO joins at
the VID-labeled impression store. Both paths converge before ResultsFulfiller
processes a requisition.

Each trusted workload is shown in its own Confidential Space TEE. Sensitive data
passes between ResultsFulfiller and TrusTEE as an encrypted contribution; the
two components do not share one TEE boundary.

Both diagrams use KMS labels only for sensitive payloads. Campaign metadata,
`BlobDetails`, and `done` markers remain readable to services authorized to
access the private storage location. Internal queues, retries, and
model-specific VID-labeling stages are omitted because they do not change the
export contract.

## 2. The two input feeds

| Feed | What the exporter sends | What connects it to the other feed |
| --- | --- | --- |
| Campaign information | `EventGroup` records describing campaigns and their data intervals | An `entity_key`, such as a campaign ID |
| Daily impressions | PME-encrypted Parquet with one row per impression, followed by a `done` marker | The same entity type and ID mapped from a Parquet column |

Campaign information may come from a different source system than the daily
impression export. It does not need to be uploaded every day, but it must be
published when a campaign is added, changed, or deleted.

The impression feed is a file boundary. The exporting system does not need a
live database connector, a Halo SDK, or an API call for each measurement. It
must produce the agreed impression-level fields, apply Parquet Modular
Encryption (PME), upload every file for the date, and create the raw-upload
`done` marker last.

See the onboarding guide for the authoritative
[Parquet contract](README.md#4-agree-on-the-parquet-contract),
[campaign format](README.md#5-publish-campaign-information), and
[daily upload layout](README.md#6-upload-daily-impressions). The market operator
provides the market-specific event schema and VID-model inputs; they are not a
universal EDPA schema.

## 3. What happens after upload

### 1. Halo registers campaign information

The EventGroup sync service reads the campaign file and creates, updates, or
deletes the corresponding `EventGroup` resources in Halo. These records tell
Halo which advertiser owns the campaign, when data are available, and which
entity key identifies its impressions.

### 2. The raw-upload marker closes the daily export

When the raw-upload `done` marker arrives, EDPA lists the Parquet objects in that
date's directory, reads their plaintext footers, records their exact object
versions, and determines which VID model lines apply. The marker is a completion
signal; it contains no impression data.

Writing a new `done` object after correcting the directory creates a new upload
revision. EDPA does not silently add later files to a revision that is already
being processed.

### 3. EDPA assigns VIDs in trusted hardware

EDPA dispatches the registered files to the VID-labeling workloads.
Inside Google Confidential Space, the workload presents an attestation to the
data owner's key policy, decrypts the protected Parquet columns, and applies the
configured VID model.

The workload writes:

* encrypted RecordIO containing `LabeledImpression` messages;
* a `BlobDetails` metadata record for each encrypted blob; and
* a labeled-output `done` marker after the output is complete.

The labeled records contain the event data needed for measurement, the assigned
VID, and the entity keys used to associate impressions with campaigns. The
encrypted output can be stored and processed later without keeping the raw
columns decrypted.

### 4. Halo records which dates are available

The labeled-output marker starts a separate synchronization step. EDPA reads the
`BlobDetails` records, stores the impression metadata, and publishes the
available time intervals for each model line to Halo.

This tells Halo which dates and model lines have labeled data ready for
measurement.

### 5. Halo sends a measurement requisition

When a report needs this EDP's data, Halo creates a requisition. EDPA retrieves
the requisition, groups compatible work, and dispatches it to ResultsFulfiller.
The data exporter does not approve or process each requisition.

### 6. ResultsFulfiller builds the EDP's contribution

ResultsFulfiller runs in its own Confidential Space TEE. It decrypts the
requisition specification and the relevant labeled-impression blobs, then:

1. selects data in the requested time interval;
2. matches the requested campaigns using entity keys;
3. applies the requested event filters; and
4. counts the qualifying impressions by VID in a frequency vector.

For a non-empty TrusTEE contribution, ResultsFulfiller encrypts that vector for
TrusTEE before sending it outside the EDPA TEE.

### 7. TrusTEE computes the measurement result

TrusTEE is a separate Confidential Space workload. It decrypts the submitted
frequency-vector contributions, combines them, and computes the protocol result.
The selected protocol version and deployment configuration determine exactly
where minimum-threshold and differential-privacy operations occur; some controls
may be applied before the handoff and others inside TrusTEE.

TrusTEE sends the result through Halo for reporting. The result contains the
requested aggregate measurement, not impression-level records.

## 4. Where the data can be decrypted

Cloud-storage access and decryption access are separate. An operator service may
need to list an object or read its metadata without being able to decrypt its
impression payload.

| Artifact | Created by | Protection and visibility | Workload that can decrypt the sensitive payload |
| --- | --- | --- | --- |
| Campaign metadata | Data exporter | Kept in private operator-provided storage; it is not protected by the impression KEK | No impression-payload decryption is involved |
| Raw Parquet | Data exporter | Configured data columns are protected with PME under the data owner's KMS key; the schema, footer, `event_date`, object name, and object size are not protected by PME | Enabled VID-labeling TEE workloads |
| Raw-upload `done` marker | Data exporter | Plaintext completion signal containing no impression records | None |
| VID-labeled RecordIO | VID-labeling TEE | Payload is envelope-encrypted under the data owner's KMS key | ResultsFulfiller TEE |
| `BlobDetails` | VID-labeling TEE | Metadata sidecar containing the blob URI, interval, entity keys, model line, and encrypted data-encryption key; it does not contain impression records | None; the wrapped key is used by an authorized KMS client |
| TrusTEE contribution | ResultsFulfiller TEE | A non-empty frequency vector is envelope-encrypted for TrusTEE | TrusTEE TEE |
| Measurement result | TrusTEE and Halo | Aggregate protocol result delivered through Halo | The authorized result recipient under the selected protocol |

The operator's private bucket, IAM policy, and transport security protect all of
these objects from unauthorized access. The data owner's KMS policy adds a
separate control over the sensitive impression payloads.

## 5. What attestation and key policy enforce

The data owner configures its cloud key manager once during onboarding. It does
not approve individual files, jobs, or measurement requests.

Before a trusted workload can use the key, its Confidential Space environment
presents hardware-backed attestation claims. A production policy can require:

* a genuine, non-debug Confidential Space environment;
* an image signed by the approved Halo signing key;
* an approved operator project; and
* the expected runtime service account or workload identity, when that claim is
  available through the selected cloud's attestation path.

The policy should allow only the workload classes enabled for the integration:

* VidLabeler and any enabled VID-model preparation stages;
* ResultsFulfiller; and
* TrusTEE.

These are separate workload classes even when they trust the same KMS key. The
operator supplies the exact projects and identities used in a market.

An image signature identifies an approved publisher of builds. It does not pin
one exact build or prove that a particular source-code change was reviewed or
merged. Project and workload-identity conditions therefore remain important.

Revoking or narrowing the KMS policy prevents future decryptions even if an
identity still has access to the storage bucket.

## 6. Alternative: upload already VID-labeled data

An EDP that assigns VIDs before upload can skip the raw-Parquet and EDPA
VID-labeling stages. It uploads encrypted RecordIO, one `BlobDetails` sidecar per
blob, and a labeled-output `done` marker. From data-availability synchronization
onward, it joins the same flow shown above.

See [Upload already VID-labeled impressions](vid-labeled-impression-upload.md)
for that format and upload procedure. The alternative path is not a native
Parquet upload and should not use the raw-upload directory layout.

## 7. Related documentation

| Document | Use it for |
| --- | --- |
| [Publisher onboarding](README.md) | The recommended Parquet integration and recurring upload procedure |
| [Already VID-labeled uploads](vid-labeled-impression-upload.md) | The alternative encrypted RecordIO and `BlobDetails` contract |
| [Google Cloud KMS onboarding](edp-onboarding.md#22-kms-setup--google-cloud) | Google Cloud key and attestation policy setup |
| [AWS KMS setup](aws-kms-setup.md) | AWS key and Confidential Space federation setup |
| [EDPA deployment guide](deployment-guide.md) | Operator-owned services and infrastructure |
| [`EventGroup` schema](../../src/main/proto/wfa/measurement/edpaggregator/eventgroups/v1alpha/event_group.proto) | Authoritative campaign metadata fields |
| [`LabeledImpression` schema](../../src/main/proto/wfa/measurement/edpaggregator/v1alpha/labeled_impression.proto) | Authoritative labeled-record fields |
| [`BlobDetails` schema](../../src/main/proto/wfa/measurement/edpaggregator/v1alpha/blob_details.proto) | Authoritative labeled-blob metadata fields |
