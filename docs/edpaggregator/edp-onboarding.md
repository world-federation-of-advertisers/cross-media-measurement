# EDP Onboarding Guide

**Audience:** Event Data Provider (EDP) engineering & operations.

This guide describes how an EDP integrates with the Halo Cross-Media Measurement
System through the **EDP Aggregator** (EDPA): how to set up a key for encryption,
how to format and encrypt data, where to upload it, and the daily operational
workflow. Onboarding is per-market — repeat it for each market you participate in,
using that market's configuration.

For the operator side of the deployment, see the
[EDP Aggregator Deployment Guide](deployment-guide.md). To use **AWS KMS** instead
of GCP KMS, follow the [AWS KMS Setup Guide](aws-kms-setup.md) in place of the GCP
KMS steps here.

> This guide is not operator- or market-specific. Wherever you see a placeholder
> such as `PROJECT_ID`, `dataProviders/DATA_PROVIDER_ID`, `RESULTS_FULFILLER_SA`, or
> `EDPA_STORAGE_BUCKET`, substitute the values your market operator gives you.

---

## 0. Glossary

| Term | Meaning |
| --- | --- |
| **EDP** | Event Data Provider — the entity that provides event data to a market for measurement. |
| **Event / Impression** | An individual ad impression within an EDP's system. |
| **MC** | Measurement Consumer — an advertiser/agency (or an EDP measuring its own inventory). |
| **Market Operator** | The entity that hosts and operates the Halo CMM platform and the EDP Aggregator for a market. |
| **VID** | Virtual-Person ID — a market-specific virtual identifier for impression events across platforms. |
| **VID Model** | A model that assigns a VID to an impression. |
| **VID-Labeled Impression** | An event associated with a VID via the VID Model. |
| **KEK / DEK** | Key-Encryption-Key (your KMS key) / Data-Encryption-Key (a per-batch symmetric key). |
| **TEE** | Trusted Execution Environment (Confidential Space) where the ResultsFulfiller runs. |

---

## 1. Overview

The EDP Aggregator simplifies onboarding for EDPs. You upload **encrypted,
VID-labeled impression logs** and **event-group metadata** to a Cloud Storage bucket
on a daily (or more frequent) basis. Trusted workloads running in TEEs then process
that data to fulfill requisitions. The key functionalities in scope are:

* **Event Group Synchronization** — registering your campaigns with the platform.
* **Labeled Event Ingestion** — uploading encrypted VID-labeled impressions.
* **Requisition Fulfillment** — the TEE computes measurement results from your data.

In this phase, the EDP provides the VID labels; VID labeling **within** the
aggregator is out of scope. Onboarding involves three things: establishing a secure
trust boundary with your own KMS, arranging storage for data exchange, and
implementing the daily export workflow.

### Roles: what you own vs. what the operator owns

| You (the EDP) own | The operator owns |
| --- | --- |
| Your KMS key (KEK) in your own cloud project/account | The Cloud Storage bucket(s) |
| The Workload Identity Pool + Provider that gate access to your key by attestation | The aggregator services (functions, TEE, APIs) |
| Encrypting your impressions and DEK; uploading data | Registering your `DataProvider` in the Kingdom |
| Deciding what to share with the operator (below) | Provisioning bucket write access to your identity |

### What must be shared between you and the operator

* **From operator → you:** your `DataProvider` resource name
  (`dataProviders/DATA_PROVIDER_ID`), the storage bucket URI and your assigned
  `<edp-id>` prefix, the **ResultsFulfiller service account** (and, for TrusTEE, the
  Duchy service account), and the Confidential Space **image signature fingerprint**.
* **From you → operator:** your **KEK URI**, and (for AWS) the AWS role/region details
  — so the operator can populate your entry in the aggregator's EDP config. You never
  share your private keys; the TEE accesses your KEK only through attestation-gated
  federation.

---

## 2. Infrastructure setup

### 2.1 KMS setup — AWS

If your KMS is hosted on **AWS**, follow the [AWS KMS Setup Guide](aws-kms-setup.md)
instead of Section 2.2, then continue from Section 3. The rest of the flow (data
formatting, upload, workflow) is identical.

### 2.2 KMS setup — Google Cloud

Create a symmetric KEK in **your own** GCP project. This key lets the aggregator's TEE
decrypt the daily DEKs that protect your impression data. All steps can be scripted
with Terraform.

```bash
# 1. Key ring (region as agreed with the operator)
gcloud kms keyrings create edp-keyring --location global

# 2. Symmetric encrypt/decrypt key (this is your KEK)
gcloud kms keys create edp-key --location global --keyring edp-keyring --purpose=encryption

# 3. Service account that will access the key
gcloud iam service-accounts create edp-service-account

# 4. Grant the SA decrypt on the key
gcloud kms keys add-iam-policy-binding \
  projects/PROJECT_ID/locations/global/keyRings/edp-keyring/cryptoKeys/edp-key \
  --member=serviceAccount:edp-service-account@PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyDecrypter

# 5. Workload Identity Pool
gcloud iam workload-identity-pools create edp-workload-identity-pool --location global

# 6. Allow the pool to impersonate the SA
gcloud iam service-accounts add-iam-policy-binding \
  edp-service-account@PROJECT_ID.iam.gserviceaccount.com \
  --member="principalSet://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/edp-workload-identity-pool/*" \
  --role=roles/iam.workloadIdentityUser
```

Your **KEK URI** is:
`gcp-kms://projects/PROJECT_ID/locations/global/keyRings/edp-keyring/cryptoKeys/edp-key`.
Share it with the operator.

### 2.3 Create a Workload Identity Provider

To accept decryption requests from the aggregator's TEE, create an OIDC Workload
Identity Provider whose attestation condition verifies that:

* the code runs in a Confidential Space (`assertion.swname == 'CONFIDENTIAL_SPACE'`),
* the request comes from the operator's authorized ResultsFulfiller service account, and
* the image was signed with the expected signature fingerprint.

**Debug (non-production) provider:**

```bash
gcloud iam workload-identity-pools providers create-oidc PROVIDER_NAME \
  --location="global" \
  --workload-identity-pool="edp-workload-identity-pool" \
  --issuer-uri="https://confidentialcomputing.googleapis.com/" \
  --allowed-audiences="https://sts.googleapis.com" \
  --attribute-mapping="google.subject=assertion.sub" \
  --attribute-condition="assertion.swname == 'CONFIDENTIAL_SPACE' &&
    'RESULTS_FULFILLER_SA@OPERATOR_PROJECT.iam.gserviceaccount.com' in assertion.google_service_accounts &&
    ['SIGNATURE_ALGORITHM:KEY_ID'].exists(fingerprint,
      fingerprint in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))"
```

**Production provider** — add the requirement that the base image is not a debug
build:

```bash
  --attribute-condition="assertion.swname == 'CONFIDENTIAL_SPACE' &&
    'STABLE' in assertion.submods.confidential_space.support_attributes &&
    'RESULTS_FULFILLER_SA@OPERATOR_PROJECT.iam.gserviceaccount.com' in assertion.google_service_accounts &&
    ['SIGNATURE_ALGORITHM:KEY_ID'].exists(fingerprint,
      fingerprint in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))"
```

Obtain `RESULTS_FULFILLER_SA` and the image signature fingerprint
(`SIGNATURE_ALGORITHM:KEY_ID`) from your operator. Give the resulting provider
resource name (the `//iam.googleapis.com/projects/.../providers/...` audience) and
the service account to the operator so they can set your `kms_config`.

### 2.4 Storage bucket

Data exchange happens through a Cloud Storage bucket **owned by the operator**.

* Request a bucket (or prefix) from the operator.
* The operator provides the bucket URI and your `<edp-id>` prefix, and grants your
  identity write access.

---

## 3. Data formatting

You upload **Event Groups** (campaign identifiers + metadata) and
**VID-Labeled Impressions**. Both may be serialized as **Protobuf** or **JSON**.

### 3.1 Schemas (source of truth)

The proto definitions in the repository are authoritative — always generate against
them rather than copying inline snippets:

| Data | Proto |
| --- | --- |
| Labeled impression | `wfa/measurement/edpaggregator/v1alpha/labeled_impression.proto` |
| Event group | `wfa/measurement/edpaggregator/eventgroups/v1alpha/event_group.proto` |
| Encrypted DEK | `wfa/measurement/edpaggregator/v1alpha/encrypted_dek.proto` |
| Encryption key | `wfa/measurement/edpaggregator/v1alpha/encryption_key.proto` |

### 3.2 LabeledImpression

Each impression carries an event time, its VID, the event payload, the event-group
reference, and optional entity keys:

| Field | Type | Notes |
| --- | --- | --- |
| `event_time` | `Timestamp` | Required. |
| `vid` | `int64` | Required — the VID you assigned. |
| `event` | `Any` | Required — your market's event message (see the market-specific event template). |
| `event_group_reference_id` | `string` | Required — ties the impression to an event group. |
| `entity_keys` | `repeated EntityKey` | Optional `{ entity_type, entity_id }` tags (e.g. creative, placement) for downstream filtering. |

### 3.3 Impression format and encryption wrapper

* **Protobuf:** `LabeledImpression` messages separated by **RecordIO**. Encrypted
  files use the extension `.enc.recordio`.
* Every impression file is encrypted with a **daily DEK**. The DEK is itself
  encrypted with your **KEK** and stored alongside the data as an `EncryptedDek` in
  the day's metadata file.

`EncryptedDek` fields:

| Field | Notes |
| --- | --- |
| `kek_uri` | Required — your KEK URI (`gcp-kms://...` or `aws-kms://...`). |
| `type_url` | Either `type.googleapis.com/google.crypto.tink.Keyset` or `type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey`. |
| `protobuf_format` | `BINARY` or `JSON`. |
| `ciphertext` | The DEK ciphertext, encrypted under `kek_uri`. |

For the JSON path, the DEK is represented by the `EncryptionKey` message (an
`AesGcmHkdfStreamingKey` for streaming AEAD, or an `AesGcmKey`).

### 3.4 Event Group

`EventGroup` describes a campaign. The schema evolves — consult the proto — but the
key fields are:

| Field | Type | Notes |
| --- | --- | --- |
| `event_group_metadata` | `Metadata` | Required. `ad_metadata.campaign_metadata.{brand, campaign}` plus optional `entity_metadata`. |
| `data_availability_interval` | `Interval` | Required — the publisher's data-availability window. |
| `media_types` | `repeated MediaType` | Required — `VIDEO` / `DISPLAY` / `OTHER` / `NATIVE`. |
| `measurement_consumer` | `string` | Required **unless** `client_account_reference_id` is set. |
| `client_account_reference_id` | `string` | Reference to a client account in your ecosystem (≤ 36 chars). Required **unless** `measurement_consumer` is set. |
| `event_group_reference_id` | `string` | Optional; superseded by `entity_key` and planned for deprecation. At least one of `event_group_reference_id` / `entity_key` must be set. |
| `entity_key` | `EntityKey` | `{ entity_type, entity_id }` — the entity this event group represents. |

* **Protobuf:** `EventGroup` messages separated by RecordIO.
* **JSON:** the `EventGroups` wrapper (a list of `EventGroup`), no RecordIO required.

---

## 4. Directory structure & upload paths

The aggregator's DataWatcher relies on **strict path patterns**. Deviating from them
causes ingestion to be skipped. Below, `{bucket}` is the operator-provided bucket and
`{edp-id}` is your assigned prefix.

### 4.1 Event groups

Uploading here triggers a sync with the Kingdom:

```
gs://{bucket}/{edp-id}/event-groups/{filename}.{binpb|json}
```

### 4.2 Daily impressions (the `done` marker)

Impression data is uploaded into **date-partitioned** folders. The system does not
process a day until an empty file named `done` appears. When `done` is written, every
file in that folder **and its subfolders** is read. Once `done` is written, the
folder must not change.

```
gs://{bucket}/edp/{edp-id}/{YYYY-MM-DD}/impressions        # encrypted LabeledImpression files (.enc.recordio)
gs://{bucket}/edp/{edp-id}/{YYYY-MM-DD}/metadata.binpb     # or metadata.json — contains the EncryptedDek
gs://{bucket}/edp/{edp-id}/{YYYY-MM-DD}/done               # empty 0-byte file, uploaded LAST
```

Required per directory:

* **Metadata file** — contains the `EncryptedDek` (`.binpb` binary or `.json`).
* **Data files** — encrypted `LabeledImpression` files.
* **`done`** — an empty 0-byte file, uploaded **last**.

---

## 5. Daily operational workflow

### 5.1 Event group upload

1. **Identify campaigns** — extract active campaigns and metadata from your systems.
2. **Format** — serialize into `EventGroup` messages (Protobuf or JSON, Section 3.4).
3. **Upload** — write to `gs://{bucket}/{edp-id}/event-groups/{filename}.{binpb|json}`.
4. **Sync** — the upload automatically triggers a sync with the Kingdom.

### 5.2 Impression upload

1. **Generate** — extract the day's VID-labeled impressions.
2. **Create DEK** — generate a fresh AES DEK for this daily batch.
3. **Encrypt data** — encrypt the impression files with the DEK (RecordIO →
   `.enc.recordio`).
4. **Encrypt DEK** — wrap the DEK with your KEK (Section 2.2 / AWS guide).
5. **Create metadata** — write the metadata file containing the `EncryptedDek`.
6. **Upload** — upload the encrypted impressions and the metadata file to the
   date-partitioned path (Section 4.2).
7. **Signal completion** — upload the empty `done` file **last** to trigger
   processing.

---

## 6. Enabling TrusTEE (optional)

The TrusTEE protocol computes cross-publisher reach & frequency inside a single TEE.
When enabled, the aggregator's ResultsFulfiller re-encrypts the frequency vector
derived from your impressions and sends it to the TrusTEE Duchy, which decrypts and
aggregates vectors from all participating EDPs.

Because both the ResultsFulfiller **and** the TrusTEE Duchy need access to your KMS
key, enabling TrusTEE requires changes to your KMS permissions and Workload Identity
Provider beyond the baseline in Section 2.

### 6.1 Choose a re-encryption key (optional second key)

The ResultsFulfiller must **encrypt** the frequency vector before sending it to the
Duchy. You choose how that key is provided — **a second key is not required**:

* **Option A — reuse your existing KEK.** Simplest. Upgrade the key's permission from
  decrypt-only to encrypt+decrypt:

  ```bash
  gcloud kms keys remove-iam-policy-binding \
    projects/PROJECT_ID/locations/global/keyRings/edp-keyring/cryptoKeys/edp-key \
    --member=serviceAccount:edp-service-account@PROJECT_ID.iam.gserviceaccount.com \
    --role=roles/cloudkms.cryptoKeyDecrypter

  gcloud kms keys add-iam-policy-binding \
    projects/PROJECT_ID/locations/global/keyRings/edp-keyring/cryptoKeys/edp-key \
    --member=serviceAccount:edp-service-account@PROJECT_ID.iam.gserviceaccount.com \
    --role=roles/cloudkms.cryptoKeyEncrypterDecrypter
  ```

* **Option B — dedicated re-encryption key.** For key isolation and independent
  rotation, create a **second** key **on the same key ring**; keep the original key's
  decrypt-only role unchanged:

  ```bash
  gcloud kms keys create edp-trustee-key --location global --keyring edp-keyring --purpose=encryption
  gcloud kms keys add-iam-policy-binding \
    projects/PROJECT_ID/locations/global/keyRings/edp-keyring/cryptoKeys/edp-trustee-key \
    --member=serviceAccount:edp-service-account@PROJECT_ID.iam.gserviceaccount.com \
    --role=roles/cloudkms.cryptoKeyEncrypterDecrypter
  ```

  The re-encryption key **must** reside on the same key ring as the KEK. Tell the
  operator the new key name so they can configure the re-encryption mapping
  (`trustee_params.kek_uri_to_key_name`).

> Why encrypt *and* decrypt? Decrypt reads your daily DEK / impressions (baseline
> flow); encrypt wraps the frequency vector for the Duchy, which then decrypts it
> inside its own TEE.

### 6.2 Update the Workload Identity Provider

The baseline provider (Section 2.3) trusts only the ResultsFulfiller service account.
For TrusTEE, add the **Duchy** service account too. Ask the operator for the TrusTEE
Duchy service account (the image signature fingerprint is the same). Update the
provider's `--attribute-condition` to accept both:

```
assertion.swname == 'CONFIDENTIAL_SPACE' &&
'STABLE' in assertion.submods.confidential_space.support_attributes &&
['RESULTS_FULFILLER_SA', 'TRUSTEE_DUCHY_SA'].exists(sa, sa in assertion.google_service_accounts) &&
['SIGNATURE_ALGORITHM:KEY_ID'].exists(fingerprint,
  fingerprint in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))
```

### 6.3 AWS KMS EDPs

If your KMS is on AWS, the same principles apply:

* The AWS IAM role's KMS policy must allow **both** `kms:Encrypt` and `kms:Decrypt`.
* The GCP WIF pool in your project must trust the TrusTEE Duchy's service account
  (as in 6.2).
* For a dedicated re-encryption key, create it in the same AWS account/region and
  give the operator its ARN.

### 6.4 Notify the operator

Tell the operator that you support TrusTEE, whether you use a single key or a
dedicated re-encryption key, and — if dedicated — the key name (GCP) or ARN (AWS).
The operator registers TrusTEE support for your `DataProvider` and configures the
re-encryption mapping.

| | Single key (Option A) | Dedicated key (Option B) |
| --- | --- | --- |
| New key needed | No | Yes (same key ring) |
| Permission on KEK | Upgrade to EncrypterDecrypter | Keep Decrypter |
| Permission on re-encryption key | N/A | Grant EncrypterDecrypter |
| Operator config | No key mapping | Provide key name to operator |
