# Onboard an Event Data Provider to the EDP Aggregator

This guide contains the onboarding steps shared by every publisher or other data
provider that sends impression data to the EDP Aggregator (EDPA), a component of
the Halo cross-media measurement system. Halo calls that organization an
**Event Data Provider (EDP)**.

Complete this guide once per market, then follow exactly one impression guide:

| Input supplied by the EDP | Use this guide |
| --- | --- |
| Pre-labeled reference VIDs in encrypted Parquet | [Encrypted Parquet onboarding](parquet-onboarding.md) — recommended |
| Impressions to which the EDP has already assigned VIDs | [Already VID-labeled onboarding](vid-labeled-onboarding.md) |

Both paths publish campaign information separately, use an EDP-owned encryption
key, and place data in operator-provided private object storage. The difference is
who assigns the VID and therefore which impression format is uploaded.

## Contents

1. [Understand the shared integration](#1-understand-the-shared-integration)
2. [Exchange onboarding information](#2-exchange-onboarding-information)
3. [Set up KMS and workload trust](#3-set-up-kms-and-workload-trust)
4. [Set up storage access](#4-set-up-storage-access)
5. [Publish campaign information](#5-publish-campaign-information)
6. [Complete common validation](#6-complete-common-validation)
7. [Continue with the selected impression path](#7-continue-with-the-selected-impression-path)

## 1. Understand the shared integration

The EDP sends two independent feeds:

```text
Campaign information ──────────────► Halo registers and updates campaigns

Encrypted impression files ────────► EDPA registers data availability
                                    ► Trusted workloads answer measurements
```

With the recommended Parquet path, EDPA first assigns VIDs inside Google
Confidential Space, a trusted execution environment (TEE). With the already
VID-labeled path, the EDP assigns the VIDs before upload. After data is uploaded,
Halo decrypts sensitive impression data only inside approved TEE workloads.

Before the EDP's cloud key manager grants access, a workload presents a
hardware-signed attestation describing the software and environment that are
running. Object-storage access alone does not grant decryption access.

For the full lifecycle and the separate EDPA and TrusTEE trust boundaries, see
[How data moves through the EDP Aggregator](data-flow.md).

### Terms used in these guides

| Term | Meaning |
| --- | --- |
| EDP | Event Data Provider: the publisher or other organization supplying impression data |
| VID | Virtual Person ID assigned by a market-specific model or by the EDP |
| KEK | Key-encryption key owned by the EDP in Google Cloud KMS or AWS KMS |
| DEK | Data-encryption key used to protect a particular file or batch |
| TEE | Trusted execution environment; Halo uses Google Confidential Space |
| EventGroup | Halo's representation of a campaign |
| Model line | A version of the market's VID model |

### Responsibilities

| The EDP owns | The market operator owns |
| --- | --- |
| The KMS key in the EDP's cloud account | Registration of the EDP in Halo |
| The policy deciding which attested workloads may use the key | Private storage and assigned upload prefixes |
| The campaign and impression exports | VID models, field mappings, and trusted Halo workloads |
| The identity that uploads files | Operation of labeling, fulfillment, and optional TrusTEE services |

The EDP never gives the operator a private key or downloaded cloud credential.
The EDP authorizes approved workload classes through attestation policy instead
of approving individual VMs, jobs, or measurement requests.

## 2. Exchange onboarding information

The operator gives the EDP:

* its Halo `DataProvider` resource name;
* the private bucket URI and exact campaign and impression prefixes;
* the operator projects, runtime identities, and Halo image-signing key IDs that
  the EDP must allow in its attestation policy;
* the active model lines;
* the campaign/entity-key convention;
* the event message type and fields used by measurement filters; and
* the operator's target size for each Parquet file or encrypted RecordIO blob;
  and
* for the Parquet path, the identity and demographic inputs required by the VID
  model.

The EDP gives the operator:

* the cloud identity that will upload files;
* the selected impression path: encrypted Parquet or already VID-labeled
  RecordIO;
* its KMS key URI and the cloud-specific federation values from Section 3;
* whether TrusTEE is enabled and whether it uses the input key or a separate
  re-encryption key; and
* for the Parquet path, a sample schema and the proposed column mapping.

Onboarding is per market. Do not assume that keys, identifiers, models, storage
locations, or trusted workload identities are shared between markets.

## 3. Set up KMS and workload trust

Create a symmetric KEK in an EDP-owned Google Cloud project or AWS account. Use
separate identities for the EDP's export process and Halo's attested workloads.

### Configure TrusTEE if enabled

TrusTEE is an optional integration capability, not a prerequisite for every EDPA
deployment. Before assigning KMS permissions, ask the operator whether this
market and integration use it.

When enabled, ResultsFulfiller encrypts the EDP's frequency-vector contribution
and sends it to the TrusTEE aggregation workload. That TEE decrypts and combines
contributions from participating EDPs.

Choose one key arrangement:

* **Reuse the input KEK.** ResultsFulfiller needs encrypt and decrypt, and the
  TrusTEE workload needs decrypt.
* **Create a dedicated re-encryption key.** ResultsFulfiller uses it to protect
  the contribution and TrusTEE decrypts it. This permits independent rotation
  and narrower artifact access. Keep the input key's existing permissions. Give
  ResultsFulfiller encrypt access and the TrusTEE workload decrypt access on the
  new key; if one federated identity represents both workloads, it needs both
  operations. For Google Cloud KMS, create the new key on the same key ring as
  the input KEK. For AWS KMS, create it in the same account and region.

Add the exact TrusTEE workload identity and project to the attestation policy.
Do not enable it by broadening the policy to every operator workload. Give the
operator the re-encryption key name and any new federation values so they can
configure the EDP's TrusTEE key mapping.

### Required key operations

| Identity or workload | Already VID-labeled path | Parquet path |
| --- | --- | --- |
| EDP export | Encrypt; decrypt only for EDP-operated validation | Encrypt; decrypt only for EDP-operated validation |
| VID-labeling workloads | Not used | Encrypt and decrypt |
| ResultsFulfiller without TrusTEE | Decrypt | Decrypt |
| ResultsFulfiller with TrusTEE | Decrypt input and encrypt the TrusTEE contribution | Decrypt labeled data and encrypt the TrusTEE contribution |
| TrusTEE workload, when enabled | Decrypt the contribution | Decrypt the contribution |

If one key is used for both input and TrusTEE, grant the combined operations that
the row requires. A separate TrusTEE re-encryption key can narrow access and be
rotated independently; Section 6 describes that choice.

### Trust workload classes, not individual jobs

A production policy should verify:

* genuine Confidential Space;
* a stable, non-debug Confidential Space image;
* a container image signed by an operator-provided Halo signing key;
* an approved workload project; and
* the exact runtime service accounts used for this EDP, when the attestation path
  exposes them.

Depending on the selected path and market configuration, the approved workloads
can include:

* VidLabeler and, for memoized model lines, SubpoolAssigner and VidRankBuilder;
* ResultsFulfiller; and
* the TrusTEE aggregation workload.

The operator must provide the exact set. Do not allow an unused workload merely
because another market deploys it. A signing key approves a class of Halo-signed
builds, not one immutable build, so retain the project and workload-identity
conditions as well.

Never grant the operator's human accounts, upload functions, file watchers, or
general-purpose VMs access to the KEK.

### Google Cloud KMS

Create the key, the service account used by attested workloads, and the Workload
Identity Pool in the EDP's project. Replace every placeholder before running the
commands.

```bash
gcloud kms keyrings create <KEY_RING> \
  --project=<EDP_PROJECT_ID> \
  --location=<KMS_LOCATION>

gcloud kms keys create <KEY_NAME> \
  --project=<EDP_PROJECT_ID> \
  --location=<KMS_LOCATION> \
  --keyring=<KEY_RING> \
  --purpose=encryption

gcloud iam service-accounts create <HALO_KMS_SERVICE_ACCOUNT> \
  --project=<EDP_PROJECT_ID>

gcloud iam workload-identity-pools create <WORKLOAD_IDENTITY_POOL> \
  --project=<EDP_PROJECT_ID> \
  --location=global

gcloud iam service-accounts add-iam-policy-binding \
  <HALO_KMS_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com \
  --project=<EDP_PROJECT_ID> \
  --member="principalSet://iam.googleapis.com/projects/<EDP_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<WORKLOAD_IDENTITY_POOL>/*" \
  --role=roles/iam.workloadIdentityUser
```

The KEK URI is:

```text
gcp-kms://projects/<EDP_PROJECT_ID>/locations/<KMS_LOCATION>/keyRings/<KEY_RING>/cryptoKeys/<KEY_NAME>
```

Grant the EDP's export identity encrypt-only access:

```bash
gcloud kms keys add-iam-policy-binding \
  projects/<EDP_PROJECT_ID>/locations/<KMS_LOCATION>/keyRings/<KEY_RING>/cryptoKeys/<KEY_NAME> \
  --member=serviceAccount:<EXPORT_SERVICE_ACCOUNT> \
  --role=roles/cloudkms.cryptoKeyEncrypter
```

Grant the federated Halo service account the least-privilege role for the chosen
path. The Parquet path requires encrypt and decrypt. The already VID-labeled path
without TrusTEE requires decrypt only:

```bash
# Parquet, or one-key already-labeled + TrusTEE configuration
gcloud kms keys add-iam-policy-binding \
  projects/<EDP_PROJECT_ID>/locations/<KMS_LOCATION>/keyRings/<KEY_RING>/cryptoKeys/<KEY_NAME> \
  --member=serviceAccount:<HALO_KMS_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyEncrypterDecrypter

# Already VID-labeled without TrusTEE
gcloud kms keys add-iam-policy-binding \
  projects/<EDP_PROJECT_ID>/locations/<KMS_LOCATION>/keyRings/<KEY_RING>/cryptoKeys/<KEY_NAME> \
  --member=serviceAccount:<HALO_KMS_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyDecrypter
```

Run only the command matching the selected profile. Grant the export identity
decrypt access only when an EDP-operated validation process needs it.

Create an OIDC provider in the pool. The following production condition requires
Confidential Space, a stable image, one of the exact workload service accounts,
and an approved image signature:

```bash
EDPA_ATTRIBUTE_CONDITION="assertion.swname == 'CONFIDENTIAL_SPACE' &&
  'STABLE' in assertion.submods.confidential_space.support_attributes &&
  ['<WORKLOAD_SA_1>','<WORKLOAD_SA_2>'].exists(
    sa, sa in assertion.google_service_accounts) &&
  ['<SIGNATURE_ALGORITHM>:<SIGNING_KEY_ID>'].exists(
    fingerprint,
    fingerprint in assertion.submods.container.image_signatures.map(
      sig, sig.signature_algorithm + ':' + sig.key_id))"

gcloud iam workload-identity-pools providers create-oidc <PROVIDER_NAME> \
  --project=<EDP_PROJECT_ID> \
  --location=global \
  --workload-identity-pool=<WORKLOAD_IDENTITY_POOL> \
  --issuer-uri=https://confidentialcomputing.googleapis.com/ \
  --allowed-audiences=https://sts.googleapis.com \
  --attribute-mapping=google.subject=assertion.sub \
  --attribute-condition="$EDPA_ATTRIBUTE_CONDITION"
```

Use only service accounts that the operator confirms are enabled for this EDP.
Share the KEK URI, federated service-account email, and provider resource name
with the operator.

### AWS KMS

For a new AWS integration, use direct Confidential Space attestation. It does
not require an EDP-owned Google Cloud project:

```text
Confidential Space workload
  └─ attestation token ─► AWS STS AssumeRoleWithWebIdentity
                           └─ temporary credentials ─► AWS KMS
```

#### 1. Create the key and export permission

Create a symmetric customer-managed AWS KMS key. Its URI is:

```text
aws-kms://arn:aws:kms:<REGION>:<AWS_ACCOUNT_ID>:key/<KEY_ID>
```

Give the EDP's export principal `kms:Encrypt`. Give it `kms:Decrypt` only if an
EDP-operated validation process needs it. The export principal is not part of
the Confidential Space trust policy.

#### 2. Register the Confidential Space issuer

Create an AWS IAM OpenID Connect provider with:

| Setting | Value |
| --- | --- |
| Provider URL | `https://confidentialcomputing.googleapis.com` |
| Audience | An EDP-chosen value other than `https://sts.google.com`, such as `https://<EDP_NAME>.confidential-space` |

Do not use the AWS console's **Google** preset; that preset uses
`accounts.google.com` and belongs to the alternative federation path below.

#### 3. Create the attested workload role

Create an AWS role that allows both `sts:AssumeRoleWithWebIdentity` and
`sts:TagSession`. The attestation claims arrive as session tags. Gate the role on
the audience, Confidential Space software name, stable-image support attribute,
operator-provided signing key IDs, and approved operator projects:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<AWS_ACCOUNT_ID>:oidc-provider/confidentialcomputing.googleapis.com"
      },
      "Action": ["sts:AssumeRoleWithWebIdentity", "sts:TagSession"],
      "Condition": {
        "StringEquals": {
          "confidentialcomputing.googleapis.com:aud": "<AUDIENCE>",
          "aws:RequestTag/swname": "CONFIDENTIAL_SPACE",
          "aws:RequestTag/confidential_space.support_attributes": "LATEST=STABLE=USABLE",
          "aws:RequestTag/container.signatures.key_ids": [
            "<SIGNING_KEY_ID_1>",
            "<SIGNING_KEY_ID_2>",
            "<SIGNING_KEY_ID_1>=<SIGNING_KEY_ID_2>"
          ],
          "aws:RequestTag/gce.project_id": ["<OPERATOR_PROJECT_1>", "<OPERATOR_PROJECT_2>"]
        }
      }
    }
  ]
}
```

If an image can carry multiple approved signatures, Confidential Space supplies
one alphabetically sorted `=`-joined tag. Enumerate each combination that the
operator can deploy. Do not use `ForAnyValue` or `ForAllValues` with
`aws:RequestTag/tag-key`; AWS defines it as a single-valued context key.

Grant the role the KMS actions from the permissions table. For example, the
Parquet path uses:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:Encrypt"],
      "Resource": "arn:aws:kms:<REGION>:<AWS_ACCOUNT_ID>:key/<KEY_ID>"
    }
  ]
}
```

#### 4. Give the operator the direct-AWS configuration

Provide the KEK URI, role ARN, region, role-session name, and audience. The
operator configures:

```textproto
event_data_provider_config {
  data_provider: "dataProviders/<DATA_PROVIDER_ID>"
  kms_config {
    kms_type: AWS_CONFIDENTIAL_SPACE
    aws_role_arn: "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<ROLE_NAME>"
    aws_role_session_name: "<SESSION_NAME>"
    aws_region: "<REGION>"
    aws_audience: "<AUDIENCE>"
    kek_uri: "aws-kms://arn:aws:kms:<REGION>:<AWS_ACCOUNT_ID>:key/<KEY_ID>"
  }
}
```

#### 5. Verify direct AWS access

Run an onboarding test that exercises the chosen path. Confirm in AWS CloudTrail
that `AssumeRoleWithWebIdentity` succeeds and that only the expected KMS actions
occur. CloudTrail includes the session tags on successful calls but redacts them
on `AccessDenied`; compare a denial with a successful call when diagnosing a
claim mismatch.

#### Alternative: AWS through an EDP-owned Google Cloud project

Use this supported path only when the EDP needs a Google Cloud Workload Identity
Pool in the chain. The pool and service account must be in the EDP's project, not
the operator's project:

```text
Confidential Space attestation
  └─► EDP Workload Identity Pool
       └─► EDP Google service account ID token
            └─► AWS STS ─► AWS KMS
```

Create an `accounts.google.com` OIDC provider in AWS and an AWS role restricted
to the Google service account's numeric unique ID. In the EDP's Google Cloud
project, create the attestation-gated Workload Identity Pool as described in the
Google Cloud section, then create the bridge service account:

```bash
gcloud iam service-accounts create <AWS_BRIDGE_SERVICE_ACCOUNT> \
  --project=<EDP_PROJECT_ID> \
  --display-name="EDP AWS KMS bridge"

gcloud iam service-accounts add-iam-policy-binding \
  <AWS_BRIDGE_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com \
  --project=<EDP_PROJECT_ID> \
  --role=roles/iam.serviceAccountTokenCreator \
  --member="principalSet://iam.googleapis.com/projects/<EDP_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<WORKLOAD_IDENTITY_POOL>/*"

gcloud iam service-accounts add-iam-policy-binding \
  <AWS_BRIDGE_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com \
  --project=<EDP_PROJECT_ID> \
  --role=roles/iam.serviceAccountOpenIdTokenCreator \
  --member="serviceAccount:<AWS_BRIDGE_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com"

gcloud iam service-accounts describe \
  <AWS_BRIDGE_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com \
  --project=<EDP_PROJECT_ID> \
  --format="value(uniqueId)"
```

Add that numeric ID both as an audience on the AWS OIDC provider and as the
`accounts.google.com:sub` condition on the AWS role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<AWS_ACCOUNT_ID>:oidc-provider/accounts.google.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "accounts.google.com:sub": ["<SERVICE_ACCOUNT_UNIQUE_ID>"]
        }
      }
    }
  ]
}
```

Provide the operator with the following configuration:

```textproto
event_data_provider_config {
  data_provider: "dataProviders/<DATA_PROVIDER_ID>"
  kms_config {
    kms_type: AWS
    kms_audience: "//iam.googleapis.com/projects/<EDP_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<WORKLOAD_IDENTITY_POOL>/providers/<PROVIDER_NAME>"
    service_account: "<AWS_BRIDGE_SERVICE_ACCOUNT>@<EDP_PROJECT_ID>.iam.gserviceaccount.com"
    aws_role_arn: "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<ROLE_NAME>"
    aws_role_session_name: "<SESSION_NAME>"
    aws_region: "<REGION>"
    aws_audience: "<SERVICE_ACCOUNT_UNIQUE_ID>"
    kek_uri: "aws-kms://arn:aws:kms:<REGION>:<AWS_ACCOUNT_ID>:key/<KEY_ID>"
  }
}
```

This alternative requires `serviceAccountTokenCreator`, rather than only
`workloadIdentityUser`, because the bridge must generate an OIDC ID token for
AWS. It also requires `serviceAccountOpenIdTokenCreator` on itself.

## 4. Set up storage access

The market operator owns the private object-storage bucket and assigns prefixes
for campaign information and impressions. The operator grants the EDP's upload
identity write access only to those prefixes.

Use a dedicated, keyless workload identity for automated exports. Do not share a
downloaded service-account key with the operator. The upload identity needs
storage write access and the KMS encrypt permission described above; it does not
need access to run Halo workloads or answer measurement requests.

The operator must provide the exact bucket, prefixes, path pattern, and retention
expectations. Do not upload plaintext impression files to the operator's bucket.

## 5. Publish campaign information

Campaign information is common to both impression paths. Publish it before the
first measurement, then again when a campaign is added, changed, or removed. It
is not part of every daily impression upload.

The operator provides a path similar to:

```text
gs://<BUCKET>/<EDP_PREFIX>/event-groups/<FILENAME>.json
gs://<BUCKET>/<EDP_PREFIX>/event-groups/<FILENAME>.binpb
```

Campaign files may use either format:

* JSON files contain an `EventGroups` wrapper.
* Binary protobuf files contain Mesos RecordIO-framed `EventGroup` messages.

Filenames must end with lowercase `.json` or `.binpb`; otherwise EDPA ignores
them. Generate against the authoritative
[EventGroup schema](../../src/main/proto/wfa/measurement/edpaggregator/eventgroups/v1alpha/event_group.proto).

Each campaign includes:

* brand and campaign names;
* the interval for which impression data is available;
* media types such as video or display;
* the advertiser identity; and
* the entity key that connects the campaign to impression rows or records.

Identify the advertiser by setting either:

1. `measurement_consumer` to the advertiser's Halo resource name; or
2. `client_account_reference_id` to the EDP's account ID after the operator
   links it to the advertiser. See the
   [Self-Serve Onboarding Guide](self-serve-onboarding.md).

Use `entity_key` for new integrations. `event_group_reference_id` is the legacy
alternative. The entity type and ID must exactly match the corresponding values
produced by the selected impression path.

Omitting a campaign from a later file does not delete it. To remove a campaign,
upload an `EventGroup` tombstone with `state = DELETED`, a complete `entity_key`,
and an explicit `measurement_consumer`. A `client_account_reference_id` alone is
not sufficient for deletion.

## 6. Complete common validation

Before sending production data, confirm with the operator that:

1. The `DataProvider` is registered for the correct market.
2. The EDP upload identity can write only to its assigned prefixes.
3. The export process can encrypt with the KEK without a downloaded credential.
4. Each required Halo workload can obtain attested credentials and perform only
   its expected KMS operations.
5. A sample campaign file registers the expected campaign and advertiser.
6. Cloud audit logs show only the expected federated identities or AWS role using
   the key.
7. If TrusTEE is enabled, its workload can use the selected re-encryption key.

Then complete the impression-path validation in the selected guide. Onboarding
is complete only after a representative end-to-end measurement succeeds; a file
upload by itself is not sufficient.

## 7. Continue with the selected impression path

* [Onboard with encrypted Parquet containing pre-labeled reference VIDs](parquet-onboarding.md)
  — agree on the schema, apply Parquet Modular Encryption, publish dates, and
  validate EDPA VID labeling.
* [Onboard with already VID-labeled impressions](vid-labeled-onboarding.md) —
  construct `LabeledImpression` RecordIO, envelope-encrypt it, publish
  `BlobDetails`, and validate decryption before `done`.

For a stuck or failed measurement after onboarding, use the
[Report Debugging Guide](report-debugging-guide.md).
