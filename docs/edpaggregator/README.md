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
* the event message type and fields used by measurement filters;
* the operator's target size for each Parquet file or encrypted RecordIO blob;
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

Manage the symmetric KEK, workload federation, and IAM permissions with Terraform
in an EDP-owned Google Cloud project or AWS account. Use separate identities for
the EDP's export process and Halo's attested workloads.

Halo provides reusable modules and complete examples for each supported path:

| KMS and federation path | Terraform example |
| --- | --- |
| Google Cloud KMS | [Google Cloud EDP KMS](../../src/main/terraform/gcloud/examples/edp-kms) |
| AWS KMS with direct Confidential Space attestation | [Direct AWS EDP KMS](../../src/main/terraform/aws/examples/edp-kms) |
| AWS KMS through an EDP-owned Google Cloud project | [Google-to-AWS bridge](../../src/main/terraform/aws/examples/edp-kms-google-bridge) |

Configure the example's variables with the values exchanged in Section 2. Keep
each resource under one Terraform configuration and state. Import infrastructure
that already exists before applying; if it is already managed by Terraform,
update that configuration or explicitly migrate its state. Review the plan for
key replacement and permission changes. See each module's adoption instructions.

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
rotated independently; the key arrangements are described above.

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

Use the [EDP-owned Google Cloud KMS module](../../src/main/terraform/gcloud/modules/edp-aggregator-edp-deployment)
and its [complete example](../../src/main/terraform/gcloud/examples/edp-kms). It
provisions the key ring, symmetric KEK, EDP-owned federated service account,
globally scoped workload identity pool, attestation-gated OIDC provider, and KMS
permissions. The KMS location is independent of the pool's global scope.

Set the example's variables:

| Variables | Values to supply |
| --- | --- |
| `project_id`, `kms_location`, `kms_keyring`, `kms_key`, `rotation_period` | EDP-owned project, key location, resource names, and rotation policy |
| `service_account_name`, `service_account_display_name`, `workload_identity_pool_id`, `wip_display_name`, `workload_identity_pool_provider_id` | EDP-owned federation resource names |
| `workload_project_ids`, `workload_service_accounts`, `image_signature_fingerprints` | Exact operator projects, runtime service-account emails, and `signature_algorithm:key_id` fingerprints approved for this EDP |
| `allowed_audiences` | Confidential Space token audiences; the example uses `https://sts.googleapis.com` |
| `attested_kms_role` | The role required by the selected impression path and TrusTEE key arrangement |
| `export_principal_roles` | EDP export IAM members and their KMS roles; normally `roles/cloudkms.cryptoKeyEncrypter` |

The example constructs a CEL attestation condition that requires Confidential
Space, stable-image support, an approved workload project, an approved runtime
service account, and an approved image signature. Use only workloads the operator
confirms are enabled for this EDP. The reusable module also accepts an explicit
`attribute_condition` when your trust policy needs further restrictions.

For `attested_kms_role`, use:

* `roles/cloudkms.cryptoKeyDecrypter` for already VID-labeled input without
  TrusTEE;
* `roles/cloudkms.cryptoKeyEncrypterDecrypter` for Parquet labeling, or when the
  same key protects input and TrusTEE contributions; or
* permissions appropriate to each key when TrusTEE uses a separate re-encryption
  key. Create that key in the same key ring and manage its grants in Terraform,
  using the module's `key_ring_id` and `service_account_email` outputs.

Grant the export identity decrypt access only for EDP-operated validation.
Keep a separate TrusTEE key's permissions distinct from the input key's grants.

From a Halo checkout, configure and apply the example. Configure a suitable
shared-state backend before applying:

```bash
cd src/main/terraform/gcloud/examples/edp-kms
cp terraform.tfvars.example terraform.tfvars
# Replace every sample value with the agreed EDP and operator configuration.
terraform init
terraform plan -out=onboarding.tfplan
terraform apply onboarding.tfplan

terraform output -raw kek_uri
terraform output -raw service_account_email
terraform output -raw kms_audience
```

The key URI has this format:

```text
gcp-kms://projects/<EDP_PROJECT_ID>/locations/<KMS_LOCATION>/keyRings/<KEY_RING>/cryptoKeys/<KEY_NAME>
```

Give the operator the outputs for the KEK URI, federated service-account email,
and KMS audience. The audience includes the provider's full resource name with
the `//iam.googleapis.com/` prefix required by Halo's `kms_audience` field.
The module also exposes `workload_identity_provider_name` without that prefix.

### Halo-managed test deployments

The shared [Google Cloud KMS root](../../src/main/terraform/gcloud/edp-kms) and
[AWS KMS root](../../src/main/terraform/aws/edp-kms) adopt the existing test EDP
resources into separate states. Their project/account credentials are independent
of the dev/head/qa CMMS deployment credentials. The
[Deploy EDP KMS workflow](../../.github/workflows/deploy-edp-kms.yml) deploys keys,
IAM trust, and image-signature conditions before CMMS updates. Dev and head trust
the test image-signing key; QA's Google provider trusts the release key. The shared
AWS role retains both approved keys. Unsigned developer images cannot obtain key
access through these attested paths.

The legacy configuration uses one provider and one federated account within
each environment. The [per-job workload KMS deployment](../operations/tee-workload-kms.md)
adds dedicated job identities, purpose-specific Google/AWS KEKs, job signing keys,
and approved image repositories. Its staged activation is committed separately
for dev, head, and QA. New purpose keys admit only their configured jobs. Retire
legacy federation after migration to remove shared access to existing input
keys and historical frequency vectors.

Bootstrap the Google deployment identity through its Terraform instructions
before the first workflow run. Imports preserve key URIs and existing permissions;
do not give multiple environment states ownership of shared keys or identity pools.

### AWS KMS

For per-job keys and repository attestation, use the
[dual-cloud example](../../src/main/terraform/gcloud/examples/tee-workload-kms),
which admits AWS access through Google federation. The direct Confidential Space
path below supports signature attestation and requires no EDP-owned Google Cloud
project; AWS principal tags do not expose `image_reference`:

```text
Confidential Space workload
  └─ attestation token ─► AWS STS AssumeRoleWithWebIdentity
                           └─ temporary credentials ─► AWS KMS
```

Use the [AWS EDP KMS module](../../src/main/terraform/aws/modules/edp-kms) and
[direct AWS example](../../src/main/terraform/aws/examples/edp-kms). Terraform
creates a symmetric customer-managed key, alias, attested-workload role, and
Confidential Space OIDC provider. If the AWS account already has that issuer's
provider, pass its ARN as `existing_oidc_provider_arn` and manage its allowed
audiences in the Terraform configuration that owns it. AWS permits one provider
per issuer URL in an account.

#### 1. Configure the key and attestation trust

Set the example's variables:

| Variables | Values to supply |
| --- | --- |
| `region`, `kms_key_description`, `kms_key_alias`, `enable_key_rotation`, `deletion_window_in_days` | EDP region, key description and alias, rotation setting, and deletion waiting period |
| `key_administrator_arns` | Key administrators, including the identity applying Terraform |
| `role_name` | Name of the attested-workload IAM role |
| `audience` | An EDP-chosen value other than `https://sts.google.com`, such as `https://<EDP_NAME>.confidential-space` |
| `oidc_thumbprints`, `existing_oidc_provider_arn` | Verified issuer CA thumbprints and an existing provider ARN, or `null` to create a provider |
| `image_signature_key_id_tags`, `workload_project_ids` | Approved Halo signing-key tags and exact operator projects |
| `workload_kms_actions`, `export_principal_permissions` | Workload and export-principal operations from the permissions table above |

The direct example uses the issuer `https://confidentialcomputing.googleapis.com`
and permits both `sts:AssumeRoleWithWebIdentity` and `sts:TagSession`. Its role
trust condition checks audience, Confidential Space software name, stable-image
support, signing-key tags, and approved operator projects.

If an image can carry multiple approved signatures, Confidential Space supplies
one alphabetically sorted `=`-joined signing-key tag. Include each deployable
combination in `image_signature_key_id_tags`. The example uses `StringEquals`
because AWS defines `aws:RequestTag/tag-key` as a single-valued context key.

The Parquet path requires `kms:Encrypt` and `kms:Decrypt`. Already VID-labeled
input without TrusTEE requires `kms:Decrypt` only. Export principals normally
need `kms:Encrypt`; add `kms:Decrypt` only for EDP-operated validation. Apply the
combined permissions for a shared input/TrusTEE key, or manage a separate
TrusTEE key in the same account and region with its own Terraform grants.

#### 2. Apply Terraform and give the operator the configuration

From a Halo checkout, configure a suitable shared-state backend and apply the
example:

```bash
cd src/main/terraform/aws/examples/edp-kms
cp terraform.tfvars.example terraform.tfvars
# Replace every sample value with the agreed EDP and operator configuration.
terraform init
terraform plan -out=onboarding.tfplan
terraform apply onboarding.tfplan

terraform output -raw kek_uri
terraform output -raw aws_role_arn
terraform output -raw aws_region
terraform output -raw aws_audience
```

The KEK URI is:

```text
aws-kms://arn:aws:kms:<REGION>:<AWS_ACCOUNT_ID>:key/<KEY_ID>
```

Provide the outputs and the agreed role-session name to the operator, who
configures:

```textproto
event_data_provider_config {
  data_provider: "dataProviders/<DATA_PROVIDER_ID>"
  kms_config {
    kms_type: AWS_CONFIDENTIAL_SPACE
    aws_role_arn: "<AWS_ROLE_ARN_OUTPUT>"
    aws_role_session_name: "<SESSION_NAME>"
    aws_region: "<AWS_REGION_OUTPUT>"
    aws_audience: "<AWS_AUDIENCE_OUTPUT>"
    kek_uri: "<KEK_URI_OUTPUT>"
  }
}
```

#### 3. Verify direct AWS access

Run an onboarding test that exercises the chosen path. Confirm in AWS CloudTrail
that `AssumeRoleWithWebIdentity` succeeds and that only the expected KMS actions
occur. CloudTrail includes session tags on successful calls but redacts them on
`AccessDenied`; compare a denial with a successful call when diagnosing a claim
mismatch.

#### Alternative: AWS through an EDP-owned Google Cloud project

Use this supported path when the EDP needs a Google Cloud workload identity pool
in the chain. The pool and service account must be in the EDP's project:

```text
Confidential Space attestation
  └─► EDP workload identity pool
       └─► EDP Google service account ID token
            └─► AWS STS ─► AWS KMS
```

Use the [Google-to-AWS bridge example](../../src/main/terraform/aws/examples/edp-kms-google-bridge).
It combines the reusable Google Cloud
[Confidential Space workload identity module](../../src/main/terraform/gcloud/modules/confidential-space-workload-identity)
with the [AWS EDP KMS module](../../src/main/terraform/aws/modules/edp-kms).
Configure both providers for the EDP's accounts. Supply the same Google
attestation inputs and AWS key permissions described above.

Terraform grants `roles/iam.serviceAccountTokenCreator` to the attested pool
principal and `roles/iam.serviceAccountOpenIdTokenCreator` to the bridge account
on itself. This permits the bridge to generate the OIDC ID token AWS needs.
The AWS OIDC provider uses `https://accounts.google.com`; its role trust condition
requires the Google service account's numeric unique ID as subject and audience.

If the AWS account already has an `accounts.google.com` provider, set
`existing_oidc_provider_arn` to reuse it. Add the bridge's numeric unique ID to
that provider's audience list through the Terraform configuration that owns it.
The bridge module exposes the numeric ID, so it can be passed between Terraform
configurations without a service-account lookup command.

Configure and apply the bridge example using its `terraform.tfvars.example`,
then give the operator these outputs and the agreed role-session name:

```bash
terraform output -raw kek_uri
terraform output -raw aws_role_arn
terraform output -raw aws_region
terraform output -raw aws_audience
terraform output -raw service_account_email
terraform output -raw kms_audience
```

The operator configures:

```textproto
event_data_provider_config {
  data_provider: "dataProviders/<DATA_PROVIDER_ID>"
  kms_config {
    kms_type: AWS
    kms_audience: "<KMS_AUDIENCE_OUTPUT>"
    service_account: "<SERVICE_ACCOUNT_EMAIL_OUTPUT>"
    aws_role_arn: "<AWS_ROLE_ARN_OUTPUT>"
    aws_role_session_name: "<SESSION_NAME>"
    aws_region: "<AWS_REGION_OUTPUT>"
    aws_audience: "<AWS_AUDIENCE_OUTPUT>"
    kek_uri: "<KEK_URI_OUTPUT>"
  }
}
```

## 4. Set up storage access

The market operator owns the private object-storage bucket and assigns prefixes
for campaign information and impressions. The operator manages the bucket and
its IAM grants through Terraform and gives the EDP's upload identity write access
only to those prefixes.

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

Use lowercase `.json` or `.binpb` filename suffixes. Other suffixes are
unsupported and cause campaign synchronization to fail. Generate against the
authoritative
[EventGroup schema](../../src/main/proto/wfa/measurement/edpaggregator/eventgroups/v1alpha/event_group.proto).

Each campaign includes:

* brand and campaign names;
* the interval for which impression data is available;
* media types such as video or display;
* the advertiser identity; and
* the entity key that connects the campaign to impression rows or records.

Identify the advertiser by setting either:

1. `measurement_consumer` to the advertiser's full Halo resource name,
   `measurementConsumers/<MEASUREMENT_CONSUMER_ID>`; or
2. `client_account_reference_id` to the EDP's account ID after the operator
   links it to the advertiser. See the
   [Self-Serve Onboarding Guide](self-serve-onboarding.md).

Do not put only the measurement-consumer ID in `measurement_consumer`. For
example, use `measurementConsumers/example-advertiser`, not
`example-advertiser`.

`client_account_reference_id` must be at most 36 characters and contain only
letters (`A-Z`, `a-z`), digits (`0-9`), or `.`, `_`, `~`, and `-`. Do not include
slashes, spaces, or percent-encoded characters. If both advertiser identifiers
are provided, each must be valid.

The `data_availability_interval` describes when impression data are available,
which may differ from the campaign's flight dates. `start_time` is required. If
`end_time` is set, it must be later than `start_time`; using the same timestamp
for both is invalid.

Use `entity_key` for new integrations. `event_group_reference_id` is the legacy
alternative. The entity type and ID must exactly match the corresponding values
produced by the selected impression path.

Within a (`DataProvider`, `MeasurementConsumer`) pair, each (`entity_type`,
`entity_id`) pair must identify exactly one `EventGroup`, regardless of its
parent in an entity hierarchy. If an EDP's native entity IDs are unique only
within their parent, the EDP must provide stable, URL-safe IDs that are unique
across parents and use those same IDs in its campaign information and impression
data.

### Validate campaign information before upload

Download `VerifyEventGroups_deploy.jar` from the project's
[Halo releases](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases),
then validate the local file:

```bash
java -jar VerifyEventGroups_deploy.jar \
  --event-groups-file='<LOCAL_EVENT_GROUPS_FILE>.json'
```

The command also accepts `.binpb` files. Repeat `--event-groups-file` to check
more than one file in the same run.

The verifier reads the same formats and applies the same field checks as the
EventGroup sync service. It checks every EventGroup it can parse and reports
each invalid record, including malformed advertiser resource names, unordered
timestamps, missing campaign metadata, and invalid media types or entity keys.
Empty files and parse failures are reported as file-level errors. A parse
failure reports how many EventGroups were successfully parsed before validation
stopped for that file; validation continues with any additional files. A
JSON file must contain one complete wrapper object; extra JSON or other content
after it is a parse failure. Trailing whitespace is allowed. A
successful run reports `PASS` and exits with status `0`; any error makes it exit
nonzero. Do not upload the campaign file until it passes.

This local check cannot confirm that a `client_account_reference_id` has been
linked by the operator or that the referenced advertiser exists in Halo. Confirm
those results after the file is synchronized.

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
