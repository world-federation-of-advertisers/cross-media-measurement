# AWS KMS Setup Guide

**Audience:** an EDP (with their operator) that wants to use **AWS KMS** instead of
GCP KMS to protect impression data.

This guide replaces the GCP KMS steps in the [EDP Onboarding Guide](edp-onboarding.md)
(Section 2.2). Everything else in that guide — data formatting, upload paths, and the
daily workflow — is unchanged.

> Generic placeholders throughout: substitute your own `REGION`, `AWS_ACCOUNT_ID`,
> `KEY_ID`, `ROLE_NAME`, `AUDIENCE`, the aggregator's `OPERATOR_PROJECT`, and — for
> Option 2 only — GCP `EDP_PROJECT` / `EDP_PROJECT_NUMBER`, the operator's
> `RESULTS_FULFILLER_SA`, and the image signature fingerprint
> (`SIGNATURE_ALGORITHM:KEY_ID`).

---

## Choosing an option

Storage and encryption are **decoupled** either way. Impressions stay in the
operator's shared GCS bucket; only the encryption key changes from GCP KMS to AWS KMS.
What differs is how the Confidential Space workload proves its identity to AWS.

| | [Option 1 — Direct attestation](#option-1--direct-confidential-space-attestation) | [Option 2 — GCP federation](#option-2--gcp-federation) |
| --- | --- | --- |
| EDP-owned GCP project | Not required | **Required** |
| GCP resources you operate | None | Workload Identity Pool + Service Account |
| AWS OIDC provider | `confidentialcomputing.googleapis.com` | `accounts.google.com` |
| Hops to AWS credentials | 1 | 3 |
| `kms_type` in the EDP config | `AWS_CONFIDENTIAL_SPACE` | `AWS` |

**Option 1 is the simpler path and is preferred for new EDPs.** Choose Option 2 only
if you have a reason to keep a GCP Workload Identity Pool in the chain.

**Privacy model.** Under both options the AWS role trust policy is yours, so you
decide which attested workloads may reach your key. Under Option 2 the Workload
Identity Pool and Service Account **must** live in **your** GCP project, not the
operator's — if the operator controlled the WIF, they could mint tokens without a
legitimate Confidential VM.

---

# Option 1 — Direct Confidential Space attestation

## Overview

Set up AWS KMS with no GCP project at all. The Confidential Space workload federates
to AWS directly: it fetches an
[`AWS_PRINCIPALTAGS`](https://docs.cloud.google.com/confidential-computing/confidential-space/docs/reference/token-claims#aws-principal-tag-claims)
attestation token from the launcher and calls AWS STS `AssumeRoleWithWebIdentity`,
with no intermediary GCP Workload Identity.

This path works everywhere an EDP encrypts or decrypts: the EDP Aggregator
ResultsFulfiller, the VID Labeler pipeline, and the Duchy TrusTEE mill (which
decrypts the frequency vector). Because the workload authenticates with its own
attestation, there is nothing for you to run on GCP.

## Architecture

```
Confidential VM (ResultsFulfiller / VID Labeler / Duchy TrusTEE mill)
  |  1. Fetch an AWS_PRINCIPALTAGS attestation token from the launcher socket
  |     (/run/container_launcher/teeserver.sock, POST /v1/token)
  v
AWS STS AssumeRoleWithWebIdentity    <-- OIDC provider = confidentialcomputing.googleapis.com
  |     attestation claims arrive as AWS session principal tags
  |  2. Temporary AWS credentials
  v
AWS KMS (EDP's AWS account)          <-- you control this
```

The attestation token is used directly as the web identity credential to AWS STS.

## Prerequisites

* An EDP-owned AWS account. **No EDP GCP project is required.**
* The GCP project(s) in which the aggregator's Confidential Space workloads run
  (ResultsFulfiller, VID Labeler, and the Duchy TrusTEE mill). You gate the AWS role
  on these via `gce.project_id`.
* Your `DataProvider` resource name (`dataProviders/DATA_PROVIDER_ID`).
* The container image signing key IDs, surfaced by the attestation token and used in
  the AWS trust policy.

## Resources you will create

| Resource | Example format |
| --- | --- |
| AWS KMS key | `arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID` |
| AWS KMS key URI | `aws-kms://arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID` |
| AWS IAM role | `arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME` |
| AWS OIDC provider | `arn:aws:iam::AWS_ACCOUNT_ID:oidc-provider/confidentialcomputing.googleapis.com` |

> Note the OIDC provider. This path uses `confidentialcomputing.googleapis.com`, the
> attestation issuer — not `accounts.google.com`.

## Step 1 — Create an AWS KMS key

1. AWS Console → KMS → Customer managed keys.
2. Create a **symmetric** encryption key in your preferred region.
3. Note the key ARN: `arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID`.

## Step 2 — Create an AWS OIDC identity provider for Confidential Space

1. AWS Console → IAM → Identity providers → Add provider.
2. Provider type: **OpenID Connect**.
3. Provider URL: `https://confidentialcomputing.googleapis.com`.
4. Audience: `AUDIENCE` (see below).

> **Do not use the "Google" preset.** The AWS "Add provider" dropdown entry labelled
> *Google* maps to `accounts.google.com`, which is Option 2's provider.
> `confidentialcomputing.googleapis.com` is a separate provider you must add
> explicitly.

**Choosing the audience.** Pick one generic value — anything except
`https://sts.google.com` — for example `https://EDP_NAME.confidential-space`. The same
value goes in the trust policy (Step 3) and the EDP config (Step 5).

## Step 3 — Create an AWS IAM role for direct Confidential Space federation

1. AWS Console → IAM → Roles → Create role.
2. Trusted entity type: **Web identity**.
3. Identity provider: `confidentialcomputing.googleapis.com`.
4. Set the trust policy to gate on the attestation claims, which are delivered as AWS
   session principal tags:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::AWS_ACCOUNT_ID:oidc-provider/confidentialcomputing.googleapis.com"
      },
      "Action": ["sts:AssumeRoleWithWebIdentity", "sts:TagSession"],
      "Condition": {
        "StringEquals": {
          "confidentialcomputing.googleapis.com:aud": "AUDIENCE",
          "aws:RequestTag/swname": "CONFIDENTIAL_SPACE",
          "aws:RequestTag/confidential_space.support_attributes": "LATEST=STABLE=USABLE",
          "aws:RequestTag/container.signatures.key_ids": [
            "SIGNING_KEY_ID_1",
            "SIGNING_KEY_ID_2",
            "SIGNING_KEY_ID_1=SIGNING_KEY_ID_2"
          ],
          "aws:RequestTag/gce.project_id": ["OPERATOR_PROJECT_1", "OPERATOR_PROJECT_2"]
        }
      }
    }
  ]
}
```

> The role must permit **`sts:TagSession`** in addition to
> `sts:AssumeRoleWithWebIdentity`. The attestation claims arrive as session tags, so
> without it the call is rejected before any condition is evaluated.

Notes:

* The official Halo image signing key ID is
  `e117571844aad697303b883969daec142b3dd12ac6c8a73cba620f029a653864`.
* `gce.project_id` values are strings, and are the projects in which the operator runs
  the workloads.

> **An image with more than one signature sends one concatenated tag value.**
> `container.signatures.key_ids` is not a list. Confidential Space joins multiple
> signature key IDs into a single `=`-separated string, sorted alphabetically — so an
> image signed with both `SIGNING_KEY_ID_1` and `SIGNING_KEY_ID_2` presents
> `SIGNING_KEY_ID_1=SIGNING_KEY_ID_2`, which matches neither key ID on its own. List
> every combination the workload can legitimately present, as above. Omitting the
> combined value fails with `Not authorized to perform sts:AssumeRoleWithWebIdentity`
> only for the double-signed images, so it can pass in one environment and fail in
> another that signs the same code with an additional key.
>
> Do **not** reach for `ForAnyValue:` / `ForAllValues:` here.
> [`aws:RequestTag/tag-key` is a single-valued context key][aws-multivalue], and AWS
> warns that set operators on single-valued keys can produce overly permissive
> policies.

[aws-multivalue]: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-single-vs-multi-valued-context-keys.html

## Step 4 — Grant the role `kms:Decrypt` on the key

Attach a permissions policy to the role, or add the role as a principal in the KMS key
policy. Include `kms:Encrypt` / `kms:GenerateDataKey` if you also want to support
TrusTEE re-encryption:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"],
      "Resource": "arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID"
    }
  ]
}
```

Note the role ARN for the EDP config: `arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME`.

## Step 5 — Give the operator your AWS KMS configuration (step for the operator)

Provide the operator with your KEK URI, role ARN, region, and audience. The operator
sets your entry in the aggregator's EDP config (`event_data_provider_configs`, message
`EventDataProviderConfig.KmsConfig`) to use `AWS_CONFIDENTIAL_SPACE`:

```textproto
event_data_provider_config {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  kms_config {
    kms_type: AWS_CONFIDENTIAL_SPACE
    aws_role_arn: "arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME"
    aws_role_session_name: "SESSION_NAME"
    aws_region: "REGION"
    aws_audience: "AUDIENCE"
    kek_uri: "aws-kms://arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID"
    # No service_account and no kms_audience: there is no GCP hop.
  }
  tls_config { /* unchanged */ }
  consent_signaling_config { /* unchanged */ }
}
```

## Step 6 — Deploy and verify

1. Deploy a build that supports `AWS_CONFIDENTIAL_SPACE` (ResultsFulfiller, VID
   Labeler, and Duchy TrusTEE mill images).
2. Recreate the workload MIG VMs so they pick up the new config.
3. Trigger a requisition or a labeling run that exercises this EDP, then confirm three
   things: the launcher serves `/v1/token`, STS `AssumeRoleWithWebIdentity` succeeds,
   and AWS KMS `Decrypt` succeeds.

> AWS CloudTrail's `AssumeRoleWithWebIdentity` events are the authoritative place to
> see which trust policy conditions matched. Note that CloudTrail records
> `requestParameters` — including the session tags — only for calls that **succeed**;
> on an `AccessDenied` they are redacted, so a denial shows which role and provider were
> used but not which condition rejected it. Comparing against a successful call from
> another environment is usually the fastest way to spot the differing claim.

---

# Option 2 — GCP federation

## Overview

This option keeps a GCP Workload Identity Pool in the chain, so it requires an
EDP-owned GCP project. The Confidential VM authenticates to your AWS KMS key through
a chain that starts with GCP attestation.

## Architecture

```
Confidential VM (ResultsFulfiller TEE)
  |  1. Attestation token (GCP-native JWT)
  v
GCP Workload Identity Pool (EDP's GCP project)      <-- you control this
  |  2. Impersonate a GCP Service Account
  v
GCP Service Account (EDP's GCP project)             <-- you control this
  |  3. SA generates an OIDC ID token (generateIdToken on itself)
  v
AWS STS AssumeRoleWithWebIdentity
  |  4. Temporary AWS credentials
  v
AWS KMS (EDP's AWS account)                         <-- you control this
```

## Prerequisites

* An EDP-owned GCP project.
* An EDP-owned AWS account.
* The `gcloud` CLI authenticated with sufficient permissions.
* Your `DataProvider` resource name (`dataProviders/DATA_PROVIDER_ID`).

## Resources you will create

| Resource | Example format |
| --- | --- |
| AWS KMS key | `arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID` |
| AWS KMS key URI | `aws-kms://arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID` |
| AWS IAM role | `arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME` |
| AWS OIDC provider | `arn:aws:iam::AWS_ACCOUNT_ID:oidc-provider/accounts.google.com` |

## Step 1 — Create an AWS KMS key

1. AWS Console → KMS → Customer managed keys.
2. Create a **symmetric** encryption key in your preferred region.
3. Note the key ARN: `arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID`.

## Step 2 — Create an AWS OIDC identity provider for Google

If one does not already exist:

1. AWS Console → IAM → Identity providers → Add provider.
2. Provider type: **OpenID Connect**.
3. Provider URL: `https://accounts.google.com`.
4. Audience: leave empty for now (you add the SA unique ID in Step 10).

## Step 3 — Create an AWS IAM role for GCP federation

1. AWS Console → IAM → Roles → Create role.
2. Trusted entity type: **Web identity**.
3. Identity provider: `accounts.google.com`.
4. Attach a policy granting `kms:Decrypt` (add `kms:Encrypt` / `kms:GenerateDataKey`
   if you plan to support TrusTEE re-encryption) on your key:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"],
      "Resource": "arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID"
    }
  ]
}
```

Note the role ARN: `arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME`.

## Step 4 — Create a GCP Workload Identity Pool in your GCP project

1. GCP Console → IAM & Admin → Workload Identity Federation.
2. Create a pool (e.g. `edp-aws-workload-identity-pool`).
3. Add a provider of type **OIDC** (not AWS, not SAML) — the Confidential VM's
   attestation token is a JWT validated via OIDC.
4. Issuer URI: `https://confidentialcomputing.googleapis.com/`.
5. Allowed audiences: include `https://sts.googleapis.com` (the audience baked into
   the Confidential Space attestation token).
6. Attribute mapping: `google.subject = assertion.sub`.
7. Attribute condition (CEL) restricting access to your approved image + the
   operator's ResultsFulfiller SA:

```
assertion.swname == 'CONFIDENTIAL_SPACE'
  && 'RESULTS_FULFILLER_SA@OPERATOR_PROJECT.iam.gserviceaccount.com' in assertion.google_service_accounts
  && ['SIGNATURE_ALGORITHM:KEY_ID'].exists(
       fingerprint,
       fingerprint in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))
```

> **Troubleshooting — audience mismatch.** If the ResultsFulfiller fails with
> `invalid_grant: The audience in ID Token [https://sts.googleapis.com] does not
> match the expected audience`, add `https://sts.googleapis.com` to the provider's
> allowed audiences.

## Step 5 — Create a GCP Service Account for the AWS KMS bridge

```bash
gcloud iam service-accounts create SA_NAME \
  --project=EDP_PROJECT \
  --display-name="EDP AWS KMS Bridge SA"
# Result: SA_NAME@EDP_PROJECT.iam.gserviceaccount.com
```

## Step 6 — Let the WIF pool impersonate the SA

```bash
gcloud iam service-accounts add-iam-policy-binding \
  SA_NAME@EDP_PROJECT.iam.gserviceaccount.com \
  --project=EDP_PROJECT \
  --role=roles/iam.serviceAccountTokenCreator \
  --member="principalSet://iam.googleapis.com/projects/EDP_PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_NAME/*"
```

This lets the attested Confidential VM impersonate the SA (get access tokens) **and**
generate OIDC ID tokens for AWS.

> The GCP-only KMS flow uses `roles/iam.workloadIdentityUser` (only `getAccessToken`).
> The AWS flow needs `roles/iam.serviceAccountTokenCreator` because it also requires
> `getOpenIdToken`.

## Step 7 — Let the SA generate its own ID tokens

Unique to this flow: after impersonation, the SA must call `generateIdToken` on
itself to produce the OIDC JWT for AWS. By default it cannot.

```bash
gcloud iam service-accounts add-iam-policy-binding \
  SA_NAME@EDP_PROJECT.iam.gserviceaccount.com \
  --project=EDP_PROJECT \
  --role=roles/iam.serviceAccountOpenIdTokenCreator \
  --member="serviceAccount:SA_NAME@EDP_PROJECT.iam.gserviceaccount.com"
```

> **Troubleshooting — `iam.serviceAccounts.getOpenIdToken denied`.** This step was
> missed. Both bindings are required and are different:
>
> | Binding | Purpose |
> | --- | --- |
> | WIF pool → `serviceAccountTokenCreator` on SA | Attested VM impersonates the SA and gets access tokens |
> | SA → `serviceAccountOpenIdTokenCreator` on itself | Impersonated SA generates OIDC ID tokens for AWS |

## Step 8 — Get the SA's unique ID

```bash
gcloud iam service-accounts describe SA_NAME@EDP_PROJECT.iam.gserviceaccount.com \
  --project=EDP_PROJECT --format="value(uniqueId)"
# e.g. 109474708679978054863 — needed for the AWS trust policy and OIDC audience
```

## Step 9 — Update the AWS IAM role trust policy

AWS Console → IAM → Roles → `ROLE_NAME` → Trust relationships. Add the SA unique ID
to the `accounts.google.com:sub` condition:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Federated": "arn:aws:iam::AWS_ACCOUNT_ID:oidc-provider/accounts.google.com" },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": { "StringEquals": { "accounts.google.com:sub": ["SA_UNIQUE_ID"] } }
    }
  ]
}
```

## Step 10 — Add the SA unique ID as an OIDC provider audience in AWS

AWS Console → IAM → Identity providers → `accounts.google.com` → add `SA_UNIQUE_ID`
as an audience. AWS validates two things independently for
`AssumeRoleWithWebIdentity`:

* the `sub` claim → checked against the trust policy (Step 9), and
* the `aud` claim → checked against the OIDC provider's audience list (this step).

Both must pass.

## Step 11 — Give the operator your AWS KMS configuration

Provide the operator with your KEK URI, role ARN, region, and SA unique ID. The
operator sets your entry in the aggregator's EDP config (`event_data_provider_configs`,
message `EventDataProviderConfig.KmsConfig`) to use AWS:

```textproto
event_data_provider_config {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  kms_config {
    kms_type: AWS
    # GCP WIF provider — the Confidential VM uses GCP attestation as the first hop
    kms_audience: "//iam.googleapis.com/projects/EDP_PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_NAME/providers/PROVIDER_NAME"
    service_account: "SA_NAME@EDP_PROJECT.iam.gserviceaccount.com"
    # AWS credentials
    aws_role_arn: "arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME"
    aws_role_session_name: "SESSION_NAME"
    aws_region: "REGION"
    aws_audience: "SA_UNIQUE_ID"
    kek_uri: "aws-kms://arn:aws:kms:REGION:AWS_ACCOUNT_ID:key/KEY_ID"
  }
  tls_config { /* unchanged */ }
  consent_signaling_config { /* unchanged */ }
}
```

---

## Reference — IAM bindings compared (Option 2)

Option 2 needs more bindings than the GCP-only flow:

| Binding | Who → role → on what | Purpose |
| --- | --- | --- |
| WIF pool → SA | `principalSet://...POOL/*` → `serviceAccountTokenCreator` → SA | Attested VM impersonates SA, gets access tokens |
| SA → itself | SA → `serviceAccountOpenIdTokenCreator` → SA | Impersonated SA generates OIDC ID tokens for AWS |
| Your user → SA | `user:EMAIL` → `serviceAccountTokenCreator` → SA | You can impersonate the SA locally (e.g. for test data) |

A GCP-only EDP needs only:

| Binding | Who → role → on what | Purpose |
| --- | --- | --- |
| WIF pool → SA | `principalSet://...POOL/*` → `workloadIdentityUser` → SA | Attested VM impersonates SA for GCP KMS access |

Option 1 needs none of these: there is no GCP project in the chain.

---

## Reference — why Option 2 needs GCP WIF

Under Option 2 the AWS OIDC provider is `accounts.google.com`, which cannot validate
a Confidential Space attestation token. The GCP WIF pool + SA is a translation layer:

1. GCP WIF validates the attestation token (GCP-native format).
2. The SA generates a standard OIDC JWT (understood by AWS).
3. AWS STS validates the OIDC JWT and issues temporary credentials.

The WIF **must** be in your project to preserve the privacy guarantee.

Option 1 removes the translation layer by registering
`confidentialcomputing.googleapis.com` itself as the AWS OIDC provider, so AWS
validates the attestation token directly.
