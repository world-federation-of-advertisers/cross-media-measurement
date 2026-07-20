# AWS KMS Setup Guide

**Audience:** an EDP (with their operator) that wants to use **AWS KMS** instead of
GCP KMS to protect impression data.

This guide replaces the GCP KMS steps in the [EDP Onboarding Guide](edp-onboarding.md)
(Section 2.2). Everything else in that guide — data formatting, upload paths, and the
daily workflow — is unchanged.

> Generic placeholders throughout: substitute your own `REGION`, `AWS_ACCOUNT_ID`,
> `KEY_ID`, `ROLE_NAME`, GCP `EDP_PROJECT` / `EDP_PROJECT_NUMBER`, the operator's
> `RESULTS_FULFILLER_SA` / `OPERATOR_PROJECT`, and the image signature fingerprint
> (`SIGNATURE_ALGORITHM:KEY_ID`).

---

## Overview

Storage and encryption are **decoupled**. Impressions stay in the operator's shared
GCS bucket; only the encryption key changes from GCP KMS to AWS KMS. The
Confidential VM (the aggregator's ResultsFulfiller TEE) authenticates to your AWS KMS
key through a chain that starts with GCP attestation:

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

**Privacy model:** the Workload Identity Pool and Service Account **must** live in
**your** GCP project, not the operator's. That keeps you in control of who can access
your KMS key through attestation. If the operator controlled the WIF, they could
generate tokens without a legitimate Confidential VM.

---

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

---

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

Unique to the AWS flow: after impersonation, the SA must call `generateIdToken` on
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

## Reference — IAM bindings compared

The AWS flow needs more bindings than the GCP-only flow:

| Binding | Who → role → on what | Purpose |
| --- | --- | --- |
| WIF pool → SA | `principalSet://...POOL/*` → `serviceAccountTokenCreator` → SA | Attested VM impersonates SA, gets access tokens |
| SA → itself | SA → `serviceAccountOpenIdTokenCreator` → SA | Impersonated SA generates OIDC ID tokens for AWS |
| Your user → SA | `user:EMAIL` → `serviceAccountTokenCreator` → SA | You can impersonate the SA locally (e.g. for test data) |

A GCP-only EDP needs only:

| Binding | Who → role → on what | Purpose |
| --- | --- | --- |
| WIF pool → SA | `principalSet://...POOL/*` → `workloadIdentityUser` → SA | Attested VM impersonates SA for GCP KMS access |

---

## Reference — why GCP WIF is needed for AWS KMS

The Confidential VM runs on GCP, so its attestation token is GCP-specific and AWS
cannot validate it directly. The GCP WIF pool + SA is a translation layer:

1. GCP WIF validates the attestation token (GCP-native format).
2. The SA generates a standard OIDC JWT (understood by AWS).
3. AWS STS validates the OIDC JWT and issues temporary credentials.

The WIF **must** be in your project to preserve the privacy guarantee.
