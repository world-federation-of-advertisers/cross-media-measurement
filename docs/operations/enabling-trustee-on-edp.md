# Enabling TrusTEE on EDP

This guide describes how an Event Data Provider (EDP) can enable the TrusTEE
protocol.

There are two ways to enable TrusTEE. The first uses an additional layer of
encryption and binary attestation to ensure that EDP data can only be read by an
authorized TrusTEE binary. The other, while easier to implement, allows EDP data
to be read by anyone with access to the Halo system receiving the data. Which
option is best depends upon the sensitivity of the EDP data as determined by the
Halo operator and the EDP. Note that both methods still provide for encryption
during transmission using TLS and encryption at rest using cloud storage.

If you want to enable TrusTEE with attestation see the next section, otherwise
if you want to enable TrusTEE without attestation see the section below entitled
"TrusTEE without Attestation".

## Overview - TrusTEE with Attestation

Enabling TrusTEE on an EDP involves the following steps:

1.  **Update GCP Resources**: Configure the necessary infrastructure, including
    KMS and Workload Identity Federation.
2.  **Configure WIF Policy**: Define the policy for the Workload Identity
    Federation (WIF) provider, including image signature verification and other
    constraints.
3.  **Update Code**: Update the EDP simulator or application to use the TrusTEE
    library and parameters.

## 1. Configure EDP-owned Resources with Terraform

Use the [EDP KMS module](../../src/main/terraform/gcloud/modules/edp-aggregator-edp-deployment)
and its [Google Cloud example](../../src/main/terraform/gcloud/examples/edp-kms)
to manage the key ring, key, federated service account, identity pool and provider,
and key permissions. Grant the EDP export identity encrypt access; select the
attested workload's permissions from the
[TrusTEE key-arrangement table](../edpaggregator/README.md#required-key-operations).

Import existing resources into the selected state before applying. The
[onboarding guide](../edpaggregator/README.md#3-set-up-kms-and-workload-trust)
also provides AWS and Google-to-AWS Terraform examples. Keep key management and
federation policy in the EDP's infrastructure configuration.

## 2. Configure WIF Policy

The Workload Identity Provider needs an attribute condition (policy) to ensure
that only trusted workloads can assume the identity. This policy verifies the
image signature and other environment properties.

### 2.1 Get the Image Signing Fingerprint

Obtain the public key for the signer approved by the EDP. Halo publishes its
[test and release public keys](../../README.md#container-images). Calculate the
key ID from the SHA-256 hash of the DER-encoded public key:

```bash
KEY_ID=$(openssl pkey -pubin -in cosign-release-pub-key.pem -outform DER | openssl sha256 | cut -d' ' -f2)
FINGERPRINT="ECDSA_P256_SHA256:${KEY_ID}"
```

The base64 public-key annotation on the cosign signature is not the attestation
key ID. Use the fingerprint above in the Google policy, or its key ID in the
AWS session-tag policy.

### 2.2 Configure the Attribute Condition with Terraform

Use the [EDP KMS Terraform example](../../src/main/terraform/gcloud/examples/edp-kms).
Supply the fingerprint through `image_signature_fingerprints`, the exact project
and service accounts, and the key permissions required by TrusTEE. Its condition
requires Confidential Space, stable images, approved projects and identities, and
an approved image signature. Configure the Mill's `signed_image_repo` so the
Confidential Space launcher can read its image signatures.

To restrict admission to the Mill's repository across releases, use the
[purpose-specific workload module and example](../../src/main/terraform/gcloud/examples/tee-workload-kms).
Supply the Mill's dedicated signing fingerprint and approved `image_repositories`.
The condition accepts a tag or digest in those repositories and verifies the
job's signature; a globally shared Halo signing key alone does not distinguish
the Mill from other Halo jobs. Add approved mirrors explicitly. The module
enforces the same conditions for AWS KMS through Google federation.

For Halo-managed test deployments, use the
[shared roots and workflow](../edpaggregator/README.md#halo-managed-test-deployments).
See [EDP KMS onboarding](../edpaggregator/README.md#3-set-up-kms-and-workload-trust)
for imports, key arrangement, and workflow bootstrap instructions.

## 3. Update Code

**Note**: No code updates are required for EDPs that integrate with the EDP
aggregator.

You need to update your EDP application (or simulator) to use the TrusTEE
library and pass the required parameters. This is necessary to generate the
requisition fulfillment data for the TrusTEE protocol.

### TrusTEE Library

Use the
[`FulfillRequisitionRequestBuilder`](../../src/main/kotlin/org/wfanet/measurement/eventdataprovider/requisition/v2alpha/trustee/FulfillRequisitionRequestBuilder.kt)
class (specifically the `EncryptionParams` nested class) in the
`org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee` package
to configure encryption for the TrusTEE protocol.

### Key Parameters

When configuring `EncryptionParams`, you will need to provide the following
information:

*   **KMS KEK URI**: The URI of the Cloud KMS Key Encryption Key (KEK) created
    in step 1.1. This key is used to wrap the data encryption key.
    *   *Example*:
        `gcp-kms://projects/my-project/locations/us-central1/keyRings/my-key-ring/cryptoKeys/my-key`
*   **Workload Identity Provider**: The resource name of the Workload Identity
    Provider created in step 1.3. This is used by the TEE workload to
    authenticate.
    *   *Example*:
        `projects/123456789012/locations/global/workloadIdentityPools/my-pool/providers/my-provider`
*   **Impersonated Service Account**: The email address of the TEE Decrypter
    Service Account created in step 1.2. The TEE workload impersonates this
    account to decrypt data.
    *   *Example*: `tee-decrypter@my-project.iam.gserviceaccount.com`

Ensure your application initializes a `GcpKmsClient` with default credentials
and passes it along with these parameters to the `EncryptionParams` constructor.

## TrusTEE without Attestation

In this case the EDP does not need to set up any additional cloud infrastructure
and simply needs to follow the instructions in the
[requisiton fulfillment API](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/requisition_fulfillment_service.proto)
for sending a FrequencyVector for the TrusTEE protocol with the
`FREQUENCY_VECTOR` DataFormat.
