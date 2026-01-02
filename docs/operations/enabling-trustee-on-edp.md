# Enabling TrusTEE on EDP

This guide describes how to enable the TrusTEE protocol on an Event Data Provider (EDP).

## Overview

Enabling TrusTEE on an EDP involves the following steps:

1.  **Update GCP Resources**: Configure the necessary infrastructure, including KMS and Workload Identity Federation.
2.  **Configure WIF Policy**: Define the policy for the Workload Identity Federation (WIF) provider, including image signature verification and other constraints.
3.  **Update Code**: Update the EDP simulator or application to use the TrusTEE library and parameters.

## 1. Update GCP Resources

You need to update your infrastructure to include the necessary resources for TrusTEE. This primarily involves setting up Cloud KMS for key management and Workload Identity Federation for authentication.

You can use the [Simulator Terraform Module](../../src/main/terraform/gcloud/modules/simulator/README.md) as a reference. If you are not using Terraform, you need to manually create the following resources:

### 1.1 Cloud KMS Key Ring and Crypto Key
Create a Key Ring and a Crypto Key in Cloud KMS. This key is owned by the EDP and will be used as the Key Encryption Key (KEK) for encrypting data. The TrusTEE mill will use this key for decryption after successfully authenticating via Workload Identity Federation (WIF).
*   **Key Ring**: Create a key ring in the desired location.
*   **Crypto Key**: Create a key with purpose `ENCRYPT_DECRYPT`.

### 1.2 Service Accounts
Create two service accounts:
*   **EDP Service Account**: The main service account for your EDP application.
    *   Grant `roles/cloudkms.cryptoKeyEncrypterDecrypter` on the Crypto Key to this account.
*   **TEE Decrypter Service Account**: A dedicated service account for the TEE workload to decrypt data. This account will be impersonated by the TrusTEE workload.
    *   Grant `roles/cloudkms.cryptoKeyDecrypter` on the Crypto Key to this account.

### 1.3 Workload Identity Pool and Provider
Set up Workload Identity Federation to allow the TEE workload (running in Confidential Space) to impersonate the TEE Decrypter Service Account.

*   **Workload Identity Pool**: Create a pool to manage external identities.
*   **Workload Identity Provider**: Create an OIDC provider within the pool.
    *   **Issuer URI**: `https://confidentialcomputing.googleapis.com`
    *   **Allowed Audiences**: `https://sts.googleapis.com`
    *   **Attribute Mapping**: Map `google.subject` to `assertion.sub`.

### 1.4 Grant Impersonation Rights
Grant the `roles/iam.workloadIdentityUser` role to the principal set representing the Workload Identity Pool on the **TEE Decrypter Service Account**.
*   **Member**: `principalSet://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<POOL_ID>/*`

## 2. Configure WIF Policy

The Workload Identity Provider needs an attribute condition (policy) to ensure that only trusted workloads can assume the identity. This policy verifies the image signature and other environment properties.

### 2.1 Get Image Signing Public Key
To ensure that only trusted code runs in the TEE, the Docker image for the TrusTEE mill is signed. You need to obtain the public key corresponding to the private key used for signing.

The signing public key should be accessible by the EDP, or the derived `encoded_pub_key` should be known by the EDP.

If you have access to the public key in Cloud KMS, you can retrieve it using the `gcloud` CLI. The following command retrieves the public key and encodes it in base64.

```bash
# Replace with your actual KMS key resource name
GCP_KMS_KEY_RESOURCE_NAME="projects/YOUR_PROJECT/locations/YOUR_LOCATION/keyRings/YOUR_KEYRING/cryptoKeys/YOUR_KEY/cryptoKeyVersions/YOUR_VERSION"

gcloud kms keys versions get-public-key "${GCP_KMS_KEY_RESOURCE_NAME}" --output-file pubkey.pem

# Encode the public key in base64 and strip newlines
encoded_pub_key=$(cat pubkey.pem | openssl base64 | tr -d '[:space:]' | sed 's/[=]*$//')

echo "Encoded Public Key: ${encoded_pub_key}"
```

### 2.2 Define Attribute Condition
Configure the attribute condition on the Workload Identity Provider to enforce the following:

*   **Software Name**: Must be `CONFIDENTIAL_SPACE`.
*   **Image Signature**: The running container's image signature must match the expected public key (retrieved in step 2.1).
*   **Image Name**: The running container's image name must match the expected image name.
*   **Project ID**: Restrict to a specific Google Cloud Project ID.
*   **Debug Mode**: Enforce whether debug mode is allowed or not. Debug mode is used for development and debugging, and it exposes the TEE application host to the operator. Although the host still cannot access the memory of the TEE application, debug mode should be avoided in production environments unless necessary debugging is inevitable.

**Example Condition:**

```cel
assertion.swname == 'CONFIDENTIAL_SPACE' &&
['<ALGORITHM>:<ENCODED_PUBLIC_KEY>'].exists(fingerprint, fingerprint in assertion.submods.container.image_signatures.map(sig,sig.signature_algorithm+':'+sig.key_id)) &&
assertion.submods.container.image_reference == '<IMAGE_NAME>' &&
assertion.submods.gce.project_id == '<YOUR_PROJECT_ID>' &&
assertion.submods.confidential_space.support_attributes.debug_mode == 'false'
```

Replace `<ALGORITHM>` (e.g., `ECDSA_P256_SHA256`), `<ENCODED_PUBLIC_KEY>`, `<IMAGE_NAME>`, and `<YOUR_PROJECT_ID>` with your specific values.

## 3. Update Code

**Note**: No code updates are required for EDPs that integrate with the EDP aggregator.

You need to update your EDP application (or simulator) to use the TrusTEE library and pass the required parameters. This is necessary to generate the requisition fulfillment data for the TrusTEE protocol.

### TrusTEE Library

Use the [`FulfillRequisitionRequestBuilder`](../../src/main/kotlin/org/wfanet/measurement/eventdataprovider/requisition/v2alpha/trustee/FulfillRequisitionRequestBuilder.kt) class (specifically the `EncryptionParams` nested class) in the `org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee` package to configure encryption for the TrusTEE protocol.

### Key Parameters

When configuring `EncryptionParams`, you will need to provide the following information:

*   **KMS KEK URI**: The URI of the Cloud KMS Key Encryption Key (KEK) created in step 1.1. This key is used to wrap the data encryption key.
    *   *Example*: `gcp-kms://projects/my-project/locations/us-central1/keyRings/my-key-ring/cryptoKeys/my-key`
*   **Workload Identity Provider**: The resource name of the Workload Identity Provider created in step 1.3. This is used by the TEE workload to authenticate.
    *   *Example*: `projects/123456789012/locations/global/workloadIdentityPools/my-pool/providers/my-provider`
*   **Impersonated Service Account**: The email address of the TEE Decrypter Service Account created in step 1.2. The TEE workload impersonates this account to decrypt data.
    *   *Example*: `tee-decrypter@my-project.iam.gserviceaccount.com`

Ensure your application initializes a `GcpKmsClient` with default credentials and passes it along with these parameters to the `EncryptionParams` constructor.
