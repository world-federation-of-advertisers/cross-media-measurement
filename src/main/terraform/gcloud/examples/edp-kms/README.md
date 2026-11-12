# Google Cloud KMS for an EDP

The Google provider uses the EDP-owned project. Enable the Cloud KMS,
IAM, and IAM Credentials APIs through your project's Terraform configuration.
Supply exact operator projects, runtime service accounts, and signing-key
fingerprints. The example checks all of them and stable Confidential Space.

The sample selects decrypt-only access for already VID-labeled input without
TrusTEE. For Parquet, use encrypt/decrypt and list every required labeling
workload. For TrusTEE, use the [onboarding permissions table](../../../../../../docs/edpaggregator/README.md#required-key-operations).
The outputs are `kek_uri`, `service_account_email`, and `kms_audience`.

## Use the example

Use Terraform 1.7 or later with authenticated, keyless cloud credentials and
permission to manage the resources. Copy this directory with its referenced
modules, or use it from a Halo checkout. Configure an appropriate remote backend
for shared state before use.

```bash
cp terraform.tfvars.example terraform.tfvars
# Replace every sample value with the agreed EDP and operator configuration.
terraform init
terraform plan -out=onboarding.tfplan
terraform apply onboarding.tfplan
terraform output
```

All sample values are placeholders. Import any existing infrastructure into this
state before applying, or reuse resources through the documented module inputs.
Manage subsequent changes through Terraform. Review the plan for key replacement
and permission changes; keys are protected with `prevent_destroy`.

## Image-signing fingerprints

Halo publishes its [test and release signing keys](../../../../../../README.md#container-images).
For each approved public key, compute `KEY_ID` from DER bytes and supply
`ECDSA_P256_SHA256:<KEY_ID>` through `image_signature_fingerprints`:

```bash
openssl pkey -pubin -in cosign-release-pub-key.pem -outform DER | openssl sha256
```

Configure the workload's signed-image repository so Confidential Space can fetch
the cosign signatures. Verify a published image against the approved public key
before applying its trust policy. Approve additional signing keys before rotating
the signer, then remove retired keys after all workloads have moved.
