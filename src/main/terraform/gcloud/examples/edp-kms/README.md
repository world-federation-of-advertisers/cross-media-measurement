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
