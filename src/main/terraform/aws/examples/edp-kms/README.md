# Direct AWS KMS for an EDP

Supply a custom audience, verified issuer CA thumbprints, exact operator
projects, and signing-key tags. A multi-signature tag is one alphabetically
sorted `=`-joined string; include every approved combination. The example checks
Confidential Space, stable-image support, audience, signing-key tags, and project.
Do not use the audience `https://sts.google.com`.

The sample permits decrypt-only for already VID-labeled input. For Parquet,
set `workload_kms_actions` to encrypt and decrypt. Use the
[onboarding permissions table](../../../../../../docs/edpaggregator/README.md#required-key-operations)
for TrusTEE. Export normally needs encrypt only.
Use outputs `kek_uri`, `aws_role_arn`, `aws_region`, and `aws_audience` with
`kms_type: AWS_CONFIDENTIAL_SPACE`. The operator supplies `aws_role_session_name`.

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

When adopting an existing key, set `key_policy_json` to its complete policy when
existing IAM delegation or grants must be retained. Pass `null` for a new key.
