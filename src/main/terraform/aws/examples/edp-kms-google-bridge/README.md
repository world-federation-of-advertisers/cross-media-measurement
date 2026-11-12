# AWS KMS through an EDP-owned Google Cloud project

Configure both providers for the EDP's accounts. The Google module gates
Confidential Space attestations on stable-image support, signing keys, projects,
and exact runtime service accounts. It grants token creation to the federated
identity and self ID-token creation to the bridge account. The AWS role requires
that account's numeric unique ID as its subject and audience.

AWS permits one `accounts.google.com` provider per account. Set
`existing_oidc_provider_arn` to reuse one and add the bridge's numeric ID to that
provider's audience list through the Terraform configuration that owns it.
The numeric ID is available from the `aws_audience` output.

Use outputs `kek_uri`, `aws_role_arn`, `aws_region`, `aws_audience`,
`service_account_email`, and `kms_audience` with `kms_type: AWS`.
The operator supplies `aws_role_session_name`. This example creates no Google
Cloud KMS key. Apply the [onboarding permissions table](../../../../../../docs/edpaggregator/README.md#required-key-operations)
to `workload_kms_actions` and the export principal grants.

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
