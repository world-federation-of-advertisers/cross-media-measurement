# EDP-owned AWS KMS integration

Creates a symmetric KMS key, alias, and attested-workload IAM role. It creates an
OIDC provider when `existing_oidc_provider_arn` is null, or reuses the supplied
provider. AWS permits only one provider per issuer URL in an account; a reused
provider must already allow the configured audiences.

The role trusts only that OIDC provider. `trust_condition` is a required JSON IAM
Condition; `sts_actions` must include `sts:AssumeRoleWithWebIdentity`. Direct
Confidential Space federation also needs `sts:TagSession` for attestation claims.
Use the [direct AWS example](../../examples/edp-kms), or the
[Google Cloud bridge example](../../examples/edp-kms-google-bridge).

`workload_kms_actions` selects encrypt, decrypt, or both according to the
[onboarding permissions table](../../../../../../docs/edpaggregator/README.md#required-key-operations).
`export_principal_permissions` maps export principal ARNs to their required
operations. Export normally needs encrypt only. `key_administrator_arns` must
include the identity managing Terraform. Rotation, deletion waiting period,
role name, key description, alias, issuer CA thumbprints, and audiences are
explicit inputs. Outputs provide the key URI and role ARN.

The key has `prevent_destroy` to protect existing encrypted data. Import existing
keys, roles, aliases, and providers before adoption; verify the plan does not
replace the key. Each separate TrusTEE key must be in the same account and region
and have Terraform-managed grants for its required workloads.
