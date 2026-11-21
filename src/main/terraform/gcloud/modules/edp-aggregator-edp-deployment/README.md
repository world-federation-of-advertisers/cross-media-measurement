# EDP-owned Google Cloud KMS integration

Creates a symmetric KEK and key ring, plus the
[Confidential Space workload identity module](../confidential-space-workload-identity).
Configure the Google provider for the EDP's project. `location` applies only to
KMS; the workload identity pool always uses `global`.

The [complete example](../../examples/edp-kms) exposes the resource names,
rotation period, workload trust inputs, and export permissions as variables.

Set `attested_kms_role` to `roles/cloudkms.cryptoKeyEncrypterDecrypter` for
Parquet labeling, or `roles/cloudkms.cryptoKeyDecrypter` for already VID-labeled
input without TrusTEE. Include encrypt access when the same key protects TrusTEE
contributions. Use `additional_kms_iam_members` for export identities and other
explicit key grants. Export identities normally need encrypt only.

Outputs include `kek_uri`, `service_account_email`, and `kms_audience` for Halo's
EDP configuration, plus the key and key-ring resource names for additional IAM
grants or a separate TrusTEE key. A separate TrusTEE key must be created in the
same key ring with its own Terraform-managed permissions.

## Upgrade an existing module deployment

Add `workload_identity_pool_provider_id`, `attribute_condition`,
`allowed_audiences`, `attested_kms_role`, and `additional_kms_iam_members` to the
module call. Use an empty map for additional grants when none are needed.
Existing key addresses are preserved. Moved blocks preserve the service account,
pool, and federated IAM binding as they move into the nested module.

If the OIDC provider or additional IAM grants already exist outside this state,
import them before applying. For infrastructure created manually, import all
existing resources into the corresponding module addresses. Review the plan for
resource replacement and permission changes. The KEK has `prevent_destroy` to
protect existing encrypted data.
