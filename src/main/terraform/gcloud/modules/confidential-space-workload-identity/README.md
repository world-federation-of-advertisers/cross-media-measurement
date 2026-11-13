# Confidential Space workload identity

Creates an EDP-owned service account, globally scoped workload identity pool,
attestation-gated Confidential Space OIDC provider, and federated IAM binding.
It creates no KMS key, so the same module can serve Google KMS or the AWS bridge.

Supply a nonempty CEL `attribute_condition` that checks Confidential Space,
stable-image support, approved signing keys, operator projects, and the exact
workload service accounts. `allowed_audiences` sets the accepted token audiences.
Resource IDs and display names are separate inputs.

Set `federated_service_account_role` to `roles/iam.workloadIdentityUser` for
Google KMS. For the AWS bridge, use `roles/iam.serviceAccountTokenCreator`; the
module also grants the service account `roles/iam.serviceAccountOpenIdTokenCreator`
on itself. The AWS role must restrict the resulting ID token's numeric subject.

See the [Google KMS example](../../examples/edp-kms) and
[AWS bridge example](../../../aws/examples/edp-kms-google-bridge).
