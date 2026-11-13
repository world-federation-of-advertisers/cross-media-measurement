# Shared EDP Google Cloud KMS

This root adopts the existing EDP-owned input key, TrusTEE re-encryption key,
service account, identity pool, and dev/head/qa providers. It uses one state for
shared resources, separate from each CMMS deployment state. Existing keys are
imported and protected from destruction; their rotation settings are retained.
Existing direct export and CI key grants are retained alongside the attested
workload path.

The workload policy requires stable Confidential Space, the exact operator
project and approved service accounts, and an approved image-signing fingerprint.
Dev and head trust the test key; QA trusts the release key. Signatures must be
available through each workload's `tee-signed-image-repos` metadata.

## Deployment credentials

The EDP's Terraform account needs its existing Cloud KMS and Workload Identity
Pool administration permissions. The deployment provider is restricted to the
`Deploy EDP KMS` workflow on main. Its service-account permissions are scoped to
the Terraform account and the existing runtime account.

An administrator must bootstrap this federation once before activating the
updated CMMS workflow, so KMS deployment can authenticate. Use existing administrator credentials for the EDP project and the
state bucket; the Terraform bootstrap creates the dedicated deployment pool and
provider, federation grant, and scoped service-account administration grants:

```bash
python3 .github/workflows/scripts/prepare_edp_kms.py --output-directory /tmp/edp-kms-inputs
terraform -chdir=src/main/terraform/gcloud/edp-kms init -backend-config=bucket=halo-cmm-dev-tf
terraform -chdir=src/main/terraform/gcloud/edp-kms plan \
  -var-file=/tmp/edp-kms-inputs/google.tfvars.json \
  -target=google_iam_workload_identity_pool_provider.deployment \
  -target=google_service_account_iam_member.deployment_federation \
  -target=google_service_account_iam_member.deployment_account_admin \
  -out=/tmp/edp-kms-inputs/bootstrap.tfplan
terraform -chdir=src/main/terraform/gcloud/edp-kms apply /tmp/edp-kms-inputs/bootstrap.tfplan
```

Review the bootstrap plan before applying. It must not create or replace a KMS
key. Run the shared deployment workflow from main with `apply=false`, review both
cloud plans, then run it with `apply=true`. The first full plan imports the live
resources. Do not import them into per-environment CMMS states.

The [Deploy EDP KMS workflow](../../../../../.github/workflows/deploy-edp-kms.yml)
verifies deployed and candidate image signatures, plans both cloud roots, rejects
resource removal, key recreation and rotation changes, and applies saved plans.
CMMS updates call the workflow before pausing or replacing consumers. Changes to
the shared configuration on main also trigger it.
