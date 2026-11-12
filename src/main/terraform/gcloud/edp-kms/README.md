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
`Deploy EDP KMS` workflow on main. It grants administration of service accounts in the EDP project so Terraform
can manage the existing runtime account and the new per-job identities. Runtime
workloads do not receive those administration permissions. The shared root also
owns the project IAM administrator binding and removes the historical grant to
the shared runtime account. Its committed member list retains only the EDP
deployment account; review any additional key-owner administrators before apply.

An administrator must bootstrap this federation once before activating the
updated CMMS workflow, so KMS deployment can authenticate. Use existing administrator credentials for the EDP project and the
state bucket; the Terraform bootstrap creates the dedicated deployment pool and
provider, federation grant, and service-account administration grants:

```bash
python3 .github/workflows/scripts/prepare_edp_kms.py --output-directory /tmp/edp-kms-inputs
terraform -chdir=src/main/terraform/gcloud/edp-kms init -backend-config=bucket=halo-cmm-dev-tf
terraform -chdir=src/main/terraform/gcloud/edp-kms plan \
  -var-file=/tmp/edp-kms-inputs/google.tfvars.json \
  -target=google_iam_workload_identity_pool_provider.deployment \
  -target=google_service_account_iam_member.deployment_federation \
  -target=google_service_account_iam_member.deployment_account_admin \
  -target=google_project_iam_member.deployment_job_identity_admin \
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

See the [per-job rollout guide](../../../../../docs/operations/tee-workload-kms.md)
for signing-key provisioning, runtime activation, and retirement of this root's
legacy shared federation.
