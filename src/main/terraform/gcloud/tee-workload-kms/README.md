# Halo TEE workload KMS deployment

This shared root configures all five TEE jobs in dev, head, and QA. Google
identities and Google/AWS purpose-specific KEKs are planned in one dependency
graph. Its GCS state prefix is `terraform/state/edp-kms/workloads`, separate from
the existing shared key states and each CMMS state.

Use [Deploy EDP KMS](../../../../../.github/workflows/deploy-edp-kms.yml). The
workflow supplies the EDP-owned Google and AWS deployment credentials separately
and retains independent credentials for the state bucket. It derives job signing
fingerprints from the Terraform-managed public keys. Resource deletion or
replacement is rejected for keys and identities. Revoking a job's Google key
grant is supported.

The committed [job configuration](../../../../../.github/env-configs/tee-workloads.json)
defines each job's repository and purpose permissions. Runtime configuration is
rendered from the outputs so actual AWS key IDs are used without storing generated
state in source control. Follow the [rollout guide](../../../../../docs/operations/tee-workload-kms.md)
before activation or retiring the legacy federation paths.
