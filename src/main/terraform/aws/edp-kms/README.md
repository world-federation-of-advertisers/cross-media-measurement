# Shared EDP AWS KMS

This root adopts the existing symmetric key and alias, Confidential Space OIDC
provider, attested-workload role, and Google-federated export role. All three
environments share these resources and this root's state. The AWS provider pins
the account ID parsed from the committed key URI; it does not use a CMMS project's
credentials.

The role retains its existing attestation requirements: stable Confidential
Space, approved operator projects, and test/release image-signing key IDs. Both
orders of a two-signature tag are accepted. The existing IAM-delegating key policy,
export subjects, inline policies, and disabled key rotation are preserved.

The [Deploy EDP KMS workflow](../../../../../.github/workflows/deploy-edp-kms.yml)
assumes the existing GitHub OIDC deployment role, uses a separate remote-state
prefix, imports existing resources, and applies a saved plan. The key is protected
with `prevent_destroy`; the workflow also rejects key creation, replacement,
resource deletion, or rotation changes. No access keys are stored in GitHub.

Shared settings live in [.github/env-configs/edp-kms.json](../../../../../.github/env-configs/edp-kms.json).
Input key URIs and per-environment export subjects remain sourced from the committed
CMMS environment files. Review `Deploy EDP KMS` with `apply=false` before its first
apply; the Google deployment identity requires the one-time
[Terraform bootstrap](../../gcloud/edp-kms/README.md#deployment-credentials).
