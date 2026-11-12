# Shared EDP KMS attestation checks

Run `terraform init` and `terraform test` from this directory with Terraform 1.11
or later. These plan-only tests validate each environment's required claims and
reject missing signers or incorrectly encoded key IDs; no cloud resources are used.
