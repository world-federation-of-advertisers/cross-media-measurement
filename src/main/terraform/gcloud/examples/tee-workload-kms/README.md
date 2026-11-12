# Per-job TEE KMS example for Google Cloud and AWS

This example creates isolated identities for TrusTEE Mill, Results Fulfiller,
Subpool Assigner, VID Rank Builder, and VID Labeler. It creates four output-purpose
KEKs in each cloud and grants access to existing publisher input KEKs.

1. Copy `terraform.tfvars.json.example` to `terraform.tfvars.json`.
2. Replace all example projects, accounts, input key IDs, administrator ARNs,
   launch accounts, and signer fingerprints. The supplied fingerprints are
   placeholders; they do not identify usable signing keys. Supply the SHA-256
   fingerprint of the DER public key for each job's approved signer. Use different
   signing keys for different jobs and add approved mirror repositories explicitly.
3. Provide an existing Google key ring and input keys managed by publisher
   Terraform. The AWS input key policy must permit IAM delegation for the job role
   policies. Use a remote state backend suitable for the key owner and authenticate
   separately to the Google identity/key project and the AWS key account.
4. Run `terraform init`, `terraform plan`, and review the key permissions before
   `terraform apply`. New keys are protected from destruction.
5. Use `workloads`, `google_kek_uris`, and `aws_kek_uris` outputs to populate the
   job-specific runtime config. For AWS, use `kms_type: AWS` and the numeric bridge
   audience from the job output. Publish signatures in the format Confidential
   Space supports and configure `tee-signed-image-repos` on the launcher.

See the [runtime and migration guide](../../../../../../docs/operations/tee-workload-kms.md)
for the permission matrix, TrusTEE recipient header, and legacy ciphertext handling.
