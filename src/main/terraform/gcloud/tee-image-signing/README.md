# Dedicated TEE job image signing

This root creates one ECDSA P-256 signing key per TEE job, a dedicated signing
service account with access only to those keys, and federation restricted to the
trusted `Deploy EDP KMS` workflow from main. It reuses the existing `image-signing`
ring. Signing key versions are stable across releases and protected from deletion.

The deployment workflow applies it once in the test signing project and once in
the release signing project, with separate prefixes in each environment's existing
state bucket. Its outputs contain
only public keys, key resource names, and the signing identity. The workflow checks
the existing Halo signature and job repository before adding the job signature.
See the [rollout guide](../../../../../docs/operations/tee-workload-kms.md).
