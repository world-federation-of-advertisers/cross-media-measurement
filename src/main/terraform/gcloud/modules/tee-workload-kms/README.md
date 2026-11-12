# TEE job identities and purpose-specific KEKs

Create one isolated Confidential Space pool and service account per job, one AWS
bridge role per job, and separate Google and AWS KEKs by purpose. Each job input
specifies its launch accounts and projects, dedicated signing fingerprints,
approved untagged repositories, and permitted key operations.

Repository matching accepts either a tag or digest after a full repository
boundary. Different jobs cannot share signing fingerprints, service accounts,
pools, or AWS roles. AWS uses Google's federation admission so its KMS access
includes the repository condition that direct AWS principal tags do not expose.

Existing input keys are referenced without importing or replacing them. Their
owner must permit the job grants; existing AWS key policies must enable IAM
delegation for the supplied role policies. New keys have `prevent_destroy`.
Rotation and deletion-window choices are explicit inputs.

See the [complete example](../../examples/tee-workload-kms) and
[rollout guide](../../../../../../docs/operations/tee-workload-kms.md).
