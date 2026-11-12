# Purpose-specific KMS access for TEE workloads

Each TEE job has its own Google workload identity pool and service account, and
its own AWS IAM role. The pool requires stable Confidential Space, the approved
launch project and service account, a signing key dedicated to that job, and an
approved container repository. Tags and digests can change across releases.

The [reusable module](../../src/main/terraform/gcloud/modules/tee-workload-kms)
and [complete example](../../src/main/terraform/gcloud/examples/tee-workload-kms)
support both Google Cloud KMS and AWS KMS. Every permission and resource name is
an explicit input. Publishers manage their own keys and admission policies.

## Keys and permissions

| Data purpose | Workloads that encrypt | Workloads that decrypt |
| --- | --- | --- |
| Publisher input | Publisher upload identity | Results Fulfiller or VID Labeler, according to the input path |
| Subpool map | Subpool Assigner | VID Rank Builder |
| Rank index | VID Rank Builder | VID Rank Builder and VID Labeler |
| Labeled impressions | VID Labeler | Results Fulfiller |
| TrusTEE frequency vector | Results Fulfiller | TrusTEE Mill |

The Halo deployment creates separate output-purpose keys for each environment
in both clouds. It preserves existing publisher input keys. TrusTEE Mill's new
identity has only decrypt access to its frequency-vector key. Results Fulfiller
uses its own identity to wrap the payload, and sends the Mill's identity and
frequency-vector KEK in the requisition header.

For migration, VID Rank Builder can also decrypt the existing input key because
historical subpool and rank-index objects were encrypted with it. Remove that
grant from the committed job permission map after those objects are rebuilt or
expire. VID Labeler still needs the publisher input key to read raw impressions.
Key administrators and publisher upload identities retain their explicitly
configured administration and input-data permissions.

## Image approval and mirrors

Confidential Space attests the image digest, verified signing keys, and launched
image reference. An operator selects the image reference when launching a VM;
the operator cannot rewrite Google's signed attestation claims. The reference
does not by itself prove which program the bytes implement.

A signing key dedicated to a job approves that job's image bytes. The trusted
signing workflow first verifies the existing Halo signature and the signed
repository identity in its payload, then signs the verified digest with the
matching job key. Copying a different globally signed job into the expected
repository does not satisfy that check. Protect publishing access to approved
repositories and access to the signing identities.

An image and its accompanying signatures can be mirrored. The bytes and signature
remain valid, while the launched `image_reference` names the mirror. Add every
approved mirror to that job's `image_repositories` allowlist. The condition accepts
`repository:tag` and `repository@digest`; it does not accept a sibling repository
such as `repository-extra`. Configure the launcher to discover the copied
signatures through `tee-signed-image-repos` as well.

AWS's direct Confidential Space principal tags expose signing key IDs and image
digests, but do not expose `image_reference`. This module therefore uses the
Google federation bridge for AWS: Google checks all attestation conditions, then
AWS admits only the job's numeric Google service-account identity and matching
audiences. This path requires an EDP-owned Google identity project even when the
KEK is in AWS. Runtime `kms_type` is `AWS`, not `AWS_CONFIDENTIAL_SPACE`.

## Halo deployment and rollout

The committed [.github/env-configs/tee-workloads.json](../../.github/env-configs/tee-workloads.json)
defines all five jobs, approved repositories, purpose permissions, signing
projects, and activation controls. Dev and head use the test project job keys;
QA uses the release project job keys. Private signing keys remain in Cloud KMS.
Dedicated signing accounts have access only to the job keys and trust the
`Deploy EDP KMS` workflow from main.

The [signing root](../../src/main/terraform/gcloud/tee-image-signing) uses separate
states in the two signing projects' existing CMMS state buckets. The
[workload KMS root](../../src/main/terraform/gcloud/tee-workload-kms) uses a separate
shared state for Google job identities and both clouds' purpose-specific keys.
Its Google and AWS credentials belong to the respective key owners. Existing
input keys remain owned by their original shared roots.

The [deployment workflow](../../.github/workflows/deploy-edp-kms.yml) provisions
signing keys, verifies and signs deployed and candidate images, plans job identity
and key permissions, and applies saved plans. It uses the legacy Simple Signing
format supported by Confidential Space. It then renders runtime configuration
from Terraform outputs, including the newly assigned AWS key IDs. CMMS Terraform
uploads the selected environment's configuration while TEE consumers are stopped.

Activation is configured for all three environments. The workflow refuses to
install job profiles for a release without the new protobuf field; existing
images keep their legacy configuration until a compatible release is deployed.
Follow these steps:

1. Complete the [EDP deployment identity bootstrap](../../src/main/terraform/gcloud/edp-kms/README.md),
   including service-account administration for the new job identities. The CMMS
   Terraform identities also need their existing IAM and KMS administration
   permissions in the two signing projects.
2. Review and provision the two signing roots. Their public keys must exist
   before Terraform can plan the derived signer fingerprints. On the initial
   dry run the workflow stops after the signing plans if no public keys exist.
   Once provisioned, `apply=false` plans the complete derived trust and KEK graph.
3. Publish a release containing `workload_kms_configs` support and deploy it
   through Update CMMS. Job profiles activate with that release. Repeat for dev,
   head, and QA. Inspect the applied job identity and
   purpose-key outputs and the uploaded runtime configuration for each environment.
4. Drain requisitions whose TrusTEE headers reference the shared identity or old
   KEK. Keep legacy federation enabled while any environment or queued request
   still needs it. New requests use the purpose-specific recipient KEK.
5. Set `retire_legacy_federation` to true after all three environments are migrated
   and old requests have drained. This makes Google shared admission conditions
   false and gives the old direct AWS role an audience its OIDC provider cannot
   accept. It preserves keys for historical data; it does not destroy them.

The shared Google root removes the historical project IAM administrator grant
from the legacy runtime account before provisioning job keys. Otherwise that
account could grant itself access and bypass the new policies. New purpose keys
admit only their configured job identities. Legacy federation still admits the
shared identity to existing input keys and historical frequency vectors until
retirement. Older AWS frequency vectors and publisher inputs share a KEK, so
their purpose cannot be separated retrospectively by changing IAM. Drain or
rewrap that ciphertext before revoking the old recipient path. Historical VID
objects retain their metadata KEK and are read with explicitly granted job access.

After activation, each runner selects its own `workload_kms_configs` entry. A
missing or duplicate job entry fails rather than reverting to shared credentials.
The rank builder and labeler use explicit output-purpose KEKs instead of reusing
their input KEKs. Legacy configurations with no job entries retain their existing
behavior for the staged rollout.
