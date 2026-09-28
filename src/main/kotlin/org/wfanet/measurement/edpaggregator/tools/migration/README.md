# VID-labeled impression migration

`migrate-vid-labeled-impressions` copies immutable, encrypted VID-labeled impression blobs from
one model-line date tree into the canonical output tree for another model line. It rewrites the
plaintext `BlobDetails` sidecars so Data Availability Sync registers the copied blobs under the
destination model line.

Use this tool only when the source and destination model lines produce compatible VID assignments.
The tool cannot prove compatibility. In particular, do not mix output from independently ranked
memoized model lines: changing a user VID at the migration boundary corrupts multi-day reach.

## Behavior

For each UTC date in the inclusive requested range, the tool:

1. Skips the date if the destination date directory contains any object.
2. Skips the date if the source date directory is absent or has no `done` marker.
3. Reads every non-empty `.binpb` or `.json` metadata object whose filename contains `metadata`.
4. Validates that each `BlobDetails.model_line` is the requested source model line and that its
   referenced encrypted data object exists.
5. Streams the source through authenticated decryption and re-encrypts it at the destination key.
6. Writes a binary `BlobDetails` sidecar with only `blob_uri` and `model_line` changed.
7. Writes an empty `done` object last.

Source objects are never modified. Because the storage object key is authenticated as associated
data, ciphertext cannot be copied directly to a new key. The CLI unwraps the existing DEK through
GCP KMS, decrypts and re-encrypts the data as a bounded stream, and preserves the wrapped DEK,
interval, event-group reference ID, and entity keys.

The source prefix must be the directory immediately above its ISO date directories:

```text
gs://source-bucket/existing/path/model-line/OLD_ID/
  2026-06-01/
  2026-06-02/
```

The destination prefix is the same base prefix configured as `edp_impression_path` for VID-labeled
output. The tool derives the canonical destination path:

```text
gs://destination-bucket/vid-labeled-prefix/
  model-line/NEW_ID/2026-06-01/
```

## Confidential Space, authentication, and IAM

Production write runs must execute the `migrate_vid_labeled_impressions_image` target as a
Confidential Space workload. The EDP KMS trust policy must authorize the exact signed migration
image through the same Workload Identity Federation pattern used by the VID Labeler; do not grant
the runtime service account direct access to EDP plaintext or KEKs.

The Confidential Space VM service account uses Application Default Credentials for GCS. Grant it:

- `roles/storage.objectViewer` on the source metadata bucket and every bucket referenced by a
  source `BlobDetails.blob_uri`;
- `roles/storage.objectUser` on the destination bucket;
- `roles/confidentialcomputing.workloadUser` in the project hosting the Confidential Space VM;
- `roles/logging.logWriter` in that project when Cloud Logging output is enabled.

Pass the EDP KMS Workload Identity provider audience and target service account with
`--kms-wif-audience` and `--kms-service-account`. The target EDP service account needs
`roles/cloudkms.cryptoKeyDecrypter` on every GCP KEK referenced by the source sidecars and must
allow the configured Workload Identity principal to impersonate it. The CLI supports `gcp-kms://`
KEKs and reuses the existing wrapped DEK; it does not generate or rewrap key material.

## Run

The release workflow publishes and signs the
`edp-aggregator/migrate-vid-labeled-impressions` image. Set the variables below to that signed image,
its repository, and the deployment values, then create the argument file for a dry run:

```shell
IMAGE_URI=REGISTRY/REPOSITORY/edp-aggregator/migrate-vid-labeled-impressions:TAG
SIGNED_IMAGE_REPOSITORY=REGISTRY/REPOSITORY/edp-aggregator/migrate-vid-labeled-impressions
PROJECT_ID=OPERATOR_PROJECT
ZONE=ZONE
SUBNET=SUBNET
RUNTIME_SERVICE_ACCOUNT=MIGRATION_RUNTIME_SERVICE_ACCOUNT

TEE_ARGS=(
  --start-date=2026-06-01
  --end-date=2026-08-14
  --source-model-line=modelProviders/PROVIDER/modelSuites/SUITE/modelLines/OLD
  --destination-model-line=modelProviders/PROVIDER/modelSuites/SUITE/modelLines/NEW
  --source-date-prefix=gs://BUCKET/existing/path/model-line/OLD
  --destination-blob-prefix=gs://BUCKET/vid-labeled/path
  --gcs-project="$PROJECT_ID"
  --kms-wif-audience=//iam.googleapis.com/projects/NUMBER/locations/global/workloadIdentityPools/POOL/providers/PROVIDER
  --kms-service-account=EDP_KMS_SERVICE_ACCOUNT
  --model-lines-are-compatible
  --dry-run
)
jq -n --args '$ARGS.positional' -- "${TEE_ARGS[@]}" > tee-cmd.json
```

Launch the one-shot Confidential Space VM:

```shell
gcloud compute instances create vid-labeled-impression-migration \
  --project="$PROJECT_ID" \
  --zone="$ZONE" \
  --machine-type=n2d-standard-4 \
  --min-cpu-platform="AMD Milan" \
  --confidential-compute-type=SEV \
  --maintenance-policy=TERMINATE \
  --shielded-secure-boot \
  --image-project=confidential-space-images \
  --image-family=confidential-space \
  --service-account="$RUNTIME_SERVICE_ACCOUNT" \
  --scopes=cloud-platform \
  --subnet="$SUBNET" \
  --no-address \
  --metadata="tee-image-reference=$IMAGE_URI,tee-signed-image-repos=$SIGNED_IMAGE_REPOSITORY,tee-container-log-redirect=true,google-logging-enabled=true" \
  --metadata-from-file=tee-cmd=tee-cmd.json
```

Inspect the serial or Cloud Logging output for the summary. To perform the migration, remove
`--dry-run` from `tee-cmd.json`, use a new VM name, and repeat the launch. Delete each VM after its
container exits:

```shell
gcloud compute instances delete vid-labeled-impression-migration \
  --project="$PROJECT_ID" \
  --zone="$ZONE"
```

The default attestation-token path is `/run/container_launcher/attestation_verifier_claims_token`;
override it with `--kms-credential-source-file` only when the Confidential Space launcher exposes
the token at a different path. Add the deployment-specific network flags needed to reach GCS and
Google STS if they differ from the example.

The release artifact `MigrateVidLabeledImpressions_deploy.jar` and this Bazel command are useful for
help output and development, but a production write still requires the attested runtime:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/migration:MigrateVidLabeledImpressions \
  -- \
  <the same flags>
```

## Operational safety

- Pause writes to the destination model line for the requested historical range while the CLI is
  running.
- Do not include dates already produced by the VID-labeling pipeline. A non-empty destination date
  is skipped in its entirety.
- The tool writes `done` only after all copied data and metadata for a date succeed. DataWatcher
  can then trigger Data Availability Sync normally.
- GCS has no transaction spanning all objects in a date. If a copy fails, the partial destination
  has no `done` marker and remains inert, but a subsequent run skips it because it is non-empty.
  Inspect the failure, remove only that incomplete destination date, and rerun it.
- Keep the source objects until the rollback window and every report that can reference them have
  expired.

The final summary reports copied, planned, skipped, and failed dates plus object and byte counts.
