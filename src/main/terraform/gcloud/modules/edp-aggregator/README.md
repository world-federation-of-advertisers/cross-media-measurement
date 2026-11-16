
# EDP Aggregator System

Edp Aggregator components for Google Cloud.
## Durable EventGroupSync WorkItems

Event-group finalize routes enqueue a generation-specific `WorkItem` on
`event-group-sync-queue`. DataWatcher returns after the control plane durably accepts the task.
The Pub/Sub push consumer claims an attempt, renews its lease, and records success after the
mapping is published. Redelivery of a succeeded task does not repeat the sync. An active task
remains protected by its lease even if push delivery expires or reaches the DLQ listener.

Deploy the Secure Computation lease-aware API and updated queue registry first, then the EGS
consumer and queue IAM, and finally the DataWatcher routes. Drain legacy HTTP deliveries before
switching. Do not configure Scheduler and DataWatcher to run EGS for the same provider.

Each route uses `EventGroupSyncParams` and a stable `serialization_key` per provider. Keep keys
distinct from data availability tasks. The function retains one single-request instance.
Ordered publication and attempt leases protect redeliveries; they do not promise exactly-once
side effects after a crash between publishing a mapping and recording task completion.

The source must remain the current generation under the configured input directory. EGS downloads
that exact generation and checks the source and attempt lease before each Kingdom request and
before publishing the mapping. Superseded sources are terminal failures; the newer finalize event
supplies the current task. Existing HTTP JSON formats remain supported for migration and manual
invocations.

The consumer uses `SECURE_COMPUTATION_CONTROL_PLANE_TARGET`, optional
`SECURE_COMPUTATION_CERT_HOST`, and the `SECURE_COMPUTATION_CERT_FILE`,
`SECURE_COMPUTATION_PRIVATE_KEY_FILE`, and `SECURE_COMPUTATION_CERT_COLLECTION_FILE` paths for a
Secure Computation client identity. The provider's `cmms_connection` remains exclusive to Kingdom.
Terraform mounts both trust-domain identities and the control-plane CA. Monitor WorkItem states and
mapping freshness alongside push/DLQ metrics: delivery failures are distinct from synchronization
outcomes.

For queued GCS inputs, configure each provider's `event_groups_blob_uri` with its canonical
input object URI in the watched root directory. The consumer accepts
objects in that bucket and directory, including descendants such as dated upload folders, and
reads the exact finalized generation. Update this URI when moving the watched root.

Set `event_group_sync_memory` to the existing environment allocation before redeployment.
Leaving it unset uses the existing `512MB` default. Changing the allocation replaces the
function deployment action.

## External data availability WorkItems

External labeled-output `done` routes now enqueue `ExternalDataAvailabilitySyncParams` on
`data-availability-sync-queue`. DataWatcher returns once the task is durably accepted. The
existing consumer uses the same attempt lifecycle and per-provider synchronization lease as
VID Labeler tasks, and records success after availability publication. Attempt renewal continues through completion
bookkeeping and the final control-plane write. It discovers external
sidecars by scanning the done object's folder and does not require internal raw-upload names
or mark VID Labeler pipeline completion rows.

Deploy the consumer and DataWatcher binary before switching route configuration. Drain legacy
HTTP deliveries. External tasks use
`${external_impression_path}/model-line/<line>/<date>/done`; when `external_impression_path` is
empty they use `edp_impression_path`. Providers also using VID Labeler must configure separate
prefixes. `vid_labeled_impression_path` must match the dispatcher and monitor's internal output prefix;
internal tasks continue to be created by VID Labeler. The deployment validator rejects both
HTTP and queued DataWatcher routes that match internal outputs.
The consumer validates the external bucket, prefix, date, and finalized object generation.
Both producers use the provider's `data-availability:<DataProvider>` ordering key and the
existing provider lease. DataAvailabilitySync allows five single-request instances and a
60-minute invocation timeout. A completed WorkItem skips redelivered work; an interrupted
attempt remains retryable through the lease-aware control plane and DLQ listener.

The supplied environment configs preserve the external upload paths and place VID Labeler
outputs under `vid-labeled-impressions/edp/...`. Before adopting this configuration, drain active
VID Labeler and availability tasks and migrate dispatcher, monitor, and sync configs together.
Keep previously published impression metadata; new tasks derive their outputs from the new
internal prefix. External upload producers do not need to change their paths.

## Metadata cleanup in versioned buckets

The metadata deletion watcher receives both object-deleted and object-archived
events. In a versioned bucket, deleting a live object without specifying a
generation makes that version noncurrent and emits an archive event. Overwriting
an object also archives its previous version.

Cleanup reads the metadata version before checking whether a live object still
exists. If no live object remains, it soft-deletes only that version. A replacement
synchronized during cleanup changes the metadata etag, so the earlier cleanup is
skipped and the refreshed metadata stays active. Repeated notifications are harmless.

When upgrading, deploy the updated internal metadata API server, then the public
metadata API server, before the cleanup functions. Cleanup requires metadata etags
and does not fall back to unconditional deletion. Legacy API delete requests that
omit an etag remain unconditional; race protection starts with the updated cleanup
functions.

Both triggers use the same service account and dead-letter queue. Enabling archive
events handles future removals; it does not repair metadata left behind by earlier
missed notifications.

### Known gaps

These gaps apply with object versioning enabled or disabled:

*   **Removing only the encrypted impressions file.** The metadata record's
    `blob_uri` identifies its metadata sidecar. If the removed data file does
    not carry an ImpressionMetadata resource ID, cleanup looks up the data
    file's URI and can find no record, leaving metadata `ACTIVE`. Availability
    sync currently stamps the resource ID on the sidecar only.

### Deployment check

Deploy the infrastructure through Terraform. The storage commands below change
only dedicated test objects; they do not change triggers, subscriptions,
permissions, or functions.

Create dedicated test impression data and a metadata sidecar under a prefix
matched by the deletion watcher's configuration, with no reports or other jobs
using them. Register metadata through the normal availability sync flow. Use
the sidecar as `TEST_OBJECT_PATH` below and record both object URIs, generations,
and the metadata resource name.

After deploying the triggers:

1.  Verify that both archive and permanent-delete triggers target the existing
    deletion watcher, use its trigger service account, and have the configured
    dead-letter policy on their Eventarc subscriptions.
2.  Remove the test object's live version without specifying a generation:

    ```shell
    gcloud storage rm gs://BUCKET/TEST_OBJECT_PATH
    ```

    In a versioned bucket, confirm an archived version remains and no live
    version exists. Wait for the archive notification to pass through the
    deletion watcher and cleanup handler. Use `GetImpressionMetadata` with the
    recorded name, or list with `show_deleted=true`, and verify the record is
    soft-deleted (`DELETED`).
3.  Create and sync a fresh test object, then overwrite it with a valid
    replacement at the same URI:

    ```shell
    gcloud storage cp REPLACEMENT_FILE gs://BUCKET/TEST_OBJECT_PATH
    ```

    Confirm the old generation was archived, the replacement remains live, and
    cleanup processed the archive notification and skipped deletion. Verify
    metadata for the live file remains `ACTIVE`.
4.  Permanently remove the old, archived generation while the replacement is
    still live:

    ```shell
    gcloud storage rm 'gs://BUCKET/TEST_OBJECT_PATH#OLD_GENERATION'
    ```

    Wait for cleanup to process the permanent-delete notification and verify
    metadata remains `ACTIVE`. Repeated cleanup of an already-deleted metadata
    record should succeed without changing other records.

Check handler logs and metadata state together; a successful upload or a passing
report alone does not establish that cleanup received the notification. Inspect
the dead-letter subscription for failed deliveries and keep the event, object,
and metadata identifiers in the test record. Remove the remaining test objects
and verify cleanup after completing the check.
