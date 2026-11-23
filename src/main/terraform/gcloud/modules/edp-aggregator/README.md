
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

### WorkItem delivery

The deletion watcher stores one cleanup WorkItem for each file generation before
acknowledging the notification. Delete and archive notifications for the same
generation share a job. Older notifications without a generation use their
notification ID. The worker looks up current metadata by the file path and
checks for a live file when it executes. Queued jobs do not store object
metadata or a metadata etag.

The deletion watcher also limits queue writes: it runs at most
`data_watcher_delete_max_instances` instances (default 10), with one event per
instance. `data_watcher_delete_control_plane_throttler_millis` sets the minimum
interval between each instance's control-plane requests (default 100 milliseconds).

Cleanup uses `data-availability-cleanup-queue`, independently of availability
sync. Different files can run in parallel. Each cleanup instance handles one
request, and `data_availability_cleanup_max_instances` limits the number of
instances (default 5). `THROTTLER_MILLIS` sets the minimum interval between each
instance's metadata requests (default 250 milliseconds).
`CLEANUP_RPC_TIMEOUT_SECONDS` bounds metadata requests (default 30 seconds).
Availability sync retains its own throttler and five-instance limit. During the
initial rollout, its inline cleanup uses that sync throttler too.

Failed attempts retry with the queue's increasing delivery delays. Exhausted
deliveries go to `data-availability-cleanup-queue-dlq`; the control plane records
the failed WorkItem. Use the existing WorkItem retry operation after repairing
the cause. Check both failed WorkItems and the queue's oldest unacknowledged
message age. Configure `data_availability_cleanup_alert_notification_channels`
for notifications; the queue age threshold is
`data_availability_cleanup_max_queue_age_seconds` (default 30 minutes).
Alerts also count deliveries sent to the dead-letter queue, even after those
deliveries have been acknowledged.

#### Switch cleanup to WorkItems

Uploads and file deletions can continue during these steps:

1. Apply the queue infrastructure with
   `data_availability_cleanup_work_item_dispatch_enabled = false`. This keeps
   notifications on the existing HTTP route while creating the cleanup queue
   and its permissions.
2. Deploy the updated Secure Computation internal API server and its queue
   registry, cleanup function, and deletion watcher. Wait for those deployments
   to finish before changing the routing setting. The updated cleanup function
   accepts both queued jobs and existing HTTP requests.
3. Set `data_availability_cleanup_work_item_dispatch_enabled = true` in the
   environment's Terraform configuration and run the normal deployment
   workflow. New notifications now create WorkItems.
4. Check failed WorkItems and both the source-event and WorkItem dead-letter
   queues. Retry failed source events with the replay command below; retry
   failed WorkItems with the WorkItem retry operation.

Keep the updated cleanup function deployed while queued jobs remain. To return
new notifications to the HTTP route, set the routing setting to `false`; queued
jobs still need the updated worker to finish. Older workers receiving a queued
job return an error, preserving it for retry. Older metadata APIs likewise
cause retryable failures until they support conditional deletion.

The metadata deletion watcher receives both object-deleted and object-archived
events. In a versioned bucket, deleting a live object without specifying a
generation makes that version noncurrent and emits an archive event. Overwriting
an object also archives its previous version.

Cleanup reads the metadata version before checking whether a live object still
exists. If no live object remains, it soft-deletes only that version. A replacement
synchronized during cleanup changes the metadata etag, so the earlier cleanup is
rechecked. A live replacement keeps its metadata active; a replacement deleted
again can be cleaned up using its new etag. Repeated conflicts fail the invocation
so that the notification follows the configured retry and dead-letter policy.
Repeated notifications are harmless.

### Upgrade

To reduce cleanup retries during the upgrade, update these components in order:

1.  EDPA internal metadata API server.
2.  EDPA public API server.
3.  Availability sync and cleanup functions.

Uploads and deletions can continue throughout the upgrade. Deletion events already
waiting to be processed need no changes.

If an updated cleanup function reaches an API server still running the old code,
the cleanup attempt fails and the system retries the event. Events that reach the
retry limit go to the existing cleanup dead-letter queue. After the upgrade, check
that queue and [retry any failed events](#retry-a-failed-cleanup-event). A failed
attempt names the internal metadata API or public API when that server needs an
upgrade.

Cleanup jobs still running the old code can still mistakenly mark a replacement
file's metadata as deleted. The fix applies when the updated cleanup code processes
an event.

Both triggers use the same service account and dead-letter queue. Enabling archive
events handles future removals; it does not repair metadata left behind by earlier
missed notifications.

### Retry a failed cleanup event

Use `ReplayStorageCleanupEvent` after the API servers and functions have been
upgraded. This command sends one saved deletion or archive event to the original
deletion watcher, which selects the data provider and calls cleanup. It accepts
events queued before the upgrade, including events without a generation or
metadata resource ID. It does not acknowledge the queue message.

You need a local checkout, Bazel, `gcloud`, `jq`, permission to pull and acknowledge
messages on the dead-letter subscription, and permission to obtain an identity
token for a service account that can invoke the deletion watcher.

1.  Set the dead-letter **subscription** and deletion watcher URL for the failed
    event. Use the URL of the deletion watcher attached to that queue. Both
    deletion and archive triggers use the deletion watcher.

    ```shell
    CLEANUP_DLQ_SUBSCRIPTION='projects/PROJECT/subscriptions/DELETION_WATCHER_DLQ_SUBSCRIPTION'
    CLEANUP_WATCHER_URL='https://DELETION_WATCHER_URL'
    CLEANUP_INVOKER_SERVICE_ACCOUNT='INVOKER_SERVICE_ACCOUNT_EMAIL'
    umask 077
    CLEANUP_REPLAY_DIR=$(mktemp -d)
    ```

2.  Pull one message without acknowledging it. If the output is `[]`, the queue
    has no message available; stop here. Otherwise, extend its acknowledgment
    deadline while you inspect it.

    ```shell
    gcloud pubsub subscriptions pull "$CLEANUP_DLQ_SUBSCRIPTION" \
      --limit=1 --format=json > "$CLEANUP_REPLAY_DIR/message.json"
    CLEANUP_REPLAY_ACK_ID=$(jq -er '.[0].ackId // empty' "$CLEANUP_REPLAY_DIR/message.json")
    gcloud pubsub subscriptions modify-message-ack-deadline "$CLEANUP_DLQ_SUBSCRIPTION" \
      --ack-ids="$CLEANUP_REPLAY_ACK_ID" --ack-deadline=600
    jq '.[0].message.attributes' "$CLEANUP_REPLAY_DIR/message.json"
    jq -r '.[0].message.data | @base64d' "$CLEANUP_REPLAY_DIR/message.json"
    ```

    Confirm this is a deletion or archive event for a bucket handled by the
    selected watcher. Keep the original JSON; do not add or change its version or
    metadata fields. The command supports native Cloud Storage Pub/Sub
    notifications and exported events with `ce-*` attributes.

3.  Obtain a token and replay the saved event from the repository root. The queue
    message is acknowledged only if the watcher returns HTTP 200.

    ```shell
    gcloud auth print-identity-token \
      --impersonate-service-account="$CLEANUP_INVOKER_SERVICE_ACCOUNT" \
      --audiences="$CLEANUP_WATCHER_URL" > "$CLEANUP_REPLAY_DIR/identity-token"
    if bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:ReplayStorageCleanupEvent -- \
      --message-file="$CLEANUP_REPLAY_DIR/message.json" \
      --watcher-url="$CLEANUP_WATCHER_URL" \
      --identity-token-file="$CLEANUP_REPLAY_DIR/identity-token"; then
      gcloud pubsub subscriptions ack "$CLEANUP_DLQ_SUBSCRIPTION" \
        --ack-ids="$CLEANUP_REPLAY_ACK_ID"
    fi
    rm "$CLEANUP_REPLAY_DIR/identity-token"
    ```

    On failure, leave the message unacknowledged and check the watcher and cleanup
    logs. On success, cleanup either marked the missing file's metadata deleted or
    found that cleanup was no longer needed, for example because a replacement
    is live. Confirm the metadata state and repeat from step 2 for other failed
    events. If the acknowledgment deadline expires, pull the message again before
    replaying and acknowledging it; repeated cleanup attempts are safe.

    Empty objects other than `done` markers are skipped by the watcher, so the
    replay command rejects those events for manual investigation. Invalid or
    unrelated events are also rejected before an HTTP request is sent.

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

## Durable VID dispatch WorkItems

Configure `vid_labeling_dispatch_work_item_queue` before switching raw-upload
watcher routes. The dispatcher accepts both queued jobs and legacy HTTP deliveries.
See the [migration and retry guide](../../../../../../docs/operations/migrating-vid-labeling-dispatch.md)
for deployment order, historical recovery, and rollback constraints.

### VID dispatch cutover

`activate_vid_labeling_dispatch_config` selects the prepared v2 watcher configuration.
Preparation preserves the active legacy key. Activation changes both DataWatcher and
healing to v2 and rolls their cached configuration. The legacy object stays unchanged
for older parsers and rollback. Queue/IAM/push resources remain Terraform-owned;
readiness commands only read them.

Use the automated online or maintenance workflow in
[the dispatch migration guide](../../../../../../docs/operations/migrating-vid-labeling-dispatch.md).
