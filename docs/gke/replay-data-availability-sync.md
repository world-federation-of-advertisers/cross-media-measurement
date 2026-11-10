# Replay DataAvailabilitySync

`ReplayDataAvailabilitySync` reprocesses finalized folders selected by their `done` blobs. Use it
to backfill EventGroup activity, replay data-availability publication, or repair metadata after an
operator has determined that a full replay is appropriate.

The required `--mode` flag makes the scope explicit:

* `DATA_AVAILABILITY_ONLY` reruns `DataAvailabilitySync` without publishing EventGroup activity. It
  scans finalized metadata, creates or updates ImpressionMetadata, restores matching soft-deleted
  metadata, republishes model-line data availability, and updates completion markers.
* `EVENT_GROUP_ACTIVITY_ONLY` reads only active ImpressionMetadata and publishes positive
  EventGroup activity. It does not create, update, or undelete metadata, publish model-line data
  availability, or modify completion blobs.
* `FULL_SYNC` reruns `DataAvailabilitySync`. It scans finalized metadata, creates or updates
  ImpressionMetadata, restores matching soft-deleted metadata, republishes model-line data
  availability, updates completion markers, and publishes EventGroup activity when configured.

## Before you start

Prepare a textproto containing one `DataAvailabilitySyncConfig`, not the
`DataAvailabilitySyncConfigs` wrapper used by the deployed Cloud Function. Use the same
DataProvider, storage, impression path, TLS, model-line, and EventGroup activity settings as the
deployed configuration. See the
[EDP Aggregator deployment guide](../edpaggregator/deployment-guide.md#dataavailabilitysync-config-dataavailabilitysyncconfigs)
for the full configuration.

Keep both configured prefixes in the standalone replay config: `edp_impression_path` for
internal VID Labeler outputs and `external_impression_path` for external uploads. When the
external prefix is unset, external uploads use `edp_impression_path`. Discovery uses the
selected completion blob's prefix; gap checks include both configured prefixes. A metadata
replay does not record VID Labeler pipeline completion; use the pipeline's WorkItem recovery
path when that bookkeeping also needs repair.

The runtime identity needs:

* read, list, and metadata-update access to the configured GCS bucket for
  `DATA_AVAILABILITY_ONLY` and `FULL_SYNC`;
* list access to that bucket for `EVENT_GROUP_ACTIVITY_ONLY` when selecting by prefix;
* read access to the EventGroup map when publishing EventGroup activity;
* network access to the Kingdom and ImpressionMetadata public APIs;
* permission to list and write ImpressionMetadata in `DATA_AVAILABILITY_ONLY` and `FULL_SYNC`;
* permission to acquire, renew, and release DataAvailabilitySyncLeases on the ImpressionMetadata
  endpoint in `DATA_AVAILABILITY_ONLY` and `FULL_SYNC`;
* permission to list ImpressionMetadata and update EventGroup activity in
  `EVENT_GROUP_ACTIVITY_ONLY`; and
* access to the TLS files referenced by the config.

## Select completion blobs

Choose exactly one selection mode:

* `--blob-prefix` accepts a `gs://` URI or bucket-relative prefix beneath either configured
  impression path. It recursively finds external `done` blobs or the VID Labeler completion
  filenames appropriate to that path.
* Repeat `--done-blob-uri` to replay an explicit set of completion blobs. Every URI must use
  the configured bucket, fall beneath a configured impression path, and match that path's
  completion filename.

To backfill EventGroup activity below an impression prefix without changing metadata or data
availability:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:ReplayDataAvailabilitySync \
  -- \
  --mode=EVENT_GROUP_ACTIVITY_ONLY \
  --config-file=/absolute/path/to/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --blob-prefix=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions
```

To replay data availability for named completion blobs without publishing EventGroup activity:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:ReplayDataAvailabilitySync \
  -- \
  --mode=DATA_AVAILABILITY_ONLY \
  --config-file=/absolute/path/to/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --done-blob-uri=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions/2026-09-12/done \
  --done-blob-uri=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions/2026-09-15/done
```

Optional flags:

* `--kingdom-public-api-cert-host`
* `--impression-metadata-api-cert-host`
* `--throttler-minimum-interval` (default `1s`): shared minimum interval between metadata,
  availability, and activity RPCs, including activity retries.
* `--impression-metadata-batch-size` (default `100`)

## Understand replay behavior

Completion blobs are sorted and processed sequentially. A failed folder does not prevent later
folders from being attempted, and the process exits nonzero if any folder fails.
`DATA_AVAILABILITY_ONLY` and `FULL_SYNC` also exit nonzero when `error_if_gaps_exist` blocks
publication.
Failures are logged with the affected completion blob and the exception's cause chain, including
underlying RPC statuses.

Each data-availability replay acquires a renewable DataProvider synchronization lease and reads
its completion blob's current GCS generation. Metadata mutations include that lease and generation.
The lease is released after each folder, including when synchronization fails. Activity-only
replay does not acquire a synchronization lease.

`DATA_AVAILABILITY_ONLY` and `FULL_SYNC` capture external metadata and impressions at
the completion blob's creation-time cutoff, revalidate them before publication, and persist
their generations for fulfillment. Additions or overwrites after `done` require a new
`done` marker before replay. A generationless synchronization cannot update metadata
that already has stored generations.

`EVENT_GROUP_ACTIVITY_ONLY` lists only active ImpressionMetadata for each selected folder. Deleted
metadata is excluded, including metadata left behind by an interrupted eviction. This mode does
not read changed sidecars; synchronize those changes first with `DATA_AVAILABILITY_ONLY` or
`FULL_SYNC`.

`DATA_AVAILABILITY_ONLY` and `FULL_SYNC` reproduce the normal metadata and data-availability effects
of processing a `done` blob. They may restore soft-deleted ImpressionMetadata when the
corresponding sidecar and impressions blobs still exist. `DATA_AVAILABILITY_ONLY` never reads the
EventGroup map or publishes EventGroup activity, even when activity publishing is configured. Use
`FULL_SYNC` when both data availability and EventGroup activity should be replayed. Do not use
either mode to backfill only EventGroup activity, and do not run either mode during an eviction.

EventGroup activity updates are append-only and idempotent. Existing EventGroup/date pairs remain
unchanged, and omitted dates are not deleted. For explicit append or delete corrections, use
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
in PATCH mode. Do not use SYNC mode for the same DataProvider because it can delete dates published
by `DataAvailabilitySync`.
