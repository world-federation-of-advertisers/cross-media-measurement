# Replay DataAvailabilitySync

`ReplayDataAvailabilitySync` reprocesses finalized folders selected by their `done` blobs. Use it
to backfill EventGroup activity, replay data-availability publication, or repair metadata after an
operator has determined that a full replay is appropriate.

The required `--mode` flag makes the scope explicit:

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

The runtime identity needs:

* read, list, and metadata-update access to the configured GCS bucket for `FULL_SYNC`;
* list access to that bucket for `EVENT_GROUP_ACTIVITY_ONLY` when selecting by prefix;
* read access to the EventGroup map when publishing EventGroup activity;
* network access to the Kingdom and ImpressionMetadata public APIs;
* permission to list and write ImpressionMetadata in `FULL_SYNC`;
* permission to list ImpressionMetadata and update EventGroup activity in
  `EVENT_GROUP_ACTIVITY_ONLY`; and
* access to the TLS files referenced by the config.

## Select completion blobs

Choose exactly one selection mode:

* `--blob-prefix` accepts a `gs://` URI or bucket-relative prefix. It recursively finds blobs whose
  basename is exactly `done`. The prefix must be the configured `edp_impression_path` or one of its
  descendants.
* Repeat `--done-blob-uri` to replay an explicit set of `gs://.../done` blobs. Every URI must use
  the configured bucket and be below the configured `edp_impression_path`.

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

To fully replay named completion blobs:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:ReplayDataAvailabilitySync \
  -- \
  --mode=FULL_SYNC \
  --config-file=/absolute/path/to/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --done-blob-uri=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions/2026-09-12/done \
  --done-blob-uri=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions/2026-09-15/done
```

Optional flags:

* `--kingdom-public-api-cert-host`
* `--impression-metadata-api-cert-host`
* `--throttler-minimum-interval` (default `1s`)
* `--impression-metadata-batch-size` (default `1000`)

## Understand replay behavior

Completion blobs are sorted and processed sequentially. A failed folder does not prevent later
folders from being attempted, and the process exits nonzero if any folder fails. `FULL_SYNC` also
exits nonzero when `error_if_gaps_exist` blocks publication.

`EVENT_GROUP_ACTIVITY_ONLY` lists only active ImpressionMetadata for each selected folder. Deleted
metadata is excluded, including metadata left behind by an interrupted eviction.

`FULL_SYNC` intentionally reproduces the normal effect of processing a `done` blob. It may restore
soft-deleted ImpressionMetadata when the corresponding sidecar and impressions blobs still exist.
Do not use this mode to backfill only EventGroup activity, and do not run it during an eviction.

EventGroup activity updates are append-only and idempotent. Existing EventGroup/date pairs remain
unchanged, and omitted dates are not deleted. For explicit append or delete corrections, use
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
in PATCH mode. Do not use SYNC mode for the same DataProvider because it can delete dates published
by `DataAvailabilitySync`.
