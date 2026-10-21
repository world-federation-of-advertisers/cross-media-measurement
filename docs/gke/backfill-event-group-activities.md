# Backfill EventGroup activity

`BackfillEventGroupActivities` republishes positive EventGroup activity from active
`ImpressionMetadata` in finalized folders. Use it after enabling
`event_group_activity_publishing` or correcting an EventGroup map.

The command is activity-only. It does not scan impression files, create, update, or undelete
`ImpressionMetadata`, publish model-line data availability, or modify completion blobs.

## Before you start

Prepare a textproto containing one `DataAvailabilitySyncConfig`, not the
`DataAvailabilitySyncConfigs` wrapper used by the deployed Cloud Function. Use the same
DataProvider, storage, impression path, TLS, and EventGroup activity settings as the deployed
configuration. See the
[EDP Aggregator deployment guide](../edpaggregator/deployment-guide.md#dataavailabilitysync-config-dataavailabilitysyncconfigs)
for the full configuration.

The runtime identity needs:

* list access to the configured GCS bucket when selecting by prefix;
* read access to the EventGroup map;
* network access to the Kingdom and ImpressionMetadata public APIs;
* permission to list ImpressionMetadata and update EventGroup activity; and
* access to the TLS files referenced by the config.

## Select completion blobs

Choose exactly one selection mode:

* `--blob-prefix` accepts a `gs://` URI or bucket-relative prefix. It recursively finds blobs whose
  basename is exactly `done`. The prefix must be the configured `edp_impression_path` or one of its
  descendants.
* Repeat `--done-blob-uri` to backfill an explicit set of `gs://.../done` blobs. Every URI must use
  the configured bucket and be below the configured `edp_impression_path`.

To recursively backfill every completion blob below an impression prefix:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:BackfillEventGroupActivities \
  -- \
  --config-file=/absolute/path/to/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --blob-prefix=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions
```

To backfill only named completion blobs:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:BackfillEventGroupActivities \
  -- \
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
* `--impression-metadata-page-size` (default `1000`)

## Understand backfill behavior

Completion blobs are sorted and processed sequentially. For each selected folder, the command
lists only active ImpressionMetadata and publishes activity derived from those resources. Deleted
metadata is excluded, including metadata left behind by an interrupted eviction.

A failed folder does not prevent later folders from being attempted. The summary reports selected
completion blobs, published EventGroup/date pairs, no-work folders, and failures. The process exits
nonzero if any folder fails.

The activity update is append-only and idempotent. Existing EventGroup/date pairs remain unchanged,
and omitted dates are not deleted. For explicit append or delete corrections, use
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
in PATCH mode. Do not use SYNC mode for the same DataProvider because it can delete dates published
by `DataAvailabilitySync` or this backfill command.
