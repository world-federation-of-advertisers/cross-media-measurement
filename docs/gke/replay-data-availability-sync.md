# Replay DataAvailabilitySync

`ReplayDataAvailabilitySync` reproduces the normal effect of writing an existing `done` blob. Use
it to re-run data-availability publication or to backfill EventGroup activity after enabling
`event_group_activity_publishing` or correcting an EventGroup map.

The command invokes `DataAvailabilitySync` for each selected completion blob. It reads the
finalized metadata in that blob's folder, updates ImpressionMetadata, republishes model-line data
availability, and, when configured, appends EventGroup activity.

## Before you start

Prepare a textproto containing one `DataAvailabilitySyncConfig`, not the
`DataAvailabilitySyncConfigs` wrapper used by the deployed Cloud Function. Use the same
DataProvider, storage, impression path, TLS, model-line, and EventGroup activity settings as the
deployed configuration. See the
[EDP Aggregator deployment guide](../edpaggregator/deployment-guide.md#dataavailabilitysync-config-dataavailabilitysyncconfigs)
for the full configuration.

The runtime identity needs:

* read and metadata-update access to the configured GCS bucket;
* read access to the EventGroup map when EventGroup activity publishing is enabled;
* network access to the Kingdom and ImpressionMetadata public APIs; and
* access to the TLS files referenced by the config.

## Select completion blobs

Choose exactly one selection mode:

* `--blob-prefix` accepts a `gs://` URI or bucket-relative prefix. It recursively finds blobs whose
  basename is exactly `done`. The prefix must be the configured `edp_impression_path` or one of its
  descendants.
* Repeat `--done-blob-uri` to replay an explicit set of `gs://.../done` blobs. Every URI must use
  the configured bucket and be below the configured `edp_impression_path`.

To recursively replay every completion blob below an impression prefix:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:ReplayDataAvailabilitySync \
  -- \
  --config-file=/absolute/path/to/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --blob-prefix=gs://EDPA_STORAGE_BUCKET/edp/EDP_ID/vid-labeled-impressions
```

To replay only named completion blobs:

```shell
bazel run \
  //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:ReplayDataAvailabilitySync \
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
* `--impression-metadata-batch-size` (default `1000`)

## Understand replay behavior

Completion blobs are sorted and processed sequentially to bound load on the Kingdom and
ImpressionMetadata services. A failed blob does not prevent later blobs from being attempted. The
summary reports selected, published, no-work, gap-blocked, and failed counts; the process exits
nonzero if any replay fails or is blocked by `error_if_gaps_exist`.

The activity update is append-only and idempotent. Existing EventGroup/date pairs remain unchanged,
and omitted dates are not deleted. For explicit append or delete corrections, use
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
in PATCH mode. Do not use SYNC mode for the same DataProvider because it can delete dates published
by `DataAvailabilitySync`.

The replay command and the deployed `DataAvailabilitySyncFunction` do not share a distributed lock.
Avoid replaying a folder while its upload is being finalized or while another replay or recovery
run is processing the same folder.
