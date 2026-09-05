# Recover missing ImpressionMetadata

`RecoverMissingImpressionMetadata` reconciles impression metadata files in blob storage with the
EDP Aggregator `ImpressionMetadataService`.

The command discovers ISO-date folders in the requested lookback window and processes one folder
at a time. For each folder, it lists storage objects and pages through the metadata store using the
folder's blob URI prefix. This bounds memory to one date folder instead of loading the full
lookback window.

It repairs four inconsistencies:

- A finalized metadata file has no active or deleted `ImpressionMetadata` resource. A folder is
  finalized only when it contains a `done` blob. The command re-runs `DataAvailabilitySync`
  through a filtered storage view containing only the missing files, then re-queries the folder and
  verifies that every missing resource is now active.
- An `ImpressionMetadata` resource is deleted while its metadata file still exists. The command
  calls `UndeleteImpressionMetadata`, includes the restored blob in the finalized folder's
  `DataAvailabilitySync`, and reports any resource that could not be restored.
- A finalized metadata blob has an active resource but no `synced-by` marker, which can happen
  when an upload overwrites a previously processed blob or synchronization stops while stamping
  blobs. The command includes every such blob in the retry.
- A finalized folder has the metadata-store `synced-by` marker but its latest
  `data-availability-sync-id` does not match `data-availability-published-sync-id`. The command
  re-runs `DataAvailabilitySync` with one representative marked blob per model line and verifies
  that the publication IDs match. Missing, restored, and unmarked blobs share the same retry.

Deleted-record checks also include folders without `done`, since that existence check is
independent of upload finalization.

If `error_if_gaps_exist` blocks publication, the publication IDs remain mismatched and the command
exits nonzero so the folder remains retryable after the gap is corrected.

## Run the CLI

The config file is a `DataAvailabilitySyncConfig` textproto. Its GCS bucket, data provider,
impression path, model-line mapping, and TLS file paths are reused by the recovery command.

```shell
RecoverMissingImpressionMetadata \
  --config-file=/etc/halo-cmms/edp-aggregator/config/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --lookback-days=90 \
  --end-days-ago=0
```

Optional flags:

- `--kingdom-public-api-cert-host`
- `--impression-metadata-api-cert-host`
- `--throttler-minimum-interval` (default `1s`)
- `--impression-metadata-batch-size` (default `1000`)
- `--lookback-days` (default `90`; the oldest eligible date is 89 days before today)

`--end-days-ago` is required and sets the newest eligible date relative to the current UTC date.
It must be nonnegative and less than `--lookback-days`. For example,
`--lookback-days=90 --end-days-ago=30` scans from 89 days ago through 30 days ago, inclusive.
The weekly CronJob passes `--end-days-ago=0`, so it scans the most recent 90 dates including
today.

The command exits nonzero when any folder scan, resynchronization, verification, or undelete fails.
A successfully repaired inconsistency does not cause a nonzero exit.

## Weekly Kubernetes CronJob

The `recover_missing_impression_metadata_image` target publishes the
`edp-aggregator/recover-missing-impression-metadata` image. The EDP Aggregator GKE configuration
deploys `recover-missing-impression-metadata-edp7-cronjob` with:

- schedule `0 6 * * 0` (Sunday at 06:00 UTC);
- `concurrencyPolicy: Forbid`;
- a 90-day lookback;
- `--end-days-ago=0` to include the current UTC date;
- list and write batches of 1,000 records; and
- a 100 ms minimum interval between API requests.

The job uses the same Workload Identity service account as
`sync-event-group-activities-edp7-cronjob`. That identity must have
`roles/storage.objectAdmin` on the configured bucket because recovery reads blobs and updates
their metadata during synchronization.

The deployment workflow derives the job's single-EDP config from the `edp/edp7` entry in the
existing `DATA_AVAILABILITY_SYNC_CONFIG_CONTENT` GitHub environment variable and rewrites its TLS
paths for the Kubernetes mounts. The expected Cloud Function TLS paths must remain present in that
source config so the workflow can validate and replace each one explicitly.

The pod needs:

- read and metadata-update access to the configured bucket;
- network access to the Kingdom and EDP Aggregator public APIs;
- the TLS files referenced by `DataAvailabilitySyncConfig` mounted at their configured paths; and
- OpenTelemetry injection or equivalent exporter configuration for alerts.

The Kubernetes job sets `OTEL_METRIC_EXPORT_INTERVAL=5000`, allowing short no-op runs to export
their metrics before the process exits.

To trigger the deployed CronJob immediately:

```shell
kubectl create job \
  --from=cronjob/recover-missing-impression-metadata-edp7-cronjob \
  recover-missing-impression-metadata-edp7-manual-$(date +%s)
```

Then inspect its logs:

```shell
kubectl logs -l app=recover-missing-impression-metadata-edp7-app --tail=200
```

## Operational boundaries

The recovery job and `DataAvailabilitySyncFunction` do not share a distributed lock. The scheduled
job runs during a low-traffic weekly window, and `concurrencyPolicy: Forbid` prevents scheduled
runs from overlapping. It does not serialize manually created Jobs or prevent the Cloud Function
from processing the same date folder. Do not start multiple manual recovery Jobs concurrently,
and avoid manually starting recovery while a target folder is actively being finalized.

Post-sync verification proves that every repaired `ImpressionMetadata` resource is active and that
the sync and publication IDs match after the Kingdom update. The existing `synced-by` marker
continues to record the metadata-store phase. The sync attempt ID is written before metadata-store
mutation, and publication updates only its own marker, so overlapping attempts cannot erase a
newer incomplete attempt. These separate markers allow the monitor to distinguish metadata
persistence failures from Kingdom publication failures.

## Scale and performance

For two model lines, 90 days, and 5,000 campaigns per model line per day, the window contains about
900,000 metadata resources in 180 date folders. With 1,000-resource pages, a healthy run makes
about 900 paginated list RPCs plus storage listings. It never builds a 900,000-entry in-memory map;
the largest working set is one folder, about 5,000 resources in this example.

The 100 ms production throttle contributes about 90 seconds to the healthy metadata-store scan.
Storage listing, network latency, JVM startup, and any actual repairs add to that lower bound. A
reasonable healthy-run budget is therefore several minutes, not the earlier one-to-two-minute
estimate based on one metadata file per folder. Missing-record repair is slower because
`DataAvailabilitySync` parses the selected blobs, performs writes, recomputes model-line bounds,
and publishes availability intervals for each affected folder.

The main speedups are folder-prefix filtering, 1,000-record pages, and bounded per-folder memory.
For a folder that only lacks the publication marker, recovery feeds one representative metadata
blob per model line back through `DataAvailabilitySync`; it does not rewrite all 5,000 resources.
The first run after this marker is introduced backfills the last 90 days, after which healthy runs
skip already-published folders. Markerless legacy folders outside the configured window are not
reported as failed publication attempts; operators can backfill a chosen historical range with the
CLI. A successful synchronization performs three small metadata patches on `done`: attempt start,
metadata-store completion, and Kingdom-publication completion.
Parallelizing folders would shorten the scan but is intentionally avoided because concurrent syncs
can race while replacing provider-wide Kingdom availability intervals.

## Metrics

- `edpa.data_availability_recovery.missing_blobs`
- `edpa.data_availability_recovery.deleted_records_with_blobs`
- `edpa.data_availability_recovery.failed_blobs`
- `edpa.data_availability_recovery.failed_undeletes`

All four are per-run gauges. Alert when either inconsistency gauge or either repair-failure gauge is
greater than zero. Each point has an
`edpa.data_availability_recovery.edp_impression_path` attribute. Successful counts are in the
completion log; a separate recovered gauge would duplicate `missing_blobs - failed_blobs`.
The data-availability monitor also emits
`edpa.data_availability.date_count{date_status="unpublished_availability"}` for folders whose new
sync-attempt ID does not match the Kingdom publication marker after the configured threshold.
Legacy `synced-by` folders without a sync-attempt ID are migrated by recovery within its configured
date range and do not create permanent monitor alerts outside that range.
