# Recover missing ImpressionMetadata

`RecoverMissingImpressionMetadata` reconciles impression metadata files in blob storage with the
EDP Aggregator `ImpressionMetadataService`.

The command discovers ISO-date folders in the requested lookback window and processes one folder
at a time. For each folder, it lists storage objects and pages through the metadata store using the
folder's blob URI prefix. This bounds memory to one date folder instead of loading the full
lookback window.

It repairs two inconsistencies:

- A finalized metadata file has no active or deleted `ImpressionMetadata` resource. A folder is
  finalized only when it contains a `done` blob. The command re-runs `DataAvailabilitySync`
  through a filtered storage view containing only the missing files, then re-queries the folder and
  verifies that every missing resource is now active.
- An `ImpressionMetadata` resource is deleted while its metadata file still exists. The command
  calls `UndeleteImpressionMetadata`, includes the restored blob in the finalized folder's
  `DataAvailabilitySync`, and reports any resource that could not be restored.

Deleted-record checks also include folders without `done`, since that existence check is
independent of upload finalization.

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
job runs during a low-traffic weekly window and `concurrencyPolicy: Forbid` prevents recovery jobs
from overlapping each other, but it does not prevent the Cloud Function from processing the same
date folder. Avoid manually starting recovery while that folder is actively being finalized.

Post-sync verification proves that repaired `ImpressionMetadata` resources are active. It does not
independently prove the later Kingdom data-availability publication: the existing `synced-by`
marker is written before that RPC. Durable detection and retry of a failure between those phases is
tracked in [#4463](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4463).

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
