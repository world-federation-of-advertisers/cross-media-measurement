# Recover missing ImpressionMetadata

`RecoverMissingImpressionMetadata` reconciles finalized impression metadata files in blob storage
with the EDP Aggregator `ImpressionMetadataService`.

The command performs one storage listing and classifies two inconsistencies:

- A finalized metadata file has no active or deleted `ImpressionMetadata` resource. The command
  re-runs `DataAvailabilitySync` for the file's date folder, which registers all missing files in
  that folder and republishes the data availability intervals.
- An `ImpressionMetadata` resource is deleted while its metadata file still exists. The command
  reports this condition but does not reactivate the resource, because a lingering file can also
  indicate delayed storage cleanup.

Only folders containing a `done` blob are eligible for missing-record recovery. Deleted-record
checks include metadata files in folders without `done`, since the existence check is independent
of upload finalization.

## Run the CLI

The config file is a `DataAvailabilitySyncConfig` textproto. Its GCS bucket, data provider,
impression path, model-line mapping, and TLS file paths are reused by the recovery command.

```shell
RecoverMissingImpressionMetadata \
  --config-file=/etc/halo-cmms/edp-aggregator/config/data-availability-sync-config.textproto \
  --kingdom-public-api-target=KINGDOM_HOST:8443 \
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443
```

Optional flags:

- `--kingdom-public-api-cert-host`
- `--impression-metadata-api-cert-host`
- `--throttler-minimum-interval` (default `1s`)
- `--impression-metadata-batch-size` (default `100`)

The command exits nonzero when any affected date folder fails to resync. Finding deleted resources
with existing files does not cause a nonzero exit.

## Run as a Kubernetes Job

The `recover_missing_impression_metadata_image` target publishes the
`edp-aggregator/recover-missing-impression-metadata` image. It can be scheduled as a Job or CronJob
using the same ConfigMap, TLS Secret, Workload Identity, and network-policy pattern as
`SyncEventGroupActivities`.

The pod needs:

- read and metadata-update access to the configured bucket;
- network access to the Kingdom and EDP Aggregator public APIs;
- the TLS files referenced by `DataAvailabilitySyncConfig` mounted at their configured paths; and
- OpenTelemetry injection or equivalent exporter configuration for alerts.

Set `concurrencyPolicy: Forbid` for a CronJob so two reconciliation runs cannot overlap.

## Metrics

- `edpa.data_availability_recovery.missing_blobs`
- `edpa.data_availability_recovery.deleted_records_with_blobs`
- `edpa.data_availability_recovery.recovered_blobs`
- `edpa.data_availability_recovery.failed_blobs`

All four are per-run gauges. Alert when either inconsistency gauge or the failure gauge is greater
than zero. Each point has an `edpa.data_availability_recovery.edp_impression_path` attribute.
