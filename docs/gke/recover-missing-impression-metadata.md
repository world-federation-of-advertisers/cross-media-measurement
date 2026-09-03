# Recover missing ImpressionMetadata

`RecoverMissingImpressionMetadata` reconciles finalized impression metadata files in blob storage
with the EDP Aggregator `ImpressionMetadataService`.

The command performs one storage listing and classifies two inconsistencies:

- A finalized metadata file has no active or deleted `ImpressionMetadata` resource. The command
  re-runs `DataAvailabilitySync` through a filtered storage view containing only the missing files
  in that date folder, then republishes the data availability intervals. This leaves the normal
  `DataAvailabilitySync` implementation unchanged.
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
  --impression-metadata-api-target=EDP_AGGREGATOR_HOST:8443 \
  --lookback-days=90
```

Optional flags:

- `--kingdom-public-api-cert-host`
- `--impression-metadata-api-cert-host`
- `--throttler-minimum-interval` (default `1s`)
- `--impression-metadata-batch-size` (default `100`)
- `--lookback-days` (default `90`, including the current UTC date)

The command exits nonzero when any affected date folder fails to resync. Finding deleted resources
with existing files does not cause a nonzero exit.

## Weekly Kubernetes CronJob

The `recover_missing_impression_metadata_image` target publishes the
`edp-aggregator/recover-missing-impression-metadata` image. The EDP Aggregator GKE configuration
deploys `recover-missing-impression-metadata-edp7-cronjob` with:

- schedule `0 6 * * 0` (Sunday at 06:00 UTC);
- `concurrencyPolicy: Forbid`;
- a 90-day lookback;
- list and write batches of 1,000 records; and
- a 100 ms minimum interval between API requests.

The job uses the same Workload Identity service account as
`sync-event-group-activities-edp7-cronjob`. That identity must have
`roles/storage.objectAdmin` on the configured bucket because recovery reads blobs and updates
their metadata after registration.

Configure `DATA_AVAILABILITY_RECOVERY_EDP7_CONFIG_CONTENT` in each GitHub environment before
deploying. Its value is a single `DataAvailabilitySyncConfig`:

```textproto
# proto-file: wfa/measurement/config/edpaggregator/data_availability_sync_config.proto
# proto-message: wfa.measurement.config.edpaggregator.DataAvailabilitySyncConfig
data_provider: "dataProviders/<DATA_PROVIDER_ID>"
data_availability_storage {
  gcs {
    project_id: "<GCP_PROJECT>"
    bucket_name: "<EDPA_STORAGE_BUCKET>"
  }
}
cmms_connection {
  cert_file_path: "/etc/halo-cmms/edp-aggregator/edp7-tls/tls.crt"
  private_key_file_path: "/etc/halo-cmms/edp-aggregator/edp7-tls/tls.key"
  cert_collection_file_path: "/etc/halo-cmms/edp-aggregator/config/kingdom_root.pem"
}
impression_metadata_storage_connection {
  cert_file_path: "/etc/halo-cmms/edp-aggregator/data-availability-tls/tls.crt"
  private_key_file_path: "/etc/halo-cmms/edp-aggregator/data-availability-tls/tls.key"
  cert_collection_file_path: "/etc/halo-cmms/edp-aggregator/config/trusted_certs.pem"
}
edp_impression_path: "<EDP_IMPRESSION_PATH>"
```

The pod needs:

- read and metadata-update access to the configured bucket;
- network access to the Kingdom and EDP Aggregator public APIs;
- the TLS files referenced by `DataAvailabilitySyncConfig` mounted at their configured paths; and
- OpenTelemetry injection or equivalent exporter configuration for alerts.

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

The 90-day filter is based on the ISO date directory immediately containing each metadata file.
Files without a `YYYY-MM-DD` parent directory are outside the recovery scope.

## Metrics

- `edpa.data_availability_recovery.missing_blobs`
- `edpa.data_availability_recovery.deleted_records_with_blobs`
- `edpa.data_availability_recovery.recovered_blobs`
- `edpa.data_availability_recovery.failed_blobs`

All four are per-run gauges. Alert when either inconsistency gauge or the failure gauge is greater
than zero. Each point has an `edpa.data_availability_recovery.edp_impression_path` attribute.
