# VID Labeling migration to v0.5.53

## Database changes

| Table | Change |
| --- | --- |
| RawImpressionUploadCorrectionCandidate | New |
| DataAvailabilitySyncLease | New |
| VidLabelingEvictionFenceMutation | New |
| ReportRoutingDecision | New |
| ImpressionMetadata | Updated |
| RawImpressionUpload | Updated |
| RawImpressionUploadModelLine | Updated |
| UploadHealingOperation | Updated |
| UploadHealingStep | Updated |
| VidLabelingEvictionFence | Updated |
| WorkItems | Updated |
| WorkItemAttempts | Updated |

## Required configuration changes

### [vid_labeling_config.proto](../../src/main/proto/wfa/measurement/config/edpaggregator/vid_labeling_config.proto)

Update both dispatcher and monitor configurations:

* Set `raw_impressions_blob_prefix` to the DataProvider's bucket-relative raw-input
  root, without leading or trailing `/`.
* Add `event_template_version` to each `model_line_configs` entry, using the schema
  version for which that ModelLine's PopulationSpec was defined.
* Remove `event_template_field_mapping` entries for population attributes active
  in that version; keep `labeler_input_field_mapping`.

### [data_availability_sync_config.proto](../../src/main/proto/wfa/measurement/config/edpaggregator/data_availability_sync_config.proto)

* Add `secure_computation_control_plane_connection` with the client certificate,
  private key, and root-CA paths; set `SECURE_COMPUTATION_CONTROL_PLANE_TARGET` on
  DataAvailabilitySync.
* Before resuming standalone `ReplayDataAvailabilitySync` and
  `RecoverMissingImpressionMetadata` runs or CronJobs, update their binaries/images and
  per-provider config; align cutovers, source buckets/prefixes, model-line
  mappings, gap policy, and TLS mounts with deployed DataAvailabilitySync.
* Separate internal VidLabeler output from external labeled uploads:

| Configuration | Value |
| --- | --- |
| Both VID configs: `edp_impression_path` | `<INTERNAL_OUTPUT_PREFIX>` |
| Sync config: `vid_labeled_impression_path` | `<INTERNAL_OUTPUT_PREFIX>` |
| Sync config: `edp_impression_path` | `<EXTERNAL_OUTPUT_PREFIX>` |

Internal and external output prefixes must not overlap. Each source's actual
bucket and prefix must match its default source configuration or its
`model_line_data_sources` entry; contributors may use different buckets.

### [data_watcher_config.proto](../../src/main/proto/wfa/measurement/config/securecomputation/data_watcher_config.proto) and [queues_config.proto](../../src/main/proto/wfa/measurement/config/securecomputation/queues_config.proto)

* Remove routes matching internal VidLabeler output and ResultsFulfiller
  requisition blobs; keep the raw-input route to VidLabelingDispatcher.
* Route external output `done` events to `data-availability-sync-queue` with
  `serialization_key: "data-availability:<DATA_PROVIDER_RESOURCE_NAME>"`; route
  event-group events to `event-group-sync-queue` with
  `serialization_key: "event-group-sync:<DATA_PROVIDER_RESOURCE_NAME>"`.
* Use `control_plane_queue_sink` with typed `app_params` containing `data_provider`:
  `wfa.measurement.edpaggregator.v1alpha.ExternalDataAvailabilitySyncParams` for
  external output and `wfa.measurement.edpaggregator.v1alpha.EventGroupSyncParams`
  for event groups.
* Register both queues with `app_params_type_url` set to
  `type.googleapis.com/wfa.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams`;
  add their dead-letter subscriptions to the control-plane listener.
* Configure DataWatcherDelete to cover both labeled-output prefixes.

### [requisition_fetcher_config.proto](../../src/main/proto/wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto)

* Remove the top-level `storage_path_prefix`; use `work_item_dispatch` with its
  prefix, queue, control-plane connection, and ResultsFulfiller parameters.
* Set `SECURE_COMPUTATION_CONTROL_PLANE_TARGET` on RequisitionFetcher and mount
  the TLS files referenced by `work_item_dispatch.control_plane_connection`.

### [event_group_sync_config.proto](../../src/main/proto/wfa/measurement/config/edpaggregator/event_group_sync_config.proto)

* Set EventGroupSync's `SECURE_COMPUTATION_CONTROL_PLANE_TARGET`,
  `SECURE_COMPUTATION_CERT_FILE`, `SECURE_COMPUTATION_PRIVATE_KEY_FILE`, and
  `SECURE_COMPUTATION_CERT_COLLECTION_FILE`; mount the referenced TLS files.

### ModelLine cutover

When enabling a cutover, use the same
[model_line_cutover_config.proto](../../src/main/proto/wfa/measurement/config/edpaggregator/model_line_cutover_config.proto)
values in RequisitionFetcher, DataAvailabilitySync, and DataAvailabilityMonitor:

| Field | Value |
| --- | --- |
| `external_model_line` | `<VISIBLE_MODEL_LINE>` |
| `historical_model_line` | `<HISTORICAL_MODEL_LINE>` |
| `replacement_model_line` | `<REPLACEMENT_MODEL_LINE>` |
| `cutover_date` | `<CUTOVER_DATE_UTC>` |
| `legacy_required_through_date` | `<CUTOVER_DATE_UTC + 100 DAYS>` |
| `maximum_report_duration` | `<MAXIMUM_REPORT_DURATION_SECONDS>` |

The overlap is **100 days**; maximum Report duration must be positive, a whole
number of days, and no greater than the overlap.

* RequisitionFetcher: add `work_item_dispatch.model_line_cutovers`. In its
  [results_fulfiller_params.proto](../../src/main/proto/wfa/measurement/edpaggregator/v1alpha/results_fulfiller_params.proto)
  `model_line_data_sources`, configure both sources' `<BLOB_URI_PREFIX>` immediately
  above `model-line/`; set
  historical `valid_end_date` to cutover + 100 days and replacement
  `valid_start_date` to cutover.
* DataAvailabilitySync: add `model_line_cutovers` and `model_line_data_sources`
  containing each source's `<STORAGE>` and `<IMPRESSION_PREFIX>`.
* [data_availability_monitor_config.proto](../../src/main/proto/wfa/measurement/config/edpaggregator/data_availability_monitor_config.proto):
  add top-level `model_line_cutovers` and monitor both sources with their
  DataProvider, bucket, and prefix.
* Add visible, historical, and replacement ModelLines to ResultsFulfiller's
  ModelLine configuration. Remove visible-line aliases from ResultsFulfiller's
  `model_line_map` and DataAvailabilitySync's `model_line_map` values; remove any
  entry left empty.
  The selected source must use the same PopulationSpec and event schema as the
  visible line.

## New components

* `data-availability-sync-queue`: topic, authenticated push subscription,
  and dead-letter queue for DataAvailabilitySync tasks.
* `event-group-sync-queue`: topic, authenticated push subscription, and
  dead-letter queue for EventGroupSync tasks.
* On both push subscriptions, set `enable_message_ordering = true` and
  `push_config.no_wrapper.write_metadata = true` for binary WorkItem delivery.
* VidLabelingHealingController: Cloud Function, service account, and five-minute
  scheduler for approved correction plans; set `RAW_IMPRESSION_UPLOAD_TARGET`,
  `CONTROL_PLANE_TARGET`,
  `EDPA_CONFIG_STORAGE_BUCKET`, `CONFIG_BLOB_KEY` (dispatcher VID config), and
  `DATA_WATCHER_CONFIG_BLOB_KEY`; mount the VID config's metadata/control-plane
  TLS files.
* Queue IAM: publisher, push-invoker, token-creation, and dead-letter subscriber
  permissions.
* Healing IAM: read input/configuration, delete configured internal output
  prefixes, rewrite raw `done` markers, and grant the controller `roles/run.invoker`
  on VidLabelingDispatcher.
* Availability alerts: configure `vid_labeling_availability_alert_thresholds`
  (`pending_age_seconds`, `failed_executions`, `publication_age_seconds`). For
  managed deployments, set `vid_labeling_healing_alert_notification_channels` to
  a nonempty list of verified channels and confirm delivery to the intended
  operator.
* Publication-status IAM: grant DataAvailabilitySync write access to each
  additional source root's `.availability-publication/` directory; configure
  `data_availability_publication_status_storage_roots` with its bucket and
  bucket-relative root prefix.

## Rollout

1. Pause raw `done` publication, dispatcher calls for new uploads, new correction
   approvals, and new Reports; allow started calls to finish.
2. Keep workers, publication, RequisitionFetcher, VID dispatch/health schedules,
   and the healing controller running until affected phase work, downstream
   availability work, requisitions, and active healing operations finish and
   their fences are released. Then pause the fetcher, VID, and controller
   schedules and allow their started calls to finish.
3. Confirm no unfinished WorkItems missing `event_template_version` or the full
   Report interval remain, including active attempts and dead-letter work. If
   they cannot finish, stop and obtain a tested compatibility or parameter
   migration procedure; retries do not add missing parameters.
4. Stop consumption and pause publication/dead-letter handling. Apply the full
   EDPA and Secure Computation changelogs and deploy the APIs. Upload both VID
   configs before deploying the version-requiring workers. Deploy the functions
   and queue infrastructure with matching configuration.
5. For cutovers, deploy Reporting support for `reporting_metadata.report_interval`
   and activate matching cutover/source settings only after both sources are
   ready.
6. Re-enable publication, consumption, schedules, raw uploads, and Reports.
