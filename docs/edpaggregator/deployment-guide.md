# EDP Aggregator Deployment Guide

## Objective

This is the operator-facing guide for deploying the **EDP Aggregator** (EDPA): the
Halo component that lets Event Data Providers (EDPs) hand off encrypted, VID-labeled
impression logs and event-group metadata through a Cloud Storage bucket, and have
requisitions fulfilled by trusted workloads running in Confidential Space
Trusted Execution Environments (TEEs).

It is intended for the **market operator** — the engineers who set up, maintain, and
debug the EDPA infrastructure for a market. It describes every component, the
configuration each one requires, and how that configuration is expressed in
Terraform.

For the EDP-facing side (how a data provider prepares keys, formats data, and
uploads it) see the companion guides:

| Guide | Audience | Purpose |
| --- | --- | --- |
| [EDP Onboarding Guide](edp-onboarding.md) | EDP | Integrate a data provider: KMS, schemas, encryption, upload paths, daily workflow |
| [AWS KMS Setup Guide](aws-kms-setup.md) | EDP + operator | Use AWS KMS instead of GCP KMS for a data provider |
| [Metadata Operator Guide](metadata-operator-guide.md) | Operator | RequisitionFetcher + ImpressionMetadata internals, tuning, scaling |
| [Report Debugging Guide](report-debugging-guide.md) | Operator | Trace a report end-to-end and diagnose failures |
| [Reporting Dashboard Deployment](dashboard/deployment-guide.md) | Operator | Deploy the EDPA reporting dashboard |

To validate a deployment, an end-to-end **cloud test** simulates a single-publisher
Reach & Frequency (R&F) measurement. See [Validation](#validation-cloud-test).

## Conventions

This guide uses **generic placeholders**. Substitute your market's real values.
None of the names below are prescriptive — they are only examples.

| Placeholder | Meaning |
| --- | --- |
| `PROJECT_ID` | The operator's GCP project ID |
| `REGION` | Deployment region (e.g. `us-central1`) |
| `EDPA_STORAGE_BUCKET` | Cloud Storage bucket holding EDP data (event groups, requisitions, impressions) |
| `EDPA_CONFIG_BUCKET` | Cloud Storage bucket holding runtime configuration files |
| `VID_MODELS_BUCKET` | (Optional) bucket holding compiled VID model blobs |
| `KINGDOM_PUBLIC_API_TARGET` | Kingdom public API gRPC target, e.g. `v2alpha.kingdom.example.org:8443` |
| `SECURE_COMPUTATION_API_TARGET` | Secure Computation API gRPC target |
| `SECURE_COMPUTATION_CERT_HOST` | DNS name in the Secure Computation API server certificate |
| `CLIENT_CERT_PEM` / `CLIENT_KEY_PEM` | Operator client certificate and private key authorized to call the Secure Computation API |
| `TRUSTED_ROOTS_PEM` | Root certificates used to verify the Secure Computation API |
| `SPANNER_INSTANCE` / `SECURE_COMPUTATION_DATABASE` | Spanner instance and database backing the Secure Computation API |
| `EDPA_METADATA_API_TARGET` | EDP Aggregator (Metadata Storage) API gRPC target |
| `dataProviders/DATA_PROVIDER_ID` | An EDP's `DataProvider` resource name in the Kingdom |
| `<edp-id>` | The per-EDP storage prefix the operator assigns to a data provider |

> This guide does **not** cover the project's CI automation. In production the
> operator owns the infrastructure and applies it directly with Terraform. Every
> value described here is set through **Terraform variables** and the **config
> files** uploaded to `EDPA_CONFIG_BUCKET` — not through any CI-specific mechanism.

---

## Architecture

The EDP Aggregator is a distributed system spanning the following Google Cloud
services:

* **Kubernetes (GKE)** — hosts the Secure Computation API and the EDP Aggregator
  (Metadata Storage) API.
* **Spanner** — backing store for the EDP Aggregator (Metadata Storage) API.
* **Confidential VMs (Managed Instance Groups)** — run the ResultsFulfiller TEE
  application, and optionally the VID Labeling TEE applications.
* **Cloud Storage** — the data bucket (EDP inputs + pipeline artifacts) and the
  config bucket.
* **Cloud Functions** — the event-driven and scheduled glue: DataWatcher,
  DataWatcherDelete, EventGroupSync, RequisitionFetcher, DataAvailabilitySync,
  DataAvailabilityCleanup, DataAvailabilityMonitor, plus the VidLabelingDispatcher
  and VidLabelingMonitor (which deploy unconditionally — see
  [Optional: VID Labeling pipeline](#optional-vid-labeling-pipeline)).
* **Secret Manager** — TLS keypairs, root CAs, and per-EDP consent/encryption
  material.
* **Pub/Sub** — work queues that feed the TEE Managed Instance Groups.
* **Cloud Scheduler** — periodic triggers (RequisitionFetcher, DataAvailabilityMonitor).

### Data flow at a glance

```
EDP uploads                          Operator services                     TEE
-----------                          -----------------                     ---
event-groups/*  ── finalize ──► DataWatcher ──► EventGroupSync ──► Kingdom public API
                                                                       │
Kingdom requisitions ◄── Cloud Scheduler ──► RequisitionFetcher ──────┘
        writes requisitions/* ──► Secure Computation API ──► Pub/Sub
                                                                  │
edp/<edp-id>/<date>/{impressions,metadata,done}                               ▼
        done ── finalize ──► DataWatcher ──► DataAvailabilitySync ──► ResultsFulfiller MIG
                                                    │                    (Confidential Space)
                                                    ▼                         │
                                        EDP Aggregator (Metadata) API ◄───────┘
                                                    │
        blob delete ── delete ──► DataWatcherDelete ──► DataAvailabilityCleanup
```

---

## Component reference

### EDP Aggregator Storage bucket

A Cloud Storage bucket (`EDPA_STORAGE_BUCKET`) that holds all core data inputs:

* **Event Groups** — uploaded by EDPs; an upload triggers registration with the
  Kingdom.
* **Requisitions** — written by the RequisitionFetcher after pulling from the
  Kingdom public API.
* **Impressions and metadata** — uploaded by EDPs; consumed by the ResultsFulfiller
  and by data-availability updates.

The bucket is **private**: only the aggregator service accounts and the
ResultsFulfiller TEE service account may read or write. You may use a single shared
bucket for all EDPs, or one bucket per EDP. With per-EDP buckets, the DataWatcher
needs one trigger per bucket (see [Multiple buckets](#deploying-datawatcher-for-multiple-buckets)).

**Object versioning is enabled** on this bucket, and per-prefix **lifecycle rules**
manage retention — see [Object Lifecycle Management](#object-lifecycle-management)
and [Object Versioning & ImpressionMetadata Cleanup](#object-versioning--impressionmetadata-cleanup).

### Config bucket

A separate Cloud Storage bucket (`EDPA_CONFIG_BUCKET`) that stores the configuration
files Cloud Functions and the ResultsFulfiller read at runtime. It is **not**
accessible to EDPs — only the aggregator service accounts may read it. The operator
uploads the config files here (all are `.textproto` serializations of the config
protos described in [Configuration reference](#configuration-reference-terraform)).

### VID models bucket (optional)

A bucket (`VID_MODELS_BUCKET`) holding the compiled VID model blobs read by the
optional VID Labeling TEE applications. Only required when the VID Labeling pipeline
is enabled (see [Optional: VID Labeling pipeline](#optional-vid-labeling-pipeline)).

### Secret Manager

The operator manages all secrets; EDPs no longer upload their own certificates.
Secret Manager stores root CAs, service-level TLS keypairs, and per-EDP private
material.

**Shared certificates:**

* `securecomputation-root-ca` — root cert for verifying the Secure Computation API.
* `edpa-tee-app-tls-key` / `edpa-tee-app-tls-pem` — TLS keypair used by the
  ResultsFulfiller TEE app to authenticate to the Secure Computation API. Signed by
  `securecomputation-root-ca`.
* `edpa-data-watcher-tls-key` / `edpa-data-watcher-tls-pem` — DataWatcher and
  DataWatcherDelete TLS keypair for the Secure Computation API. Signed by
  `securecomputation-root-ca`.
* `edpa-requisition-fetcher-tls-key` / `edpa-requisition-fetcher-tls-pem` —
  dedicated RequisitionFetcher TLS keypair for the Metadata Storage and Secure Computation APIs.
  Grant this identity only the methods needed by RequisitionFetcher; do not give it the
  DataWatcher private key.
* `edpa-data-availability-tls-key` / `edpa-data-availability-tls-pem` —
  DataAvailabilitySync / DataAvailabilityCleanup TLS keypair for the Metadata
  Storage API. Signed by the Metadata Storage root CA.
* `edpaggregator-root-ca` (Metadata Storage root CA) — root cert for the EDP
  Aggregator (Metadata Storage) API.
* Kingdom / Duchy root CAs — provided as a single **trusted root CA collection**
  used to authenticate the Kingdom public API and the Duchies.

**Per-EDP material** (created and managed by the operator, one set per data provider):

* `<edp-id>-cert-der` — DER-encoded consent-signing certificate.
* `<edp-id>-private-der` — DER-encoded consent-signing private key.
* `<edp-id>-enc-private` — Tink keyset for encrypting ResultsFulfiller outputs.
* `<edp-id>-tls-key` / `<edp-id>-tls-pem` — the EDP's TLS keypair for authenticating
  to the CMMS.

These names must match the references inside the DataWatcher, RequisitionFetcher, and
`event_data_provider_configs` files. In Terraform they are supplied through the
`edps_certs` map (see [Secrets](#secrets)).

### DataWatcher

A Cloud Function triggered on the `finalized` (object-create) event of the storage
bucket. On each new object it receives the GCS blob URI and consults its config to
decide whether the path matches a watched pattern, which processing flow to activate,
and which downstream API or function to call. It fans every incoming EDP file into
the correct pipeline.

The two watched-path types per EDP:

1. **Event group detection** — an event-group blob invokes EventGroupSync.
2. **Impressions / data availability** — a `done` marker invokes DataAvailabilitySync.

Requisition blobs are not watched in the recommended configuration. RequisitionFetcher creates
their WorkItems directly after the blob and all associated metadata rows are durable.

Config: [`DataWatcherConfig`](#datawatcher-config-datawatcherconfig).

### DataWatcherDelete

A Cloud Function triggered on the `deleted` event of the storage bucket. It
complements the DataWatcher: when an EDP file is removed, it matches the deleted path
against its config and activates the corresponding cleanup flow — invoking
DataAvailabilityCleanup. Config uses the same `DataWatcherConfig` proto as the
DataWatcher.

### EventGroupSync

A Cloud Function invoked by the DataWatcher when a new event-group file is written.
It registers or updates the EDP's event groups in the Kingdom public API. To onboard
a new EDP, add an `event-groups` watched path in the DataWatcher config and a
per-EDP entry in the EventGroupSync config.

Config: [`EventGroupSyncConfigs`](#eventgroupsync-config-eventgroupsyncconfigs).

### RequisitionFetcher

A Cloud Function triggered by **Cloud Scheduler**. It retrieves requisitions from the Kingdom
public API, writes each grouped payload to `EDPA_STORAGE_BUCKET`, creates its RequisitionMetadata,
and submits a deterministic WorkItem to the Secure Computation API. Before submission, it records
the WorkItem name and `QUEUED` state on every metadata row in the group. A retry checks for the
deterministic WorkItem before creating it. If ResultsFulfiller exhausts its queue retries, the
control plane leaves the WorkItem `FAILED` for operator investigation; scheduled fetches do not
restart its dead-letter cycle. Rows already in `PROCESSING` remain discoverable for that recovery.

The function runs with `max_instances = 1` and a `timeout_seconds` that exceeds the
internal drain ticker interval (default `600` / 10 min in test environments; raise
toward the gen2 HTTP maximum of `3600` for large backlogs). Widen the scheduler
interval past the expected drain time so successive invocations never overlap. See
the [Metadata Operator Guide](metadata-operator-guide.md) for sizing.

Config: [`RequisitionFetcherConfig`](#requisitionfetcher-config-requisitionfetcherconfig).

### DataAvailabilitySync

A Cloud Function invoked by the DataWatcher when an empty `done` file is written
under an impressions date directory, signaling that a day's impressions and metadata
have finished uploading. It locates the directory, scans metadata (including
subfolders), records impression availability in the Metadata Storage database, and
notifies the Kingdom that impressions for that day are available.

Config: [`DataAvailabilitySyncConfigs`](#dataavailabilitysync-config-dataavailabilitysyncconfigs).

### DataAvailabilityCleanup

A Cloud Function invoked by DataWatcherDelete when an impression object is deleted.
Because object versioning is enabled, it checks whether a **live** version of the
object still exists before soft-deleting the `ImpressionMetadata` record:

* Live version still exists → cleanup is skipped, the record stays `ACTIVE`.
* No live version (object permanently deleted) → the record is soft-deleted
  (`state = DELETED`).

See [Object Versioning & ImpressionMetadata Cleanup](#object-versioning--impressionmetadata-cleanup)
for the full event/trigger matrix. Config uses the same `DataWatcherConfig` proto,
matching deletion events.

### DataAvailabilityMonitor

A scheduled Cloud Function (triggered by Cloud Scheduler) that audits impression
data-availability health per model line. Per its config it flags:

* **Stale model lines** — no upload within `max_stale_days` (default 3).
* **Unprocessed `done` blobs** — a `done` blob older than
  `unprocessed_done_threshold` (default 24h) that DataAvailabilitySync never stamped.
* **Unpublished availability** — a `done` blob with a sync-attempt ID whose Kingdom-publication ID
  does not match after `unprocessed_done_threshold`. Markerless legacy folders are not flagged.
* **Spurious deletions** — `ImpressionMetadata` marked deleted while its blob still
  exists on the bucket (enabled when `spurious_deletion_lookback_days > 0`).

For the `gap`, `zero_impression`, `without_done_blob`, `late_arriving`,
`unprocessed_done`, `unpublished_availability`, and `spurious_deletion` statuses,
`edpa.data_availability.date_count` includes
`edpa.data_availability_monitor.data_date=YYYY-MM-DD`. Configure issue alerts to preserve or group
by `data_date`, `model_line`, and `date_status`; aggregating away `data_date` preserves the total
count but loses the date that an operator needs for targeted recovery. Healthy-date and
legitimate-deletion count points omit `data_date` to avoid creating non-actionable per-date series.
See [Recover missing ImpressionMetadata](../gke/recover-missing-impression-metadata.md) for the
one-day manual Job procedure.

Config: [`DataAvailabilityMonitorConfigs`](#dataavailabilitymonitor-config-dataavailabilitymonitorconfigs).

### ResultsFulfiller (TEE)

The ResultsFulfiller is the trusted workload that fulfills requisitions. It runs as a
**Confidential Space** application on a Managed Instance Group, pulling WorkItems
from a Pub/Sub subscription. Inside the TEE it:

1. Reads the encrypted impressions and the encrypted DEK from storage.
2. Uses the EDP's KMS key (KEK) — via attestation-gated Workload Identity Federation
   — to unwrap the DEK and decrypt the impressions.
3. Computes the requisition result, applies the configured noise / k-anonymity, signs
   the result with the EDP's consent key, and returns it to the CMMS.

Its per-WorkItem parameters are defined in RequisitionFetcher's `work_item_dispatch` configuration
as the versioned `ResultsFulfillerParams` message. RequisitionFetcher validates and
passes that message unchanged as the WorkItem payload; its per-EDP TLS / consent / KMS material is
carried in the `event_data_provider_configs` file. See
[ResultsFulfiller parameters](#resultsfulfiller-parameters) and
[EDP config (event_data_provider_configs)](#edp-config-event_data_provider_configs).

### Secure Computation API

A gRPC service on GKE, reachable from RequisitionFetcher. RequisitionFetcher creates a WorkItem
after the requisition payload and metadata are durable; the API routes WorkItems to the configured
Pub/Sub queues. Enqueuing to a non-configured queue is an error.

The Secure Computation API remains workload-agnostic:

- RequisitionFetcher calls the Requisition Metadata and WorkItems APIs independently.
- The Secure Computation API stores and republishes opaque WorkItem parameters. It does not
  interpret `ResultsFulfillerParams`.
- ResultsFulfiller, not the Secure Computation API, calls the Kingdom Requisition API and the
  Requisition Metadata and Impression Metadata APIs while executing the WorkItem.
- Do not add a Secure Computation API dependency on either metadata service.

### EDP Aggregator (Metadata Storage) API

A gRPC service on GKE, backed by Spanner, that stores impression metadata
(`ImpressionMetadata`) and requisition metadata. The RequisitionFetcher,
DataAvailabilitySync, DataAvailabilityCleanup, and DataAvailabilityMonitor talk to
it. Deployed to GKE with a Spanner database (default name `edp-aggregator`) and an
internal service account bound via Workload Identity.

### Pub/Sub

Each TEE MIG is fed by a Pub/Sub topic + subscription. The Secure Computation API's
internal service account is granted `roles/pubsub.publisher` on each topic so it can
enqueue WorkItems.

---

## Object Lifecycle Management

Lifecycle rules on `EDPA_STORAGE_BUCKET` automatically delete objects per a retention
policy, so impression data does not accumulate indefinitely. Rules are configured
per **prefix**, so each EDP can have its own retention period.

The underlying `storage-bucket` module accepts a `lifecycle_rules` list. Each entry
supports:

| Field | Description | Default |
| --- | --- | --- |
| `name` | Identifier for the rule (documentation only) | — |
| `prefix` | Object prefix to match (e.g. `edp/<edp-id>/`) | — |
| `retention_days` | Days after **Custom-Time** (the impression date) before deletion | — |
| `enable_fallback` | Add an age-based safety-net delete rule | `true` |
| `fallback_retention_days` | Days after **upload** before the fallback deletes | `3650` (10y) |

The module emits two rules per entry: a **Custom-Time** delete
(`days_since_custom_time = retention_days`) and, when `enable_fallback` is true, an
**age-based** delete (`age = fallback_retention_days`) as a safety net.

> **Current limitation.** The `edp-aggregator` module does **not** yet expose
> `lifecycle_rules` as an input variable — it sets `versioning_enabled = true` and the
> per-prefix `lifecycle_rules` **inside the module source** (its `edp_aggregator_bucket`
> block). Today, changing retention therefore means editing the module source rather
> than passing a variable. The rule shape below is what the module sets internally:

```hcl
# Set inside the edp-aggregator module's storage-bucket invocation
lifecycle_rules = [
  {
    name           = "edp-a"
    prefix         = "edp/<edp-a-id>/"
    retention_days = 3650  # 10 years
  },
  {
    name           = "edp-b"
    prefix         = "edp/<edp-b-id>/"
    retention_days = 730   # 13 months
  },
]
```

Consider extending the `edp-aggregator` module to surface `lifecycle_rules` (and
`versioning_enabled`) as variables so operators can tune retention without editing
module source.

> GCS lifecycle actions run asynchronously — there is no timing guarantee. Google's
> guidance is not to rely on lifecycle actions occurring within any fixed window; in
> practice evaluation typically runs within hours.

To delete immediately (without waiting for lifecycle rules), permanently delete the
object version — see the next section for why only *permanent* deletion triggers
cleanup.

---

## Object Versioning & ImpressionMetadata Cleanup

Object versioning is **enabled** on `EDPA_STORAGE_BUCKET` (`versioning_enabled = true`)
to avoid a race where overwriting a file would fire both `OBJECT_DELETE` and
`OBJECT_FINALIZE` in parallel. With versioning, an overwrite fires
`OBJECT_ARCHIVE` + `OBJECT_FINALIZE` instead, so DataAvailabilityCleanup is never
triggered by an overwrite.

How GCS events map to cleanup:

| Action | GCS event | Cleanup runs? |
| --- | --- | --- |
| Upload new file | `OBJECT_FINALIZE` | No |
| Overwrite existing file | `OBJECT_ARCHIVE` + `OBJECT_FINALIZE` | No — archive is not watched |
| Delete live object from console (regular view) | `OBJECT_ARCHIVE` | No — becomes noncurrent |
| Delete a specific version by generation number | `OBJECT_DELETE` | Yes |
| Lifecycle rule deletes live object | `OBJECT_ARCHIVE` | No — becomes noncurrent |
| Lifecycle rule deletes noncurrent object | `OBJECT_DELETE` | Yes |

When DataAvailabilityCleanup runs (on `OBJECT_DELETE`, routed via DataWatcherDelete)
it checks whether a live version still exists:

* **Live version exists** (a noncurrent version was deleted) → cleanup is skipped;
  the `ImpressionMetadata` record stays `ACTIVE`.
* **No live version** (the object was permanently deleted) → the record is
  soft-deleted (`state = DELETED`).

Key behaviors to remember:

* Deleting from the regular console view **archives**, it does not permanently
  delete — no `OBJECT_DELETE`, no cleanup.
* Only **permanent** deletion (by generation number, via version history, or via a
  lifecycle rule deleting a noncurrent version) fires `OBJECT_DELETE` and triggers
  cleanup.
* Overwrites are safe — they never trigger cleanup.

Manual permanent deletion (when you cannot wait for lifecycle rules):

```bash
# List all versions of an object (including noncurrent)
gcloud storage ls --all-versions gs://EDPA_STORAGE_BUCKET/path/to/object

# Permanently delete one version by generation number (fires OBJECT_DELETE)
gcloud storage rm gs://EDPA_STORAGE_BUCKET/path/to/object#GENERATION_NUMBER

# Permanently delete all versions (live + noncurrent)
gcloud storage rm --all-versions gs://EDPA_STORAGE_BUCKET/path/to/object
```

---

## Configuration reference (Terraform)

All EDPA configuration is expressed two ways:

1. **Terraform variables** on the `edp-aggregator` module — infrastructure shape
   (buckets, service accounts, cloud-function definitions, MIG sizing, schedulers,
   networking, Spanner).
2. **Config files** (`.textproto`) uploaded to `EDPA_CONFIG_BUCKET` — per-EDP
   behavior read at runtime by the functions and the TEE.

The subsections below list the configuration each component needs. Optional items
are marked; supply only what your deployment requires.

### Storage buckets

```hcl
module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  edp_aggregator_bucket_name      = "EDPA_STORAGE_BUCKET"
  config_files_bucket_name        = "EDPA_CONFIG_BUCKET"
  vid_models_bucket_name          = "VID_MODELS_BUCKET"   # required by the module (see the VID Labeling note)
  edp_aggregator_buckets_location = "REGION"
  # ... (continued below)
}
```

Versioning (`versioning_enabled = true`) and the per-prefix lifecycle rules for
`EDPA_STORAGE_BUCKET` are currently set **inside the module source**, not via an input
variable — see the limitation noted under
[Object Lifecycle Management](#object-lifecycle-management).

### Secrets

Shared certificates are declared as individual variables; each takes a
`{ secret_id, secret_local_path, is_binary_format }` object referencing a Secret
Manager secret:

`edpa_tee_app_tls_key`, `edpa_tee_app_tls_pem`, `data_watcher_tls_key`,
`data_watcher_tls_pem`, `data_availability_tls_key`, `data_availability_tls_pem`,
`requisition_fetcher_tls_key`, `requisition_fetcher_tls_pem`,
`secure_computation_root_ca`, `metadata_storage_root_ca`, `trusted_root_ca_collection`.

Per-EDP material is a **map** keyed by EDP id:

```hcl
edps_certs = {
  "<edp-a-id>" = {
    cert_der    = { secret_id = "<edp-a-id>-cert-der",    secret_local_path = "...", is_binary_format = true }
    private_der = { secret_id = "<edp-a-id>-private-der", secret_local_path = "...", is_binary_format = true }
    enc_private = { secret_id = "<edp-a-id>-enc-private", secret_local_path = "...", is_binary_format = true }
    tls_key     = { secret_id = "<edp-a-id>-tls-key",     secret_local_path = "...", is_binary_format = false }
    tls_pem     = { secret_id = "<edp-a-id>-tls-pem",     secret_local_path = "...", is_binary_format = false }
  }
  # repeat per EDP
}
```

### Cloud Functions

All nine functions are configured through a single `cloud_function_configs` map.
Each entry provides:

| Field | Meaning |
| --- | --- |
| `function_name` | Deployed Cloud Function name |
| `entry_point` | Fully-qualified entry-point class |
| `extra_env_vars` | Comma-separated `KEY=VALUE` environment variables |
| `secret_mappings` | Comma-separated `local_path=secret_id:version` mounts |
| `uber_jar_path` | Path to the function's uber JAR |

```hcl
cloud_function_configs = {
  data_watcher              = { function_name = "...", entry_point = "...", extra_env_vars = "...", secret_mappings = "...", uber_jar_path = "..." }
  data_watcher_delete       = { ... }
  requisition_fetcher       = { ... }
  event_group_sync          = { ... }
  data_availability_sync    = { ... }
  data_availability_cleanup = { ... }
  data_availability_monitor = { ... }
  # Required even for a baseline R&F deployment (deploy unconditionally):
  vid_labeling_dispatcher   = { ... }
  vid_labeling_monitor      = { ... }
}
```

> The last two entries (`vid_labeling_dispatcher`, `vid_labeling_monitor`) are
> **mandatory** — those functions deploy unconditionally regardless of
> `vid_labeling_workers`. See
> [Optional: VID Labeling pipeline](#optional-vid-labeling-pipeline).

**`extra_env_vars` — the meaningful variables per function** (all functions also take
the standard OpenTelemetry variables `OTEL_SERVICE_NAME`, `OTEL_METRICS_EXPORTER`,
`OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER`, `OTEL_METRIC_EXPORT_INTERVAL`):

| Function | Key variables |
| --- | --- |
| `data_watcher` / `data_watcher_delete` | `CERT_FILE_PATH`, `PRIVATE_KEY_FILE_PATH`, `CERT_COLLECTION_FILE_PATH`, `CONTROL_PLANE_TARGET`, `CONTROL_PLANE_CERT_HOST`, `EDPA_CONFIG_STORAGE_BUCKET`, `GOOGLE_PROJECT_ID`, `CONFIG_BLOB_KEY` |
| `requisition_fetcher` | `KINGDOM_TARGET`, `EDPA_CONFIG_STORAGE_BUCKET`, `GOOGLE_PROJECT_ID`, `GRPC_REQUEST_INTERVAL`, `METADATA_STORAGE_TARGET`, `SECURE_COMPUTATION_CONTROL_PLANE_TARGET` |
| `event_group_sync` | `KINGDOM_TARGET` |
| `data_availability_sync` | `KINGDOM_TARGET`, `IMPRESSION_METADATA_TARGET` |
| `data_availability_cleanup` | `KINGDOM_TARGET`, `IMPRESSION_METADATA_TARGET` |
| `data_availability_monitor` | `IMPRESSION_METADATA_TARGET`, `EDPA_CONFIG_STORAGE_BUCKET`, `GOOGLE_PROJECT_ID`, `CONFIG_BLOB_KEY` |

**`secret_mappings` — path/secret consistency is critical.** Each mounted path must
match, character for character, the path referenced in the corresponding config
file. For example, for the DataWatcher:

| Mounted path | Must equal env var |
| --- | --- |
| `/secrets/key/data_watcher_tls.key` | `PRIVATE_KEY_FILE_PATH` |
| `/secrets/cert/data_watcher_tls.pem` | `CERT_FILE_PATH` |
| `/secrets/ca/secure_computation_root.pem` | `CERT_COLLECTION_FILE_PATH` |

And for the per-EDP TLS material referenced by EventGroupSync / DataAvailabilitySync /
RequisitionFetcher, the mount paths must equal the `cmmsConnection.*` /
`impressionMetadataStorageConnection.*` paths inside the DataWatcher and fetcher
config files. RequisitionFetcher's direct-dispatch `control_plane_connection` uses the dedicated
RequisitionFetcher certificate rather than the DataWatcher identity. Its three paths must match the
mounted `requisition_fetcher_tls_key`, `requisition_fetcher_tls_pem`, and
`secure_computation_root_ca` secrets.

> A region mismatch between a Cloud Function and the endpoint the DataWatcher calls
> (`http_endpoint_sink.endpoint_uri`) causes an HTTP 404 at invocation time. Confirm
> the deployed function URLs match the config.

### Config files (uploaded to `EDPA_CONFIG_BUCKET`)

Each config file is declared as a `{ local_path, destination }` object and uploaded
by the module:

| Variable | Proto message | Consumed by |
| --- | --- | --- |
| `data_watcher_config` | `DataWatcherConfig` | DataWatcher |
| `data_watcher_delete_config` | `DataWatcherConfig` | DataWatcherDelete |
| `requisition_fetcher_config` | `RequisitionFetcherConfig` | RequisitionFetcher |
| `event_group_sync_config` | `EventGroupSyncConfigs` | EventGroupSync |
| `data_availability_sync_config` | `DataAvailabilitySyncConfigs` | DataAvailabilitySync |
| `data_availability_monitor_config` | `DataAvailabilityMonitorConfigs` | DataAvailabilityMonitor |
| `edps_config` | `EventDataProviderConfigs` | ResultsFulfiller (TEE) |
| `results_fulfiller_event_descriptor` | (serialized event descriptor set) | ResultsFulfiller (TEE) |
| `results_fulfiller_population_spec` | (population spec) | ResultsFulfiller (TEE) |

The exact structure of each is in [Config file formats](#config-file-formats).

### ResultsFulfiller MIG

The ResultsFulfiller queue + Confidential Space worker is one
`requisition_fulfiller_config` object:

```hcl
requisition_fulfiller_config = {
  queue = {
    subscription_name    = "results-fulfiller-subscription"
    topic_name           = "results-fulfiller-queue"
    ack_deadline_seconds = 600
  }
  worker = {
    instance_template_name        = "results-fulfiller-template"
    base_instance_name            = "results-fulfiller"
    managed_instance_group_name   = "results-fulfiller-mig"
    mig_service_account_name      = "results-fulfiller-sa"
    single_instance_assignment    = 1
    min_replicas                  = 1
    max_replicas                  = 10
    machine_type                  = "n2d-standard-8"
    java_tool_options             = "-Xmx..."   # optional
    docker_image                  = "ghcr.io/.../results_fulfiller:<tag>"
    mig_distribution_policy_zones = ["REGION-a", "REGION-b"]
    app_flags                     = ["--flag=value", ...]
  }
}
```

The MIG runs on the `confidential-space` disk image family
(`results_fulfiller_disk_image_family`), on a private subnetwork with Cloud NAT and a
private DNS zone for `*.googleapis.com` (all configurable, see
[Networking](#networking)). The TEE service account is granted `objectViewer` +
`objectCreator` on `EDPA_STORAGE_BUCKET` and `objectViewer` on `EDPA_CONFIG_BUCKET`.

### Schedulers

Four schedulers are configured, each with a `{ schedule, time_zone, name,
function_url, scheduler_sa_display_name, scheduler_sa_description,
scheduler_job_description }` object:

* `requisition_fetcher_scheduler_config` — triggers the RequisitionFetcher. Set the
  interval **greater than** the expected drain time (see
  [RequisitionFetcher](#requisitionfetcher)).
* `data_availability_monitor_scheduler_config` — triggers the DataAvailabilityMonitor.
* `vid_labeling_monitor_scheduler_config` — triggers the VidLabelingMonitor health
  cadence. Required (deploys unconditionally).
* `vid_labeling_dispatch_scheduler_config` — triggers the VidLabelingMonitor dispatch
  cadence. Required (deploys unconditionally).

### Networking

The module provisions a private subnetwork, a Cloud Router + NAT, and a private DNS
zone so the Confidential VMs reach Google APIs over private paths. Defaults are
provided; override as needed:

`private_subnetwork_name`, `private_subnetwork_network` (default `default`),
`private_subnetwork_cidr_range` (default `192.168.0.0/16`), `private_router_name`,
`nat_name`, `dns_managed_zone_name`.

### Spanner and the Metadata Storage API

* `spanner_instance = { name = "..." }` — existing Spanner instance.
* `spanner_database_name` — defaults to `edp-aggregator`.
* `edp_aggregator_service_account_name` — internal API server SA (bound to the GKE
  service account `internal-edp-aggregator-server` via Workload Identity).
* `edp_aggregator_api_server_ip_address` — optional static IP for the API server.

### Service accounts

One variable per function/worker service account:
`data_watcher_service_account_name`, `data_watcher_trigger_service_account_name`,
`data_watcher_delete_service_account_name`,
`data_watcher_delete_trigger_service_account_name`,
`requisition_fetcher_service_account_name`, `event_group_sync_service_account_name`,
`data_availability_sync_service_account_name`,
`data_availability_cleanup_service_account_name`,
`data_availability_monitor_service_account_name`, plus `terraform_service_account`
(used to attach MIG service accounts to VMs) and `pubsub_iam_service_account_member`
(the Secure Computation control-plane SA granted publisher on the queues).

The module also requires the deployed **function names** for the functions the
DataWatcher / DataWatcherDelete invoke over HTTP (used to grant `run.invoker`):
`event_group_sync_function_name`, `data_availability_sync_function_name`, and
`data_availability_cleanup_function_name`. These must equal the corresponding
`cloud_function_configs.*.function_name` values.

### Optional: VID Labeling pipeline

The module can additionally deploy the memoized VID Labeling pipeline (Phase 0
SubpoolAssigner, Phase 1 VidRankBuilder, Phase 2 VidLabeler) as Confidential Space
TEE apps, plus a VidLabelingDispatcher and VidLabelingMonitor. VID labeling within
the aggregator is out of scope for the baseline (Phase 1) R&F deployment.

**Important:** only the **Phase 0/1/2 TEE MIGs and their Pub/Sub queues** are gated by
the `vid_labeling_workers` map (which defaults to `{}`). Setting an empty map does
**not** fully disable the pipeline — the **VidLabelingDispatcher** and
**VidLabelingMonitor** Cloud Functions, their **two schedulers** (dispatch and
health cadence), and the **`VID_MODELS_BUCKET`** deploy **unconditionally**, and
their inputs are **required**. Even for a baseline R&F deployment you must therefore
supply:

* `vid_models_bucket_name`;
* the `vid_labeling_dispatcher_*` and `vid_labeling_monitor_*` service-account,
  config, and scheduler variables; and
* `vid_labeling_dispatcher` / `vid_labeling_monitor` entries in
  `cloud_function_configs`.

Leave `vid_labeling_workers = {}` to skip the phase workers/queues; provide worker
entries only once your market has adopted VID labeling.

#### VID Labeling outbound RPC rate limits

Every VID Labeling process shares four client-side throttlers across its coroutines. The defaults
pace Kingdom reads at **2 QPS** (one call every 500 milliseconds), EDP-Aggregator metadata reads at
**10 QPS** (one call every 100 milliseconds), metadata mutations at **5 QPS** (one call every 200
milliseconds), and Secure Computation `WorkItems`/`WorkItemAttempts` calls at **4 QPS** (one call
every 250 milliseconds). These are per-process limits, not measured downstream capacities or
deployment-wide quotas.

The checked-in topology permits 24 phase-worker VMs, one dispatcher function, one monitor/dispatch
function, one internal API server, and one concurrently running operator retry process. If every
eligible process saturates a throttle class, the aggregate ceilings are 4 Kingdom RPCs per second
(two functions), 280 metadata reads per second (28 processes), 140 metadata mutations per second
(28 processes), and 112 control-plane RPCs per second (28 processes). These are conservative
per-class envelopes, not per-method bounds: each class contains multiple methods, and each method
has its own caller set. Compare each method's measured traffic and configured server limit before
tuning these values.

The Terraform module caps both the dispatcher and monitor at one instance. The checked-in Kingdom
`RateLimitConfig` gives each VID Repository method its own 5-QPS average / 20-request burst bucket,
so the two functions' worst-case 4 QPS leaves 20 percent headroom without competing with
ResultsFulfiller, RequisitionFetcher, EventGroupSync, or DataAvailabilitySync in the shared default
bucket. The configured methods are `ModelLines/GetModelLine`, `ModelLines/ListModelLines`,
`ModelRollouts/ListModelRollouts`, and `ModelShards/ListModelShards`; for example:

```textproto
per_method_rate_limit {
  key: "wfa.measurement.api.v2alpha.ModelLines/ListModelLines"
  value {
    maximum_request_count: 20
    average_request_rate: 5
  }
}
```

Both VID Labeling scheduler jobs use a 660-second attempt deadline, retaining 60 seconds of response
headroom beyond the monitor function's 600-second timeout. The GCS-triggered DataWatcher has a
540-second timeout and calls a dispatcher capped at 480 seconds, so that synchronous caller also
retains one minute of shutdown and response headroom.

The dispatcher and monitor accept these optional environment variables:

* `VID_LABELING_KINGDOM_RPC_MIN_INTERVAL`
* `VID_LABELING_METADATA_READ_RPC_MIN_INTERVAL`
* `VID_LABELING_METADATA_WRITE_RPC_MIN_INTERVAL`
* `VID_LABELING_CONTROL_PLANE_RPC_MIN_INTERVAL`

The TEE worker applications accept the corresponding command-line flags
`--kingdom-rpc-min-interval`, `--metadata-read-rpc-min-interval`,
`--metadata-write-rpc-min-interval`, and `--control-plane-rpc-min-interval`. The Secure Computation
internal API server uses the latter three flags for its dead-letter listeners. Values are complete
human-readable durations such as `500ms` or `1000ms` and must be positive.

---

## Config file formats

All examples use generic placeholders. Each file is uploaded to `EDPA_CONFIG_BUCKET`
as a `.textproto`.

### DataWatcher config (`DataWatcherConfig`)

Proto: `wfa/measurement/config/securecomputation/data_watcher_config.proto`.
A list of `watched_paths`; each has an `identifier`, a `source_path_regex`, and
exactly one sink — either an `http_endpoint_sink` (JSON `app_params`) or a
`control_plane_queue_sink` (typed `Any` `app_params`).

```textproto
# proto-file: wfa/measurement/config/securecomputation/data_watcher_config.proto
# proto-message: wfa.measurement.config.securecomputation.DataWatcherConfig

# 1) Event groups -> EventGroupSync (HTTP)
watched_paths {
  identifier: "event-groups"
  source_path_regex: "gs://EDPA_STORAGE_BUCKET/<edp-id>/event-groups/(.*)"
  http_endpoint_sink {
    endpoint_uri: "https://REGION-PROJECT_ID.cloudfunctions.net/event-group-sync"
    app_params {
      fields { key: "dataProvider" value { string_value: "dataProviders/DATA_PROVIDER_ID" } }
      fields { key: "eventGroupMapBlobUri"
               value { string_value: "gs://EDPA_STORAGE_BUCKET/<edp-id>/event-groups-map/groups.pb" } }
      fields { key: "cmmsConnection" value { struct_value {
        fields { key: "certFilePath"           value { string_value: "/secrets/cert/<edp-id>_tls.pem" } }
        fields { key: "privateKeyFilePath"     value { string_value: "/secrets/key/<edp-id>_tls.key" } }
        fields { key: "certCollectionFilePath" value { string_value: "/secrets/ca/kingdom_root.pem" } }
      } } }
      # eventGroupStorage / eventGroupMapStorage: gcs { projectId, bucketName }
    }
  }
}

# 2) Data availability -> DataAvailabilitySync (HTTP), fires on the `done` marker
watched_paths {
  identifier: "data-availability"
  source_path_regex: "^gs://EDPA_STORAGE_BUCKET/edp/<edp-id>/.+/done$"
  http_endpoint_sink {
    endpoint_uri: "https://REGION-PROJECT_ID.cloudfunctions.net/data-availability-sync"
    app_params {
      fields { key: "dataProvider" value { string_value: "dataProviders/DATA_PROVIDER_ID" } }
      # dataAvailabilityStorage.gcs { projectId, bucketName }
      # cmmsConnection.{certFilePath, privateKeyFilePath, certCollectionFilePath}
      # impressionMetadataStorageConnection.{certFilePath, privateKeyFilePath, certCollectionFilePath}
    }
  }
}
```

Repeat the two watched paths per EDP. The DataWatcherDelete config
(`data_watcher_delete_config`) uses the same proto with a `data-availability-cleanup`
identifier whose `endpoint_uri` points at the DataAvailabilityCleanup function.

### RequisitionFetcher config (`RequisitionFetcherConfig`)

Proto: `wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto`.
One `configs` entry per EDP.

```textproto
# proto-file: wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto
# proto-message: wfa.measurement.config.edpaggregator.RequisitionFetcherConfig
requisition_refusal_duration {
  seconds: 172800  # 48 hours
}
configs {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  requisition_storage { gcs { project_id: "PROJECT_ID" bucket_name: "EDPA_STORAGE_BUCKET" } }
  storage_path_prefix: "<edp-id>/requisitions"  # Legacy recovery only; no new writes.
  cmms_connection {
    cert_file_path: "/secrets/cert/<edp-id>_tls.pem"
    private_key_file_path: "/secrets/key/<edp-id>_tls.key"
    cert_collection_file_path: "/secrets/ca/kingdom_root.pem"
  }
  edp_private_key_path: "/secrets/private/<edp-id>_enc_private.tink"
  requisition_metadata_storage_connection {
    cert_file_path: "/secrets/cert_requisition_fetcher/requisition_fetcher_tls.pem"
    private_key_file_path: "/secrets/key_requisition_fetcher/requisition_fetcher_tls.key"
    cert_collection_file_path: "/secrets/ca/cert_metadata_storage/edp_aggregator_root.pem"
  }
  # Required. All newly fetched requisitions use direct dispatch.
  work_item_dispatch {
    # Dedicated namespace for directly dispatched groups. Do not match this
    # path in the legacy DataWatcher source_path_regex.
    storage_path_prefix: "<edp-id>/requisitions-v2"
    control_plane_connection {
      cert_file_path: "/secrets/cert_requisition_fetcher/requisition_fetcher_tls.pem"
      private_key_file_path: "/secrets/key_requisition_fetcher/requisition_fetcher_tls.key"
      cert_collection_file_path: "/secrets/ca/securecomputation_root.pem"
    }
    queue: "results-fulfiller-queue"
    results_fulfiller_params {
      data_provider: "dataProviders/DATA_PROVIDER_ID"
      storage_params {
        labeled_impressions_blob_details_uri_prefix: "gs://EDPA_STORAGE_BUCKET"
        gcs_project_id: "PROJECT_ID"
      }
      consent_params {
        result_cs_cert_der_resource_path: "/tmp/edp_certs/<edp-id>_cs_cert.der"
        result_cs_private_key_der_resource_path: "/tmp/edp_certs/<edp-id>_cs_private.der"
        private_encryption_key_resource_path: "/tmp/edp_certs/<edp-id>_enc_private.tink"
        edp_certificate_name: "dataProviders/DATA_PROVIDER_ID/certificates/CERT_ID"
      }
      cmms_connection {
        client_cert_resource_path: "/tmp/edp_certs/<edp-id>_tls.pem"
        client_private_key_resource_path: "/tmp/edp_certs/<edp-id>_tls.key"
      }
      noise_params { noise_type: CONTINUOUS_GAUSSIAN }
    }
  }
}
```

`requisition_refusal_duration` bounds how long RequisitionFetcher will attempt to fulfill an
unfulfilled Kingdom Requisition. The field is optional and defaults to 48 hours; when specified it
must be positive. Age is measured from the public Requisition's Kingdom `update_time`, and the
Requisition is refused with `DECLINED` only when it is strictly older than the configured duration.
The exact boundary remains eligible for fulfillment. A Requisition whose `update_time` is absent or
invalid is logged and is not automatically refused because its age cannot be established.

Age-based refusal also applies to Requisitions with existing `STORED`, `QUEUED`, or `PROCESSING`
metadata. RequisitionFetcher refuses the Kingdom Requisition first, which makes the terminal
Kingdom state authoritative even if a ResultsFulfiller worker is already running, and then marks
the matching metadata `REFUSED`. If every metadata member in the group is terminal, the fetcher
generation-fails its WorkItem; otherwise ResultsFulfiller skips terminal Kingdom Requisitions and
continues eligible siblings. Failing a WorkItem fences its control-plane state but does not forcibly
stop an already-running TEE, so the prior Kingdom refusal is the safety boundary. If the Kingdom
refusal races with fulfillment, withdrawal, or another refusal, the fetcher reads the authoritative
Kingdom state and applies the matching local terminal transition. If the Requisition remains
`UNFULFILLED` or its state cannot be resolved, it and its group are excluded from dispatch for the
current run; a later scheduled invocation retries the refusal.

`work_item_dispatch` is required for every configured data provider. RequisitionFetcher writes every
new grouped blob under its nested `storage_path_prefix` and dispatches it directly. The top-level
`storage_path_prefix` is retained only to recognize and recover pre-cutover DataWatcher-owned groups.
Every legacy and direct prefix sharing a bucket must be disjoint globally: no prefix may equal,
contain, or be contained by another at a path-segment boundary, even when the prefixes belong to
different data providers. The fetcher validates all namespaces before processing any provider. Keep
the DataWatcher `results-fulfiller` watched path restricted to the top-level legacy prefix.
RequisitionFetcher also requires
`SECURE_COMPUTATION_CONTROL_PLANE_TARGET` and, when needed,
`SECURE_COMPUTATION_CONTROL_PLANE_CERT_HOST`. The repository's Terraform entry point injects the
target from `secure_computation_public_api_target` and mounts the
`securecomputation-root-ca` secret at `/secrets/secure-computation-ca/secure_computation_root.pem`; each
`control_plane_connection.cert_collection_file_path` must name that mounted path.

#### Migrating from DataWatcher dispatch

The legacy and direct paths use separate object namespaces. All new groups are created atomically in
`QUEUED` under the direct prefix. Pre-cutover legacy groups remain `STORED` under the original
prefix. Recovery uses each group's persisted `blob_uri`; it never moves a group between namespaces.
Keep the direct prefix unchanged while any direct group remains `STORED`, `QUEUED`, or `PROCESSING`.
RequisitionFetcher determines ownership by comparing each persisted `blob_uri` with the URI derived
from the currently configured prefix; changing it sooner makes those groups unrecognizable and
strands their recovery.

A legacy group with any `PROCESSING` row remains owned by its existing
DataWatcher WorkItem: RequisitionFetcher neither dispatches it directly nor rebuilds a missing blob.
It still processes newly discovered requisitions for the same report through the direct namespace.

To activate direct dispatch, operators only need to:

1. Add the required `work_item_dispatch` block to every provider in
   `REQUISITION_FETCHER_CONFIG_CONTENT`. Preserve the existing top-level `storage_path_prefix`,
   choose a dedicated nested prefix such as `<edp-id>/requisitions-v2` that is disjoint from every
   legacy and direct prefix sharing the bucket.
2. Run the repository's top-level **Update CMMS** workflow, or automation that implements the same
   environment lock and ordered barriers.
3. If deployment fails before workers are restored, rerun the complete process. Do not enable
   direct dispatch, TEE MIGs, or individual deployment phases independently.

The workflow's environment-scoped concurrency lock prevents overlapping deployments from
interleaving rollout phases. Before its first Terraform apply, the workflow validates the exact
RequisitionFetcher and DataWatcher textprotos from the selected GitHub environment. It requires a
direct-dispatch block for every configured data provider, a control-plane target, queue, TLS paths,
and valid ResultsFulfiller parameters; it also rejects overlapping storage prefixes or any deployed
DataWatcher regex that matches a representative direct-path object. Validation failure therefore
stops deployment before any worker is quiesced. The workflow first rolls both Secure Computation API
deployments with WorkItem publication, legacy reconciliation, and dead-letter processing disabled.
Its first Terraform apply then pauses the RequisitionFetcher Cloud Scheduler job, uploads the
direct-only configuration and binary, and quiesces all WorkItem-consuming TEE MIGs. The workflow
waits for the fetcher's 600-second maximum invocation duration and verifies that every affected TEE
MIG has zero instances before continuing. It rolls both Secure Computation API deployments again
with publication and dead-letter processing enabled, then rolls every EDP Aggregator/Requisition
Metadata API deployment to completion. Its final Terraform apply validates the configuration again
before resuming the RequisitionFetcher scheduler and enabling the new workers.

The final Terraform apply resumes the RequisitionFetcher scheduler and enables the new workers.
Scheduler pausing is independent of the function revision, so an old fetcher cannot run during the
API and worker rollout and the new direct-only fetcher needs no legacy-mode process flag. The next
invocation polls the same unfulfilled Kingdom requisitions. DataWatcher stays active and unclaimed
Pub/Sub messages remain queued; they must not be drained.

After all legacy groups have finished and no legacy blobs require recovery, remove the legacy
ResultsFulfiller watched path from the DataWatcher configuration. Preflight accepts a DataWatcher
configuration without that route; direct dispatch does not depend on the legacy queue mapping.

Do not invoke child deployment workflows independently for this upgrade. No manual service
scaling, subscription drain, WorkItem snapshot, active-attempt query, or migration-time
failure/retry RPC is required.

The upgraded publication runner automatically repairs old `QUEUED` WorkItems without outbox rows.
The upgraded DataWatcher uses deterministic WorkItem IDs and returns transient dispatch failures to
Eventarc, so retained and future legacy events can be redelivered safely. A new lease-capable worker
atomically replaces an unleased attempt left by a stopped old worker when its Pub/Sub message is
redelivered. Events that the previous DataWatcher acknowledged after an ambiguous dispatch failure
are not recoverable from Pub/Sub; before claiming that no legacy recovery is required, identify any
legacy `STORED` group with a blob but no matching WorkItem.

This cutover does not add version-suffixed RPCs or another Secure Computation queue, Pub/Sub topic,
subscription, or dead-letter queue. It keeps the existing outbox publish-ack behavior,
`EnsureWorkItem` for idempotent dispatch, and `RegisterQueuedRequisitionMetadata` for atomic
ownership registration. The existing EDPA-aware DLQ behavior predates this change and is not
expanded for ResultsFulfiller.

After cutover, a `FAILED` WorkItem is not retried by RequisitionFetcher. Remediate the underlying
failure, then call `RetryWorkItem` explicitly. Upgraded workers renew attempt leases, and the Secure
Computation internal API automatically republishes an attempt after its lease expires.

For rollback, first pause the RequisitionFetcher scheduler and disable the WorkItem consumers, then
drain or repair all direct-prefix groups in `STORED`, `QUEUED`, or `PROCESSING`; the legacy
DataWatcher intentionally does not watch that namespace. Restore the pre-cutover RequisitionFetcher
binary and config as a unit before resuming the scheduler. Do not remove `work_item_dispatch` while
the new binary is deployed: the field is required. The legacy prefix and DataWatcher rule remain
unchanged.

### EventGroupSync config (`EventGroupSyncConfigs`)

Proto: `wfa/measurement/config/edpaggregator/event_group_sync_config.proto`.
One `configs` entry per EDP (each `data_provider` must be unique).

```textproto
# proto-message: wfa.measurement.config.edpaggregator.EventGroupSyncConfigs
configs {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  event_group_map_blob_uri: "gs://EDPA_STORAGE_BUCKET/<edp-id>/event-groups-map/groups.pb"
  cmms_connection {
    cert_file_path: "/secrets/cert/<edp-id>_tls.pem"
    private_key_file_path: "/secrets/key/<edp-id>_tls.key"
    cert_collection_file_path: "/secrets/ca/kingdom_root.pem"
  }
  event_group_storage { gcs { project_id: "PROJECT_ID" bucket_name: "EDPA_STORAGE_BUCKET" } }
  event_group_map_storage { gcs { project_id: "PROJECT_ID" bucket_name: "EDPA_STORAGE_BUCKET" } }
  # entity_key_types: ["<type>"]   # optional; default lists only "campaign" entity types
}
```

### DataAvailabilitySync config (`DataAvailabilitySyncConfigs`)

Proto: `wfa/measurement/config/edpaggregator/data_availability_sync_config.proto`.
One `configs` entry per EDP.

```textproto
# proto-message: wfa.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
configs {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  data_availability_storage { gcs { project_id: "PROJECT_ID" bucket_name: "EDPA_STORAGE_BUCKET" } }
  cmms_connection { cert_file_path: "..." private_key_file_path: "..." cert_collection_file_path: "..." }
  impression_metadata_storage_connection { cert_file_path: "..." private_key_file_path: "..." cert_collection_file_path: "..." }
  edp_impression_path: "edp/<edp-id>/vid-labeled-impressions"   # optional today; required in a future release
  # model_line_map { key: "modelLines/INTERNAL" value { model_lines: ["modelLines/EXTERNAL"] } }   # optional
  # error_if_gaps_exist: false   # optional
}
```

### DataAvailabilityMonitor config (`DataAvailabilityMonitorConfigs`)

Proto: `wfa/measurement/config/edpaggregator/data_availability_monitor_config.proto`.
One `configs` entry per monitored impression path.

```textproto
# proto-message: wfa.measurement.config.edpaggregator.DataAvailabilityMonitorConfigs
configs {
  storage { gcs { project_id: "PROJECT_ID" bucket_name: "EDPA_STORAGE_BUCKET" } }
  edp_impression_path: "edp/<edp-id>/vid-labeled-impressions"
  model_line_configs { model_line: "modelProviders/MP/modelSuites/MS/modelLines/ML" }
  max_stale_days: 3                       # optional (default 3)
  time_zone: "UTC"
  data_provider_name: "dataProviders/DATA_PROVIDER_ID"          # required for the spurious-deletion check
  impression_metadata_connection { cert_file_path: "..." private_key_file_path: "..." cert_collection_file_path: "..." }
  # spurious_deletion_lookback_days: 7    # optional; > 0 enables the spurious-deletion check
  # unprocessed_done_threshold { seconds: 86400 }   # optional (default 24h)
}
```

### EDP config (`event_data_provider_configs`)

Proto: `wfa/measurement/config/edpaggregator/event_data_provider_configs.proto`.
Read by the ResultsFulfiller TEE. One `event_data_provider_config` per EDP; carries
that EDP's KMS, TLS, and consent material.

```textproto
# proto-message: wfa.measurement.config.edpaggregator.EventDataProviderConfigs
event_data_provider_config {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  kms_config {
    kms_type: GCP                                  # or AWS — see the AWS KMS Setup Guide
    kms_audience: "//iam.googleapis.com/projects/EDP_PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL/providers/PROVIDER"
    service_account: "SA_NAME@EDP_PROJECT.iam.gserviceaccount.com"
    kek_uri: "gcp-kms://projects/EDP_PROJECT/locations/global/keyRings/RING/cryptoKeys/KEY"
  }
  tls_config {
    tls_key_secret_id: "<edp-id>-tls-key"   tls_key_local_path: "/secrets/key/<edp-id>_tls.key"
    tls_pem_secret_id: "<edp-id>-tls-pem"   tls_pem_local_path: "/secrets/cert/<edp-id>_tls.pem"
  }
  consent_signaling_config {
    cert_der_secret_id: "<edp-id>-cert-der"        cert_der_local_path: "/tmp/edp_certs/<edp-id>_cs_cert.der"
    enc_private_der_secret_id: "<edp-id>-private-der" enc_private_der_local_path: "/tmp/edp_certs/<edp-id>_cs_private.der"
    enc_private_secret_id: "<edp-id>-enc-private"  enc_private_local_path: "/tmp/edp_certs/<edp-id>_enc_private.tink"
  }
}
```

For an AWS-KMS EDP, set `kms_type: AWS` and add the `aws_role_arn`,
`aws_role_session_name`, `aws_region`, and `aws_audience` fields **in addition to**
`kms_audience` and `service_account` — the latter two are still required because the
Confidential VM uses a GCP-WIF hop before assuming the AWS role. The full walkthrough
is in the [AWS KMS Setup Guide](aws-kms-setup.md).

### ResultsFulfiller parameters

Each RequisitionFetcher `work_item_dispatch.results_fulfiller_params` field is a versioned
`wfa.measurement.edpaggregator.v1alpha.ResultsFulfillerParams` message. It crosses the Cloud
Function-to-TEE boundary as the WorkItem payload, so RequisitionFetcher validates and passes it
without converting it to a duplicated unversioned wire schema. Beyond the `data_provider`,
`storage_params`, `consent_params`, and `cmms_connection` shown above, it supports:

* `noise_params.noise_type` — `NONE` / `CONTINUOUS_GAUSSIAN` (direct single-EDP
  results).
* `k_anonymity_params` — `{ min_impressions, min_users, reach_max_frequency_per_user }`;
  a below-threshold result returns zero.
* `impression_max_frequency_per_user` — direct impression measurements only.
* `model_line_map` — optional external→internal model-line remapping for impression
  lookup.
* `trustee_params.kek_uri_to_key_name` — required for TrusTEE support; maps an input
  KEK URI to the re-encryption key name on the same key ring (see the EDP-side
  [TrusTEE section](edp-onboarding.md#6-enabling-trustee-optional)).
* `multi_party_config.supported_noise_types` — restricts accepted noise mechanisms
  for HMSS / TrusTEE requisitions.

---

## Deployment

### Prerequisites

* A GCP project with billing, and the GKE, Spanner, Cloud Functions, Cloud Run,
  Eventarc, Pub/Sub, Secret Manager, Confidential Computing, Cloud KMS, and Cloud
  Scheduler APIs enabled.
* An existing Spanner instance.
* A deployed Kingdom cluster (the EDPA services authenticate against the Kingdom
  public API).
* Container images for the Secure Computation API, the EDP Aggregator (Metadata
  Storage) API, and the ResultsFulfiller TEE app, published to your registry.
* All shared and per-EDP secrets created in Secret Manager.

### Step 1 — Storage buckets

Create `EDPA_STORAGE_BUCKET` and `EDPA_CONFIG_BUCKET` (and `VID_MODELS_BUCKET` if
using VID Labeling). The module creates them from the `*_bucket_name` variables with
versioning + lifecycle rules on the data bucket.

#### Deploying DataWatcher for multiple buckets

The DataWatcher needs **one trigger per bucket**. With a single shared bucket, the
default single-trigger deployment is enough. For per-EDP buckets:

1. Deploy the DataWatcher without triggers.
2. Add one Eventarc trigger per bucket (each on the `finalized` event, targeting the
   same function).
3. Verify all triggers exist and point at the function.

### Step 2 — Deploy infrastructure with Terraform

Invoke the `edp-aggregator` module with the variables described in
[Configuration reference](#configuration-reference-terraform). The module provisions
the buckets, secrets, all Cloud Functions and their IAM, the ResultsFulfiller queue +
MIG, the schedulers, networking, the Spanner database, and the Metadata Storage API
service account, and uploads every config file to `EDPA_CONFIG_BUCKET`.

```bash
terraform init
terraform plan
terraform apply
```

Both GKE services are populated by applying a K8s **Kustomization** generated from
[CUE](https://cuelang.org/) via Bazel. The `src/main/k8s/dev` configuration is a
usable base — substitute your own values. The steps below are one valid path; adjust
region, names, and sizing to your environment.

### Step 3 — Deploy the Secure Computation API on GKE

This service must be reachable from the DataWatcher. It assumes a Kingdom cluster is
already deployed (see [`docs/gke/kingdom-deployment.md`](../gke/kingdom-deployment.md)).

1. **Build and push the container images** (see
   [Build and push the container images](../gke/kingdom-deployment.md#build-and-push-the-container-images-optional)).
2. **Generate the Kustomization** (substitute your values):

   ```bash
   bazel build //src/main/k8s/dev:secure_computation.tar \
     --define google_cloud_project=PROJECT_ID \
     --define spanner_instance=SPANNER_INSTANCE \
     --define container_registry=ghcr.io \
     --define image_repo_prefix=IMAGE_REPO_PREFIX \
     --define image_tag=IMAGE_TAG
   ```

   Extract the archive to a secure, persistent directory (you add secrets to it next).
3. **Customize the K8s secrets.** Place these files in
   `src/main/k8s/dev/secure_computation_secrets/`:
   * `all_root_certs.pem` — the trusted root CA store: the concatenation of the root
     CAs of every entity the server talks to (Measurement Consumers, result
     producers, and the Kingdom). If your root certs end in `_root.pem` and each
     ends with a newline: `cat *_root.pem > all_root_certs.pem`.
   * `secure_computation_root.pem`, `secure_computation_tls.pem`,
     `secure_computation_tls.key` — the server's root CA and TLS keypair.
   * `data_watcher_tls.pem` / `data_watcher_tls.key` — the DataWatcher's TLS keypair
     (signed by `securecomputation-root-ca`).
   * `edpa_tee_app_tls.pem` / `edpa_tee_app_tls.key` — the ResultsFulfiller TEE app's
     TLS keypair.

   > Repo [testing keys](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/k8s/testing/secretfiles)
   > exist for test environments only — never use them in production.
4. **Customize the ConfigMap (`config-files`).** Place `queues_config.textproto`
   (message `QueuesConfig`, proto
   `wfa/measurement/config/securecomputation/queues_config.proto`) in
   `src/main/k8s/dev/secure_computation_config_files/`. **This file is required** —
   the server is started with `--queue-config=.../config-files/queues_config.textproto`
   and will not start without it. It declares each work queue and the WorkItem params
   type it accepts. At minimum it must include the ResultsFulfiller queue; add the VID
   Labeling queues only if that pipeline is enabled:

   ```textproto
   # proto-message: wfa.measurement.config.securecomputation.QueuesConfig
   queueInfos {
     queue_resource_id: "results-fulfiller-queue"
     app_params_type_url: "type.googleapis.com/wfa.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams"
   }
   ```

   Every `queue_resource_id` here must match the `control_plane_queue_sink.queue`
   values in the DataWatcher config and the Pub/Sub topics created by Terraform.
5. **Apply** and verify:

   ```bash
   kubectl apply -k src/main/k8s/dev/secure_computation
   kubectl get deployments
   kubectl get services
   ```

#### Rolling out durable WorkItem publication

The repository's top-level **Update CMMS** workflow is the supported upgrade path. Do not invoke its
child deployment workflows independently; doing so bypasses the worker-quiescence barrier.

Configure the deployment, then run **Update CMMS** once. Before changing a deployment, the workflow
validates the RequisitionFetcher and DataWatcher configuration, including direct/legacy namespace
separation and required control-plane settings. An environment-scoped concurrency lock prevents
two runs from interleaving the worker-quiescence and API-rollout phases. The workflow
performs the required order:

1. Roll `secure-computation-internal-api-server` and
   `secure-computation-public-api-server` with WorkItem publication, legacy reconciliation, and
   dead-letter consumption paused. The APIs remain available to producers and existing workers;
   transactions continue creating durable outbox rows without publishing them.
2. Apply Terraform with every WorkItem-consuming TEE managed instance group disabled. This removes
   its autoscaler, sets its target size to zero, and writes a process-level consumption gate into
   the replacement instance template so a surge instance cannot pull work while quiescing.
3. Wait for ResultsFulfiller, SubpoolAssigner, VidRankBuilder, and VidLabeler MIGs to become stable,
   then verify that each has target size zero and no remaining instances.
4. Roll Kingdom, then roll the Secure Computation APIs again with WorkItem publication,
   reconciliation, and dead-letter consumption enabled. The DLQ listener defers a delivery while a
   current-generation WorkItem still has a valid leased attempt; otherwise it terminalizes the
   exhausted WorkItem.
5. Roll every EDP Aggregator/Requisition Metadata API deployment and wait for completion.
6. Apply Terraform again with RequisitionFetcher and WorkItem TEE consumers enabled. This recreates
   the TEE autoscalers, changes both process-level gates to enabled, and starts only the new worker
   version.
7. Continue the remaining deployment and tests normally.

If the workflow fails after quiescing workers but before the final Terraform apply, leave the TEE
consumers disabled and rerun the complete **Update CMMS** workflow. Do not enable a TEE MIG
independently. Do not manually scale the API deployments to zero: their manifests do not
explicitly restore replica counts, so manual scaling can leave them stopped.

DataWatcher and Pub/Sub remain running during this process. Terraform pauses the RequisitionFetcher
Cloud Scheduler job, and the workflow then waits for invocations that started before the pause to
finish. Unclaimed messages remain queued and must not be drained. Old
and new API replicas may overlap in the first Kubernetes rolling update while old TEE workers are
still running. Publication and legacy reconciliation are disabled on every new internal API replica
during that rollout, preventing a new replica from introducing duplicate legacy deliveries. After
the first API rollout completes, the workflow stops every TEE consumer before enabling publication.
Compatibility and automatic recovery cover producer traffic during that interval:

* The publication runner continuously finds every `QUEUED` WorkItem whose generation has not been
  scheduled, creates a missing outbox row, and records the scheduled generation in the same
  transaction. Continuous reconciliation also repairs WorkItems committed by an old API replica
  after a newer publication runner has started.
* DataWatcher derives a stable WorkItem ID from the watched-path identifier, object URI, and GCS
  generation. It uses `EnsureWorkItem`, validates an existing item when falling back to an older
  API, and returns transient dispatch failures to Eventarc so the same event is retried.
* Dead-letter consumption is paused before old workers stop. After the zero-instance barrier, a
  current-generation DLQ delivery with an active unleased or expired attempt atomically fails that
  attempt and the WorkItem. A DLQ delivery for an active leased attempt is NACKed without mutation
  so the live worker retains ownership. A `QUEUED` delivery that exhausted its Pub/Sub delivery
  budget before any attempt was created is terminalized. Pub/Sub's delivery-attempt counter and
  dead-letter policy are the sole automatic retry limit.
* A lease-capable worker that receives a redelivery for an unleased active attempt atomically fails
  that legacy attempt and creates its new leased attempt at the same WorkItem generation. The MIG
  barrier makes this safe by proving that no old TEE instance remains before new workers start.
* Existing generation-less WorkItems and queue messages are treated as generation 1. New clients
  always send the expected execution generation; an explicitly supplied value below 1 is invalid.
  Generation checks make repeated terminalization idempotent and prevent stale ordinary and
  dead-letter deliveries from changing replacement executions.

No subscription drain, database snapshot, active-attempt query, or migration-time
`FailWorkItemAttempt`/`RetryWorkItem` call is required. New WorkItem creation, `EnsureWorkItem`, and
`RetryWorkItem` maintain the publication-generation marker and outbox transactionally. Every retry
must include the generation the operator inspected, so a delayed or replayed request cannot retry a
later execution. The
publisher deletes the outbox row only after Pub/Sub acknowledges the message. This reuses the
existing WorkItems RPCs, queues, topics, subscriptions, and DLQs. The Secure Computation API remains
workload-agnostic: it stores and republishes opaque WorkItem parameters and does not call the
Requisition Metadata or Impression Metadata APIs.

#### Recovering after correcting a queue mapping

When the publisher cannot resolve a WorkItem's queue, it deprioritizes that pending publication so
it cannot block healthy work. After correcting the Secure Computation API queue mapping, wait for
the publication deferral interval to expire (one minute by default). If an affected WorkItem does
not resume automatically, call `RetryWorkItem` for that WorkItem. The targeted attempt bypasses
priority order while still respecting an active publication lease:

```bash
grpcurl -cert CLIENT_CERT_PEM -key CLIENT_KEY_PEM -cacert TRUSTED_ROOTS_PEM \
  -authority SECURE_COMPUTATION_CERT_HOST \
  -d '{"name":"workItems/WORK_ITEM_ID","expectedWorkItemGeneration":"GENERATION"}' \
  SECURE_COMPUTATION_API_TARGET \
  wfa.measurement.securecomputation.controlplane.v1alpha.WorkItems/RetryWorkItem
```

#### Recovering an abandoned running WorkItem

Workers created after this rollout renew their active attempt lease. If a worker exits or can no
longer reach the control plane, the lease expires after five minutes by default. Pub/Sub redelivers
the same WorkItem generation, and `CreateWorkItemAttempt` atomically fails the expired attempt and
creates its replacement. A late heartbeat or completion from the abandoned worker is rejected
because its attempt is no longer authoritative.

A lease-capable worker that catches a retryable workload failure marks its exact attempt `FAILED`
and NACKs the original delivery. If the failure RPC cannot be confirmed, it still NACKs. The
redelivery creates the next attempt at the same WorkItem generation. A non-retryable workload
failure uses generation-fenced `FailWorkItem` and ACKs only after terminal failure is durable.
Pub/Sub owns retry timing, approximate delivery-attempt counting, and dead-letter forwarding;
attempt history is diagnostic and does not control retry policy. The deprecated
`max_work_item_attempts` and `dead_letter_queue_resource_id` queue fields are ignored by the new
retry path.

An attempt created by an old worker has no lease. After the workflow's MIG quiescence barrier, the
stopped worker's Pub/Sub delivery is either redelivered to a new lease-capable worker or is handled
by the upgraded DLQ listener. Main-queue takeover fails the exact unleased attempt and creates the
replacement leased attempt in one Spanner transaction without advancing the WorkItem generation.
DLQ handling atomically fails the unleased attempt and WorkItem. Both paths are automatic and
generation-fenced. A leased active attempt is never replaced by a duplicate delivery; the worker
retains that delivery, with its acknowledgment deadline extended, until the active attempt becomes
terminal or its lease can be replaced.

#### Monitoring active attempts

Alert on expired leased attempts and on unleased `ACTIVE` attempts. A new worker normally replaces
either kind of abandoned attempt when Pub/Sub redelivers the WorkItem after the automated
quiescence step. Either result remaining for
more than a short grace period needs investigation:

```bash
gcloud spanner databases execute-sql SECURE_COMPUTATION_DATABASE \
  --instance=SPANNER_INSTANCE \
  --project=PROJECT_ID \
  --sql='SELECT W.WorkItemResourceId,
      A.WorkItemAttemptResourceId,
      A.LeaseExpirationTime,
      A.CreateTime
    FROM WorkItemAttempts AS A
    JOIN WorkItems AS W USING (WorkItemId)
    WHERE A.State = 1
      AND (A.LeaseExpirationTime IS NULL
        OR A.LeaseExpirationTime < CURRENT_TIMESTAMP())
    ORDER BY A.LeaseExpirationTime, A.CreateTime'
```

`WorkItemAttempt.State.ACTIVE` is stored as `1`. Workers retry transient lease, completion, and
failure RPC errors with bounded backoff. A redelivered message resolves an expired or legacy
unleased attempt transactionally with a concurrent renewal or completion. Investigate an active
attempt that does not receive a replacement delivery after lease expiry. Use generation-fenced
`RetryWorkItem` only for explicit operator-authorized recovery after ordinary Pub/Sub delivery is
no longer available.

### Step 4 — Deploy the EDP Aggregator (Metadata Storage) API on GKE

Backed by the Spanner database created in Step 2, with the internal service account
bound via Workload Identity.

1. **Build and push the container images.**
2. **Generate the Kustomization:**

   ```bash
   bazel build //src/main/k8s/dev:edp_aggregator.tar \
     --define google_cloud_project=PROJECT_ID \
     --define spanner_instance=SPANNER_INSTANCE \
     --define container_registry=ghcr.io \
     --define image_repo_prefix=IMAGE_REPO_PREFIX \
     --define image_tag=IMAGE_TAG
   ```
3. **Customize the K8s secrets.** Place these in
   `src/main/k8s/dev/edp_aggregator_secrets/`:
   * `all_root_certs.pem` — as in Step 3 (`cat *_root.pem > all_root_certs.pem`).
   * `metadata_storage_root.pem` — the Metadata Storage server's root CA.
   * `secure_computation_root.pem` — the Secure Computation server's root CA.
   * `edp_aggregator_tls.pem` / `edp_aggregator_tls.key` — the Metadata Storage
     server's TLS keypair.
   * `requisition_fetcher_tls.pem` / `requisition_fetcher_tls.key` — the
     RequisitionFetcher's TLS keypair.
   * `edpa_tee_app_tls.pem` / `edpa_tee_app_tls.key` — the ResultsFulfiller TEE app's
     TLS keypair.
   * `data_availability_tls.pem` / `data_availability_tls.key` — the
     DataAvailabilitySync's TLS keypair.
4. **Apply** and verify:

   ```bash
   kubectl apply -k src/main/k8s/dev/edp_aggregator
   kubectl get deployments
   kubectl get services
   ```

---

## Validation (cloud test)

An end-to-end cloud test simulates a single-publisher R&F measurement and exercises
the full pipeline: event-group sync → requisition fetch → impression upload →
data-availability → ResultsFulfiller → result returned to the CMMS.

### Prerequisites

1. **Register a `DataProvider`** in the Kingdom (see the Kingdom deploy tools).
2. **Set up the data provider's KMS + Workload Identity Provider** (see the
   [EDP Onboarding Guide](edp-onboarding.md), or [AWS KMS Setup](aws-kms-setup.md)).
3. **Upload the config files** to `EDPA_CONFIG_BUCKET`.
4. **Generate and encrypt synthetic data** with the data provider's KMS using the
   `GenerateSyntheticData` CLI, e.g. (generic values):

   ```bash
   bazel --host_jvm_args=-Xmx20g run \
     //src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/tools:GenerateSyntheticData -- \
     --edp-name=<edp-id> \
     --config-file=impression_test_data_config.textproto \
     --kms-type=GCP \
     --kek-uri=gcp-kms://projects/EDP_PROJECT/locations/global/keyRings/RING/cryptoKeys/KEY \
     --output-bucket=EDPA_STORAGE_BUCKET \
     --scheme=gs:// \
     --event-message-type-url=type.googleapis.com/<your.event.Message> \
     --model-line=modelProviders/MP/modelSuites/MS/modelLines/ML \
     --create-done-blobs
   ```

   `--edp-name`, `--config-file`, `--kek-uri`, and `--model-line` are required.
   `--kek-uri` is the data provider's KEK. The event-group reference id, population
   spec, and data spec are **not** CLI flags — they are fields of the
   `ImpressionTestDataConfig` textproto passed via `--config-file`
   (`eventGroupReferenceId`, `populationSpecResourcePath`, `dataSpecResourcePath`).
   `--create-done-blobs` writes the `done` marker in each date directory so the
   pipeline picks up the data.

### Cloud test steps

The test walks through: (1) event-group creation, (2) upload of the event group to
the bucket, (3) creating a measurement request, (4) triggering the RequisitionFetcher
to pull the new requisitions, (5) storing requisitions and their metadata, then creating a WorkItem
via the Secure Computation API, (6) the Secure
Computation API persists the WorkItem in Spanner and publishes to Pub/Sub, (7) the
ResultsFulfiller (a Pub/Sub subscriber) processes the WorkItem and fulfills the
requisitions against the Kingdom, and (8) evaluating the results. Confirm the run
produces the expected reach & frequency for the simulated publisher.

### Confidential Space debugging

The production SEV Confidential Space image type does **not** support container
logging. For troubleshooting an EDP's attestation/decryption, use a debug image:

1. Set the disk image family to `confidential-space-debug` (via
   `results_fulfiller_disk_image_family`) instead of `confidential-space`.
2. Add the MIG instance metadata `tee-container-log-redirect = "true"`.

The debug image also requires the EDP's Workload Identity Provider to **omit** the
`'STABLE' in assertion.submods.confidential_space.support_attributes` clause (a debug
image is not `STABLE`). Never use a debug image in production.

---

## Debugging notes

* **Config caching** — the ResultsFulfiller and functions generally read their config at process
  start. After changing a config file in `EDPA_CONFIG_BUCKET`, recreate the affected MIG VMs or
  redeploy the function so the new config is picked up. **Update CMMS** performs this automatically.
* **Secret path mismatches** — the single most common failure. Every mounted secret
  path must match, character for character, the path in the config file that
  references it.
* **Region mismatch** — an `endpoint_uri` in a different region than the DataWatcher
  yields HTTP 404 on invocation.

For tracing a specific report and the full failure-mode catalog (model-line mismatch,
missing impression blobs, KMS-type mismatch, MIG scaling, capacity exhaustion), see
the [Report Debugging Guide](report-debugging-guide.md).
