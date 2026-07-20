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
  DataAvailabilityCleanup, DataAvailabilityMonitor.
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
        writes requisitions/*
requisitions/*  ── finalize ──► DataWatcher ──► Secure Computation API ──► Pub/Sub
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
* `edpa-data-watcher-tls-key` / `edpa-data-watcher-tls-pem` — DataWatcher /
  DataWatcherDelete TLS keypair for the Secure Computation API. Signed by
  `securecomputation-root-ca`.
* `edpa-requisition-fetcher-tls-key` / `edpa-requisition-fetcher-tls-pem` —
  RequisitionFetcher TLS keypair for the Metadata Storage API. Signed by the
  Metadata Storage root CA.
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

The three watched-path types per EDP:

1. **Requisition detection** — a requisition file forwards to the Secure Computation
   API, which creates a WorkItem for the ResultsFulfiller.
2. **Event group detection** — an event-group blob invokes EventGroupSync.
3. **Impressions / data availability** — a `done` marker invokes DataAvailabilitySync.

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

A Cloud Function triggered by **Cloud Scheduler**. It retrieves requisitions from
the Kingdom public API and writes any new requisitions to `EDPA_STORAGE_BUCKET`. The
DataWatcher then detects those files and creates the corresponding WorkItems.

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
* **Spurious deletions** — `ImpressionMetadata` marked deleted while its blob still
  exists on the bucket (enabled when `spurious_deletion_lookback_days > 0`).

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

Its per-WorkItem parameters are carried in the DataWatcher `results-fulfiller`
watched path as a `ResultsFulfillerParams` message; its per-EDP TLS / consent /
KMS material is carried in the `event_data_provider_configs` file. See
[ResultsFulfiller parameters](#resultsfulfiller-parameters) and
[EDP config (event_data_provider_configs)](#edp-config-event_data_provider_configs).

### Secure Computation API

A gRPC service on GKE, reachable from the DataWatcher. When the DataWatcher enqueues
a requisition it creates a WorkItem; the API routes WorkItems to the configured
Pub/Sub queues. Enqueuing to a non-configured queue is an error.

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

The `storage-bucket` module accepts a `lifecycle_rules` list. Each entry supports:

| Field | Description | Default |
| --- | --- | --- |
| `name` | Identifier for the rule (documentation only) | — |
| `prefix` | Object prefix to match (e.g. `edp/<edp-id>/`) | — |
| `retention_days` | Days after **Custom-Time** (the impression date) before deletion | — |
| `enable_fallback` | Add an age-based safety-net delete rule | `true` |
| `fallback_retention_days` | Days after **upload** before the fallback deletes | `3650` (10y) |

```hcl
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

The module emits two rules per entry: a **Custom-Time** delete
(`days_since_custom_time = retention_days`) and, when `enable_fallback` is true, an
**age-based** delete (`age = fallback_retention_days`) as a safety net.

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
  vid_models_bucket_name          = "VID_MODELS_BUCKET"   # required by the module; only read when VID Labeling is enabled
  edp_aggregator_buckets_location = "REGION"
  # ... (continued below)
}
```

Versioning and lifecycle rules for `EDPA_STORAGE_BUCKET` are set inside the module as
described in [Object Lifecycle Management](#object-lifecycle-management).

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

All seven functions are configured through a single `cloud_function_configs` map.
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
}
```

**`extra_env_vars` — the meaningful variables per function** (all functions also take
the standard OpenTelemetry variables `OTEL_SERVICE_NAME`, `OTEL_METRICS_EXPORTER`,
`OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER`, `OTEL_METRIC_EXPORT_INTERVAL`):

| Function | Key variables |
| --- | --- |
| `data_watcher` / `data_watcher_delete` | `CERT_FILE_PATH`, `PRIVATE_KEY_FILE_PATH`, `CERT_COLLECTION_FILE_PATH`, `CONTROL_PLANE_TARGET`, `CONTROL_PLANE_CERT_HOST`, `EDPA_CONFIG_STORAGE_BUCKET`, `GOOGLE_PROJECT_ID`, `CONFIG_BLOB_KEY` |
| `requisition_fetcher` | `KINGDOM_TARGET`, `EDPA_CONFIG_STORAGE_BUCKET`, `GOOGLE_PROJECT_ID`, `GRPC_REQUEST_INTERVAL`, `METADATA_STORAGE_TARGET` |
| `event_group_sync` | `KINGDOM_TARGET` |
| `data_availability_sync` | `KINGDOM_TARGET`, `IMPRESSION_METADATA_TARGET` |
| `data_availability_cleanup` | `IMPRESSION_METADATA_TARGET` |
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
config files.

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

Two schedulers are configured with a `{ schedule, time_zone, name, function_url,
scheduler_sa_display_name, scheduler_sa_description, scheduler_job_description }`
object:

* `requisition_fetcher_scheduler_config` — triggers the RequisitionFetcher. Set the
  interval **greater than** the expected drain time (see
  [RequisitionFetcher](#requisitionfetcher)).
* `data_availability_monitor_scheduler_config` — triggers the DataAvailabilityMonitor.

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

### Optional: VID Labeling pipeline

The module can additionally deploy the memoized VID Labeling pipeline (Phase 0
SubpoolAssigner, Phase 1 VidRankBuilder, Phase 2 VidLabeler) as Confidential Space
TEE apps, plus a VidLabelingDispatcher and VidLabelingMonitor. **This is disabled by
default** — the `vid_labeling_workers` variable defaults to `{}`. VID labeling within
the aggregator is out of scope for the baseline (Phase 1) R&F deployment; leave
`vid_labeling_workers` empty unless your market has adopted it. When enabled it uses
`VID_MODELS_BUCKET`, the `vid_labeling_*` service-account / config / scheduler
variables, and adds `vid_labeling_dispatcher` / `vid_labeling_monitor` entries to
`cloud_function_configs`.

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

# 2) Requisitions -> Secure Computation API (control-plane queue)
watched_paths {
  identifier: "results-fulfiller"
  source_path_regex: "gs://EDPA_STORAGE_BUCKET/<edp-id>/requisitions/(.*)"
  control_plane_queue_sink {
    queue: "results-fulfiller-queue"
    app_params {
      [type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.ResultsFulfillerParams] {
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
}

# 3) Data availability -> DataAvailabilitySync (HTTP), fires on the `done` marker
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

Repeat the three watched paths per EDP. The DataWatcherDelete config
(`data_watcher_delete_config`) uses the same proto with a `data-availability-cleanup`
identifier whose `endpoint_uri` points at the DataAvailabilityCleanup function.

### RequisitionFetcher config (`RequisitionFetcherConfig`)

Proto: `wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto`.
One `configs` entry per EDP.

```textproto
# proto-file: wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto
# proto-message: wfa.measurement.config.edpaggregator.RequisitionFetcherConfig
configs {
  data_provider: "dataProviders/DATA_PROVIDER_ID"
  requisition_storage { gcs { project_id: "PROJECT_ID" bucket_name: "EDPA_STORAGE_BUCKET" } }
  storage_path_prefix: "<edp-id>/requisitions"
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
}
```

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

For an AWS-KMS EDP, set `kms_type: AWS` and the `aws_role_arn`,
`aws_role_session_name`, `aws_region`, and `aws_audience` fields instead of the
GCP-only fields — the full walkthrough is in the [AWS KMS Setup Guide](aws-kms-setup.md).

### ResultsFulfiller parameters

The DataWatcher `results-fulfiller` watched path carries a `ResultsFulfillerParams`
message (proto:
`wfa/measurement/edpaggregator/v1alpha/results_fulfiller_params.proto`). Beyond the
`data_provider`, `storage_params`, `consent_params`, and `cmms_connection` shown
above, it supports:

* `noise_params.noise_type` — `NONE` / `CONTINUOUS_GAUSSIAN` (direct single-EDP
  results).
* `k_anonymity_params` — `{ min_impressions, min_users, reach_max_frequency_per_user }`;
  a below-threshold result returns zero.
* `impression_max_frequency_per_user` — direct impression measurements only.
* `model_line_map` — optional external→internal model-line remapping for impression
  lookup.
* `trustee_params.kek_uri_to_key_name` — required for TrusTEE support; maps an input
  KEK URI to the re-encryption key name on the same key ring (see the EDP-side
  [TrusTEE section](edp-onboarding.md#enabling-trustee-optional)).
* `multi_party_config.supported_noise_types` — restricts accepted noise mechanisms
  for HMSS / TrusTEE requisitions.

---

## Deployment

### Prerequisites

* A GCP project with billing, and the GKE, Spanner, Cloud Functions, Cloud Run,
  Eventarc, Pub/Sub, Secret Manager, Confidential Computing, and Cloud Scheduler
  APIs enabled.
* An existing Spanner instance.
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

### Step 3 — Deploy the Secure Computation API on GKE

Build and push the container image, generate the Kubernetes Kustomization, customize
the secrets (TLS keypair signed by `securecomputation-root-ca`) and the ConfigMap,
then apply. This service must be reachable from the DataWatcher.

### Step 4 — Deploy the EDP Aggregator (Metadata Storage) API on GKE

Build and push the container image, generate the Kustomization, customize the secrets
(TLS keypair signed by the Metadata Storage root CA), then apply. The service uses
the Spanner database created in Step 2 and the internal service account bound via
Workload Identity.

---

## Validation (cloud test)

An end-to-end cloud test simulates a single-publisher R&F measurement and exercises
the full pipeline: event-group sync → requisition fetch → impression upload →
data-availability → ResultsFulfiller → result returned to the CMMS.

To run it you need a data provider registered in the Kingdom, the EDP's KMS key and
Workload Identity Provider configured (see the [EDP Onboarding Guide](edp-onboarding.md)),
and the config files uploaded to `EDPA_CONFIG_BUCKET`. Confirm the run produces the
expected reach & frequency for the simulated publisher.

---

## Debugging notes

* **Config caching** — the ResultsFulfiller and functions read their config at
  process start. After changing a config file in `EDPA_CONFIG_BUCKET`, recreate the
  affected MIG VMs / redeploy the function so the new config is picked up.
* **Secret path mismatches** — the single most common failure. Every mounted secret
  path must match, character for character, the path in the config file that
  references it.
* **Region mismatch** — an `endpoint_uri` in a different region than the DataWatcher
  yields HTTP 404 on invocation.

For tracing a specific report and the full failure-mode catalog (model-line mismatch,
missing impression blobs, KMS-type mismatch, MIG scaling, capacity exhaustion), see
the [Report Debugging Guide](report-debugging-guide.md).
