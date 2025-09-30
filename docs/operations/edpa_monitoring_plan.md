# EDPA Aggregator Monitoring Plan

## 1. TL;DR
- **Objective:** Provide production-grade observability for the Halo Data Provider Module (EDP Aggregator) to detect and remediate issues within MTTR targets.
- **Scope:** Six EDPA components requiring instrumentation: Data Watcher, Data Availability Sync, Event Group Sync, Results Fulfiller, Requisition Fetcher, and Dead Letter Queue Listener.
- **Approach:** OpenTelemetry instrumentation for Cloud Functions and Confidential Space MIGs, exporting to Google Cloud Monitoring via managed OTLP collectors.

## 2. System Overview

### Runtime Topology

- Cloud Storage buckets per EDP hold impression and requisition artifacts.
- Cloud Functions trigger: Data Watcher
- HTTP Cloud Functions: Event Group Sync, Data Availability Sync, Requisition Fetcher
- Results Fulfiller runs in a Confidential Space managed instance group (MIG) per deployment, leveraging TEE attestation.
- Secure Computation Public API operates as a Kubernetes deployment (GKE), fronting the control plane. When deployed to Kubernetes, the public and internal API servers run as separate deployments (`secure-computation-public-api-server` and `secure-computation-internal-api-server`).
- Dead Letter Queue Listener runs embedded in the `secure-computation-internal-api-server` (Kubernetes), monitoring the Pub/Sub DLQ subscription for failed work items.
- Impression and Requisition Metadata Storage operate as Kubernetes deployment.

### Observability Platform

- Hybrid OpenTelemetry architecture:
  - Cloud Functions and Confidential Space MIGs emit telemetry via OTel SDKs to a centrally managed OTel Collector running on Cloud Run (OTLP/gRPC and OTLP/HTTP). Function runtime logs continue to stream directly to Cloud Logging.
  - Kubernetes deployments (GKE) use auto-instrumentation via OpenTelemetry Operator. The operator injects language-specific agents into annotated pods, which export telemetry via OTLP to a centrally managed OTel Collector deployment (`default-collector`) running in the cluster. The collector applies resource detection (GKE metadata), batching, memory limiting, and label transformations, then exports metrics to Cloud Monitoring and traces to Cloud Trace using the `googlecloud` exporter with Workload Identity for authentication.
- Export targets: Google Cloud Monitoring (metrics). Dashboards, SLOs, and alerts are defined in Cloud Monitoring.

### Current Instrumentation Status (Kubernetes)

The Halo CMMS platform **already has OpenTelemetry instrumentation in place** for Kubernetes-based components:

#### Infrastructure

- **OTel Collector** (`default`) running as deployment with:
  - OTLP/gRPC receiver on port 4317
  - Google Cloud exporter for metrics and traces (`googlecloud` exporter)
  - Resource detection, batch processing, memory limiting
  - Label transformation to prefix GCP-reserved names (`exported_location`, `exported_cluster`, etc.)
  - _Config:_ [src/main/k8s/open_telemetry.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/open_telemetry.cue) (collector), [src/main/k8s/dev/open_telemetry_gke.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/open_telemetry_gke.cue) (GKE-specific)
- **Workload Identity** service account `open-telemetry` with `roles/monitoring.metricWriter` and `roles/cloudtrace.agent`
  - _Terraform:_ [src/main/terraform/gcloud/cmms/main.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/main.tf#L91-L109)

#### Telemetry schema (consistent across environments)

**Core Metrics (all components):**

- **Kubernetes gRPC services** (auto-instrumented via OTel Java agent):
  - `rpc.server.duration` (histogram) - attributes: `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `service.name`, `deployment.environment`, `edp_id?`
  - `rpc.client.duration` (histogram) - for outbound calls with same attributes
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` (JVM runtime metrics)

- **Cloud Functions** (manual OTel SDK instrumentation required):
  - `function.execution.latency` (histogram) - native Cloud Functions metric (auto-emitted)
  - `rpc.client.duration` (histogram) - for outbound gRPC calls (requires manual OTel gRPC interceptor)
    - Attributes: `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `service.name`, `edp_id?`
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` (auto-emitted by OTel Java SDK)
  - **No `rpc.server.duration`**: Cloud Functions use HTTP/CloudEvents triggers, not gRPC servers

- **Confidential MIG (Results Fulfiller)** (manual OTel SDK instrumentation):
  - `rpc.client.duration` (histogram) - for Kingdom gRPC calls (requires manual interceptor)
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` (auto-emitted by OTel Java SDK)
  - Exports to centralized Cloud Run OTel Collector (same as Cloud Functions)

**Required common attributes on all telemetry:**

- `service.name` - Set via `OTEL_SERVICE_NAME` env var (Cloud Functions/MIG) or auto-detected (K8s)
- `service.version` - Inject from deployment metadata or git commit SHA
- `deployment.environment` (`dev|qa|prod`) - Set via `OTEL_RESOURCE_ATTRIBUTES=deployment.environment=<env>`
- `edp_id` (when applicable) - Extract from request context or config; omit if unavailable
- `runtime` - Set to `k8s`, `cloud_function`, or `confidential_space`

**Configuration requirements:**

- Cloud Functions & MIG: Set env vars `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT` (Cloud Run collector URL), `OTEL_RESOURCE_ATTRIBUTES`
- All components: Initialize OTel SDK outside request handlers (Cloud Functions: global scope; MIG: application startup)

#### Deployment Coverage

All Kubernetes Server deployments (`#ServerDeployment` in [src/main/k8s/base.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/base.cue#L609-L621)) inherit OpenTelemetry auto-instrumentation, including:

- `secure-computation-public-api-server` ([src/main/k8s/secure_computation.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/secure_computation.cue#L103))
- `secure-computation-internal-api-server` ([src/main/k8s/secure_computation.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/secure_computation.cue#L77)) — includes Dead Letter Queue Listener
- `impression-metadata-storage`
- `requisition-metadata-storage`
- Kingdom API servers ([src/main/k8s/kingdom.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/kingdom.cue))
- Duchy workers ([src/main/k8s/duchy.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/duchy.cue))
- Reporting servers ([src/main/k8s/reporting_v2.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/reporting_v2.cue))

#### Gaps for EDPA Components

The following EDPA-specific components are **not yet instrumented** because they run outside Kubernetes:

1. **Cloud Functions** (Data Watcher, Event Group Sync, Data Availability Sync, Requisition Fetcher) – require explicit OTel SDK integration
2. **Results Fulfiller MIG** (Confidential Space VMs) – require OTel SDK integration with OTLP export to Cloud Run collector
3. **Custom metrics** for EDPA workflows (impression freshness, requisition backlog, etc.) – need instrumentation in application code

## 3. Monitoring Goals & SLOs
The targets below are actionable starting points (SLI, Target, Window). Adjust to contractual and operational realities.

### Summary of 5 SLOs

1. **API Availability** (99.9%, 28d) - Secure Computation APIs respond successfully <500ms
2. **Data Availability Freshness** (99.9%, 28d) - Impression uploads → Kingdom update <1 hour
3. **Requisition Fulfillment Timeliness** (99.9%, 28d) - Requisition created → fulfilled <1 hour
4. **Fulfillment Correctness** (99.99%, 28d) - Queue writes → successful processing correlation
5. **Event Group Sync Timeliness** (99.9%, 28d) - Data Watcher → Event Group created <20 minutes

### 3.1 SLO 1: API Availability (Public + Metadata APIs)
- Scope: `secure-computation-public-api-server`, `impression-metadata-storage`, `requisition-metadata-storage`.
- SLI: Proportion of valid gRPC requests to these services that complete successfully (gRPC status OK) in < 500ms.
- Target: 99.9%
- Window: Rolling 28 days
- Implementation:
  - MQL (conceptual): success_under_threshold / total, filtered by `service.name` in the API set above
    - success_under_threshold = count of `workload.googleapis.com/rpc.server.duration` with `rpc.grpc.status_code=0` and `latency < 500ms`
    - total = count of all requests (exclude health checks `rpc.method=Check`)
  - Dashboard: p50/p95/p99 latency per service; error budget burn alerts per service and aggregated.
- Failure Modes:
  - GKE pod crashes/OOMKill → deployment not available → gRPC UNAVAILABLE
  - Database (Spanner) latency spike → slow queries → timeout or high latency
  - Certificate expiration/rotation failure → TLS handshake errors → connection refused
  - Resource exhaustion (CPU/memory) → request queuing → latency > 500ms or timeout
  - Network partition between GKE and dependencies → downstream RPC failures → INTERNAL/DEADLINE_EXCEEDED

### 3.2 SLO 2: Data Availability Freshness
- Scope: Data Availability Sync workflow (impressions → metadata → Kingdom availability intervals).
- SLI: Proportion of impression uploads (signaled by `*.done` blob) that result in Kingdom availability intervals being updated within 1 hour.
- Target: 99.9%
- Window: Rolling 28 days
- Implementation:
  - Emit `edpa.data_availability.sync_duration` (histogram) in Data Availability Sync with value = `kingdom_update_complete_time - done_blob_timestamp`.
  - Data Watcher records `done_blob_timestamp` when triggering the sync function.
  - Data Availability Sync records the histogram upon successful Kingdom update.
  - SLI: syncs with `sync_duration < 3600s` divided by total syncs.
- Failure Modes:
  - Data Watcher misses `*.done` blob (event delivery delay) → sync never triggered
  - Data Availability Sync Cloud Function cold start → high latency on first invocation
  - GCS bucket read permissions denied → metadata files unreadable → sync fails
  - Impression Metadata Storage API unavailable → metadata persistence fails → partial sync
  - Kingdom API unavailable → availability interval update fails → workflow incomplete
  - Malformed metadata files (invalid protobuf/JSON) → parsing errors → sync aborts
  - Network partition between Cloud Function and GKE → RPC timeout → retry storm

### 3.3 SLO 3: Requisition Fulfillment Timeliness
- Scope: Requisition fulfillment workflow (requisition created → Results Fulfiller completes).
- SLI: Proportion of requisitions fulfilled within 1 hour of being created in the Kingdom.
- Target: 99.9%
- Window: Rolling 28 days
- Implementation:
  - Emit `edpa.results_fulfiller.fulfillment_latency` (histogram) with value = `fulfillment_complete_time - requisition_create_time`.
  - Results Fulfiller records histogram upon successful fulfillment.
  - SLI: fulfillments with `fulfillment_latency < 3600s` divided by total fulfilled.
  - **Correlation**: Use `report_id` to group multiple requisitions for the same report, enabling report-level latency dashboards and p99 analysis per report.
- Failure Modes:
  - Requisition Fetcher scheduler delays → requisitions not fetched on time → processing delayed
  - Requisition Fetcher Cloud Function timeout → incomplete fetch → retry backoff increases latency
  - GCS write failures → requisitions not persisted → Results Fulfiller can't read them
  - Results Fulfiller MIG scaling lag → queue backlog grows → high processing latency
  - Confidential VM attestation failure → VM can't start → no fulfillment capacity
  - Impression data not yet available → Results Fulfiller waits → blocks queue
  - KMS decryption rate limit → crypto operations throttled → fulfillment stalls
  - Duchy API unavailable → can't submit results → fulfillment incomplete
  - Memory exhaustion in frequency vector computation → OOM → VM restart → lost work

### 3.4 SLO 4: Fulfillment Correctness
- SLI: Proportion of `edpa.data_watcher.queue_writes` that result in a matching `edpa.results_fulfiller.requisitions_processed{status="success"}` within the window.
- Target: 99.99%
- Window: Rolling 28 days
- Implementation:
  - Include `work_item_id` and `report_id` as labels on both events.
  - Compute SLI in Monitoring via logs/metrics join (or BigQuery export of metrics) matching on `work_item_id`.
  - Use `report_id` for aggregated report-level success rate dashboards.
- Failure Modes:
  - Pub/Sub message loss (rare, but possible) → work item never dequeued → silent data loss
  - Event Group Sync failure → requisition can't be created → workflow aborts before Results Fulfiller
  - Requisition state mismatch (already fulfilled elsewhere) → Results Fulfiller skips it
  - Unhandled exception in Results Fulfiller → crash → work item returns to queue but exceeds retry limit → routed to dead letter queue (DLQ)
  - Dead Letter Queue Listener failure → DLQ items not reprocessed → manual intervention required
  - Invalid requisition spec (malformed crypto) → decryption fails → requisition refused → counted as failure
  - Data provider certificate revoked → signature verification fails → requisition rejected
  - Work item timeout (processing > max deadline) → abandoned before completion
  - Duplicate work item IDs → overcounting queue_writes or undercounting processed → SLI calculation error

### 3.5 SLO 5: Event Group Sync Timeliness
- **Scope**: Event Group Sync workflow (Data Watcher queue write → Event Group created in Kingdom).
- **SLI**: Proportion of Event Group Sync operations that complete successfully within 20 minutes.
- **Target**: 99.9%
- **Window**: Rolling 28 days
- **Implementation**:
  - Emit `edpa.event_group.sync_latency` (histogram) with value = `sync_complete_time - work_item_create_time`.
  - Include labels: `edp_id`, `work_item_id`
  - SLI: syncs with `sync_latency < 1200s` (20 min) divided by `edpa.event_group.sync_attempts`
  - Success criteria: `edpa.event_group.sync_success / edpa.event_group.sync_attempts >= 0.999`
  - Dashboard: p50/p95/p99 sync latency; success rate per EDP; error budget burn rate
- **Failure Modes**:
  - Cloud Function cold start → first invocation takes >1 min → increases p99 latency
  - Kingdom API unavailable → Event Group creation fails → retry backoff increases latency
  - Invalid event group metadata (malformed proto) → validation error → sync aborted
  - Cloud Function timeout (default 9 min) → incomplete sync → automatic retry
  - Network partition between Cloud Function and GKE → RPC DEADLINE_EXCEEDED
  - Rate limiting on Kingdom API → HTTP 429 → throttling delays processing
  - Work item never dequeued from Pub/Sub → upstream Data Watcher issue → workflow stalled
  - Duplicate event group creation attempts → Kingdom returns ALREADY_EXISTS → counted as failure
  - Certificate validation failure → mTLS handshake error → connection refused
  - Memory exhaustion in Cloud Function → OOM crash → cold restart required

### 3.6 Internal Health SLIs (Non-SLO)
- Requisition backlog drain time < 1 hour (derived from queue depth and processing rate).
- CMMS RPC error rate for Data Availability Sync < 2/min sustained 10 min.
- Dead Letter Queue depth < 100 messages sustained 15 min (indicates systemic failures).
- Dead Letter Queue reprocess success rate > 95% (items should succeed on retry after transient failures resolved).

## 4. Core Signals & Instrumentation
### 4.1 Shared Practices
- **Resource attributes:** Tag every metric with `service.name`, `deployment.environment` (`dev`, `qa`, `prod`), `edp_id`, and `runtime` (`cloud_function`, `confidential_space`, `standalone`).
- **Correlation ID (`report_id`):** Include `report_id` as a label on all EDPA metrics to enable end-to-end lifecycle tracking.
  - Format: `measurementConsumers/{measurement_consumer}/reports/{report_id}`
  - Extract from: `requisitionMetadata.report` or `measurementSpec.reportingMetadata.report`
  - Available throughout the lifecycle: Data Watcher → Event Group Sync → Requisition Fetcher → Results Fulfiller
  - Use for: Calculating fulfillment latency, correlating events across components, dashboarding by report
  - Benefits: Lower cardinality than `external_computation_id` (1 report → N measurements), aligns with user mental model
- **Collector configuration:**
  - Cloud Functions use the OpenTelemetry Exporter for Cloud Functions, auto-exporting to Cloud Monitoring (set `OTEL_EXPORTER_OTLP_ENDPOINT` to regional collector, configure GCP service account with Monitoring Metric Writer).
  - Results Fulfiller MIGs run the Ops Agent + OTel Collector sidecar sending metrics over OTLP/gRPC with TLS.
  - Confidential Space nodes require attested secure channel (use SPIFFE/SVID or mutual TLS keyed by confidential VM identity).

### 4.2 Extracting and Using `report_id` for Correlation

#### Extraction by Component

1. **Data Watcher (Cloud Function)**
   - Extract from: `RequisitionMetadata.report` when querying `ImpressionMetadataStorageService`
   - Format: `measurementConsumers/{mc}/reports/{report_id}`
   - Include as label on: `edpa.data_watcher.queue_writes`, `edpa.data_watcher.match_latency`

2. **Event Group Sync (Cloud Function)**
   - Extract from: Work item payload or query `RequisitionMetadataService` by `external_requisition_id`
   - Include as label on: `edpa.event_group.sync_latency`, `edpa.event_group.sync_attempts`

3. **Requisition Fetcher (Cloud Function)**
   - Extract from: `Requisition.measurementSpec.reportingMetadata.report` (unpacked from `requisition.measurementSpec.message`)
   - Already used for grouping in `RequisitionGrouperByReportId.kt` (line 62)
   - Include as label on: `edpa.requisition_fetcher.fetch_latency`, `edpa.requisition_fetcher.requisitions_fetched`

4. **Results Fulfiller (Confidential MIG)**
   - Extract from: `GroupedRequisitions.requisitions[0].requisition.measurementSpec.reportingMetadata.report`
   - Code example (Kotlin):
     ```kotlin
     val requisition: Requisition = groupedRequisitions.requisitionsList.first().requisition.unpack()
     val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()
     val reportId: String = measurementSpec.reportingMetadata.report
     // Use reportId as histogram label
     fulfillmentLatencyHistogram.record(latencySeconds, Attributes.of("report_id", reportId))
     ```
   - Include as label on: `edpa.results_fulfiller.fulfillment_latency`, `edpa.results_fulfiller.requisitions_processed`


#### Dashboard Query Example (Cloud Monitoring MQL)

```
fetch workload.googleapis.com/edpa.results_fulfiller.fulfillment_latency
| filter resource.service_name == 'results-fulfiller'
| group_by [metric.report_id], [percentile: percentile(value, 99)]
| filter percentile > 3600  # Alert on reports taking > 1 hour
```

#### Benefits of `report_id` for Correlation
- **End-to-end tracing**: Follow a single report through Data Watcher → Event Group Sync → Requisition Fetcher → Results Fulfiller
- **Report-level SLOs**: Calculate p99 latency per report, identify slow reports
- **Lower cardinality**: 1 report typically maps to multiple measurements/requisitions, reducing metric explosion
- **User alignment**: "Report ABC123 took 45 minutes" is more meaningful than "Measurement 987654 took..."
- **Cross-component join**: Correlate `edpa.data_watcher.queue_writes{report_id}` with `edpa.results_fulfiller.requisitions_processed{report_id}` for SLO 4 correctness

### 4.3 Required Attributes for Observability

All EDPA metrics must include a consistent set of attributes to enable correlation, filtering, and debugging.

#### Attribute Availability by Component

| Component | `report_id` | `work_item_id` |
|-----------|----------|-------------|----------------|
| Data Watcher | Yes | Yes |
| Event Group Sync | No | Yes |
| Requisition Fetcher | Yes | No |
| Results Fulfiller | Yes | Yes |
| Data Availability Sync | No | No |
| Dead Letter Queue Listener | Partial | Yes |

**Legend:** Yes = Available | Partial = Needs implementation | No = Not applicable

## 5. Strategy & Architecture
### 5.1 Telemetry Strategy (Hybrid: Cloud Functions + Confidential MIGs)
- Cloud Functions and MIG:
  - Use OpenTelemetry SDKs for metrics, initialized outside the handler to avoid cold‑start gaps.
  - Export OTLP to a central OTel Collector on Cloud Run over TLS using OTLP/gRPC (4317) or OTLP/HTTP (4318).

### 5.2 Collector Topology
- Central Collector (Cloud Run):
  - Receives OTLP from Cloud Functions and Confidential Space MIGs at `:4317` (gRPC) and `:4318` (HTTP).
  - Private ingress (VPC only) with minimum instances to stay warm; scales horizontally with request load.
  - Exports metrics to Google Cloud backends using the `googlecloud` exporter.
- Export Paths:
  - Metrics → Cloud Monitoring (workload.googleapis.com)

### 5.3 Collector Configuration Examples
#### Central Collector (Cloud Run)
```yaml
# Accept OTLP from Cloud Functions and Confidential MIGs
# gRPC is preferred for performance, HTTP improves compatibility and resilience during cold starts
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

# Protect serverless memory and smooth export bursts
# No resourcedetection here; CF resource context is provided by producers/exporter
processors:
  memory_limiter:
    check_interval: 5s
    limit_percentage: 75
    spike_limit_percentage: 20
  batch:
    send_batch_size: 2048
    timeout: 5s

# Native export to Cloud Monitoring
# Use workload.googleapis.com prefix for metrics namespace
exporters:
  googlecloud:
    project: ${GCP_PROJECT}
    user_agent: edpa-otel-collector
    metric:
      prefix: workload.googleapis.com

# Health endpoints and lightweight profiling/diagnostics
extensions:
  health_check: {}
  pprof: {}
  zpages: {}

# Pipeline for Cloud Functions and MIG metrics
service:
  extensions: [health_check, pprof, zpages]
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [googlecloud]
```

### 5.4 Operational Best Practices
- Batching & timeouts (SDK env vars for Cloud Functions):
  - `OTEL_BSP_MAX_QUEUE_SIZE=2048`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512`, `OTEL_BSP_SCHEDULE_DELAY=200ms`, `OTEL_BSP_EXPORT_TIMEOUT=3s`.
- Cold‑start mitigation (Cloud Functions):
  - Initialize OTel SDK outside the handler, set OTLP endpoints via env vars, and keep the central collector warm (Cloud Run min instances).
- Scaling:
  - Cloud Run collector: set min instances and autoscaling based on concurrent requests/CPU.

## 6. Measurements Catalog

### Label Convention

All EDPA custom metrics (`edpa.*`) should include `report_id` label when the requisition context is available (Data Watcher onwards).

### 6.1 Implemented Metrics (Auto-instrumented)

| Component | Type | Metric Name |
| --- | --- | --- |
| All Kubernetes Services | histogram | rpc.server.duration |
| All Kubernetes Services | histogram | rpc.client.duration |
| All JVM Services | gauge | jvm.memory.used |
| All JVM Services | histogram | jvm.gc.duration |
| All JVM Services | gauge | jvm.threads.count |
| Cloud Functions | histogram | function.execution.latency |
| Cloud Functions | gauge | function.memory.usage |
| Cloud Functions | counter | function.execution.count |
| Cloud Functions | counter | function.execution.failures |
| Confidential VMs | gauge | vm.cpu.utilization |
| Confidential VMs | gauge | vm.memory.usage |
| Confidential VMs | counter | vm.attestation.failures |
| Pub/Sub Queues | gauge | pubsub.subscription.num_undelivered_messages |
| Cloud Scheduler | counter | scheduler.job_execution_count |
| Cloud Scheduler | counter | scheduler.job_attempt_delayed |

### 6.2 Planned EDPA Metrics (Custom Instrumentation Required)

| Component | Type | Metric Name |
| --- | --- | --- |
| Secure Computation API | counter | secure_computation.work_items.created |
| Secure Computation API | gauge | secure_computation.active_work_items |
| Secure Computation API | counter | tls.handshake.errors |
| Data Watcher | histogram | edpa.data_watcher.match_latency |
| Data Watcher | counter | edpa.data_watcher.queue_writes |
| Data Watcher | gauge | edpa.data_watcher.done_blob_timestamp |
| Data Availability Sync | histogram | edpa.data_availability.sync_duration |
| Data Availability Sync | counter | edpa.data_availability.records_synced |
| Data Availability Sync | counter | edpa.data_availability.cmms_rpc_errors |
| Event Group Sync | counter | edpa.event_group.sync_attempts |
| Event Group Sync | counter | edpa.event_group.sync_success |
| Event Group Sync | histogram | edpa.event_group.sync_latency |
| Results Fulfiller | histogram | edpa.results_fulfiller.fulfillment_latency |
| Results Fulfiller | counter | edpa.results_fulfiller.requisitions_processed |
| Results Fulfiller | histogram | edpa.results_fulfiller.vid_index_build_duration |
| Results Fulfiller | histogram | edpa.results_fulfiller.network_tasks_duration |
| Results Fulfiller | histogram | edpa.results_fulfiller.frequency_vector_duration |
| Results Fulfiller | histogram | edpa.results_fulfiller.kingdom_grpc_duration |
| Results Fulfiller | gauge | edpa.results_fulfiller.event_processing_rate |
| Results Fulfiller | histogram | edpa.results_fulfiller.builder_creation_duration |
| Results Fulfiller | histogram | edpa.results_fulfiller.send_duration |
| Requisition Fetcher | histogram | edpa.requisition_fetcher.fetch_latency |
| Requisition Fetcher | counter | edpa.requisition_fetcher.requisitions_fetched |
| Requisition Fetcher | counter | edpa.requisition_fetcher.storage_writes |
| Dead Letter Queue Listener | counter | edpa.dlq.messages_received |
| Dead Letter Queue Listener | counter | edpa.dlq.messages_reprocessed |
| Dead Letter Queue Listener | counter | edpa.dlq.reprocess_failures |

**Total**: 16 implemented (native platform metrics), 27 planned (custom EDPA metrics)

## 7. Component-Specific Monitoring

### 7.1 Secure Computation Public API (`SecureComputationApiServer`)

#### Implementation

[src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt)

#### Deployment

[src/main/k8s/secure_computation.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/secure_computation.cue#L103-L114), [src/main/k8s/dev/secure_computation_gke.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/secure_computation_gke.cue)

#### Terraform

[src/main/terraform/gcloud/modules/secure-computation/main.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/secure-computation/main.tf)

#### Current Status

**Fully instrumented** via OpenTelemetry Java agent auto-injection on Kubernetes.

#### Existing Metrics (Auto-Generated by OTel Java Agent)
- `rpc.server.duration` (histogram) by `rpc.method`, `rpc.grpc.status_code` – measures gRPC call latency
- `rpc.server.request.size` / `rpc.server.response.size` (histograms) – message size distribution
- `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` – JVM runtime metrics
- Kubernetes pod metrics via `resourcedetection` processor: `k8s.pod.name`, `k8s.namespace.name`, `k8s.deployment.name`

#### Deployment Details
- Runs as `secure-computation-public-api-server-deployment` in Kubernetes
- Container: `secure-computation-public-api-server-container` with Java agent injected via `instrumentation.opentelemetry.io/inject-java: "true"`
- Exports metrics to `default-collector-headless.default.svc:4317` (OTel Collector)
- Collector forwards to Google Cloud Monitoring

#### Recommended Additions
- **Custom metrics** (instrument in [PublicApiServer.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt) using OpenTelemetry SDK):
  - `secure_computation.work_items.created` (counter) by `queue` for monitoring Data Watcher submissions
  - `secure_computation.active_work_items` (gauge) for queue backlog visibility
  - `tls.handshake.errors` (counter) for mutual TLS failures

#### Dashboards & Alerts
- **Dashboard:** Create Monitoring dashboard querying:
  - `workload.googleapis.com/rpc.server.duration` filtered by `k8s_deployment_name="secure-computation-public-api-server-deployment"`
  - Latency heatmap (p50, p95, p99) by `rpc.method`
  - Error budget burn rate using SLO on `rpc.grpc.status_code != 0`

### 7.2 Data Watcher Cloud Function (`DataWatcherFunction`)

#### Implementation

[src/main/kotlin/org/wfanet/measurement/securecomputation/datawatcher/DataWatcher.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/datawatcher/DataWatcher.kt), [src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/datawatcher/DataWatcherFunction.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/datawatcher/DataWatcherFunction.kt)

#### Config

[src/main/proto/wfa/measurement/config/securecomputation/data_watcher_config.proto](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/securecomputation/data_watcher_config.proto)

#### Terraform

[src/main/terraform/gcloud/cmms/edp_aggregator.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L158-L165), [src/main/terraform/gcloud/modules/edp-aggregator/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator)

#### Metrics
- `function.execution.latency` (distribution) from Cloud Functions + custom `edpa.data_watcher.match_latency` for regex match to queue submission.
- `edpa.data_watcher.queue_writes` counter for control-plane queue submissions.
- `function.memory.usage` gauge; alert on >80% of allocation.
- `function.execution.count` vs. `function.execution.failures` to derive error rate.

### 7.3 Data Availability Sync Cloud Function (`DataAvailabilitySyncFunction`)

#### Config

[src/main/proto/wfa/measurement/config/edpaggregator/data_availability_sync_config.proto](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/edpaggregator/data_availability_sync_config.proto)

#### Terraform

[src/main/terraform/gcloud/cmms/edp_aggregator.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L180-L186)

#### Metrics
- `edpa.data_availability.sync_duration` histogram covering fetch + write cycle.
- `edpa.data_availability.records_synced` counter per EDP.
- `edpa.data_availability.cmms_rpc_errors` counter from CMMS API calls.

### 7.4 Event Group Sync Cloud Function (`EventGroupSyncFunction`)

#### Implementation

[src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/eventgroups/EventGroupSyncFunction.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/eventgroups/EventGroupSyncFunction.kt)

#### Config

[src/main/proto/wfa/measurement/config/edpaggregator/event_group_sync_config.proto](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/edpaggregator/event_group_sync_config.proto)

#### Terraform

[src/main/terraform/gcloud/cmms/edp_aggregator.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L173-L179)

#### Metrics
- `edpa.event_group.sync_attempts` counter.
- `edpa.event_group.sync_success` counter.
- `edpa.event_group.sync_latency` histogram.

### 7.5 Results Fulfiller (Confidential Space MIG)

#### Implementation

[ResultsFulfiller.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt), [ResultsFulfillerApp.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfillerApp.kt), [ResultsFulfillerAppRunner.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfillerAppRunner.kt)

#### Storage

[StorageEventSource.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/StorageEventSource.kt)

#### Config

[src/main/proto/wfa/measurement/edpaggregator/v1alpha/results_fulfiller_params.proto](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/edpaggregator/v1alpha/results_fulfiller_params.proto)

#### Terraform

[src/main/terraform/gcloud/cmms/edp_aggregator.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L85-L121) (MIG config), [src/main/terraform/gcloud/modules/edp-aggregator/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator) (MIG module)

#### Metrics
- **End-to-end latency:**
  - `edpa.results_fulfiller.fulfillment_latency{report_id}` (histogram) - Total time from requisition creation to fulfillment complete.
  - `edpa.results_fulfiller.requisitions_processed{report_id,work_item_id,status}` (counter) - Requisitions processed by status.
- **Pipeline stage timings (instrument in [ResultsFulfiller.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt) and [EventProcessingOrchestrator.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/EventProcessingOrchestrator.kt)):**
  - `edpa.results_fulfiller.vid_index_build_duration{report_id}` (histogram) - Time to build VID index map (location: before pipeline execution).
  - `edpa.results_fulfiller.network_tasks_duration{report_id}` (histogram) - Time to complete all network I/O (storage reads, impression metadata queries).
  - `edpa.results_fulfiller.frequency_vector_duration{report_id}` (histogram) - Time to compute frequency vectors across all requisitions (existing `frequencyVectorTime` at [line 142](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt#L142)).
  - `edpa.results_fulfiller.builder_creation_duration{protocol,report_id}` (histogram) - Time to create protocol-specific fulfiller (existing `buildTime` at [line 206](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt#L206)).
  - `edpa.results_fulfiller.send_duration{protocol,report_id}` (histogram) - Time to send fulfillment result via gRPC (existing `sendTime` at [line 210](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt#L210)).
  - `edpa.results_fulfiller.kingdom_grpc_duration{report_id,method}` (histogram) - Per-RPC latency to Kingdom for each fulfillment call.
- **Throughput monitoring:**
  - `edpa.results_fulfiller.event_processing_rate{report_id}` (gauge) - Current events/sec processing rate measured over a rolling 10-second window; emit every 2 seconds during active processing.
  - Implementation: Track event count with `AtomicLong`, compute rate as `(current_count - previous_count) / window_duration`, emit as gauge metric.
- **Infrastructure metrics:**
  - `vm.cpu.utilization`, `vm.memory.usage`, `vm.attestation.failures` from Confidential Space telemetry.
  - `pubsub.subscription.num_undelivered_messages` for results queue.

### 7.6 Requisition Fetcher Cloud Function (`RequisitionFetcherFunction`)

#### Implementation

[RequisitionFetcher.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/requisitionfetcher/RequisitionFetcher.kt), [RequisitionFetcherFunction.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/requisitionfetcher/RequisitionFetcherFunction.kt)

#### Config

[src/main/proto/wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto)

#### Terraform

[src/main/terraform/gcloud/cmms/edp_aggregator.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L166-L172) (function), [edp_aggregator.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L123-L131) (scheduler)

#### Metrics

- `edpa.requisition_fetcher.fetch_latency` histogram.
- `edpa.requisition_fetcher.requisitions_fetched` counter per EDP.
- `edpa.requisition_fetcher.storage_writes` counter (GCS writes).
- Cloud Scheduler `job_execution_count` and `job_attempt_delayed` metrics.

## 8. Dashboards
- **EDPA Summary Dashboard:**
  - SLO burn-down chart for Secure Computation API.
  - Ingestion freshness per EDP (Data Watcher → Event Group Sync timelines).
  - Queue backlog status (Pub/Sub + Scheduler).
  - Error budget table for each component.

- **Component Deep Dives:** Pre-built JSON dashboards stored alongside Terraform ([src/main/terraform/gcloud/modules/edp-aggregator/dashboards/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator/dashboards) - to be created). Terraform will deploy using `google_monitoring_dashboard` with templates referencing metrics above.
- **Reference examples:** See existing Kingdom/Duchy dashboards at [src/main/terraform/gcloud/modules/kingdom/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/kingdom) (kingdom_dashboard_*.json) and [src/main/terraform/gcloud/modules/duchy/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/duchy) (duchy_dashboard_*.json)

## 9. Testing & Validation
- **Unit tests:** Add OTel instrumentation unit tests verifying metric names registered (see pattern in existing tests).
- **Integration tests:** Extend `EdpAggregatorCorrectnessTest` ([src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt)) to assert that mock collectors receive spans for Event Group Sync path.

## 10. Implementation Action Plan

### Action Items

| # | Phase | P | Effort | Deps |
|---|-------|---|--------|------|
| 1a | Deploy Cloud Run OTel Collector | P0 | Mid | - |
| 1b | Add OTel SDK to Cloud Functions | P0 | High | #1a |
| 2a | Add OTel SDK to Results Fulfiller MIG | P0 | High | #1a |
| 2b | Configure MIG OTLP Export to Cloud Run | P0 | Low | #2a |
| 3a | Instrument Data Watcher Metrics | P0 | Mid | #1b |
| 3b | Instrument Results Fulfiller Metrics | P0 | High | #2a |
| 3c | Instrument Requisition Fetcher Metrics | P0 | Mid | #1b |
| 3d | Instrument Event Group Sync Metrics | P1 | Mid | #1b |
| 3e | Instrument Data Availability Sync Metrics | P1 | Mid | #1b |
| 3f | Instrument Secure Computation API Metrics | P1 | Mid | - |
| 4 | Resolve Metadata Storage Status | P1 | Mid | - |
| 5 | Implement DLQ Listener | P1 | High | #3b |
| 6a | Create Dashboard JSON Definitions | P0 | Mid | #3a-3f |
| 6b | Deploy Dashboards via Terraform | P0 | Low | #6a |
| 6c | Define SLOs in Terraform | P0 | Mid | #3a-3f |
| 7a | Write Unit Tests for Metrics | P0 | Mid | #3a-3f |
| 7b | Extend Integration Tests | P0 | Mid | #3a-3f |
| 7c | Run Load Testing | P1 | Low | #6b, #7b |

### Key Files

- **Terraform**: `src/main/terraform/gcloud/modules/edp-aggregator/` - Add `otel_collector_cloud_run.tf`, `dashboards.tf`, `alerts.tf`, `slos.tf`
- **Cloud Functions**: Add OTel SDK to `*Function.kt` files in [src/main/kotlin/org/wfanet/measurement/*/deploy/gcloud/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/kotlin/org/wfanet/measurement)
- **Results Fulfiller**: Instrument [ResultsFulfiller.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt) and configure OTLP export to Cloud Run collector
- **MIG Terraform**: Update `src/main/terraform/gcloud/modules/edp-aggregator/` with env vars for `OTEL_EXPORTER_OTLP_ENDPOINT`

## 11. References

### Existing Documentation
- **[Halo Metrics Deployment on GKE](../gke/metrics-deployment.md)** – Setup guide for OpenTelemetry Operator and Collector
- **[Cluster Configuration](../gke/cluster-config.md)** – GKE cluster setup including Workload Identity
- **[Correctness Test](../gke/correctness-test.md)** – Integration testing with metrics validation

### Configuration Files
- **OpenTelemetry Setup:**
  - `src/main/k8s/open_telemetry.cue` – Base OTel Collector and Instrumentation definitions
  - `src/main/k8s/dev/open_telemetry_gke.cue` – GKE-specific config with Google Cloud exporter
  - `src/main/k8s/base.cue` (lines 442, 470, 590) – Pod annotation templates for Java agent injection
- **Secure Computation:**
  - `src/main/k8s/secure_computation.cue` – Deployment specs for public/internal APIs
  - `src/main/k8s/dev/secure_computation_gke.cue` – GKE deployment configuration
  - `src/main/terraform/gcloud/modules/secure-computation/` – Terraform for GCP resources (Spanner, IAM, static IP)
- **EDP Aggregator:**
  - `src/main/terraform/gcloud/modules/edp-aggregator/` – Terraform for Cloud Functions, MIGs, Pub/Sub, GCS buckets
  - `src/main/terraform/gcloud/cmms/edp_aggregator.tf` – Root module configuration with secrets, schedulers, and function configs

### Terraform IAM for Monitoring
- **OpenTelemetry service account** created in `src/main/terraform/gcloud/cmms/main.tf`:
  - Workload Identity binding to K8s SA `open-telemetry`
  - `roles/monitoring.metricWriter` (lines 105-108)
  - `roles/cloudtrace.agent` (lines 99-102)
- **Secure Computation internal API** SA in `src/main/terraform/gcloud/modules/secure-computation/main.tf`:
  - `roles/monitoring.metricWriter` (lines 25-29) for custom metrics
