# EDPA Aggregator Monitoring Plan

## 1. TL;DR
- **Objective:** Provide production-grade observability for the Halo Data Provider Module (EDP Aggregator) so that issues are detected and remediated within the MTTR targets.<br>
- **Scope:** Complete observability for EDPA-specific components (Data Availability Sync, Data Watcher, Event Group Sync, Results Fulfiller, Requisition Fetcher, Dead Letter Queue Listener) across Cloud Functions, Confidential Space MIGs, and Kubernetes, building on existing Kubernetes OpenTelemetry instrumentation for Secure Computation APIs and Impression/Requisition Metadata Service.<br>
- **Approach:** Instrument all services with OpenTelemetry (metrics, traces, logs) and export to Google Cloud Monitoring and Logging through the managed OTLP pipeline.

## 2. System Overview

**Runtime Topology.**
- Cloud Storage buckets per EDP hold impression and requisition artifacts.
- Cloud Functions trigger: Data Watcher
- HTTP Cloud Functions: Event Group Sync, Data Availability Sync, Requisition Fetcher
- Results Fulfiller runs in a Confidential Space managed instance group (MIG) per deployment, leveraging TEE attestation.
- Secure Computation Public API operates as a Kubernetes deployment (GKE), fronting the control plane. When deployed to Kubernetes, the public and internal API servers run as separate deployments (`secure-computation-public-api-server` and `secure-computation-internal-api-server`).
- Dead Letter Queue Listener runs embedded in the `secure-computation-internal-api-server` (Kubernetes), monitoring the Pub/Sub DLQ subscription for failed work items.
- Impression and Requisition Metadata Storage operate as Kubernetes deployment.

**Observability Platform.**
- Hybrid OpenTelemetry architecture:
  - Cloud Functions emit telemetry via OTel SDKs to a centrally managed OTel Collector running on Cloud Run (OTLP/gRPC and OTLP/HTTP). Function runtime logs continue to stream to Cloud Logging.
  - Confidential Space MIGs run a local OTel Collector agent inside each VM (within the enclave trust boundary) that ingests application OTLP signals plus host metrics and exports directly to Google Cloud backends.
  - Kubernetes deployments (GKE) use auto-instrumentation via OpenTelemetry Operator. The operator injects language-specific agents into annotated pods, which export telemetry via OTLP to a centrally managed OTel Collector deployment (`default-collector`) running in the cluster. The collector applies resource detection (GKE metadata), batching, memory limiting, and label transformations, then exports metrics to Cloud Monitoring and traces to Cloud Trace using the `googlecloud` exporter with Workload Identity for authentication.
- Export targets: Google Cloud Monitoring (metrics), Cloud Trace (traces), and Cloud Logging (logs). Dashboards, SLOs, and alerts are defined in Cloud Monitoring.
- Error reporting via Cloud Logging Error Reporting with trace correlation.

### Current Instrumentation Status (Kubernetes)
The Halo CMMS platform **already has OpenTelemetry instrumentation in place** for Kubernetes-based components:

**Infrastructure:**
- **OpenTelemetry Operator** (v0.99.0+) deployed with cert-manager (v1.14.5+) for automatic instrumentation
- **OTel Collector** (`default`) running as deployment with:
  - OTLP/gRPC receiver on port 4317
  - Google Cloud exporter for metrics and traces (`googlecloud` exporter)
  - Resource detection, batch processing, memory limiting
  - Label transformation to prefix GCP-reserved names (`exported_location`, `exported_cluster`, etc.)
  - _Config:_ `src/main/k8s/open_telemetry.cue` (collector), `src/main/k8s/dev/open_telemetry_gke.cue` (GKE-specific)
- **Workload Identity** service account `open-telemetry` with `roles/monitoring.metricWriter` and `roles/cloudtrace.agent`
  - _Terraform:_ `src/main/terraform/gcloud/cmms/main.tf` (lines 91-109)

**Telemetry schema (consistent across environments):**
- Metrics we emit and expect everywhere (names are stable):
  - `rpc.server.duration` (histogram) with attributes: `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `service.name`, `deployment.environment`, `edp_id?`
  - `rpc.client.duration` (histogram) for outbound calls with same attributes
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` (runtime)
  - Component counters (add where applicable):
    - `edpa.data_watcher.queue_writes` (counter) attrs: `edp_id`, `queue`
    - `edpa.requisition_fetcher.requisitions_fetched` (counter) attrs: `edp_id`
    - `edpa.results_fulfiller.requisitions_processed` (counter) attrs: `edp_id`, `model_line`
    - `edpa.ingestion.freshness_seconds` (gauge or histogram) attrs: `edp_id`
- Required common attributes on all telemetry: `service.name`, `service.version`, `deployment.environment` (`dev|qa|prod`), `edp_id` (when applicable), `runtime` (`k8s|cloud_function|confidential_space`).
- Tracing: W3C trace context for all inter-service calls; do not emit traces for health checks (`rpc.method=Check`).

**Deployment Coverage:**
All Kubernetes Server deployments (`#ServerDeployment` in `src/main/k8s/base.cue` lines 609-621) inherit OpenTelemetry auto-instrumentation, including:
- `secure-computation-public-api-server` (`src/main/k8s/secure_computation.cue` line 103)
- `secure-computation-internal-api-server` (`src/main/k8s/secure_computation.cue` line 77) — includes Dead Letter Queue Listener
- `impression-metadata-storage`
- `requisition-metadata-storage`
- Kingdom API servers (`src/main/k8s/kingdom.cue`)
- Duchy workers (`src/main/k8s/duchy.cue`)
- Reporting servers (`src/main/k8s/reporting_v2.cue`)

**Gaps for EDPA Components:**
The following EDPA-specific components are **not yet instrumented** because they run outside Kubernetes:
1. **Cloud Functions** (Data Watcher, Event Group Sync, Data Availability Sync, Requisition Fetcher) – require explicit OTel SDK integration
2. **Results Fulfiller MIG** (Confidential Space VMs) – require Ops Agent or sidecar collector deployment
3. **Custom metrics** for EDPA workflows (impression freshness, requisition backlog, etc.) – need instrumentation in application code

## 3. Monitoring Goals & SLOs
The targets below are actionable starting points (SLI, Target, Window). Adjust to contractual and operational realities.

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

### 3.5 Internal Health SLIs (Non-SLO)
- Requisition backlog drain time < 1 hour (derived from queue depth and processing rate).
- CMMS RPC error rate for Data Availability Sync < 2/min sustained 10 min.
- Event Group Sync latency p99 < 20 min.
- Dead Letter Queue depth < 100 messages sustained 15 min (indicates systemic failures).
- Dead Letter Queue reprocess success rate > 95% (items should succeed on retry after transient failures resolved).

## 4. Core Signals & Instrumentation
### 4.1 Shared Practices
- **Resource attributes:** Tag every span/metric/log with `service.name`, `deployment.environment` (`dev`, `qa`, `prod`), `edp_id`, and `runtime` (`cloud_function`, `confidential_space`, `standalone`).
- **Correlation ID (`report_id`):** Include `report_id` as a label on all EDPA metrics and logs to enable end-to-end lifecycle tracking.
  - Format: `measurementConsumers/{measurement_consumer}/reports/{report_id}`
  - Extract from: `requisitionMetadata.report` or `measurementSpec.reportingMetadata.report`
  - Available throughout the lifecycle: Data Watcher → Event Group Sync → Requisition Fetcher → Results Fulfiller
  - Use for: Calculating fulfillment latency, correlating events across components, dashboarding by report
  - Benefits: Lower cardinality than `external_computation_id` (1 report → N measurements), aligns with user mental model
- **Trace context propagation:** Use W3C traceparent headers when calling Secure Computation public API and internal HTTP endpoints.
- **Sampling:** 100% traces in dev, 10% in prod with tail-based sampling on errors/latency.
- **Collector configuration:**
  - Cloud Functions use the OpenTelemetry Exporter for Cloud Functions, auto-exporting to Cloud Monitoring (set `OTEL_EXPORTER_OTLP_ENDPOINT` to regional collector, configure GCP service account with Monitoring Metric Writer).
  - Results Fulfiller MIGs run the Ops Agent + OTel Collector sidecar sending metrics/logs over OTLP/gRPC with TLS.
  - Confidential Space nodes require attested secure channel (use SPIFFE/SVID or mutual TLS keyed by confidential VM identity).

### 4.2 Extracting and Using `report_id` for Correlation

**Extraction by Component:**

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


**Dashboard Query Example (Cloud Monitoring MQL):**

```
fetch workload.googleapis.com/edpa.results_fulfiller.fulfillment_latency
| filter resource.service_name == 'results-fulfiller'
| group_by [metric.report_id], [percentile: percentile(value, 99)]
| filter percentile > 3600  # Alert on reports taking > 1 hour
```

**Benefits of `report_id` for Correlation:**
- **End-to-end tracing**: Follow a single report through Data Watcher → Event Group Sync → Requisition Fetcher → Results Fulfiller
- **Report-level SLOs**: Calculate p99 latency per report, identify slow reports
- **Lower cardinality**: 1 report typically maps to multiple measurements/requisitions, reducing metric explosion
- **User alignment**: "Report ABC123 took 45 minutes" is more meaningful than "Measurement 987654 took..."
- **Cross-component join**: Correlate `edpa.data_watcher.queue_writes{report_id}` with `edpa.results_fulfiller.requisitions_processed{report_id}` for SLO 4 correctness

## 3. Strategy & Architecture
### 3.1 Telemetry Strategy (Hybrid: Cloud Functions + Confidential MIGs)
- Cloud Functions (serverless):
  - Use OpenTelemetry SDKs (traces, metrics, logs) initialized outside the handler to avoid cold‑start gaps.
  - Export OTLP to a central OTel Collector on Cloud Run over TLS using OTLP/gRPC (4317) or OTLP/HTTP (4318).
  - Continue sending runtime logs to Cloud Logging; include trace correlation fields from OTel context.
- Confidential Space MIGs (Confidential VMs):
  - Run an OTel Collector agent in each VM (within the enclave). Collect:
    - Application telemetry over OTLP from in‑enclave workloads.
    - Host metrics via `hostmetrics` receiver.
    - Optional structured logs via `otlp` or `filelog` receivers.
  - Export directly to Google Cloud Monitoring/Trace/Logging using the `googlecloud` exporter with least‑privilege service accounts and enclave‑gated credentials.

### 3.2 Collector Topology
- Central Collector (Cloud Run):
  - Receives OTLP from Cloud Functions at `:4317` (gRPC) and `:4318` (HTTP).
  - Private ingress (VPC only) with minimum instances to stay warm; scales horizontally with request load.
  - Exports traces/metrics/logs to Google Cloud backends using the `googlecloud` exporter.
- Local Agent Collector (Confidential MIG):
  - Runs inside each Confidential VM; ingests OTLP from the app and scrapes `hostmetrics`.
  - Remains within the enclave trust boundary; exports directly to Google Cloud over TLS.
- Export Paths:
  - Traces → Cloud Trace
  - Metrics → Cloud Monitoring (workload.googleapis.com)
  - Logs → Cloud Logging (resource‑aware, with trace correlation)

### 3.3 Collector Configuration Examples
#### Central Collector (Cloud Run)
```yaml
# Accept OTLP from Cloud Functions; gRPC is preferred for performance,
# HTTP improves compatibility and resilience during cold starts
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

# Native export to Cloud Monitoring/Trace/Logging
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

# Single entry for all CF telemetry; logs via OTLP if emitted by app
service:
  extensions: [health_check, pprof, zpages]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [googlecloud]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [googlecloud]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [googlecloud]
```

#### Agent Collector (Confidential Space MIG VM)
```yaml
# Ingest app telemetry inside the enclave; support gRPC/HTTP clients
# Scrape host metrics allowed within the enclave trust boundary
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  hostmetrics:
    collection_interval: 30s
    scrapers: {cpu: {}, memory: {}, disk: {}, filesystem: {}, network: {}, load: {}, process: {}}

# Attach GCE metadata for filtering/attribution; tune memory for VM footprint
processors:
  resourcedetection:
    detectors: [gce]
  memory_limiter:
    check_interval: 5s
    limit_percentage: 80
    spike_limit_percentage: 25
  batch:
    send_batch_size: 2048
    timeout: 5s

# Direct export from inside the enclave using least-privilege SA
exporters:
  googlecloud:
    project: ${GCP_PROJECT}
    user_agent: edpa-mig-agent
    metric:
      prefix: workload.googleapis.com

# Unify workload and host metrics; optionally add filelog receiver if apps write JSON logs
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [resourcedetection, memory_limiter, batch]
      exporters: [googlecloud]
    metrics:
      receivers: [otlp, hostmetrics]
      processors: [resourcedetection, memory_limiter, batch]
      exporters: [googlecloud]
    logs:
      receivers: [otlp]
      processors: [resourcedetection, memory_limiter, batch]
      exporters: [googlecloud]
```

### 3.4 Security & Compliance (Confidential VMs)
- Enclave trust boundary:
  - Run the OTel Collector agent inside the Confidential VM; keep telemetry processing and credentials within the enclave.
  - Use attestation‑gated access to secrets (e.g., Google Secret Manager with TEE constraints) to provision short‑lived credentials.
- Transport security:
  - TLS for all OTLP traffic
  - Exporters use Google‑managed TLS to Cloud backends; no plaintext telemetry.
- Identity & access:
  - Least‑privilege service accounts with `roles/monitoring.metricWriter`, `roles/cloudtrace.agent`, and, if exporting logs, `roles/logging.logWriter`.
  - Optionally enforce VPC Service Controls and CMEK for logs/metrics storage.
- Data handling:
  - Retention and access policies aligned with compliance requirements; audit via Cloud Logging.

### 3.5 Operational Best Practices
- Sampling:
  - Dev: 100% traces. Prod: 10% head sampling in SDKs; enable tail‑based sampling in collector for high‑latency or error spans when needed.
- Batching & timeouts (SDK env vars for Cloud Functions):
  - `OTEL_BSP_MAX_QUEUE_SIZE=2048`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512`, `OTEL_BSP_SCHEDULE_DELAY=200ms`, `OTEL_BSP_EXPORT_TIMEOUT=3s`.
- Cold‑start mitigation (Cloud Functions):
  - Initialize OTel SDK outside the handler, set OTLP endpoints via env vars, and keep the central collector warm (Cloud Run min instances).
- Scaling:
  - Cloud Run collector: set min instances and autoscaling based on concurrent requests/CPU.
  - MIG agents: right‑size CPU/memory; scrape intervals ≥30s for host metrics.
- Log–trace correlation:
  - Include `logging.googleapis.com/trace=projects/${PROJECT_ID}/traces/${TRACE_ID}` and `logging.googleapis.com/spanId=${SPAN_ID}` in structured logs; set `logging.googleapis.com/trace_sampled=true` when sampled.

### 3.6 Reference Architecture (Telemetry Flow)
- Cloud Functions → Central OTel Collector (Cloud Run) → Google Cloud Monitoring/Trace/Logging
- Confidential Space MIGs → Local OTel Collector Agent → Google Cloud Monitoring/Trace/Logging

```
 [Cloud Functions]
      │  OTLP (gRPC/HTTP, TLS)
      ▼
 [Cloud Run OTel Collector]
      │  googlecloud exporter
      ▼
 [Cloud Monitoring | Cloud Trace | Cloud Logging]

 [Confidential VM Workload]──OTLP──▶[OTel Agent in Enclave]
                                      │  hostmetrics
                                      ▼  googlecloud exporter
                         [Cloud Monitoring | Cloud Trace | Cloud Logging]
```

## 5. Measurements Catalog

**Label Convention:** All EDPA custom metrics (`edpa.*`) should include `report_id` label when the requisition context is available (Data Watcher onwards).

| Component | Type | Name | Description | Implemented |
| --- | --- | --- | --- | --- |
| Shared (All services) | histogram | rpc.server.duration | gRPC server latency by method and status code | OTel Java agent (auto-instrumented via K8s Operator) |
| Shared (All services) | histogram | rpc.client.duration | Outbound gRPC client latency | OTel Java agent (auto-instrumented via K8s Operator) |
| Shared (JVM) | gauge | jvm.memory.used | JVM memory used | OTel Java agent (auto-instrumented via K8s Operator) |
| Shared (JVM) | histogram | jvm.gc.duration | JVM GC duration distribution | OTel Java agent (auto-instrumented via K8s Operator) |
| Shared (JVM) | gauge | jvm.threads.count | JVM thread count | OTel Java agent (auto-instrumented via K8s Operator) |
| Secure Computation API | counter | secure_computation.work_items.created | Work items enqueued by API (by queue, edp_id) | no |
| Secure Computation API | gauge | secure_computation.active_work_items | Estimated backlog size | no |
| Secure Computation API | counter | tls.handshake.errors | TLS/mTLS handshake failures | no |
| Data Watcher (CF) | histogram | function.execution.latency | End-to-end function execution latency | GCP Cloud Functions (native) |
| Data Watcher (CF) | gauge | function.memory.usage | Function memory usage | GCP Cloud Functions (native) |
| Data Watcher (CF) | counter | function.execution.count | Total executions | GCP Cloud Functions (native) |
| Data Watcher (CF) | counter | function.execution.failures | Failed executions | GCP Cloud Functions (native) |
| Data Watcher (CF) | histogram | edpa.data_watcher.match_latency | Time from GCS event to queue submit | no |
| Data Watcher (CF) | counter | edpa.data_watcher.queue_writes{report_id,work_item_id} | Control-plane queue writes (for SLO 4 correctness) | no |
| Data Availability Sync (CF) | histogram | edpa.data_availability.sync_duration | Time from done blob write to Kingdom availability update (SLO 2) | no |
| Data Availability Sync (CF) | counter | edpa.data_availability.records_synced | Metadata records persisted per EDP | no |
| Data Availability Sync (CF) | counter | edpa.data_availability.cmms_rpc_errors | CMMS RPC error count | no |
| Event Group Sync (CF) | counter | edpa.event_group.sync_attempts | Sync attempts made | no |
| Event Group Sync (CF) | counter | edpa.event_group.sync_success | Successful syncs | no |
| Event Group Sync (CF) | histogram | edpa.event_group.sync_latency{report_id} | Sync latency distribution | no |
| Results Fulfiller (MIG) | histogram | edpa.results_fulfiller.fulfillment_latency{report_id} | Time from requisition creation to fulfillment complete (SLO 3) | no |
| Results Fulfiller (MIG) | counter | edpa.results_fulfiller.requisitions_processed{report_id,work_item_id,status} | Requisitions processed (for SLO 4 correctness) | no |
| Results Fulfiller (MIG) | gauge | edpa.results_fulfiller.frequency_vector_build_time | Time to build frequency vectors | no |
| Results Fulfiller (MIG) | gauge | vm.cpu.utilization | VM CPU utilization | GCP Compute Engine (native via hostmetrics receiver) |
| Results Fulfiller (MIG) | gauge | vm.memory.usage | VM memory usage | GCP Compute Engine (native via hostmetrics receiver) |
| Results Fulfiller (MIG) | counter | vm.attestation.failures | Confidential VM attestation failures | GCP Confidential Computing (native) |
| Results Fulfiller (MIG) | gauge | pubsub.subscription.num_undelivered_messages | Undelivered messages in results queue | GCP Pub/Sub (native) |
| Requisition Fetcher (CF) | histogram | edpa.requisition_fetcher.fetch_latency{report_id} | Fetch latency distribution | no |
| Requisition Fetcher (CF) | counter | edpa.requisition_fetcher.requisitions_fetched{report_id,edp_id} | Requisitions fetched per EDP | no |
| Requisition Fetcher (CF) | counter | edpa.requisition_fetcher.storage_writes | GCS writes performed | no |
| Requisition Fetcher (CF) | counter | scheduler.job_execution_count | Cloud Scheduler job executions | GCP Cloud Scheduler (native) |
| Requisition Fetcher (CF) | counter | scheduler.job_attempt_delayed | Delayed scheduler attempts | GCP Cloud Scheduler (native) |
| Data Watcher (CF) | gauge | edpa.data_watcher.done_blob_timestamp | Timestamp when done blob was written (for SLO 2 correlation) | no |
| Dead Letter Queue Listener | counter | edpa.dlq.messages_received | Dead letter queue messages received | no |
| Dead Letter Queue Listener | counter | edpa.dlq.messages_reprocessed | Successfully reprocessed DLQ messages | no |
| Dead Letter Queue Listener | counter | edpa.dlq.reprocess_failures | Failed reprocessing attempts | no |
| Dead Letter Queue Listener | gauge | pubsub.subscription.num_undelivered_messages | Undelivered messages in DLQ subscription | GCP Pub/Sub (native) |

## 6. Component-Specific Monitoring

### 5.1 Secure Computation Public API (`SecureComputationApiServer`)

**Implementation:** `src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt`  
**Deployment:** `src/main/k8s/secure_computation.cue` (lines 103-114), `src/main/k8s/dev/secure_computation_gke.cue`  
**Terraform:** `src/main/terraform/gcloud/modules/secure-computation/main.tf`

**Current Status:** ✅ **Fully instrumented** via OpenTelemetry Java agent auto-injection on Kubernetes.

**Existing Metrics (Auto-Generated by OTel Java Agent):**
- `rpc.server.duration` (histogram) by `rpc.method`, `rpc.grpc.status_code` – measures gRPC call latency
- `rpc.server.request.size` / `rpc.server.response.size` (histograms) – message size distribution
- `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` – JVM runtime metrics
- Kubernetes pod metrics via `resourcedetection` processor: `k8s.pod.name`, `k8s.namespace.name`, `k8s.deployment.name`

**Existing Traces:**
- Server spans auto-generated for each gRPC call with attributes: `rpc.service`, `rpc.method`, `rpc.system=grpc`, `net.peer.name`, `net.peer.port`
- Trace context propagated via W3C traceparent to internal API and downstream services
- Health check calls (`rpc.method=Check`) filtered out by OTel Collector

**Deployment Details:**
- Runs as `secure-computation-public-api-server-deployment` in Kubernetes
- Container: `secure-computation-public-api-server-container` with Java agent injected via `instrumentation.opentelemetry.io/inject-java: "true"`
- Exports metrics/traces to `default-collector-headless.default.svc:4317` (OTel Collector)
- Collector forwards to Google Cloud Monitoring and Cloud Trace

**Recommended Additions:**
- **Custom metrics** (instrument in `PublicApiServer.kt` using OpenTelemetry SDK):
  - `secure_computation.work_items.created` (counter) by `queue`, `edp_id` for monitoring Data Watcher submissions
  - `secure_computation.active_work_items` (gauge) for queue backlog visibility
  - `tls.handshake.errors` (counter) for mutual TLS failures
- **Logs:**
  - Structured JSON logs with `request_id`, `edp_id`, `method` (currently using `java.util.logging`)
  - Export to Cloud Logging with correlation via `logging.googleapis.com/trace` field
  - Create log-based metric `secure_computation_api_errors` counting ERROR/SEVERE logs
- **Span attributes:**
  - Add `edp_id` and `work_item_id` to server spans for better trace filtering

**Dashboards & Alerts.**
- **Dashboard:** Create Monitoring dashboard querying:
  - `workload.googleapis.com/rpc.server.duration` filtered by `k8s_deployment_name="secure-computation-public-api-server-deployment"`
  - Latency heatmap (p50, p95, p99) by `rpc.method`
  - Error budget burn rate using SLO on `rpc.grpc.status_code != 0`
- **Alerts:**
  - `A1`: High latency – p99 > 200ms for `CreateWorkItem` sustained 5 min
  - `A2`: Error rate – >5% of calls with gRPC status >= 500 over 5 min
  - `A3`: Pod crashes – `k8s.deployment.available` < replicas for 2 min

**Runbook Highlights.**
1. Check GKE cluster health and pod status: `kubectl get pods -l app=secure-computation-public-api-server-app`
2. View logs: `kubectl logs -l app=secure-computation-public-api-server-app --tail=100`
3. Validate internal API connectivity: `kubectl exec -it <pod> -- nc -zv secure-computation-internal-api-server 8443`
4. Check certificate validity mounted at `/var/run/secrets/files/secure_computation_tls.pem`
5. Scale deployment if CPU/memory saturated: `kubectl scale deployment secure-computation-public-api-server-deployment --replicas=N`

### 5.2 Data Watcher Cloud Function (`DataWatcherFunction`)

**Implementation:** `src/main/kotlin/org/wfanet/measurement/securecomputation/datawatcher/DataWatcher.kt`, `src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/datawatcher/DataWatcherFunction.kt`  
**Config:** `src/main/proto/wfa/measurement/config/securecomputation/data_watcher_config.proto`  
**Terraform:** `src/main/terraform/gcloud/cmms/edp_aggregator.tf` (lines 158-165), `src/main/terraform/gcloud/modules/edp-aggregator/`

**Metrics.**
- `function.execution.latency` (distribution) from Cloud Functions + custom `edpa.data_watcher.match_latency` for regex match to queue submission.
- `edpa.data_watcher.queue_writes` counter for control-plane queue submissions.
- `function.memory.usage` gauge; alert on >80% of allocation.
- `function.execution.count` vs. `function.execution.failures` to derive error rate.

**Logs.**
- Structured log `Matched path` includes `config_id`, `source_path_regex` (`DataWatcher.kt` line 56).
- Log-based metric for skipped empty blobs not ending with `done` to detect unexpected inputs (`DataWatcherFunction.kt` lines 63-66).

**Tracing.**
- Wrap `receivePath` execution in span with attributes `bucket`, `blob`, `sink_type` (`DataWatcher.kt` line 50).
- Set span links to triggered WorkItem IDs (add to `createWorkItemRequest` at line 86).

**Alerts.**
- `A4`: Failure rate >5% over 5 min.
- `A5`: Queue write stall – no `queue_writes` for 15 min while `function.execution.count` > 0.
- `A6`: High latency – p95 of `match_latency` > 3 min.

**Runbook Steps.**
1. Inspect Cloud Function logs for regex mismatch or credential errors (check logger.severe at `DataWatcher.kt` line 71).
2. Validate `CONTROL_PLANE_TARGET` reachability (env var in `DataWatcherFunction.kt` line 79).
3. Re-deploy function via Terraform: `terraform apply -target=module.edp_aggregator` from `src/main/terraform/gcloud/cmms/`

### 5.3 Data Availability Sync Cloud Function (`DataAvailabilitySyncFunction`)

**Config:** `src/main/proto/wfa/measurement/config/edpaggregator/data_availability_sync_config.proto`  
**Terraform:** `src/main/terraform/gcloud/cmms/edp_aggregator.tf` (lines 180-186)

**Metrics.**
- `edpa.data_availability.sync_duration` histogram covering fetch + write cycle.
- `edpa.data_availability.records_synced` counter per EDP.
- `edpa.data_availability.cmms_rpc_errors` counter from CMMS API calls.

**Logs & Tracing.**
- Log spool operations with `storage_path_prefix` and `delta_bytes` to detect growth (add instrumentation).
- Trace spans around CMMS and ImpressionMetadata RPCs (instrument CMMS API calls per config at `data_availability_sync_config.proto` line 39).

**Alerts.**
- `A7`: Sync duration > 15 min p95.
- `A8`: RPC error rate > 2/min.
- `A9`: Freshness SLO violation – difference between latest impression timestamp and current time > 30 min.

**Runbook.**
1. Verify CMMS API health; correlate with Secure Computation API metrics.
2. Ensure service account secrets not rotated unexpectedly.
3. Check GCS bucket IAM for denied writes.

### 5.4 Event Group Sync Cloud Function (`EventGroupSyncFunction`)

**Implementation:** `src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/eventgroups/EventGroupSyncFunction.kt`  
**Config:** `src/main/proto/wfa/measurement/config/edpaggregator/event_group_sync_config.proto`  
**Terraform:** `src/main/terraform/gcloud/cmms/edp_aggregator.tf` (lines 173-179)

**Metrics.**
- `edpa.event_group.sync_attempts` counter.
- `edpa.event_group.sync_success` counter.
- `edpa.event_group.sync_latency` histogram.

**Logs.**
- Track `event_group_reference_id` per sync cycle (see test example at `EdpAggregatorCorrectnessTest.kt` line 127).
- Create log-based metric on failure logs containing `Waiting on Event Group Sync` longer than threshold.

**Alerts.**
- `A10`: Sync backlog – difference `sync_attempts - sync_success` > 100 over 15 min.
- `A11`: Latency p99 > 20 min.

**Runbook.**
1. Check Data Watcher pipeline to ensure requisitions created (see `DataWatcher.kt` line 93 for work item creation).
2. Validate Cloud Scheduler triggers if applicable.
3. Requeue stuck work items via Secure Computation control plane API.

### 5.5 Results Fulfiller (Confidential Space MIG)

**Implementation:** `src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt`, `ResultsFulfillerApp.kt`, `ResultsFulfillerAppRunner.kt`  
**Storage:** `src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/StorageEventSource.kt`  
**Config:** `src/main/proto/wfa/measurement/edpaggregator/v1alpha/results_fulfiller_params.proto`  
**Terraform:** `src/main/terraform/gcloud/cmms/edp_aggregator.tf` (lines 85-121 - MIG config), `src/main/terraform/gcloud/modules/edp-aggregator/` (MIG module)

**Metrics.**
- `edpa.results_fulfiller.fulfillment_latency` (histogram) from `fulfillRequisitions` coroutine.
- `edpa.results_fulfiller.requisitions_processed` counter.
- `edpa.results_fulfiller.frequency_vector_build_time` gauge (derived from logs/metrics).
- `vm.cpu.utilization`, `vm.memory.usage`, `vm.attestation.failures` from Confidential Space telemetry.
- `pubsub.subscription.num_undelivered_messages` for results queue.

**Logs & Tracing.**
- Structured logs with `model_line`, `requisition_id`, `build_ms`, `send_ms` from `logFulfillmentStats` instrumentation (`ResultsFulfiller.kt` lines 219-237).
- Trace spans around decrypt/build/send phases with attributes `population_spec` and `duchy_target`.

**Alerts.**
- `A12`: Queue backlog – `num_undelivered_messages` > 1,000 for 10 min.
- `A13`: Fulfillment latency – p95 > 15 min.
- `A14`: Confidential Space health – attestation failures > 0 over 5 min.

**Runbook.**
1. Check MIG instance health; ensure latest image pull succeeded (gcsfuse).
2. Validate TLS cert paths fetched from Secret Manager.
3. For backlog, scale MIG or investigate downstream (Kingdom) backpressure.
4. For attestation failures, trigger re-attestation or redeploy MIG with fresh nonce.

### 5.6 Requisition Fetcher Cloud Function (`RequisitionFetcherFunction`)

**Implementation:** `src/main/kotlin/org/wfanet/measurement/edpaggregator/requisitionfetcher/RequisitionFetcher.kt`, `src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/requisitionfetcher/RequisitionFetcherFunction.kt`  
**Config:** `src/main/proto/wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto`  
**Terraform:** `src/main/terraform/gcloud/cmms/edp_aggregator.tf` (lines 166-172 - function, lines 123-131 - scheduler)

**Metrics.**
- `edpa.requisition_fetcher.fetch_latency` histogram.
- `edpa.requisition_fetcher.requisitions_fetched` counter per EDP.
- `edpa.requisition_fetcher.storage_writes` counter (GCS writes).
- Cloud Scheduler `job_execution_count` and `job_attempt_delayed` metrics.

**Logs & Tracing.**
- Log `page_token`, `page_size`, `requisition_state` (`RequisitionFetcher.kt` lines 76-84); set sampling to avoid PII.
- Trace spans for each CMMS API call and GCS write; link to Data Watcher spans using `grouped_requisition_id`.

**Alerts.**
- `A15`: Scheduler failing – `job_attempt_delayed` > 0 for 3 consecutive schedules.
- `A16`: Fetch failures – error rate > 10% or `fetch_latency` p95 > 10 min.
- `A17`: Storage write failures – Cloud Function error logs containing `writeBlob` message > 5/min.

**Runbook.**
1. Validate Cloud Scheduler service account permissions on function URL (scheduler config at `edp_aggregator.tf` lines 123-131).
2. Inspect CMMS API availability (reuse `A1` dashboard).
3. Check GCS bucket quota / IAM restrictions (bucket defined in `edp_aggregator.tf` line 196).

## 6. Dashboards
- **EDPA Summary Dashboard:**
  - SLO burn-down chart for Secure Computation API.
  - Ingestion freshness per EDP (Data Watcher → Event Group Sync → Results Fulfiller timelines).
  - Queue backlog status (Pub/Sub + Scheduler).
  - Error budget table for each component.
- **Component Deep Dives:** Pre-built JSON dashboards stored alongside Terraform (`src/main/terraform/gcloud/modules/edp-aggregator/dashboards/` - to be created). Terraform will deploy using `google_monitoring_dashboard` with templates referencing metrics above.
- **Reference examples:** See existing Kingdom/Duchy dashboards at `src/main/terraform/gcloud/modules/kingdom/kingdom_dashboard_*.json` and `src/main/terraform/gcloud/modules/duchy/duchy_dashboard_*.json`

## 7. Alerting Policy Implementation
- Use Terraform (same module tree) to define `google_monitoring_alert_policy` resources (add to `src/main/terraform/gcloud/modules/edp-aggregator/main.tf`).
- Alerting channels: primary PagerDuty service, secondary email to `edpa-oncall@`.
- Adopt multi-window, multi-burn-rate alerts (M3) for SLO violations per Google CRE guidelines.
- Suppress during planned maintenance windows via `slo_maintenance_window` label in metrics.

## 8. Logging Strategy
- All services emit structured logs (JSON) via OTel logging exporter.
- Attach trace and span IDs to enable Cloud Trace correlation (`logging.googleapis.com/trace`).
- Define log-based metrics for rare edge conditions (e.g., skipped file due to size 0 at `DataWatcherFunction.kt` line 63) with retention 30 days.
- Configure Cloud Logging sinks to BigQuery for long-term analytics (365 days) anonymizing PII fields (add to `src/main/terraform/gcloud/modules/edp-aggregator/main.tf`).

## 9. Testing & Validation
- **Unit tests:** Add OTel instrumentation unit tests verifying metric names registered (see pattern in existing tests).
- **Integration tests:** Extend `EdpAggregatorCorrectnessTest` (`src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt`) to assert that mock collectors receive spans for Event Group Sync path.
- **Load tests:** Run synthetic upload + requisition flow weekly; ensure alerts stay quiet under nominal load.
- **Chaos drills:** Quarterly simulate API outage and verify alert + runbook effectiveness.

## 10. Operational Processes
- **Release Checklists:** Verify collector endpoints, secret rotations, and service account bindings in Terraform plan before deploy.
- **IncidentResponse:** Maintain runbooks in `docs/operations/runbooks/` with command snippets for log querying (`gcloud logging read`), queue inspection, and MIG management.

## 11. Future Work
- Add SLO for per-EDP throughput (requisitions/hour) once baseline data collected.
- Explore anomaly detection on impression volume trends using Cloud Monitoring Forecasting API.

## 12. References

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
