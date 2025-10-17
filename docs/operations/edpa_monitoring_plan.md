# EDPA Aggregator Monitoring Plan

## 1. TL;DR
- **Objective:** Provide production-grade observability for the Halo Data Provider Module (EDP Aggregator) to detect and remediate issues within MTTR targets.
- **Scope:** Six EDPA components requiring instrumentation: Data Watcher, Data Availability Sync, Event Group Sync, Results Fulfiller, Requisition Fetcher, and Dead Letter Queue Listener.
- **Approach:** OpenTelemetry SDK with manual instrumentation for Cloud Functions and Confidential Space MIGs, using auto-instrumentation libraries from opentelemetry-java-instrumentation for gRPC/HTTP clients. Direct export to Google Cloud Monitoring (no central collector).

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
  - **Cloud Functions**: Use OpenTelemetry SDK with manual initialization. Leverage auto-instrumentation libraries from [opentelemetry-java-instrumentation](https://github.com/open-telemetry/opentelemetry-java-instrumentation) as dependencies for gRPC client and HTTP client instrumentation. Export directly to Google Cloud Monitoring using `google-cloud-opentelemetry` exporter. Function runtime logs continue to stream directly to Cloud Logging.
  - **Confidential Space MIGs**: Same SDK-based approach as Cloud Functions. No central collector required due to VPC networking constraints.
  - **Kubernetes deployments (GKE)**: Use auto-instrumentation via OpenTelemetry Operator. The operator injects language-specific agents into annotated pods, which export telemetry via OTLP to a centrally managed OTel Collector deployment (`default-collector`) running in the cluster. The collector applies resource detection (GKE metadata), batching, memory limiting, and label transformations, then exports metrics to Cloud Monitoring and traces to Cloud Trace using the `googlecloud` exporter with Workload Identity for authentication.
- Export targets: Google Cloud Monitoring (metrics). Dashboards, SLOs, and alerts are defined in Cloud Monitoring.
- **Rationale for direct export**: Cloud Functions and Confidential Space MIGs face networking challenges with centralized collectors (VPC access configuration, cold start latency, collector availability). Direct export via Google Cloud exporter simplifies architecture, reduces failure points, and leverages native GCP integration with service account authentication.

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

##### Core Metrics (all components)

- **Kubernetes gRPC services** (auto-instrumented via OTel Java agent):
  - `rpc.server.duration` (histogram) - attributes: `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `service.name`, `deployment.environment`, `data_provider_name?`
  - `rpc.client.duration` (histogram) - for outbound calls with same attributes
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` (JVM runtime metrics)

- **Cloud Functions** (SDK-based instrumentation with auto-instrumentation libraries):
  - `function.execution.latency` (histogram) - native Cloud Functions metric (auto-emitted by GCP)
  - `rpc.client.duration` (histogram) - instrumented via `opentelemetry-grpc-1.6` library using `GrpcTelemetry` interceptor
    - Attributes: `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `service.name`, `data_provider_name?`
  - `http.client.duration` (histogram) - instrumented via `opentelemetry-okhttp-3.0` or similar library
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` - emitted via `opentelemetry-runtime-telemetry-java8`
  - **No `rpc.server.duration`**: Cloud Functions use HTTP/CloudEvents triggers, not gRPC servers
  - Custom application metrics (e.g., `edpa.data_watcher.match_latency`) use OpenTelemetry SDK Meter API

- **Confidential MIG (Results Fulfiller)** (SDK-based instrumentation with auto-instrumentation libraries):
  - `rpc.client.duration` (histogram) - instrumented via `opentelemetry-grpc-1.6` library using `GrpcTelemetry` interceptor
  - `http.client.duration` (histogram) - instrumented via HTTP client auto-instrumentation library
  - `jvm.memory.used`, `jvm.gc.duration`, `jvm.threads.count` - emitted via `opentelemetry-runtime-telemetry-java8`
  - Custom application metrics (e.g., `edpa.results_fulfiller.fulfillment_latency`) use OpenTelemetry SDK Meter API
  - Exports directly to Google Cloud Monitoring (no central collector)

##### Required common attributes on all telemetry

- `service.name` - Set via `OTEL_SERVICE_NAME` env var (Cloud Functions/MIG) or auto-detected (K8s)
- `service.version` - Inject from deployment metadata or git commit SHA
- `deployment.environment` (`dev|qa|prod`) - Set via `OTEL_RESOURCE_ATTRIBUTES=deployment.environment=<env>`
- `data_provider_name` (when applicable) - Extract from request context or config; omit if unavailable
- `runtime` - Set to `k8s`, `cloud_function`, or `confidential_space`

##### Configuration requirements

- Cloud Functions & MIG: Initialize OpenTelemetry SDK programmatically at application startup:
  - Add dependencies: `opentelemetry-sdk`, `opentelemetry-exporter-google-cloud`, `opentelemetry-grpc-1.6`, `opentelemetry-runtime-telemetry-java8`
  - Configure resource attributes: `service.name`, `deployment.environment`, `data_provider_name`, `runtime`
  - Register gRPC client interceptors from auto-instrumentation library using `GrpcTelemetry.create(openTelemetry).newClientInterceptor()`
  - Install JVM runtime metrics instrumentation
  - Configure metric readers with Google Cloud Monitoring exporter
  - Service account must have `roles/monitoring.metricWriter` and `roles/cloudtrace.agent`
- Cloud Functions: Initialize SDK in global scope (outside handler) to avoid cold start overhead
- MIG: Initialize SDK in main method before starting application logic

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

1. **Cloud Functions** (Data Watcher, Event Group Sync, Data Availability Sync, Requisition Fetcher) – require OpenTelemetry SDK initialization with auto-instrumentation libraries for gRPC/HTTP clients and custom metrics
2. **Results Fulfiller MIG** (Confidential Space VMs) – require OpenTelemetry SDK initialization with auto-instrumentation libraries and direct export to Google Cloud Monitoring
3. **Custom metrics** for EDPA workflows (impression freshness, requisition backlog, etc.) – need manual instrumentation in application code using OpenTelemetry SDK Meter API

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
  - **Correlation**: Use distributed tracing with deterministic trace IDs (Section 4.4) for per-report analysis.
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
  - Use trace correlation (Section 4.4) to track work items end-to-end.
  - Compute SLI as ratio of `edpa.results_fulfiller.requisitions_processed{status="success"}` to `edpa.data_watcher.queue_writes`.
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
- **Resource attributes:** Tag every metric with `service.name`, `deployment.environment` (`dev`, `qa`, `prod`), `data_provider_name`, and `runtime` (`cloud_function`, `confidential_space`, `standalone`).
  - Configured in `Resource` object during SDK initialization (see Section 5.3)
  - Applied automatically to all telemetry (auto-instrumented and custom metrics)
  - `data_provider_name`: Data provider resource name (e.g., `dataProviders/{data_provider}`)
- **Metric cardinality:** Do not use high-cardinality identifiers (`report_id`, `work_item_id`, `event_group_reference_id`, `measurement_id`) as metric labels. Use traces for per-entity debugging (see Section 4.4).
- **Exporter configuration:**
  - Cloud Functions and Confidential Space MIGs use OpenTelemetry SDK with programmatic initialization
  - Direct export to Google Cloud Monitoring via `google-cloud-opentelemetry` exporter library
  - Authentication via Application Default Credentials (service account attached to function/VM)
  - No collector infrastructure required

### 4.2 Business Identifier Usage

Business identifiers (`report_id`, `work_item_id`, `event_group_reference_id`) are used for:
- **Distributed tracing**: Deterministic trace IDs enable querying traces by business entity (see Section 4.4)
- **Span attributes**: Added to spans for filtering in Cloud Trace console
- **Logs**: Structured logging with business identifiers for correlation

Business identifiers are **not used for metric labels** due to high cardinality (unbounded values cause metric explosion).

### 4.4 Distributed Tracing Strategy

#### Approach: Deterministic Trace IDs from Business Identifiers

Use SHA-256 hash of business identifiers to generate deterministic trace IDs. Enables querying Cloud Trace by known business entities and correlating operations across components.

#### Trace ID Mapping

| Workflow | Business Identifier | Trace ID Generation |
|----------|-------------------|-------------------|
| Requisition Fulfillment | `report_id` from `Requisition.measurementSpec.reportingMetadata.report` | `sha256(report_id).substring(0,32)` |
| Event Group Sync | `event_group_reference_id` from EventGroup blob | `sha256(event_group_reference_id).substring(0,32)` |
| Data Availability Sync | `done_blob_path` from blob event | `sha256(done_blob_path).substring(0,32)` |

#### Rationale

- `report_id` available in Requisition Fetcher and Results Fulfiller → traces entire requisition fulfillment lifecycle
- `event_group_reference_id` uniquely identifies event group registration (independent of reports)
- `done_blob_path` uniquely identifies impression availability update batch

#### Implementation

Centralized telemetry module (Section 5.1) provides `TraceIdGenerator` utility and span creation wrappers. Components create root spans with deterministic trace IDs; downstream calls (gRPC via `GrpcTelemetry`, HTTP via `JavaHttpClientTelemetry`) automatically propagate trace context.

#### Use Case: Debug Slow Report

##### Scenario

Report `measurementConsumers/mc123/reports/report456` exceeds 1-hour SLO.

##### Steps

1. Compute trace ID: `echo -n "measurementConsumers/mc123/reports/report456" | sha256sum | cut -c1-32`
2. Query Cloud Trace by trace ID → view all spans across Requisition Fetcher, Results Fulfiller, Kingdom calls
3. Identify bottleneck span (e.g., `compute_frequency_vectors` took 85 minutes)
4. Cross-reference aggregated metrics: `edpa.results_fulfiller.frequency_vector_duration` percentiles by `data_provider_name`

## 5. Strategy & Architecture

### 5.1 Centralized Telemetry Module

#### Package

`org.wfanet.measurement.edpaggregator.telemetry`

All EDPA components (Cloud Functions, Confidential Space MIG) use shared telemetry module for consistent instrumentation. Secure Computation API (Kubernetes) uses existing auto-instrumentation and is excluded.

#### Purpose

- Single SDK initialization logic (exporters, resource attributes, sampling)
- Deterministic trace ID generation from business identifiers
- Pre-configured metric instruments with standardized labels
- Span creation wrappers with automatic trace context injection
- Clean component code (no OpenTelemetry imports outside telemetry module)

#### Key classes

- `EdpaTelemetry` - Singleton for SDK initialization, provides tracer/meter access
- `TraceIdGenerator` - Generates trace IDs from `report_id`, `event_group_reference_id`, `done_blob_path`
- `TracedOperation` - Creates spans with deterministic trace context
- `EdpaMetrics` - Pre-defined histograms and counters (fulfillment latency, queue writes, sync duration, etc.)

#### Component integration

- Call `EdpaTelemetry.initialize()` at startup
- Use `EdpaTelemetry.traceReportFulfillment()`, `traceEventGroupSync()`, etc. for traced operations
- Use `EdpaMetrics.recordFulfillmentLatency()`, etc. for metrics
- No direct OpenTelemetry imports in component code

### 5.2 Telemetry Strategy (SDK-Based Direct Export)

#### Cloud Functions

- Initialize OpenTelemetry SDK programmatically at application startup (global scope)
- Use auto-instrumentation libraries from `opentelemetry-java-instrumentation` repository:
  - `opentelemetry-grpc-1.6` - Provides `GrpcTelemetry` to instrument gRPC clients via interceptors
  - `opentelemetry-runtime-telemetry-java8` - Emits JVM metrics (memory, GC, threads)
  - `opentelemetry-okhttp-3.0` (if using OkHttp) - Instruments HTTP clients
- Custom application metrics use OpenTelemetry SDK Meter API
- Export directly to Google Cloud Monitoring using `google-cloud-opentelemetry` exporter
- No Java agent (`-javaagent`) required - all instrumentation via SDK APIs

#### Confidential Space MIGs

- Same SDK-based approach as Cloud Functions
- Initialize in main method before starting application logic
- Direct export to Google Cloud Monitoring (no collector required)
- VM service account must have `roles/monitoring.metricWriter` and `roles/cloudtrace.agent`

#### Benefits of Direct Export

- **Simplified architecture**: No central collector to deploy, monitor, or scale
- **Reduced failure points**: Eliminates collector availability as dependency
- **Lower latency**: No network hop to collector; direct GCP API calls
- **Native authentication**: Uses Application Default Credentials (service account)
- **Cost efficiency**: No Cloud Run collector instance to pay for
- **Cold start resilience**: No dependency on collector warmth during function invocations

### 5.3 Required Dependencies

#### OpenTelemetry SDK core

- `io.opentelemetry:opentelemetry-sdk` (1.34.1+)
- `io.opentelemetry:opentelemetry-sdk-metrics` (1.34.1+)

#### Google Cloud exporters

- `com.google.cloud.opentelemetry:exporter-metrics` (0.27.0+)
- `com.google.cloud.opentelemetry:exporter-trace` (0.27.0+)

#### Auto-instrumentation libraries from opentelemetry-java-instrumentation

- `io.opentelemetry.instrumentation:opentelemetry-grpc-1.6` (1.32.0-alpha+) - gRPC client instrumentation
- `io.opentelemetry.instrumentation:opentelemetry-runtime-telemetry-java8` (1.32.0-alpha+) - JVM metrics
- `io.opentelemetry.instrumentation:opentelemetry-java-http-client` (1.32.0-alpha+) - Java 11+ HttpClient (only for Data Watcher)

#### Library requirements by component

| Component | gRPC | HTTP Client | JVM Runtime |
|-----------|------|-------------|-------------|
| Data Watcher | ✅ `opentelemetry-grpc-1.6` | ✅ `opentelemetry-java-http-client` | ✅ `opentelemetry-runtime-telemetry-java8` |
| Event Group Sync | ✅ `opentelemetry-grpc-1.6` | ❌ Not needed | ✅ `opentelemetry-runtime-telemetry-java8` |
| Data Availability Sync | ✅ `opentelemetry-grpc-1.6` | ❌ Not needed | ✅ `opentelemetry-runtime-telemetry-java8` |
| Requisition Fetcher | ✅ `opentelemetry-grpc-1.6` | ❌ Not needed | ✅ `opentelemetry-runtime-telemetry-java8` |
| Results Fulfiller | ✅ `opentelemetry-grpc-1.6` | ❌ Not needed | ✅ `opentelemetry-runtime-telemetry-java8` |

#### Note

Only Data Watcher needs HTTP client instrumentation because it calls Event Group Sync Cloud Function via HTTP POST (see `DataWatcher.kt` line 103). All other components use only gRPC for network communication.

### 5.4 OpenTelemetry SDK Initialization Pattern

```kotlin
// Initialize in global scope (Cloud Functions) or main method (MIG)
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import com.google.cloud.opentelemetry.metric.MetricExporter
import io.opentelemetry.instrumentation.runtimemetrics.java8.RuntimeMetrics
import java.time.Duration
import java.util.concurrent.TimeUnit

object OpenTelemetrySetup {
  private lateinit var openTelemetry: OpenTelemetry

  fun initialize(serviceName: String, environment: String, dataProviderName: String) {
    // Define resource attributes
    val resource = Resource.create(
      Attributes.builder()
        .put(ResourceAttributes.SERVICE_NAME, serviceName)
        .put("deployment.environment", environment)
        .put("data_provider_name", dataProviderName)
        .put("runtime", "cloud_function") // or "confidential_space"
        .build()
    )

    // Configure Google Cloud Monitoring exporter
    val metricExporter = MetricExporter.createWithDefaultConfiguration()

    // Build meter provider with periodic metric reader
    val meterProvider = SdkMeterProvider.builder()
      .setResource(resource)
      .registerMetricReader(
        PeriodicMetricReader.builder(metricExporter)
          .setInterval(Duration.ofSeconds(60))
          .build()
      )
      .build()

    // Build OpenTelemetry SDK
    openTelemetry = OpenTelemetrySdk.builder()
      .setMeterProvider(meterProvider)
      .build()

    // Install JVM runtime metrics instrumentation
    RuntimeMetrics.builder(openTelemetry).build()

    // Register shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
      meterProvider.shutdown().join(10, TimeUnit.SECONDS)
    })
  }

  fun getOpenTelemetry(): OpenTelemetry = openTelemetry
}

// Initialize at application startup
// Cloud Functions: in global scope
// MIG: in main() method
val OTEL = OpenTelemetrySetup.apply {
  initialize(
    serviceName = System.getenv("SERVICE_NAME") ?: "data-watcher",
    environment = System.getenv("ENVIRONMENT") ?: "dev",
    dataProviderName = System.getenv("DATA_PROVIDER_NAME") ?: "unknown"
  )
}.getOpenTelemetry()
```

### 5.5 Instrumenting gRPC Clients with GrpcTelemetry

The `opentelemetry-grpc-1.6` library provides `GrpcTelemetry` for automatic instrumentation of gRPC clients:

```kotlin
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import io.grpc.ManagedChannelBuilder

// Create GrpcTelemetry instance
val grpcTelemetry = GrpcTelemetry.create(OTEL)

// Create gRPC channel with OpenTelemetry interceptor
val channel = ManagedChannelBuilder
  .forAddress(host, port)
  .intercept(grpcTelemetry.newClientInterceptor()) // Automatic rpc.client.duration metrics
  .build()

// Create stub using instrumented channel
val stub = SecureComputationApiServiceGrpcKt.SecureComputationApiServiceCoroutineStub(channel)
```

#### What this provides

- Automatic `rpc.client.duration` histogram for all gRPC calls
- Attributes: `rpc.service`, `rpc.method`, `rpc.grpc.status_code`
- Resource attributes from SDK initialization (`service.name`, `deployment.environment`, `data_provider_name`)
- No manual metric recording required for gRPC clients

### 5.6 Custom Metrics Instrumentation Pattern

```kotlin
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.common.AttributeKey

// Obtain meter from initialized OpenTelemetry instance
private val meter: Meter = OTEL.getMeter("edpa-instrumentation")

// Define custom metrics
private val matchLatencyHistogram: DoubleHistogram = meter
  .histogramBuilder("edpa.data_watcher.match_latency")
  .setDescription("Time from blob detection to work item creation")
  .setUnit("s")
  .build()

private val queueWritesCounter: LongCounter = meter
  .counterBuilder("edpa.data_watcher.queue_writes")
  .setDescription("Number of work items written to queue")
  .build()

// Record metrics (no high-cardinality labels)
fun recordMatchLatency(latencySeconds: Double) {
  matchLatencyHistogram.record(latencySeconds)
}

fun incrementQueueWrites() {
  queueWritesCounter.add(1)
}
```

### 5.7 IAM Requirements

Each Cloud Function and Confidential Space MIG service account requires:

```hcl
# Grant monitoring metric writer role
resource "google_project_iam_member" "function_metrics_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.data_watcher.email}"
}

# Grant Cloud Trace agent role (for distributed tracing)
resource "google_project_iam_member" "function_trace_agent" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.data_watcher.email}"
}
```

### 5.8 Operational Best Practices

- **Version management**: Pin OpenTelemetry versions to avoid breaking changes; keep auto-instrumentation library versions aligned with SDK version
- **Metric cardinality**: Use only low-cardinality labels (status, protocol, method); avoid unbounded IDs
- **Export interval**: Default 60s is appropriate for most use cases; reduce to 30s for faster visibility during incidents
- **Error handling**: Wrap metric recording in try-catch to prevent instrumentation failures from breaking business logic
- **Cold start**: Initialize OpenTelemetry SDK in global scope (Cloud Functions) to avoid re-initialization overhead
- **Testing**: Use logging exporter in local dev/unit tests to verify metrics without GCP authentication
- **Memory**: SDK uses minimal memory (~10MB); monitor function memory usage and adjust if needed

## 6. Measurements Catalog

### Label Convention

EDPA custom metrics (`edpa.*`) use only low-cardinality labels: `status`, `protocol`, `method`. Resource attributes (`data_provider_name`, `deployment.environment`, `service.name`) are automatically applied to all metrics.

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

#### Metrics Table

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

#### Total Metrics

16 implemented (native platform metrics), 27 planned (custom EDPA metrics)

## 7. Component-Specific Monitoring

### 7.1 Secure Computation Public API (`SecureComputationApiServer`)

#### Implementation

[src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt)

#### Deployment

[src/main/k8s/secure_computation.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/secure_computation.cue#L103-L114), [src/main/k8s/dev/secure_computation_gke.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/secure_computation_gke.cue)

#### Terraform

[src/main/terraform/gcloud/modules/secure-computation/main.tf](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/secure-computation/main.tf)

#### Current Status

Fully instrumented via OpenTelemetry Java agent auto-injection on Kubernetes.

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

Custom metrics to instrument in [PublicApiServer.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server/PublicApiServer.kt) using OpenTelemetry SDK:

  - `secure_computation.work_items.created` (counter) by `queue` for monitoring Data Watcher submissions
  - `secure_computation.active_work_items` (gauge) for queue backlog visibility
  - `tls.handshake.errors` (counter) for mutual TLS failures

#### Dashboards & Alerts

Create Monitoring dashboard querying:
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

##### End-to-end latency

- `edpa.results_fulfiller.fulfillment_latency` (histogram) - Total time from requisition creation to fulfillment complete.
- `edpa.results_fulfiller.requisitions_processed{status}` (counter) - Requisitions processed by status.

##### Pipeline stage timings

Instrument in [ResultsFulfiller.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt) and [EventProcessingOrchestrator.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/EventProcessingOrchestrator.kt):

  - `edpa.results_fulfiller.vid_index_build_duration` (histogram) - Time to build VID index map (location: before pipeline execution).
  - `edpa.results_fulfiller.network_tasks_duration` (histogram) - Time to complete all network I/O (storage reads, impression metadata queries).
  - `edpa.results_fulfiller.frequency_vector_duration` (histogram) - Time to compute frequency vectors across all requisitions (existing `frequencyVectorTime` at [line 142](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt#L142)).
  - `edpa.results_fulfiller.builder_creation_duration{protocol}` (histogram) - Time to create protocol-specific fulfiller (existing `buildTime` at [line 206](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt#L206)).
  - `edpa.results_fulfiller.send_duration{protocol}` (histogram) - Time to send fulfillment result via gRPC (existing `sendTime` at [line 210](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt#L210)).
  - `edpa.results_fulfiller.kingdom_grpc_duration{method}` (histogram) - Per-RPC latency to Kingdom for each fulfillment call.

##### Throughput monitoring

- `edpa.results_fulfiller.event_processing_rate` (gauge) - Current events/sec processing rate measured over a rolling 10-second window; emit every 2 seconds during active processing.
- Implementation: Track event count with `AtomicLong`, compute rate as `(current_count - previous_count) / window_duration`, emit as gauge metric.

##### Infrastructure metrics

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

### EDPA Summary Dashboard

- SLO burn-down chart for Secure Computation API.
- Ingestion freshness per EDP (Data Watcher → Event Group Sync timelines).
- Queue backlog status (Pub/Sub + Scheduler).
- Error budget table for each component.

### Component Deep Dives

Pre-built JSON dashboards stored alongside Terraform ([src/main/terraform/gcloud/modules/edp-aggregator/dashboards/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator/dashboards) - to be created). Terraform will deploy using `google_monitoring_dashboard` with templates referencing metrics above.

### Reference examples

See existing Kingdom/Duchy dashboards at [src/main/terraform/gcloud/modules/kingdom/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/kingdom) (kingdom_dashboard_*.json) and [src/main/terraform/gcloud/modules/duchy/](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/duchy) (duchy_dashboard_*.json)

## 9. Testing & Validation

### Unit tests

Add OTel instrumentation unit tests verifying metric names registered (see pattern in existing tests).

### Integration tests

Extend `EdpAggregatorCorrectnessTest` ([src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt)) to assert that mock collectors receive spans for Event Group Sync path.

## 10. Implementation Action Plan

### Action Items

| # | Phase | P | Effort | Deps |
|---|-------|---|--------|------|
| 1a | Add OTel SDK Dependencies | P0 | Low | - |
| 1b | Create OpenTelemetry SDK Initialization Helper | P0 | Mid | #1a |
| 1c | Configure Cloud Function IAM (metricWriter, traceAgent) | P0 | Low | - |
| 1d | Configure MIG IAM (metricWriter, traceAgent) | P0 | Low | - |
| 2a | Instrument Data Watcher with OTel SDK | P0 | Mid | #1b, #1c |
| 2b | Instrument Results Fulfiller with OTel SDK | P0 | High | #1b, #1d |
| 2c | Instrument Requisition Fetcher with OTel SDK | P0 | Mid | #1b, #1c |
| 2d | Instrument Event Group Sync with OTel SDK | P1 | Mid | #1b, #1c |
| 2e | Instrument Data Availability Sync with OTel SDK | P1 | Mid | #1b, #1c |
| 2f | Instrument Secure Computation API Custom Metrics | P1 | Mid | - |
| 3 | Resolve Metadata Storage Instrumentation Status | P1 | Mid | - |
| 4 | Implement DLQ Listener Custom Metrics | P1 | High | #2b |
| 5a | Create Dashboard JSON Definitions | P0 | Mid | #2a-2f |
| 5b | Deploy Dashboards via Terraform | P0 | Low | #5a |
| 5c | Define SLOs in Terraform | P0 | Mid | #2a-2f |
| 6a | Write Unit Tests for Custom Metrics | P0 | Mid | #2a-2f |
| 6b | Extend Integration Tests with Metrics Validation | P0 | Mid | #2a-2f |
| 6c | Run Load Testing | P1 | Low | #5b, #6b |

### Key Files

#### Dependencies

Add OpenTelemetry SDK and auto-instrumentation library dependencies to the build

#### Terraform

`src/main/terraform/gcloud/modules/edp-aggregator/` - Add `monitoring_iam.tf`, `dashboards.tf`, `alerts.tf`, `slos.tf`

#### SDK Initialization

Create shared helper class (e.g., `OpenTelemetrySetup.kt`) for SDK initialization across all components

#### Cloud Functions

Instrument `*Function.kt` files with SDK initialization (global scope) and custom metrics; add `GrpcTelemetry` interceptors to gRPC channels

#### Results Fulfiller

Instrument [ResultsFulfiller.kt](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/ResultsFulfiller.kt) with SDK initialization and custom metrics; add `GrpcTelemetry` interceptors to Kingdom gRPC clients

#### gRPC Clients

Add `GrpcTelemetry.create(openTelemetry).newClientInterceptor()` to all gRPC channel builders for automatic `rpc.client.duration` metrics

## 11. References

### Existing Documentation

- **[Halo Metrics Deployment on GKE](../gke/metrics-deployment.md)** – Setup guide for OpenTelemetry Operator and Collector
- **[Cluster Configuration](../gke/cluster-config.md)** – GKE cluster setup including Workload Identity
- **[Correctness Test](../gke/correctness-test.md)** – Integration testing with metrics validation

### Configuration Files

#### OpenTelemetry Setup

- `src/main/k8s/open_telemetry.cue` – Base OTel Collector and Instrumentation definitions
- `src/main/k8s/dev/open_telemetry_gke.cue` – GKE-specific config with Google Cloud exporter
- `src/main/k8s/base.cue` (lines 442, 470, 590) – Pod annotation templates for Java agent injection

#### Secure Computation

- `src/main/k8s/secure_computation.cue` – Deployment specs for public/internal APIs
- `src/main/k8s/dev/secure_computation_gke.cue` – GKE deployment configuration
- `src/main/terraform/gcloud/modules/secure-computation/` – Terraform for GCP resources (Spanner, IAM, static IP)

#### EDP Aggregator

- `src/main/terraform/gcloud/modules/edp-aggregator/` – Terraform for Cloud Functions, MIGs, Pub/Sub, GCS buckets
- `src/main/terraform/gcloud/cmms/edp_aggregator.tf` – Root module configuration with secrets, schedulers, and function configs

### Terraform IAM for Monitoring

#### OpenTelemetry service account

Created in `src/main/terraform/gcloud/cmms/main.tf`:

- Workload Identity binding to K8s SA `open-telemetry`
- `roles/monitoring.metricWriter` (lines 105-108)
- `roles/cloudtrace.agent` (lines 99-102)

#### Secure Computation internal API

SA in `src/main/terraform/gcloud/modules/secure-computation/main.tf`:

- `roles/monitoring.metricWriter` (lines 25-29) for custom metrics
