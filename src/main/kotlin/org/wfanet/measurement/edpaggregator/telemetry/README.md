# EDPA Telemetry Module

Shared OpenTelemetry infrastructure for EDPA components. Components own their metrics.

## Usage

### Initialize Once

**Cloud Functions (global scope):**
```kotlin
val TELEMETRY = EdpaTelemetry.apply { initialize(serviceName = "data-watcher") }

// Function handler - flush metrics before exit
fun handleEvent(event: CloudEvent) {
  try {
    // Process event
  } finally {
    EdpaTelemetry.flush()  // Critical: ensures metrics are exported
  }
}
```

**MIG (main method):**
```kotlin
fun main() {
  EdpaTelemetry.initialize(serviceName = "results-fulfiller")
  // ... (shutdown hook handles flush automatically)
}
```

### Create Component Metrics

Each component creates its own metrics file:

**`ResultsFulfillerMetrics.kt`:**
```kotlin
package org.wfanet.measurement.edpaggregator.resultsfulfiller

import io.opentelemetry.api.metrics.*
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry

object ResultsFulfillerMetrics {
  private val meter = EdpaTelemetry.getMeter()

  val fulfillmentLatency: DoubleHistogram = meter
    .histogramBuilder("edpa.results_fulfiller.fulfillment_latency")
    .setUnit("s")
    .build()

  val requisitionsProcessed: LongCounter = meter
    .counterBuilder("edpa.results_fulfiller.requisitions_processed")
    .build()
}
```

### Instrument Code

```kotlin
import kotlin.time.TimeSource

val start = TimeSource.Monotonic.markNow()
try {
  processRequisition(requisition)
  ResultsFulfillerMetrics.requisitionsProcessed.add(1)
} finally {
  val durationSeconds = start.elapsedNow().inWholeNanoseconds / 1e9
  ResultsFulfillerMetrics.fulfillmentLatency.record(durationSeconds)
}
```

## Distributed Tracing

**Create spans:**
```kotlin
suspend fun fulfill(reportId: String, requisition: Requisition) {
  TracedOperation.trace(
    spanName = "report_fulfillment",
    attributes = mapOf("report_id" to reportId)
  ) {
    processRequisition(requisition)
  }
}
```

**Query traces:**

Traces can be queried by business identifiers using span attributes:
```
# In Cloud Trace Console, filter by:
report_id = "measurementConsumers/mc123/reports/r456"
```

## gRPC Auto-Instrumentation

```kotlin
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry

val grpcTelemetry = GrpcTelemetry.create(EdpaTelemetry.getOpenTelemetry())

val channel = ManagedChannelBuilder
  .forAddress(host, port)
  .intercept(grpcTelemetry.newClientInterceptor())
  .build()
```

Automatically emits `rpc.client.duration` metrics.

## Metric Naming

```
edpa.{component}.{metric_name}
```

Examples:
- `edpa.results_fulfiller.fulfillment_latency`
- `edpa.data_watcher.queue_writes`
- `edpa.event_group.sync_latency`

**Attributes:** Only low-cardinality (`status`, `protocol`, `method`). Never use IDs.

**Units:** `"s"` (seconds), `"By"` (bytes), `"{items}/s"` (rate)

## Component Metrics (from monitoring plan)

### Data Watcher
- `edpa.data_watcher.match_latency` (histogram, s)
- `edpa.data_watcher.queue_writes` (counter)
- `edpa.data_watcher.done_blob_timestamp` (histogram, ms)

### Results Fulfiller
- `edpa.results_fulfiller.fulfillment_latency` (histogram, s)
- `edpa.results_fulfiller.requisitions_processed` (counter)
- `edpa.results_fulfiller.vid_index_build_duration` (histogram, s)
- `edpa.results_fulfiller.frequency_vector_duration` (histogram, s)

### Requisition Fetcher
- `edpa.requisition_fetcher.fetch_latency` (histogram, s)
- `edpa.requisition_fetcher.requisitions_fetched` (counter)

### Event Group Sync
- `edpa.event_group.sync_attempts` (counter)
- `edpa.event_group.sync_success` (counter)
- `edpa.event_group.sync_latency` (histogram, s)

### Data Availability Sync
- `edpa.data_availability.sync_duration` (histogram, s)
- `edpa.data_availability.records_synced` (counter)

## Cloud Functions Best Practices

### Always Flush Before Exit

Cloud Functions can be frozen after execution completes. **Always call `flush()`** in a `finally` block:

```kotlin
fun handleRequest(request: Request): Response {
  val start = TimeSource.Monotonic.markNow()
  try {
    // Process request
    MyMetrics.requestCounter.add(1)
    return processRequest(request)
  } catch (e: Exception) {
    MyMetrics.errorCounter.add(1)
    throw e
  } finally {
    MyMetrics.requestDuration.record(start.elapsedNow().inWholeNanoseconds / 1e9)

    // Flush metrics (adds 100-200ms latency, but necessary to prevent data loss)
    // Metrics and traces are flushed in parallel to minimize overhead
    EdpaTelemetry.flush()
  }
}
```

### Why Flush is Critical

- **Periodic export**: Metrics export every 60s by default
- **CPU freeze**: After function returns, background threads **cannot access CPU** (even during warm period)
- **Export thread frozen**: OpenTelemetry's periodic export cannot run after function terminates
- **Lost metrics**: Without flush, metrics recorded during execution will never be exported
- **Applies to all functions**: Even if instance stays warm 5-15 minutes, export thread remains frozen

### Flush Timeout and Latency Impact

**Latency overhead**: `flush()` blocks until metrics are exported (network call to Cloud Monitoring)

**Parallel flush optimization**: Metrics and traces are flushed **in parallel**, so total time = max(metric_flush, trace_flush) rather than sum.

**Estimated latency per export**:
- Metric collection: 1-5ms (serialization)
- Network round-trip to Cloud Monitoring API: 50-200ms (within GCP region)
- API processing: 20-50ms
- **Per export**: 70-250ms

**Total flush time** (parallel execution):
- If metrics take 150ms and traces take 120ms → **150ms total** (not 270ms)
- **Typical**: **100-200ms** for small batches (2-10 metrics per invocation)
- **Worst case**: Up to timeout (default 5s) if network issues or API throttling


## IAM Requirements

```hcl
resource "google_project_iam_member" "metrics_writer" {
  role   = "roles/monitoring.metricWriter"
  member = "serviceAccount:${sa_email}"
}

resource "google_project_iam_member" "trace_agent" {
  role   = "roles/cloudtrace.agent"
  member = "serviceAccount:${sa_email}"
}
```

## Testing

```kotlin
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader

val metricReader = InMemoryMetricReader.create()
// ... configure with test exporter ...

MyMetrics.recordLatency(1.5)

val metrics = metricReader.collectAllMetrics()
assertThat(metrics[0].name).isEqualTo("edpa.component.latency")
```

## Reference

- Monitoring Plan: `edpa_monitoring_plan.md` Section 5 (Strategy), Section 6 (Metrics Catalog)
- OpenTelemetry: https://opentelemetry.io/docs/instrumentation/java/
