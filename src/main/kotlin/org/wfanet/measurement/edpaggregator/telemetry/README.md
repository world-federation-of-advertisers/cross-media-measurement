# EDPA Telemetry Module

Shared OpenTelemetry infrastructure for EDPA components. Components own their metrics.

## Usage

### Create Component Metrics

Each component creates its own metrics file:

**`ResultsFulfillerMetrics.kt`:**
```kotlin
package org.wfanet.measurement.edpaggregator.resultsfulfiller

import io.opentelemetry.api.metrics.*
import org.wfanet.measurement.common.Instrumentation

object ResultsFulfillerMetrics {
  private val meter = Instrumentation.meter

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
import org.wfanet.measurement.common.Instrumentation

val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)

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

- OpenTelemetry: https://opentelemetry.io/docs/instrumentation/java/
