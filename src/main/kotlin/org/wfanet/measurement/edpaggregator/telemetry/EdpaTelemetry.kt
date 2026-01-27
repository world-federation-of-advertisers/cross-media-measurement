// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.telemetry

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.runtimemetrics.java8.Classes
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools
import io.opentelemetry.instrumentation.runtimemetrics.java8.Threads
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.logs.SdkLoggerProvider
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.trace.SdkTracerProvider
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.cancellation.CancellationException
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry utilities for EDPA Cloud Functions.
 *
 * Uses common-jvm's [Instrumentation.openTelemetry] as the single SDK instance, ensuring all
 * telemetry (gRPC traces, metrics, etc.) goes through the same pipeline and can be flushed
 * together before Cloud Functions freeze.
 *
 * Provides:
 * - JVM runtime metrics (CPU, memory, GC, threads)
 * - Flush capability for Cloud Functions
 *
 * Required environment variables (for OpenTelemetry autoconfiguration):
 * - OTEL_SERVICE_NAME: Service identifier
 * - OTEL_METRICS_EXPORTER: Metric exporter (e.g., "google_cloud_monitoring")
 * - OTEL_TRACES_EXPORTER: Trace exporter (e.g., "google_cloud_trace")
 * - OTEL_METRIC_EXPORT_INTERVAL: Export interval in ms
 *
 * See: https://opentelemetry.io/docs/languages/java/configuration/
 */
object EdpaTelemetry {
  private val logger = Logger.getLogger(EdpaTelemetry::class.java.name)

  private val meterProvider: SdkMeterProvider
  private val tracerProvider: SdkTracerProvider
  private val loggerProvider: SdkLoggerProvider
  private val meter: Meter
  private val tracer: Tracer

  /** Ensure the init block of this object is being executed. */
  fun ensureInitialized() {
    logger.info("Ensuring EdpaTelemetry is initialized")
  }

  /**
   * Initializes EdpaTelemetry using the global [Instrumentation.openTelemetry] SDK.
   *
   * Registers JVM runtime metrics and obtains references to SDK providers for flush operations.
   */
  init {
    logger.info("Initializing EdpaTelemetry using Instrumentation.openTelemetry")

    val openTelemetry = Instrumentation.openTelemetry

    // Cast to OpenTelemetrySdk to access providers for flush/shutdown operations.
    // Instrumentation.openTelemetry is configured via autoconfiguration and is an OpenTelemetrySdk.
    val sdk =
      openTelemetry as? OpenTelemetrySdk
        ?: throw IllegalStateException(
          "Instrumentation.openTelemetry must be an OpenTelemetrySdk for flush support"
        )

    meterProvider = sdk.sdkMeterProvider
    tracerProvider = sdk.sdkTracerProvider
    loggerProvider = sdk.sdkLoggerProvider

    // Install JVM runtime metrics instrumentation on the global SDK
    Classes.registerObservers(openTelemetry)
    Cpu.registerObservers(openTelemetry)
    GarbageCollector.registerObservers(openTelemetry)
    MemoryPools.registerObservers(openTelemetry)
    Threads.registerObservers(openTelemetry)

    // Create meter and tracer instances
    meter = openTelemetry.getMeter("edpa-instrumentation")
    tracer = openTelemetry.getTracer("edpa-instrumentation")

    logger.info("EdpaTelemetry initialized successfully")
  }

  /**
   * Forces immediate export of all pending metrics, traces, and logs in parallel.
   *
   * **Critical for Cloud Functions**: Call this at the end of the function handler to ensure all
   * telemetry is exported before the function instance is frozen. This flushes all telemetry
   * including gRPC traces, custom metrics, and JVM runtime metrics since they all use the same
   * global [Instrumentation.openTelemetry] SDK.
   *
   * **Performance**: Flushes metrics, traces, and logs in parallel to minimize latency. Total flush
   * time ~= max(metric_flush_time, trace_flush_time, log_flush_time).
   *
   * Example:
   * ```
   * fun handleRequest(request: Request): Response {
   *   try {
   *     // Process request
   *     return response
   *   } finally {
   *     EdpaTelemetry.flush()
   *   }
   * }
   * ```
   *
   * @param timeout Maximum time to wait for flush (default 5 seconds)
   * @return true if flush completed successfully, false if timeout or error
   */
  fun flush(timeout: Duration = Duration.ofSeconds(5)): Boolean {
    return try {
      // Start all flushes in parallel
      val metricFlush = meterProvider.forceFlush()
      val traceFlush = tracerProvider.forceFlush()
      val logFlush = loggerProvider.forceFlush()

      // Wait for all to complete (parallel execution)
      val metricFlushResult = metricFlush.join(timeout.toMillis(), TimeUnit.MILLISECONDS).isSuccess
      val traceFlushResult = traceFlush.join(timeout.toMillis(), TimeUnit.MILLISECONDS).isSuccess
      val logFlushResult = logFlush.join(timeout.toMillis(), TimeUnit.MILLISECONDS).isSuccess

      when {
        metricFlushResult && traceFlushResult && logFlushResult -> {
          logger.fine("OpenTelemetry flush completed successfully")
          true
        }
        else -> {
          logger.warning("OpenTelemetry flush completed with errors")
          false
        }
      }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Error during OpenTelemetry flush", e)
      false
    }
  }

  /**
   * Shuts down the OpenTelemetry SDK, flushing all pending telemetry.
   *
   * **Warning**: This shuts down the global [Instrumentation.openTelemetry] SDK, which will affect
   * all code using it. Only use this in tests or when the process is terminating.
   */
  fun shutdown() {
    try {
      meterProvider.shutdown().join(10, TimeUnit.SECONDS)
      tracerProvider.shutdown().join(10, TimeUnit.SECONDS)
      loggerProvider.shutdown().join(10, TimeUnit.SECONDS)
      logger.info("OpenTelemetry SDK shut down successfully")
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Error during OpenTelemetry shutdown", e)
    }
  }
}

/**
 * Executes a block within a traced span, handling span lifecycle and error recording.
 *
 * The span is automatically:
 * - Started and made current before the block executes
 * - Set to OK status on success
 * - Set to ERROR status and records the exception on failure
 * - Ended in the finally block
 *
 * CancellationException is re-thrown without being recorded as an error.
 *
 * Example:
 * ```
 * withSpan(
 *   tracer,
 *   "MyOperation",
 *   Attributes.of(AttributeKey.stringKey("user_id"), userId),
 *   errorMessage = "MyOperation failed"
 * ) { span ->
 *   // Span is active here
 *   // You can add additional attributes: span.setAttribute(...)
 *   doWork()
 * }
 * ```
 *
 * @param tracer The tracer to use for creating the span
 * @param spanName Name of the span
 * @param attributes Initial attributes to set on the span
 * @param errorMessage Default error message if exception message is null
 * @param block The block to execute within the span context
 * @return The result of the block
 * @throws Exception Any exception thrown by the block (except it's properly recorded in the span)
 */
inline fun <T> withSpan(
  tracer: Tracer,
  spanName: String,
  attributes: Attributes = Attributes.empty(),
  errorMessage: String = "Operation failed",
  block: (Span) -> T,
): T {
  val span = tracer.spanBuilder(spanName).setAllAttributes(attributes).startSpan()
  val scope = span.makeCurrent()

  try {
    val result = block(span)
    span.setStatus(StatusCode.OK)
    return result
  } catch (e: Exception) {
    if (e is CancellationException) throw e

    span.recordException(e)
    span.setStatus(StatusCode.ERROR, e.message ?: errorMessage)
    throw e
  } finally {
    scope.close()
    span.end()
  }
}
