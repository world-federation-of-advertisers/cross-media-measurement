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

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.logs.Logger as OtelLogger
import io.opentelemetry.api.logs.Severity
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import java.io.PrintWriter
import java.io.StringWriter
import io.opentelemetry.instrumentation.runtimemetrics.java8.Classes
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools
import io.opentelemetry.instrumentation.runtimemetrics.java8.Threads
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.logs.SdkLoggerProvider
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.trace.SdkTracerProvider
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.LazyThreadSafetyMode
import kotlinx.coroutines.CancellationException

/**
 * OpenTelemetry SDK initialization for EDPA components using autoconfiguration.
 *
 * Uses OpenTelemetry's SDK autoconfiguration for zero-code setup via environment variables.
 * Automatically initializes on first access and registers globally so common-jvm code
 * (GcsStorageClient, etc.) and all application code can access the configured SDK.
 *
 * Provides:
 * - Google Cloud Monitoring metric exporter
 * - Google Cloud Trace exporter
 * - Console logging exporter (logs to stdout, collected by Google Cloud)
 * - JVM runtime metrics
 *
 * Usage:
 * ```
 * // Access globally registered OpenTelemetry via Instrumentation singleton
 * import org.wfanet.measurement.common.Instrumentation
 *
 * val meter = Instrumentation.meter
 * val tracer = Instrumentation.openTelemetry.getTracer("my-component")
 * ```
 *
 * Configuration via environment variables:
 * - OTEL_SERVICE_NAME: Service identifier (e.g., "data-watcher")
 * - OTEL_METRICS_EXPORTER: Metric exporter (default: google_cloud_monitoring)
 * - OTEL_TRACES_EXPORTER: Trace exporter (default: google_cloud_trace)
 * - OTEL_LOGS_EXPORTER: Log exporter (default: logging - outputs to stdout)
 * - OTEL_METRIC_EXPORT_INTERVAL: Export interval in ms (default: 60000)
 * - K_SERVICE: Fallback service name (set by Cloud Run and Cloud Functions Gen2)
 *
 * See: https://opentelemetry.io/docs/languages/java/configuration/
 */
object EdpaTelemetry {
  private val logger = Logger.getLogger(EdpaTelemetry::class.java.name)

  private data class TelemetryComponents(
    val openTelemetry: OpenTelemetrySdk,
    val meterProvider: SdkMeterProvider,
    val tracerProvider: SdkTracerProvider,
    val loggerProvider: SdkLoggerProvider,
    val meter: Meter,
    val tracer: Tracer,
  )

  private val factoryLock = Any()

  @Volatile
  private var telemetryFactory: () -> TelemetryComponents = { createDefaultTelemetry() }

  // Lazy init runs with synchronized semantics, so the combination of factoryLock + Lazy ensures
  // test overrides install a factory exactly once and the actual SDK is built at most once.
  private val telemetry: Lazy<TelemetryComponents> =
    lazy(LazyThreadSafetyMode.SYNCHRONIZED) { telemetryFactory() }

  /**
   * Ensure the init block of this object is being executed.
   */
  fun ensureInitialized() {
    logger.info("Ensuring EdpaTelemetry is initialized")
    telemetry.value
  }

  /**
   * Gets an OpenTelemetry logger for the specified instrumentation scope.
   *
   * Use this to emit logs directly through the OpenTelemetry pipeline.
   *
   * Example:
   * ```
   * val otelLogger = EdpaTelemetry.getLogger("my-component")
   * otelLogger.info("Processing started", Attributes.builder()
   *   .put("user_id", userId)
   *   .build())
   * ```
   *
   * @param instrumentationScopeName Name of the component/module emitting logs
   * @return An OpenTelemetry Logger instance
   */
  fun getLogger(instrumentationScopeName: String): OtelLogger {
    return telemetryComponents().openTelemetry.logsBridge.get(instrumentationScopeName)
  }

  /**
   * Initializes OpenTelemetry SDK using autoconfiguration.
   *
   * Automatically called during object initialization using environment variables.
   * Idempotent - subsequent calls are ignored.
   *
   * Configuration is done via environment variables:
   * - OTEL_SERVICE_NAME or K_SERVICE: Service identifier
   * - OTEL_METRICS_EXPORTER: Metric exporter (defaults to google_cloud_monitoring)
   * - OTEL_TRACES_EXPORTER: Trace exporter (defaults to google_cloud_trace)
   * - OTEL_METRIC_EXPORT_INTERVAL: Export interval in ms (defaults to 60000)
   */
  /**
   * Overrides the default autoconfigured SDK with a caller-provided instance.
   *
   * Call this from tests before any other EdpaTelemetry method to avoid initializing
   * production exporters. The supplied SDK can use in-memory exporters or no-op providers.
   *
   * Note: This does not call GlobalOpenTelemetry.set; tests should do so if required.
   *
   * @param openTelemetrySdk Pre-configured SDK instance to use for telemetry
   * @param registerRuntimeMetrics Whether to register JVM runtime observers for the SDK
   *                               (defaults to false to keep tests lean)
   */
  fun configureForTests(
    openTelemetrySdk: OpenTelemetrySdk,
    registerRuntimeMetrics: Boolean = false,
  ) {
    synchronized(factoryLock) {
      check(!telemetry.isInitialized()) {
        "EdpaTelemetry is already initialized; configure tests before first use."
      }
      telemetryFactory = {
        if (registerRuntimeMetrics) {
          installRuntimeMetrics(openTelemetrySdk)
        }
        createTelemetryFromExistingSdk(openTelemetrySdk).also {
          logger.info("Using test OpenTelemetry SDK for EdpaTelemetry")
        }
      }
    }
  }

  /**
   * Convenience helper for tests that just need a no-op SDK.
   */
  fun useNoopTelemetryForTests() {
    configureForTests(OpenTelemetrySdk.builder().build())
  }

  /**
   * Forces immediate export of all pending metrics, traces, and logs in parallel.
   *
   * **Critical for Cloud Functions**: Call this at the end of the function handler
   * to ensure all telemetry is exported before the function instance is frozen.
   *
   * **Performance**: Flushes metrics, traces, and logs in parallel to minimize latency.
   * Total flush time ~= max(metric_flush_time, trace_flush_time, log_flush_time).
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
    val components = telemetryOrNull()
      ?: return true

    return try {
      // Start all flushes in parallel
      val metricFlush = components.meterProvider.forceFlush()
      val traceFlush = components.tracerProvider.forceFlush()
      val logFlush = components.loggerProvider.forceFlush()

      // Wait for all to complete (parallel execution)
      val metricFlushResult =
        metricFlush.join(timeout.toMillis(), TimeUnit.MILLISECONDS).isSuccess
      val traceFlushResult =
        traceFlush.join(timeout.toMillis(), TimeUnit.MILLISECONDS).isSuccess
      val logFlushResult =
        logFlush.join(timeout.toMillis(), TimeUnit.MILLISECONDS).isSuccess

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
   * Only needed for testing.
   */
  fun shutdown() {
    val components = telemetryOrNull() ?: return
    try {
      components.meterProvider.shutdown().join(10, TimeUnit.SECONDS)
      components.tracerProvider.shutdown().join(10, TimeUnit.SECONDS)
      components.loggerProvider.shutdown().join(10, TimeUnit.SECONDS)
      logger.info("OpenTelemetry SDK shut down successfully")
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Error during OpenTelemetry shutdown", e)
    }
  }

  private fun telemetryComponents(): TelemetryComponents = telemetry.value

  private fun telemetryOrNull(): TelemetryComponents? {
    return if (telemetry.isInitialized()) telemetry.value else null
  }

  private fun createDefaultTelemetry(): TelemetryComponents {
    val serviceName =
      System.getenv("OTEL_SERVICE_NAME")
        ?: System.getenv("K_SERVICE") // Cloud Run and Cloud Functions Gen2
        ?: "unknown-service"

    logger.info("Initializing OpenTelemetry SDK for $serviceName using autoconfiguration")

    val defaultConfig =
      mapOf(
        "otel.service.name" to serviceName,
        "otel.metrics.exporter" to "google_cloud_monitoring",
        "otel.traces.exporter" to "google_cloud_trace",
        "otel.metric.export.interval" to "60000",
        "otel.logs.exporter" to "console" // Console exporter - Google Cloud collects from stdout
      )

    for ((key, value) in defaultConfig) {
      if (System.getProperty(key).isNullOrEmpty()) {
        System.setProperty(key, value)
      }
    }

    val autoConfiguredSdk =
      AutoConfiguredOpenTelemetrySdk.builder().setResultAsGlobal().build()

    val openTelemetrySdk = autoConfiguredSdk.openTelemetrySdk
    installRuntimeMetrics(openTelemetrySdk)

    logger.info("OpenTelemetry SDK initialized successfully")
    return createTelemetryFromExistingSdk(openTelemetrySdk)
  }

  private fun createTelemetryFromExistingSdk(
    openTelemetrySdk: OpenTelemetrySdk
  ): TelemetryComponents {
    return TelemetryComponents(
      openTelemetry = openTelemetrySdk,
      meterProvider = openTelemetrySdk.sdkMeterProvider,
      tracerProvider = openTelemetrySdk.sdkTracerProvider,
      loggerProvider = openTelemetrySdk.sdkLoggerProvider,
      meter = openTelemetrySdk.getMeter("edpa-instrumentation"),
      tracer = openTelemetrySdk.getTracer("edpa-instrumentation"),
    )
  }

  private fun installRuntimeMetrics(openTelemetry: OpenTelemetry) {
    Classes.registerObservers(openTelemetry)
    Cpu.registerObservers(openTelemetry)
    GarbageCollector.registerObservers(openTelemetry)
    MemoryPools.registerObservers(openTelemetry)
    Threads.registerObservers(openTelemetry)
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
  block: (Span) -> T
): T {
  val span = tracer.spanBuilder(spanName)
    .setAllAttributes(attributes)
    .startSpan()
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

/**
 * Extension functions for OpenTelemetry Logger matching java.util.logging.Logger API.
 */

/**
 * Emits a FINE (DEBUG) level log record.
 *
 * @param message The log message
 * @param attributes Optional attributes to attach to the log record
 * @param throwable Optional exception to attach to the log record
 */
fun OtelLogger.fine(
  message: String,
  attributes: Attributes = Attributes.empty(),
  throwable: Throwable? = null
) {
  val builder = logRecordBuilder()
    .setSeverity(Severity.DEBUG)
    .setBody(message)
    .setAllAttributes(attributes)

  throwable?.let { addExceptionAttributes(builder, it) }
  builder.emit()
}

/**
 * Emits an INFO level log record.
 *
 * @param message The log message
 * @param attributes Optional attributes to attach to the log record
 * @param throwable Optional exception to attach to the log record
 */
fun OtelLogger.info(
  message: String,
  attributes: Attributes = Attributes.empty(),
  throwable: Throwable? = null
) {
  val builder = logRecordBuilder()
    .setSeverity(Severity.INFO)
    .setBody(message)
    .setAllAttributes(attributes)

  throwable?.let { addExceptionAttributes(builder, it) }
  builder.emit()
}

/**
 * Emits a WARNING level log record.
 *
 * @param message The log message
 * @param attributes Optional attributes to attach to the log record
 * @param throwable Optional exception to attach to the log record
 */
fun OtelLogger.warning(
  message: String,
  attributes: Attributes = Attributes.empty(),
  throwable: Throwable? = null
) {
  val builder = logRecordBuilder()
    .setSeverity(Severity.WARN)
    .setBody(message)
    .setAllAttributes(attributes)

  throwable?.let { addExceptionAttributes(builder, it) }
  builder.emit()
}

/**
 * Emits a SEVERE (ERROR) level log record.
 *
 * @param message The log message
 * @param attributes Optional attributes to attach to the log record
 * @param throwable Optional exception to attach to the log record
 */
fun OtelLogger.severe(
  message: String,
  attributes: Attributes = Attributes.empty(),
  throwable: Throwable? = null
) {
  val builder = logRecordBuilder()
    .setSeverity(Severity.ERROR)
    .setBody(message)
    .setAllAttributes(attributes)

  throwable?.let { addExceptionAttributes(builder, it) }
  builder.emit()
}

/**
 * Adds exception attributes to a log record builder following OpenTelemetry semantic conventions.
 *
 * @param builder The log record builder
 * @param throwable The exception to record
 */
private fun addExceptionAttributes(
  builder: io.opentelemetry.api.logs.LogRecordBuilder,
  throwable: Throwable
) {
  builder.setAttribute(AttributeKey.stringKey("exception.type"), throwable.javaClass.name)
  builder.setAttribute(AttributeKey.stringKey("exception.message"), throwable.message ?: "")
  builder.setAttribute(AttributeKey.stringKey("exception.stacktrace"), getStackTrace(throwable))
}

/**
 * Converts a throwable's stack trace to a string.
 */
private fun getStackTrace(throwable: Throwable): String {
  val sw = StringWriter()
  val pw = PrintWriter(sw)
  throwable.printStackTrace(pw)
  return sw.toString()
}
