/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.cloudrun

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter
import com.google.cloud.opentelemetry.trace.TraceExporter
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
import io.opentelemetry.semconv.ResourceAttributes
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * OpenTelemetry configuration for Cloud Run Functions.
 *
 * Gradle dependencies:
 *
 * ```gradle
 * dependencies {
 *   implementation 'com.google.cloud.opentelemetry:exporter-trace:0.35.0'
 *   implementation 'com.google.cloud.opentelemetry:exporter-metrics:0.35.0'
 *   implementation 'io.opentelemetry:opentelemetry-sdk:1.44.1'
 *   implementation 'io.opentelemetry:opentelemetry-sdk-trace:1.44.1'
 *   implementation 'io.opentelemetry:opentelemetry-sdk-metrics:1.44.1'
 *   implementation 'io.opentelemetry:opentelemetry-sdk-common:1.44.1'
 *   implementation 'io.opentelemetry.semconv:opentelemetry-semconv:1.22.0-alpha'
 * }
 * ```
 */
object OpenTelemetryCloudRunSetup {

  @Volatile
  private var openTelemetry: OpenTelemetry? = null

  @Volatile
  private var sdkTracerProvider: SdkTracerProvider? = null

  @Volatile
  private var sdkMeterProvider: SdkMeterProvider? = null

  init {
    Runtime.getRuntime().addShutdownHook(Thread {
      shutdown()
    })
  }

  /**
   * Configure OpenTelemetry with Cloud Monitoring and Cloud Trace exporters.
   * Thread-safe singleton initialization.
   *
   * @param serviceName The name of the service (defaults to K_SERVICE env var)
   * @param projectId GCP project ID (defaults to halo-cmm-dev)
   * @param region Cloud region (defaults to environment variable or us-central1)
   * @return Configured OpenTelemetry instance
   */
  @Synchronized
  fun configureOpenTelemetry(
    serviceName: String = System.getenv("K_SERVICE") ?: "unknown-service",
    projectId: String = System.getenv("GCP_PROJECT") ?: System.getenv("GOOGLE_CLOUD_PROJECT") ?: "halo-cmm-dev",
    region: String = System.getenv("FUNCTION_REGION") ?: System.getenv("CLOUD_REGION") ?: "us-central1"
  ): OpenTelemetry {
    if (openTelemetry != null) {
      return openTelemetry!!
    }

    val resource = Resource.getDefault().toBuilder()
      .put(ResourceAttributes.SERVICE_NAME, serviceName)
      .put("gcp.project.id", projectId)
      .put("cloud.region", region)
      .build()

    // Configure trace exporter with BatchSpanProcessor
    val traceExporter = TraceExporter.createWithDefaultConfiguration()
    val batchSpanProcessor = BatchSpanProcessor.builder(traceExporter)
      .setScheduleDelay(Duration.ofSeconds(5))
      .build()

    sdkTracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(batchSpanProcessor)
      .setResource(resource)
      .build()

    // Configure metrics exporter with PeriodicMetricReader (flush interval ≤ 5s)
    val monitoringExporter = GoogleCloudMetricExporter.createWithDefaultConfiguration()
    val periodicMetricReader = PeriodicMetricReader.builder(monitoringExporter)
      .setInterval(Duration.ofSeconds(5))
      .build()

    sdkMeterProvider = SdkMeterProvider.builder()
      .registerMetricReader(periodicMetricReader)
      .setResource(resource)
      .build()

    val sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(sdkTracerProvider!!)
      .setMeterProvider(sdkMeterProvider!!)
      .build()

    openTelemetry = sdk
    return sdk
  }

  /**
   * Get the configured OpenTelemetry instance.
   * Must call configureOpenTelemetry() first.
   */
  fun getOpenTelemetry(): OpenTelemetry {
    return openTelemetry ?: throw IllegalStateException("OpenTelemetry not configured. Call configureOpenTelemetry() first.")
  }

  /**
   * Get a tracer for instrumentation.
   */
  fun getTracer(instrumentationName: String): Tracer {
    return getOpenTelemetry().getTracer(instrumentationName)
  }

  /**
   * Get a meter for metrics.
   */
  fun getMeter(instrumentationName: String): Meter {
    return getOpenTelemetry().getMeter(instrumentationName)
  }

  /**
   * Force flush all pending telemetry data.
   */
  fun forceFlush() {
    sdkTracerProvider?.forceFlush()?.join(10, TimeUnit.SECONDS)
    sdkMeterProvider?.forceFlush()?.join(10, TimeUnit.SECONDS)
  }

  /**
   * Shutdown OpenTelemetry providers.
   * Called automatically on SIGTERM via shutdown hook.
   */
  @Synchronized
  fun shutdown() {
    sdkTracerProvider?.let { provider ->
      provider.forceFlush().join(10, TimeUnit.SECONDS)
      provider.shutdown().join(10, TimeUnit.SECONDS)
    }
    sdkMeterProvider?.let { provider ->
      provider.forceFlush().join(10, TimeUnit.SECONDS)
      provider.shutdown().join(10, TimeUnit.SECONDS)
    }
    openTelemetry = null
    sdkTracerProvider = null
    sdkMeterProvider = null
  }
}

/**
 * Example Cloud Run Function implementation using OpenTelemetry.
 *
 * IAM Requirements:
 * - roles/monitoring.metricWriter
 * - roles/cloudtrace.agent
 *
 * Uses Application Default Credentials.
 */
class ExampleCloudRunFunction {

  companion object {
    // Initialize once per container (outside handler)
    private val openTelemetry: OpenTelemetry by lazy {
      OpenTelemetryCloudRunSetup.configureOpenTelemetry(
        serviceName = "example-cloud-run-function"
      )
    }

    private val tracer: Tracer by lazy {
      OpenTelemetryCloudRunSetup.getTracer("org.wfanet.measurement.cloudrun.ExampleCloudRunFunction")
    }

    private val meter: Meter by lazy {
      OpenTelemetryCloudRunSetup.getMeter("org.wfanet.measurement.cloudrun.ExampleCloudRunFunction")
    }

    private val requestCounter: LongCounter by lazy {
      meter.counterBuilder("cloudrun.function.requests")
        .setDescription("Number of function invocations")
        .setUnit("{request}")
        .build()
    }
  }

  /**
   * Sample request handler with OpenTelemetry instrumentation.
   */
  fun handleRequest(requestData: Map<String, Any>): Map<String, Any> {
    val span = tracer.spanBuilder("ExampleCloudRunFunction.handleRequest")
      .setAttribute("request.id", requestData["id"]?.toString() ?: "unknown")
      .startSpan()

    return try {
      span.makeCurrent().use {
        // Increment request counter
        requestCounter.add(
          1,
          Attributes.builder()
            .put("status", "success")
            .build()
        )

        // Simulate processing
        val result = processRequest(requestData)

        span.setAttribute("response.size", result.size.toLong())
        span.setStatus(StatusCode.OK)

        result
      }
    } catch (e: Exception) {
      span.recordException(e)
      span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")

      // Increment error counter
      requestCounter.add(
        1,
        Attributes.builder()
          .put("status", "error")
          .put("error.type", e.javaClass.simpleName)
          .build()
      )

      throw e
    } finally {
      span.end()
    }
  }

  private fun processRequest(requestData: Map<String, Any>): Map<String, Any> {
    val childSpan = tracer.spanBuilder("processRequest")
      .startSpan()

    return try {
      childSpan.makeCurrent().use {
        // Business logic here
        mapOf(
          "status" to "success",
          "data" to requestData,
          "timestamp" to System.currentTimeMillis()
        )
      }
    } finally {
      childSpan.end()
    }
  }
}
