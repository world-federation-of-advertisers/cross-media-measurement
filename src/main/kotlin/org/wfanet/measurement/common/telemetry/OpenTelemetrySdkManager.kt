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

package org.wfanet.measurement.common.telemetry

import io.opentelemetry.instrumentation.runtimemetrics.java8.Classes
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools
import io.opentelemetry.instrumentation.runtimemetrics.java8.Threads
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger

/** Process-wide OpenTelemetry SDK initialized from standard environment variables. */
object OpenTelemetrySdkManager {
  private val logger = Logger.getLogger(OpenTelemetrySdkManager::class.java.name)
  private val sdk = AutoConfiguredOpenTelemetrySdk.initialize().openTelemetrySdk
  private val meterProvider = sdk.sdkMeterProvider
  private val tracerProvider = sdk.sdkTracerProvider
  private val loggerProvider = sdk.sdkLoggerProvider

  init {
    Classes.registerObservers(sdk)
    Cpu.registerObservers(sdk)
    GarbageCollector.registerObservers(sdk)
    MemoryPools.registerObservers(sdk)
    Threads.registerObservers(sdk)
    logger.info("OpenTelemetry SDK initialized")
  }

  /** Ensures that the SDK has been initialized and installed globally. */
  fun ensureInitialized() {
    logger.fine("OpenTelemetry SDK is initialized")
  }

  /** Flushes pending metrics, traces, and logs, allowing [timeout] for each provider. */
  fun flush(timeout: Duration): Boolean {
    return try {
      val metricFlush = meterProvider.forceFlush()
      val traceFlush = tracerProvider.forceFlush()
      val logFlush = loggerProvider.forceFlush()
      val timeoutMillis = timeout.toMillis()

      val metricFlushSucceeded = metricFlush.join(timeoutMillis, TimeUnit.MILLISECONDS).isSuccess
      val traceFlushSucceeded = traceFlush.join(timeoutMillis, TimeUnit.MILLISECONDS).isSuccess
      val logFlushSucceeded = logFlush.join(timeoutMillis, TimeUnit.MILLISECONDS).isSuccess
      val succeeded = metricFlushSucceeded && traceFlushSucceeded && logFlushSucceeded
      if (!succeeded) {
        logger.warning("OpenTelemetry flush completed with errors")
      }
      succeeded
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Error during OpenTelemetry flush", e)
      false
    }
  }

  /** Shuts down the SDK. This is only intended for tests. */
  fun shutdown() {
    try {
      meterProvider.shutdown().join(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
      tracerProvider.shutdown().join(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
      loggerProvider.shutdown().join(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Error during OpenTelemetry shutdown", e)
    }
  }

  private val SHUTDOWN_TIMEOUT = Duration.ofSeconds(10)
}
