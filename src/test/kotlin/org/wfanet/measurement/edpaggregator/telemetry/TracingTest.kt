/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.telemetry

import com.google.common.truth.Truth.assertThat
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Instrumentation

@RunWith(JUnit4::class)
class TracingTest {
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    spanExporter = InMemorySpanExporter.create()
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
  }

  @After
  fun cleanupTelemetry() {
    openTelemetry.close()
  }

  @Test
  fun `traceSuspending binds attributed span across suspension`() = runBlocking {
    val attributeKey = AttributeKey.stringKey("test.attribute")

    val spanContext =
      Tracing.traceSuspending(
        spanName = "test-span",
        attributes = Attributes.of(attributeKey, "test-value"),
      ) {
        yield()
        Span.current().spanContext
      }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(attributeKey)).isEqualTo("test-value")
    assertThat(spanContext.spanId).isEqualTo(span.spanId)
    assertThat(span.status.statusCode).isEqualTo(StatusCode.UNSET)
  }

  @Test
  fun `traceSuspending does not leak span after resuming on another thread`() = runBlocking {
    val callerExecutor = Executors.newSingleThreadExecutor()
    val resumeExecutor = Executors.newSingleThreadExecutor()
    try {
      callerExecutor.asCoroutineDispatcher().use { callerDispatcher ->
        val contextAfterTrace =
          withContext(callerDispatcher) {
            withContext(Dispatchers.Unconfined) {
              Tracing.traceSuspending(spanName = "thread-switch") {
                suspendCancellableCoroutine { continuation ->
                  resumeExecutor.execute { continuation.resume(Unit) }
                }
              }
            }
            Span.current().spanContext
          }

        assertThat(contextAfterTrace.isValid).isFalse()
      }
    } finally {
      callerExecutor.shutdownNow()
      resumeExecutor.shutdownNow()
    }
  }

  @Test
  fun `traceSuspending isolates concurrent spans`() = runBlocking {
    val observedSpanIds =
      listOf("report-1", "report-2").map { spanName ->
        async {
          Tracing.traceSuspending(spanName = spanName) {
            yield()
            Span.current().spanContext.spanId
          }
        }
      }

    val observed = observedSpanIds.awaitAll()
    val finishedSpans = spanExporter.finishedSpanItems.associateBy { it.name }
    assertThat(observed)
      .containsExactly(
        finishedSpans.getValue("report-1").spanId,
        finishedSpans.getValue("report-2").spanId,
      )
      .inOrder()
  }

  @Test
  fun `traceSuspending records exception and sets error status`() = runBlocking {
    assertFailsWith<IllegalStateException> {
      Tracing.traceSuspending(spanName = "failing-span") { error("test failure") }
    }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.status.statusCode).isEqualTo(StatusCode.ERROR)
    assertThat(span.events.map { it.name }).contains("exception")
  }

  @Test
  fun `traceSuspending preserves error status set by a handled failure`() = runBlocking {
    Tracing.traceSuspending(spanName = "handled-failure") {
      Span.current().setStatus(StatusCode.ERROR, "handled refusal")
    }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.status.statusCode).isEqualTo(StatusCode.ERROR)
    assertThat(span.status.description).isEqualTo("handled refusal")
  }
}
