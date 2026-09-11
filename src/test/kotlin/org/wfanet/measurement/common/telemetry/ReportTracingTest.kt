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

package org.wfanet.measurement.common.telemetry

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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Instrumentation

@RunWith(JUnit4::class)
class ReportTracingTest {
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
  fun `traceSuspending creates attributed span and binds it across suspension`() = runBlocking {
    val attributeKey = AttributeKey.stringKey("test.attribute")

    val spanContext =
      ReportTracing.traceSuspending(
        spanName = "test-span",
        attributes = Attributes.of(attributeKey, "test-value"),
      ) {
        yield()
        Span.current().spanContext
      }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.name).isEqualTo("test-span")
    assertThat(span.attributes.get(attributeKey)).isEqualTo("test-value")
    assertThat(spanContext.spanId).isEqualTo(span.spanId)
    assertThat(span.status.statusCode).isEqualTo(StatusCode.UNSET)
  }

  @Test
  fun `traceSuspending records exception and sets error status`() = runBlocking {
    assertFailsWith<IllegalStateException> {
      ReportTracing.traceSuspending(spanName = "failing-span") { error("test failure") }
    }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.status.statusCode).isEqualTo(StatusCode.ERROR)
    assertThat(span.events.map { it.name }).contains("exception")
  }
}
