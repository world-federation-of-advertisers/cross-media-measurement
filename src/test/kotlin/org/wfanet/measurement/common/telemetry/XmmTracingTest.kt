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
import io.grpc.Status
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Instrumentation

@RunWith(JUnit4::class)
class XmmTracingTest {
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
  fun `traceSuspending records bounded failure metadata without exception payload`() = runBlocking {
    assertFailsWith<Exception> {
      XmmTracing.traceSuspending(spanName = "failing-span") {
        throw Exception("sensitive message", Status.UNAVAILABLE.asRuntimeException())
      }
    }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.status.statusCode).isEqualTo(StatusCode.ERROR)
    assertThat(span.status.description).isEmpty()
    assertThat(span.attributes.get(XmmTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(XmmTraceAttributes.ERROR_TYPE)).isEqualTo("Exception")
    assertThat(span.attributes.get(XmmTraceAttributes.ERROR_CODE)).isEqualTo("grpc.UNAVAILABLE")
    assertThat(span.events).isEmpty()
  }
}
