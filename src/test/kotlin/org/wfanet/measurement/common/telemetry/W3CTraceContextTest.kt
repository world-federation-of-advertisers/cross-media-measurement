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
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class W3CTraceContextTest {
  @Test
  fun `inject serializes current span context`() {
    val spanContext =
      SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault())
    val context = Context.root().with(Span.wrap(spanContext))

    val fields = W3CTraceContext.inject(context)

    assertThat(fields).containsExactly("traceparent", TRACE_PARENT)
  }

  @Test
  fun `inject omits headers when span context is invalid`() {
    assertThat(W3CTraceContext.inject(Context.root())).isEmpty()
  }

  @Test
  fun `inject and extract preserve trace state`() {
    val traceState = TraceState.builder().put("vendor", "opaque-value").build()
    val spanContext = SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), traceState)

    val fields = W3CTraceContext.inject(Context.root().with(Span.wrap(spanContext)))
    val extracted = Span.fromContext(W3CTraceContext.extract(fields)).spanContext

    assertThat(fields["tracestate"]).isEqualTo("vendor=opaque-value")
    assertThat(extracted.traceState.get("vendor")).isEqualTo("opaque-value")
  }

  @Test
  fun `extract accepts case-insensitive field names`() {
    val context = W3CTraceContext.extract(mapOf("TraceParent" to TRACE_PARENT))

    val spanContext = Span.fromContext(context).spanContext
    assertThat(spanContext.traceId).isEqualTo(TRACE_ID)
    assertThat(spanContext.spanId).isEqualTo(SPAN_ID)
    assertThat(spanContext.isSampled).isTrue()
  }

  @Test
  fun `extract ignores malformed traceparent`() {
    val context = W3CTraceContext.extract(mapOf("traceparent" to "not-a-traceparent"))

    assertThat(Span.fromContext(context).spanContext.isValid).isFalse()
  }

  @Test
  fun `withExtractedContext binds context across coroutine suspension`() = runBlocking {
    val spanContext =
      W3CTraceContext.withExtractedContext(mapOf("traceparent" to TRACE_PARENT)) {
        yield()
        Span.current().spanContext
      }

    assertThat(spanContext.traceId).isEqualTo(TRACE_ID)
    assertThat(spanContext.spanId).isEqualTo(SPAN_ID)
  }

  private companion object {
    const val TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
    const val SPAN_ID = "b7ad6b7169203331"
    const val TRACE_PARENT = "00-$TRACE_ID-$SPAN_ID-01"
  }
}
