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

import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context

/**
 * Generic utilities for creating traced operations with spans.
 *
 * Components create their own traced operations using these utilities.
 */
object TracedOperation {
  /**
   * Executes a block within a new span.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  suspend fun <T> trace(
    spanName: String,
    attributes: Map<String, String> = emptyMap(),
    block: suspend () -> T,
  ): T {
    val tracer = EdpaTelemetry.getTracer()
    val spanBuilder = tracer.spanBuilder(spanName).setSpanKind(SpanKind.INTERNAL)

    attributes.forEach { (key, value) -> spanBuilder.setAttribute(key, value) }

    val span = spanBuilder.startSpan()
    return try {
      val result = withSpanContext(span) { block() }
      span.setStatus(StatusCode.OK)
      result
    } catch (e: Exception) {
      span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
      span.recordException(e)
      throw e
    } finally {
      span.end()
    }
  }

  /**
   * Executes a block within the context of a span.
   *
   * Makes the span current for downstream operations (gRPC, HTTP).
   */
  private suspend fun <T> withSpanContext(span: Span, block: suspend () -> T): T {
    val context = Context.current().with(span)
    return context.makeCurrent().use { block() }
  }
}
