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
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import org.wfanet.measurement.common.Instrumentation

/**
 * Generic utilities for creating traced operations with spans.
 *
 * Components create their own traced operations using these utilities.
 */
object Tracing {
  /**
   * Executes a block within a new span.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  inline fun <T> trace(
    spanName: String,
    attributes: Map<String, String> = emptyMap(),
    block: () -> T,
  ): T {
    return trace(spanName, attributes.toAttributes(), block)
  }

  /**
   * Executes a block within a new span using pre-built span attributes.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  inline fun <T> trace(
    spanName: String,
    attributes: Attributes,
    block: () -> T,
  ): T {
    val tracer = Instrumentation.openTelemetry.getTracer("edpa-instrumentation")
    val spanBuilder = tracer.spanBuilder(spanName).setSpanKind(SpanKind.INTERNAL)

    spanBuilder.setAllAttributes(attributes)

    val span = spanBuilder.startSpan()
    return withSpanContext(span) {
      try {
        val result = block()
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
  }

  /**
   * Executes a block within the context of a span.
   *
   * Makes the span current for downstream operations (gRPC, HTTP).
   */
  public inline fun <T> withSpanContext(span: Span, block: () -> T): T {
    val scope = span.makeCurrent()
    try {
      return block()
    } finally {
      scope.close()
    }
  }
}

fun Map<String, String>.toAttributes(): Attributes {
  if (isEmpty()) {
    return Attributes.empty()
  }
  val builder = Attributes.builder()
  forEach { (key, value) -> builder.put(key, value) }
  return builder.build()
}
