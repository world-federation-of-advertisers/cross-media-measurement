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

import com.google.cloud.functions.HttpRequest
import io.cloudevents.CloudEvent
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.extension.kotlin.asContextElement
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.Instrumentation

object Tracing {
  private val w3cPropagator: TextMapPropagator = W3CTraceContextPropagator.getInstance()

  /**
   * Executes a block within the context of a span.
   *
   * Makes the span current for downstream operations (gRPC, HTTP).
   */
  inline fun <T> withSpanContext(span: Span, block: () -> T): T {
    val scope = span.makeCurrent()
    try {
      return block()
    } finally {
      scope.close()
    }
  }

  /**
   * Installs W3C trace context for the current thread using the provided request.
   *
   * Use this overload for purely synchronous work. For suspending work, combine it with the
   * [CoroutineContext.trace] overloads to ensure coroutine context propagation.
   */
  inline fun <T> withW3CTraceContext(request: HttpRequest, block: () -> T): T {
    val parentContext = extractW3CContext(request)
    val scope = parentContext.makeCurrent()
    return try {
      block()
    } finally {
      scope.close()
    }
  }

  /**
   * Installs W3C trace context for the current thread using the provided CloudEvent.
   *
   * Use this overload for purely synchronous work. For suspending work, combine it with the
   * [CoroutineContext.trace] overloads to ensure coroutine context propagation.
   */
  inline fun <T> withW3CTraceContext(event: CloudEvent, block: () -> T): T {
    val parentContext = extractW3CContext(event)
    val scope = parentContext.makeCurrent()
    return try {
      block()
    } finally {
      scope.close()
    }
  }

  @PublishedApi
  internal fun extractW3CContext(request: HttpRequest): Context {
    return w3cPropagator.extract(Context.current(), request, CloudFunctionsHttpRequestGetter)
  }

  @PublishedApi
  internal fun extractW3CContext(event: CloudEvent): Context {
    return w3cPropagator.extract(Context.current(), event, CloudEventGetter)
  }

  /**
   * Executes a synchronous block within a new span.
   *
   * Use this overload for purely synchronous work. For suspending callers, prefer the
   * [CoroutineContext.trace] extension instead.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  inline fun <T> trace(
    spanName: String,
    attributes: Map<String, String> = emptyMap(),
    crossinline block: () -> T,
  ): T {
    return trace(spanName, attributes.toAttributes(), block)
  }

  /**
   * Executes a synchronous block within a new span using pre-built span attributes.
   *
   * Use this overload when you already have [Attributes] instances. For suspending callers, prefer
   * the [CoroutineContext.trace] overloads to ensure coroutine context propagation.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  inline fun <T> trace(spanName: String, attributes: Attributes, crossinline block: () -> T): T {
    val tracer = Instrumentation.openTelemetry.getTracer("edpa-instrumentation")
    val spanBuilder = tracer.spanBuilder(spanName).setSpanKind(SpanKind.INTERNAL)

    spanBuilder.setAllAttributes(attributes)

    val span = spanBuilder.startSpan()
    val scope = span.makeCurrent()
    try {
      val result = block()
      span.setStatus(StatusCode.OK)
      return result
    } catch (e: Exception) {
      span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
      span.recordException(e)
      throw e
    } finally {
      scope.close()
      span.end()
    }
  }

  /**
   * Executes a suspending block within a new span.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  suspend fun <T> traceSuspending(
    spanName: String,
    attributes: Map<String, String> = emptyMap(),
    block: suspend () -> T,
  ): T {
    return traceSuspending(spanName, attributes.toAttributes(), block)
  }

  /**
   * Executes a suspending block within a new span using pre-built span attributes.
   *
   * Ensures the OpenTelemetry context is propagated across suspension points by binding the current
   * context into the coroutine scope.
   *
   * @param spanName Human-readable span name
   * @param attributes Span attributes (low-cardinality only)
   * @param block Code to execute
   */
  suspend fun <T> traceSuspending(
    spanName: String,
    attributes: Attributes,
    block: suspend () -> T,
  ): T {
    val tracer = Instrumentation.openTelemetry.getTracer("edpa-instrumentation")
    val spanBuilder = tracer.spanBuilder(spanName).setSpanKind(SpanKind.INTERNAL)

    spanBuilder.setAllAttributes(attributes)

    val span = spanBuilder.startSpan()
    val scope = span.makeCurrent()
    return try {
      val result = withContext(Context.current().asContextElement()) { block() }
      span.setStatus(StatusCode.OK)
      result
    } catch (e: Exception) {
      span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
      span.recordException(e)
      throw e
    } finally {
      scope.close()
      span.end()
    }
  }

  private object CloudFunctionsHttpRequestGetter : TextMapGetter<HttpRequest> {
    override fun keys(carrier: HttpRequest): Iterable<String> = carrier.headers.keys

    override fun get(carrier: HttpRequest?, key: String): String? {
      if (carrier == null) return null

      // Fast path: try exact case match first
      val exactMatch = carrier.headers[key]?.firstOrNull()
      if (exactMatch != null) return exactMatch

      // Case-insensitive linear search
      return carrier.headers.entries
        .firstOrNull { entry -> entry.key.equals(key, ignoreCase = true) }
        ?.value
        ?.firstOrNull()
    }
  }

  /**
   * Extracts W3C trace context from CloudEvent extensions.
   *
   * CloudEvents use `traceparent` and `tracestate` as extension attributes to carry W3C Trace
   * Context information, as defined in the CloudEvents Distributed Tracing Extension spec:
   * https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md
   */
  private object CloudEventGetter : TextMapGetter<CloudEvent> {
    override fun keys(carrier: CloudEvent): Iterable<String> {
      return carrier.extensionNames
    }

    override fun get(carrier: CloudEvent?, key: String): String? {
      if (carrier == null) return null
      return carrier.getExtension(key)?.toString()
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
