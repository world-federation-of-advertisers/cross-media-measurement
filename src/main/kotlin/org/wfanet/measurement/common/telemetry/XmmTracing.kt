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

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.Instrumentation

/** Creates spans for correlated XMM workflows. */
object XmmTracing {
  /** Records a failed operation that cannot be represented by wrapping one suspending block. */
  fun recordFailure(
    spanName: String,
    attributes: Attributes,
    error: Throwable,
    instrumentationScope: String = DEFAULT_INSTRUMENTATION_SCOPE,
  ) {
    val span =
      Instrumentation.openTelemetry
        .getTracer(instrumentationScope)
        .spanBuilder(spanName)
        .setSpanKind(SpanKind.INTERNAL)
        .setAllAttributes(attributes)
        .startSpan()
    recordFailure(span, error)
    span.end()
  }

  /** Records the standard XMM failure attributes on an existing span. */
  fun recordFailure(span: Span, error: Throwable) {
    span
      .setStatus(StatusCode.ERROR)
      .setAttribute(XmmTraceAttributes.OUTCOME, "failed")
      .setAttribute(XmmTraceAttributes.ERROR_TYPE, XmmTraceAttributes.errorType(error))
    val errorCode = XmmTraceAttributes.errorCode(error)
    if (errorCode != null) {
      span.setAttribute(XmmTraceAttributes.ERROR_CODE, errorCode)
    }
  }

  suspend fun <T> traceSuspending(
    spanName: String,
    attributes: Attributes = Attributes.empty(),
    instrumentationScope: String = DEFAULT_INSTRUMENTATION_SCOPE,
    block: suspend () -> T,
  ): T {
    val span =
      Instrumentation.openTelemetry
        .getTracer(instrumentationScope)
        .spanBuilder(spanName)
        .setSpanKind(SpanKind.INTERNAL)
        .setAllAttributes(attributes)
        .startSpan()
    val context = Context.current().with(span)
    return try {
      withContext(context.asContextElement()) { block() }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      recordFailure(span, e)
      throw e
    } finally {
      span.end()
    }
  }

  private const val DEFAULT_INSTRUMENTATION_SCOPE = "xmm-tracing"
}
