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
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.Instrumentation

/** Creates report-correlated spans in services outside the EDPA-specific telemetry package. */
object ReportTracing {
  /** Records a failed operation that cannot be represented by wrapping one suspending block. */
  fun recordFailure(spanName: String, attributes: Attributes, error: Throwable) {
    val span =
      Instrumentation.openTelemetry
        .getTracer("xmm-report-tracing")
        .spanBuilder(spanName)
        .setSpanKind(SpanKind.INTERNAL)
        .setAllAttributes(attributes)
        .startSpan()
    span
      .setStatus(StatusCode.ERROR, error.message ?: "Unknown error")
      .setAttribute(ReportTraceAttributes.OUTCOME, "failed")
      .setAttribute(ReportTraceAttributes.ERROR_TYPE, ReportTraceAttributes.errorType(error))
      .recordException(error)
    span.end()
  }

  suspend fun <T> traceSuspending(
    spanName: String,
    attributes: Attributes = Attributes.empty(),
    block: suspend () -> T,
  ): T {
    val span =
      Instrumentation.openTelemetry
        .getTracer("xmm-report-tracing")
        .spanBuilder(spanName)
        .setSpanKind(SpanKind.INTERNAL)
        .setAllAttributes(attributes)
        .startSpan()
    val context = Context.current().with(span)
    return try {
      withContext(context.asContextElement()) { block() }
    } catch (e: Exception) {
      span
        .setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
        .setAttribute(ReportTraceAttributes.OUTCOME, "failed")
        .setAttribute(ReportTraceAttributes.ERROR_TYPE, ReportTraceAttributes.errorType(e))
        .recordException(e)
      throw e
    } finally {
      span.end()
    }
  }
}
