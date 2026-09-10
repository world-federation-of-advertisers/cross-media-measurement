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

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.context.propagation.TextMapSetter
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.Instrumentation

/** Stable OpenTelemetry attributes used to correlate one BasicReport across XMM services. */
object ReportTraceAttributes {
  const val BASIC_REPORT_NAME_STRING = "xmm.basic_report.name"
  const val REPORT_NAME_STRING = "xmm.report.name"
  const val METRIC_NAME_STRING = "xmm.metric.name"
  const val MEASUREMENT_NAME_STRING = "xmm.measurement.name"
  const val REQUISITION_NAME_STRING = "xmm.requisition.name"
  const val GROUP_ID_STRING = "xmm.edpa.group_id"
  const val COMPUTATION_NAME_STRING = "xmm.computation.name"
  const val WORK_ITEM_NAME_STRING = "xmm.work_item.name"

  val BASIC_REPORT_NAME: AttributeKey<String> = AttributeKey.stringKey(BASIC_REPORT_NAME_STRING)
  val REPORT_NAME: AttributeKey<String> = AttributeKey.stringKey(REPORT_NAME_STRING)
  val METRIC_NAME: AttributeKey<String> = AttributeKey.stringKey(METRIC_NAME_STRING)
  val MEASUREMENT_NAME: AttributeKey<String> = AttributeKey.stringKey(MEASUREMENT_NAME_STRING)
  val REQUISITION_NAME: AttributeKey<String> = AttributeKey.stringKey(REQUISITION_NAME_STRING)
  val GROUP_ID: AttributeKey<String> = AttributeKey.stringKey(GROUP_ID_STRING)
  val COMPUTATION_NAME: AttributeKey<String> = AttributeKey.stringKey(COMPUTATION_NAME_STRING)
  val WORK_ITEM_NAME: AttributeKey<String> = AttributeKey.stringKey(WORK_ITEM_NAME_STRING)

  /** Returns the reporting resource attributes embedded in [measurementSpec]. */
  fun fromMeasurementSpec(measurementSpec: MeasurementSpec): Attributes {
    val metadata = measurementSpec.reportingMetadata
    return Attributes.builder()
      .also { builder ->
        if (metadata.basicReport.isNotBlank()) {
          builder.put(BASIC_REPORT_NAME, metadata.basicReport)
        }
        if (metadata.report.isNotBlank()) {
          builder.put(REPORT_NAME, metadata.report)
        }
        if (metadata.metric.isNotBlank()) {
          builder.put(METRIC_NAME, metadata.metric)
        }
      }
      .build()
  }
}

/** Utilities for propagating W3C trace context through persistent asynchronous work items. */
object W3CTraceContext {
  private val propagator: TextMapPropagator = W3CTraceContextPropagator.getInstance()

  /** Serializes [context] into W3C trace-context fields suitable for a protobuf map. */
  fun inject(context: Context = Context.current()): Map<String, String> {
    val carrier = mutableMapOf<String, String>()
    propagator.inject(context, carrier, MapSetter)
    return carrier
  }

  /** Extracts a parent OpenTelemetry context from W3C trace-context [fields]. */
  fun extract(fields: Map<String, String>): Context {
    return propagator.extract(Context.current(), fields, MapGetter)
  }

  /** Runs [block] with the W3C trace context from [fields] bound to the coroutine. */
  suspend fun <T> withExtractedContext(fields: Map<String, String>, block: suspend () -> T): T {
    return withContext(extract(fields).asContextElement()) { block() }
  }

  private object MapSetter : TextMapSetter<MutableMap<String, String>> {
    override fun set(carrier: MutableMap<String, String>?, key: String, value: String) {
      carrier?.set(key, value)
    }
  }

  private object MapGetter : TextMapGetter<Map<String, String>> {
    override fun keys(carrier: Map<String, String>): Iterable<String> = carrier.keys

    override fun get(carrier: Map<String, String>?, key: String): String? {
      return carrier?.entries?.firstOrNull { it.key.equals(key, ignoreCase = true) }?.value
    }
  }
}

/** Creates report-correlated spans in services outside the EDPA-specific telemetry package. */
object ReportTracing {
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
    val scope = span.makeCurrent()
    return try {
      withContext(Context.current().asContextElement()) { block() }
    } catch (e: Exception) {
      span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
      span.recordException(e)
      throw e
    } finally {
      scope.close()
      span.end()
    }
  }
}
