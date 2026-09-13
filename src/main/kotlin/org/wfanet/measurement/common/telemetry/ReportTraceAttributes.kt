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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec

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
  const val DUCHY_ID_STRING = "xmm.duchy.id"
  const val BASIC_REPORT_STATE_STRING = "xmm.basic_report.state"
  const val REPORT_STATE_STRING = "xmm.report.state"
  const val METRIC_STATE_STRING = "xmm.metric.state"
  const val MEASUREMENT_STATE_STRING = "xmm.measurement.state"
  const val REQUISITION_STATE_STRING = "xmm.requisition.state"
  const val LIFECYCLE_STAGE_STRING = "xmm.lifecycle.stage"
  const val OUTCOME_STRING = "xmm.outcome"
  const val ERROR_TYPE_STRING = "xmm.error.type"
  const val REFUSAL_ORIGIN_STRING = "xmm.refusal.origin"
  const val REQUISITION_FETCHER_REFUSAL_ORIGIN = "requisition_fetcher"
  const val RESULTS_FULFILLER_REFUSAL_ORIGIN = "results_fulfiller"

  val BASIC_REPORT_NAME: AttributeKey<String> = AttributeKey.stringKey(BASIC_REPORT_NAME_STRING)
  val REPORT_NAME: AttributeKey<String> = AttributeKey.stringKey(REPORT_NAME_STRING)
  val METRIC_NAME: AttributeKey<String> = AttributeKey.stringKey(METRIC_NAME_STRING)
  val MEASUREMENT_NAME: AttributeKey<String> = AttributeKey.stringKey(MEASUREMENT_NAME_STRING)
  val REQUISITION_NAME: AttributeKey<String> = AttributeKey.stringKey(REQUISITION_NAME_STRING)
  val GROUP_ID: AttributeKey<String> = AttributeKey.stringKey(GROUP_ID_STRING)
  val COMPUTATION_NAME: AttributeKey<String> = AttributeKey.stringKey(COMPUTATION_NAME_STRING)
  val WORK_ITEM_NAME: AttributeKey<String> = AttributeKey.stringKey(WORK_ITEM_NAME_STRING)
  val DUCHY_ID: AttributeKey<String> = AttributeKey.stringKey(DUCHY_ID_STRING)
  val BASIC_REPORT_STATE: AttributeKey<String> = AttributeKey.stringKey(BASIC_REPORT_STATE_STRING)
  val REPORT_STATE: AttributeKey<String> = AttributeKey.stringKey(REPORT_STATE_STRING)
  val METRIC_STATE: AttributeKey<String> = AttributeKey.stringKey(METRIC_STATE_STRING)
  val MEASUREMENT_STATE: AttributeKey<String> = AttributeKey.stringKey(MEASUREMENT_STATE_STRING)
  val REQUISITION_STATE: AttributeKey<String> = AttributeKey.stringKey(REQUISITION_STATE_STRING)
  val LIFECYCLE_STAGE: AttributeKey<String> = AttributeKey.stringKey(LIFECYCLE_STAGE_STRING)
  val OUTCOME: AttributeKey<String> = AttributeKey.stringKey(OUTCOME_STRING)
  val ERROR_TYPE: AttributeKey<String> = AttributeKey.stringKey(ERROR_TYPE_STRING)
  val REFUSAL_ORIGIN: AttributeKey<String> = AttributeKey.stringKey(REFUSAL_ORIGIN_STRING)

  /** Returns the reporting resource attributes embedded in [measurementSpec]. */
  fun fromMeasurementSpec(measurementSpec: MeasurementSpec): Attributes {
    val metadata = measurementSpec.reportingMetadata
    return Attributes.builder()
      .also { builder ->
        if (metadata.basicReport.isNotEmpty()) {
          builder.put(BASIC_REPORT_NAME, metadata.basicReport)
        }
        if (metadata.report.isNotEmpty()) {
          builder.put(REPORT_NAME, metadata.report)
        }
        if (metadata.metric.isNotEmpty()) {
          builder.put(METRIC_NAME, metadata.metric)
        }
      }
      .build()
  }

  /** Returns a bounded, human-readable exception class name suitable for a span label. */
  fun errorType(error: Throwable): String {
    return error::class.java.name.substringAfterLast('.').replace('$', '.').take(200)
  }
}
