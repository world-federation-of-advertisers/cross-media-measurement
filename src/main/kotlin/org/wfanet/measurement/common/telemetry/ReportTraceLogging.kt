/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.telemetry

import java.util.logging.Logger

/** Writes payload-free lifecycle evidence that can be read when trace export is unavailable. */
object ReportTraceLogging {
  /** Logs one lifecycle [event] with allowlisted [fields]. */
  fun log(logger: Logger, event: String, vararg fields: Pair<String, String?>) {
    require(EVENT_PATTERN.matches(event)) { "Invalid report trace event name" }

    val message = buildString {
      append("event=").append(event)
      for ((name, value) in fields) {
        require(name in SAFE_FIELD_NAMES) { "Unsupported report trace log field: $name" }
        if (!value.isNullOrBlank()) {
          append(' ').append(name).append('=').append(sanitizeToken(value))
        }
      }
    }
    logger.info(message)
  }

  private fun sanitizeToken(value: String): String =
    value.replace(WHITESPACE_PATTERN, "_").take(MAX_VALUE_LENGTH)

  private val SAFE_FIELD_NAMES =
    setOf(
      ReportTraceAttributes.BASIC_REPORT_NAME_STRING,
      ReportTraceAttributes.REPORT_NAME_STRING,
      ReportTraceAttributes.METRIC_NAME_STRING,
      ReportTraceAttributes.METRIC_REQUEST_ID_STRING,
      ReportTraceAttributes.MEASUREMENT_NAME_STRING,
      ReportTraceAttributes.MEASUREMENT_REQUEST_ID_STRING,
      ReportTraceAttributes.REQUISITION_NAME_STRING,
      ReportTraceAttributes.GROUP_ID_STRING,
      ReportTraceAttributes.COMPUTATION_NAME_STRING,
      ReportTraceAttributes.WORK_ITEM_NAME_STRING,
      ReportTraceAttributes.WORK_ITEM_ATTEMPT_NAME_STRING,
      ReportTraceAttributes.DUCHY_ID_STRING,
      ReportTraceAttributes.BASIC_REPORT_STATE_STRING,
      ReportTraceAttributes.REPORT_STATE_STRING,
      ReportTraceAttributes.METRIC_STATE_STRING,
      ReportTraceAttributes.MEASUREMENT_STATE_STRING,
      ReportTraceAttributes.REQUISITION_STATE_STRING,
      ReportTraceAttributes.LIFECYCLE_STAGE_STRING,
      ReportTraceAttributes.OUTCOME_STRING,
      ReportTraceAttributes.ERROR_TYPE_STRING,
      ReportTraceAttributes.ERROR_CODE_STRING,
      ReportTraceAttributes.REFUSAL_ORIGIN_STRING,
    )
  private val EVENT_PATTERN = Regex("[a-zA-Z0-9._-]+")
  private val WHITESPACE_PATTERN = Regex("\\s+")
  private const val MAX_VALUE_LENGTH = 1000
}
