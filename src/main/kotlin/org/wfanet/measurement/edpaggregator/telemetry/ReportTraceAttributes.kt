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

package org.wfanet.measurement.edpaggregator.telemetry

import io.opentelemetry.api.common.AttributeKey

/** Stable OpenTelemetry attribute names used to correlate a report across EDPA components. */
object ReportTraceAttributes {
  /** Fully-qualified Reporting `Report` resource name. */
  const val REPORT_NAME_STRING = "xmm.report.name"

  /** Fully-qualified Kingdom `Requisition` resource name. */
  const val REQUISITION_NAME_STRING = "xmm.requisition.name"

  /** EDPA report processing group identifier. */
  const val GROUP_ID_STRING = "xmm.edpa.group_id"

  /** Attribute key for [REPORT_NAME_STRING]. */
  val REPORT_NAME: AttributeKey<String> = AttributeKey.stringKey(REPORT_NAME_STRING)

  /** Attribute key for [REQUISITION_NAME_STRING]. */
  val REQUISITION_NAME: AttributeKey<String> = AttributeKey.stringKey(REQUISITION_NAME_STRING)

  /** Attribute key for [GROUP_ID_STRING]. */
  val GROUP_ID: AttributeKey<String> = AttributeKey.stringKey(GROUP_ID_STRING)
}
