/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

private val parser =
  ResourceNameParser(
    "measurementConsumers/{measurement_consumer}/reportSchedules/{report_schedule}/iterations/{report_schedule_iteration}"
  )

/** [ResourceKey] of a ReportScheduleIteration. */
data class ReportScheduleIterationKey(
  val cmmsMeasurementConsumerId: String,
  val reportScheduleId: String,
  val reportScheduleIterationId: String,
) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to cmmsMeasurementConsumerId,
        IdVariable.REPORT_SCHEDULE to reportScheduleId,
        IdVariable.REPORT_SCHEDULE_ITERATION to reportScheduleIterationId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ReportScheduleIterationKey> {
    val defaultValue = ReportScheduleIterationKey("", "", "")

    override fun fromName(resourceName: String): ReportScheduleIterationKey? {
      return parser.parseIdVars(resourceName)?.let {
        ReportScheduleIterationKey(
          it.getValue(IdVariable.MEASUREMENT_CONSUMER),
          it.getValue(IdVariable.REPORT_SCHEDULE),
          it.getValue(IdVariable.REPORT_SCHEDULE_ITERATION)
        )
      }
    }
  }
}
