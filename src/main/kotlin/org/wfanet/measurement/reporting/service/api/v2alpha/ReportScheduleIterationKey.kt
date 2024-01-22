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
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a ReportScheduleIteration. */
data class ReportScheduleIterationKey(
  override val parentKey: ReportScheduleKey,
  val reportScheduleIterationId: String,
) : ChildResourceKey {
  constructor(
    cmmsMeasurementConsumerId: String,
    reportScheduleId: String,
    reportScheduleIterationId: String,
  ) : this(
    ReportScheduleKey(cmmsMeasurementConsumerId, reportScheduleId),
    reportScheduleIterationId,
  )

  val cmmsMeasurementConsumerId: String
    get() = parentKey.cmmsMeasurementConsumerId

  val reportScheduleId: String
    get() = parentKey.reportScheduleId

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
    const val PATTERN = "${ReportScheduleKey.PATTERN}/iterations/{report_schedule_iteration}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): ReportScheduleIterationKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return ReportScheduleIterationKey(
        idVars.getValue(IdVariable.MEASUREMENT_CONSUMER),
        idVars.getValue(IdVariable.REPORT_SCHEDULE),
        idVars.getValue(IdVariable.REPORT_SCHEDULE_ITERATION),
      )
    }
  }
}
