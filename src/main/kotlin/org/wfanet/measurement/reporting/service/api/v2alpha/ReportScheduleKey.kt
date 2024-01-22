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

import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a ReportSchedule. */
data class ReportScheduleKey(
  override val parentKey: MeasurementConsumerKey,
  val reportScheduleId: String,
) : ChildResourceKey {
  constructor(
    cmmsMeasurementConsumerId: String,
    reportScheduleId: String,
  ) : this(MeasurementConsumerKey(cmmsMeasurementConsumerId), reportScheduleId)

  val cmmsMeasurementConsumerId: String
    get() = parentKey.measurementConsumerId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to cmmsMeasurementConsumerId,
        IdVariable.REPORT_SCHEDULE to reportScheduleId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ReportScheduleKey> {
    const val PATTERN = "${MeasurementConsumerKey.PATTERN}/reportSchedules/{report_schedule}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): ReportScheduleKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return ReportScheduleKey(
        idVars.getValue(IdVariable.MEASUREMENT_CONSUMER),
        idVars.getValue(IdVariable.REPORT_SCHEDULE),
      )
    }
  }
}
