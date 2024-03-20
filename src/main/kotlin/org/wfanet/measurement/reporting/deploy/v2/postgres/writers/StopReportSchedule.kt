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

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.StopReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleReader
import org.wfanet.measurement.reporting.service.internal.ReportScheduleNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleStateInvalidException

/**
 * Updates the ReportSchedule State to STOPPED in the database.
 *
 * Throws the following on [execute]:
 * * [ReportScheduleNotFoundException] Report Schedule not found
 * * [ReportScheduleNotFoundException] Report Schedule state invalid.
 */
class StopReportSchedule(private val request: StopReportScheduleRequest) :
  PostgresWriter<ReportSchedule>() {
  override suspend fun TransactionScope.runTransaction(): ReportSchedule {
    val result =
      ReportScheduleReader(transactionContext)
        .readReportScheduleByExternalId(
          request.cmmsMeasurementConsumerId,
          request.externalReportScheduleId,
        )
        ?: throw ReportScheduleNotFoundException(
          request.cmmsMeasurementConsumerId,
          request.externalReportScheduleId,
        )

    if (result.reportSchedule.state != ReportSchedule.State.ACTIVE) {
      throw ReportScheduleStateInvalidException(
        request.cmmsMeasurementConsumerId,
        request.externalReportScheduleId,
      )
    }

    val updateTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

    val statement =
      boundStatement(
        """
      UPDATE ReportSchedules SET State = $1, UpdateTime = $2
      WHERE ReportSchedules.MeasurementConsumerId = $3
        AND ReportSchedules.ReportScheduleId = $4
      """
      ) {
        bind("$1", ReportSchedule.State.STOPPED_VALUE)
        bind("$2", updateTime)
        bind("$3", result.measurementConsumerId)
        bind("$4", result.reportScheduleId)
      }

    transactionContext.run { executeStatement(statement) }

    return result.reportSchedule.copy {
      state = ReportSchedule.State.STOPPED
      this.updateTime = updateTime.toInstant().toProtoTime()
    }
  }
}
