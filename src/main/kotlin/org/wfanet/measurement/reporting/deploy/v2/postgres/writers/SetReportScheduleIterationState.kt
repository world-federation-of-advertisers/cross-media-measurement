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
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.SetReportScheduleIterationStateRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleIterationReader
import org.wfanet.measurement.reporting.service.internal.ReportScheduleIterationNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleIterationStateInvalidException

/**
 * Updates the ReportScheduleIteration state and numAttempts in the database.
 *
 * Throws the following on [execute]:
 * * ReportScheduleIterationNotFoundException] ReportScheduleIteration not found.
 * * ReportScheduleIterationStateInvalidException] ReportScheduleIteration state invalid.
 */
class SetReportScheduleIterationState(private val request: SetReportScheduleIterationStateRequest) :
  PostgresWriter<ReportScheduleIteration>() {
  override suspend fun TransactionScope.runTransaction(): ReportScheduleIteration {
    val result =
      (ReportScheduleIterationReader(this.transactionContext)
        .readReportScheduleIterationByExternalId(
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
          externalReportScheduleId = request.externalReportScheduleId,
          externalReportScheduleIterationId = request.externalReportScheduleIterationId,
        )
        ?: throw ReportScheduleIterationNotFoundException(
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
          externalReportScheduleId = request.externalReportScheduleId,
          externalReportScheduleIterationId = request.externalReportScheduleIterationId,
        ))

    // REPORT_CREATED is a terminal state.
    if (result.reportScheduleIteration.state == ReportScheduleIteration.State.REPORT_CREATED) {
      throw ReportScheduleIterationStateInvalidException(
        cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
        externalReportScheduleId = request.externalReportScheduleId,
        externalReportScheduleIterationId = request.externalReportScheduleIterationId,
      )
    }

    val updateTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)
    val statement =
      boundStatement(
        """
      UPDATE ReportScheduleIterations SET State = $1, NumAttempts = $2, UpdateTime = $3
      WHERE MeasurementConsumerId = $4
        AND ReportScheduleId = $5
        AND ReportScheduleIterationId = $6
      """
      ) {
        bind("$1", request.state)
        bind("$2", result.reportScheduleIteration.numAttempts + 1)
        bind("$3", updateTime)
        bind("$4", result.measurementConsumerId)
        bind("$5", result.reportScheduleId)
        bind("$6", result.reportScheduleIterationId)
      }

    transactionContext.executeStatement(statement)

    return result.reportScheduleIteration.copy {
      state = request.state
      numAttempts += 1
      this.updateTime = updateTime.toInstant().toProtoTime()
    }
  }
}
