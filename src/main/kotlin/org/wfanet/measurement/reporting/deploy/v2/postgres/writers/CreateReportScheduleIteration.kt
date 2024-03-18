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
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleReader
import org.wfanet.measurement.reporting.service.internal.ReportScheduleNotFoundException

/**
 * Inserts a ReportScheduleIteration into the database.
 *
 * Throws the following on [execute]:
 * * [ReportScheduleNotFoundException] ReportSchedule not found
 */
class CreateReportScheduleIteration(private val reportScheduleIteration: ReportScheduleIteration) :
  PostgresWriter<ReportScheduleIteration>() {
  override suspend fun TransactionScope.runTransaction(): ReportScheduleIteration {
    val reportScheduleResult =
      ReportScheduleReader(transactionContext)
        .readReportScheduleByExternalId(
          cmmsMeasurementConsumerId = reportScheduleIteration.cmmsMeasurementConsumerId,
          externalReportScheduleId = reportScheduleIteration.externalReportScheduleId,
        )
        ?: throw ReportScheduleNotFoundException(
          cmmsMeasurementConsumerId = reportScheduleIteration.cmmsMeasurementConsumerId,
          externalReportScheduleId = reportScheduleIteration.externalReportScheduleId,
        )

    val reportScheduleIterationId = idGenerator.generateInternalId()
    val externalReportScheduleIterationId = idGenerator.generateExternalId().apiId.value
    val createTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

    val statement =
      boundStatement(
        """
      INSERT INTO ReportScheduleIterations
        (
          MeasurementConsumerId,
          ReportScheduleId,
          ReportScheduleIterationId,
          ExternalReportScheduleIterationId,
          ReportEventTime,
          CreateReportRequestId,
          NumAttempts,
          State,
          CreateTime,
          UpdateTime
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      """
      ) {
        bind("$1", reportScheduleResult.measurementConsumerId)
        bind("$2", reportScheduleResult.reportScheduleId)
        bind("$3", reportScheduleIterationId)
        bind("$4", externalReportScheduleIterationId)
        bind("$5", reportScheduleIteration.reportEventTime.toInstant().atOffset(ZoneOffset.UTC))
        bind("$6", reportScheduleIteration.createReportRequestId)
        bind("$7", 0)
        bind("$8", ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY)
        bind("$9", createTime)
        bind("$10", createTime)
      }

    val reportScheduleUpdateStatement =
      boundStatement(
        """
        UPDATE ReportSchedules SET LatestReportScheduleIterationId = $1
        WHERE MeasurementConsumerId = $2 AND ReportScheduleId = $3
        """
          .trimIndent()
      ) {
        bind("$1", reportScheduleIterationId)
        bind("$2", reportScheduleResult.measurementConsumerId)
        bind("$3", reportScheduleResult.reportScheduleId)
      }

    transactionContext.run {
      executeStatement(statement)
      executeStatement(reportScheduleUpdateStatement)
    }

    return reportScheduleIteration.copy {
      this.externalReportScheduleIterationId = externalReportScheduleIterationId
      state = ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY
      this.createTime = createTime.toInstant().toProtoTime()
      this.updateTime = createTime.toInstant().toProtoTime()
    }
  }
}
