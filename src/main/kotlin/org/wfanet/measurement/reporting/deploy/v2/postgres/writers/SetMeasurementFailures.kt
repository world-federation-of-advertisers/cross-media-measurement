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

import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException

/**
 * Updates the MeasurementDetails to include a Failure message for Measurements in the database.
 *
 * Throws the following on [execute]:
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found.
 * * [MeasurementNotFoundException] Measurement not found.
 */
class SetMeasurementFailures(private val request: BatchSetMeasurementFailuresRequest) :
  PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext).getByCmmsId(request.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val statement =
      valuesListBoundStatement(
        valuesStartIndex = 2,
        paramCount = 3,
        """
        UPDATE Measurements AS m SET
        MeasurementDetails = c.MeasurementDetails,
        MeasurementDetailsJson = c.MeasurementDetailsJson,
        State = $1
        FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
        AS c(MeasurementDetails, MeasurementDetailsJson, CmmsMeasurementId)
        WHERE MeasurementConsumerId = $2 AND m.CmmsMeasurementId = c.CmmsMeasurementId
        """,
      ) {
        bind("$1", Measurement.State.FAILED)
        bind("$2", measurementConsumerId)
        request.measurementFailuresList.forEach {
          val details = MeasurementKt.details { failure = it.failure }
          addValuesBinding {
            bindValuesParam(0, details)
            bindValuesParam(1, details.toJson())
            bindValuesParam(2, it.cmmsMeasurementId)
          }
        }
      }

    val result = transactionContext.executeStatement(statement)
    if (result.numRowsUpdated < request.measurementFailuresList.size) {
      throw MeasurementNotFoundException()
    }
  }
}
