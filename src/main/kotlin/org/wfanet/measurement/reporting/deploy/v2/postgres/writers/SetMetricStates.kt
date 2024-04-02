/*
 * Copyright 2024 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.reporting.v2.BatchSetMetricsStateRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricNotFoundException

/**
 * Updates the State for Metrics in the database.
 *
 * Throws the following on [execute]:
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found.
 * * [MetricNotFoundException] Metric not found.
 */
class SetMetricStates(private val request: BatchSetMetricsStateRequest) :
  PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext).getByCmmsId(request.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val statement =
      valuesListBoundStatement(
        valuesStartIndex = 1,
        paramCount = 2,
        """
        UPDATE Metrics AS m SET State = c.State
        FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
        AS c(ExternalMetricId, State)
        WHERE MeasurementConsumerId = $1 AND m.ExternalMetricId = c.ExternalMetricId
        """,
      ) {
        bind("$1", measurementConsumerId)
        request.requestsList.forEach {
          addValuesBinding {
            bindValuesParam(0, it.externalMetricId)
            bindValuesParam(1, it.state)
          }
        }
      }

    val result = transactionContext.executeStatement(statement)
    if (result.numRowsUpdated < request.requestsList.size) {
      throw MetricNotFoundException()
    }
  }
}
