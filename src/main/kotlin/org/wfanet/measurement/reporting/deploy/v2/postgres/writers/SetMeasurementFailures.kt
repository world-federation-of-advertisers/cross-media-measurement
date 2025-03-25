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

import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
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
          ?: throw MeasurementConsumerNotFoundException(request.cmmsMeasurementConsumerId))
        .measurementConsumerId

    val measurementDetailsMap =
      MeasurementReader(transactionContext)
        .readMeasurementsByCmmsId(
          measurementConsumerId,
          request.measurementFailuresList.map { it.cmmsMeasurementId },
        )
        .toList()
        .associate { it.measurement.cmmsMeasurementId to it.measurement.details }

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
          val details =
            measurementDetailsMap[it.cmmsMeasurementId]?.copy { failure = it.failure }
              ?: throw MeasurementNotFoundException()
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

    // Read all metrics tied to Measurements that were updated.
    val metricIds: List<InternalId> = buildList {
      MetricReader(transactionContext)
        .readMetricsByCmmsMeasurementId(
          measurementConsumerId,
          request.measurementFailuresList.map { it.cmmsMeasurementId },
        )
        .collect { metricReaderResult -> add(metricReaderResult.metricId) }
    }

    if (metricIds.isNotEmpty()) {
      val metricStateUpdateStatement =
        valuesListBoundStatement(
          valuesStartIndex = 2,
          paramCount = 1,
          """
        UPDATE Metrics AS m SET State = $1
        FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
        AS c(MetricId)
        WHERE MeasurementConsumerId = $2 AND m.MetricId = c.MetricId
        """,
        ) {
          bind("$1", Metric.State.FAILED)
          bind("$2", measurementConsumerId)
          metricIds.forEach { addValuesBinding { bindValuesParam(0, it) } }
        }

      transactionContext.executeStatement(metricStateUpdateStatement)
    }
  }
}
