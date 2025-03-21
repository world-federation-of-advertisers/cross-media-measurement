/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

import kotlinx.coroutines.flow.first
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.db.r2dbc.postgres.SerializableErrors.withSerializableErrorRetries
import org.wfanet.measurement.internal.reporting.v2.InvalidateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.service.internal.MetricNotFoundException

/**
 * Updates State for a row in Metrics in the database.
 *
 * Throws the following on [execute]:
 * * [MetricNotFoundException] Metric not found.
 */
class SetMetricState(private val request: InvalidateMetricRequest) : PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    val metricResult =
      try {
        MetricReader(transactionContext)
          .batchGetMetrics(
            batchGetMetricsRequest {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalMetricIds += request.externalMetricId
            }
          )
          .withSerializableErrorRetries()
          .first()
      } catch (e: NoSuchElementException) {
        throw MetricNotFoundException(
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
          externalMetricId = request.externalMetricId,
        )
      }

    val statement =
      boundStatement(
        """
      UPDATE Metrics SET State = $1
      WHERE MeasurementConsumerId = $2 AND MetricId = $3
      """
          .trimIndent()
      ) {
        bind("$1", Metric.State.INVALIDATED)
        bind("$2", metricResult.measurementConsumerId)
        bind("$3", metricResult.metricId)
      }

    transactionContext.executeStatement(statement)
  }
}
