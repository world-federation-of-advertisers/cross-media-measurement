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

import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException

/**
 * Updates the MeasurementDetails to include a Result message for Measurements in the database.
 *
 * Throws the following on [execute]:
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found.
 * * [MeasurementNotFoundException] Measurement not found.
 */
class SetMeasurementResults(private val request: BatchSetMeasurementResultsRequest) :
  PostgresWriter<List<Measurement>>() {
  override suspend fun TransactionScope.runTransaction(): List<Measurement> {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext).getByCmmsId(request.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val statement =
      boundStatement(
        """
      UPDATE Measurements SET MeasurementDetails = $1,
        MeasurementDetailsJson = $2, State = $3
      WHERE MeasurementConsumerId = $4 AND CmmsMeasurementId = $5
      """
      ) {
        val state = Measurement.State.SUCCEEDED
        request.measurementResultsList.forEach {
          val details = MeasurementKt.details { result = it.result }
          addBinding {
            bind("$1", details)
            bind("$2", details.toJson())
            bind("$3", state)
            bind("$4", measurementConsumerId)
            bind("$5", it.cmmsMeasurementId)
          }
        }
      }

    val result = transactionContext.executeStatement(statement)
    if (result.numRowsUpdated < request.measurementResultsList.size) {
      throw MeasurementNotFoundException()
    }

    val idMap = mutableMapOf<String, Measurement>()
    MeasurementReader(transactionContext)
      .readMeasurementsByCmmsId(
        measurementConsumerId,
        request.measurementResultsList.map { it.cmmsMeasurementId }
      )
      .collect {
        val measurement = it.measurement
        idMap.computeIfAbsent(measurement.cmmsMeasurementId) { measurement }
      }

    val measurements = mutableListOf<Measurement>()
    request.measurementResultsList.forEach { measurements.add(idMap[it.cmmsMeasurementId]!!) }

    return measurements
  }
}
