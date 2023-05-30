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
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException

/**
 * Updates the CmmsMeasurementId for Measurements in the database.
 *
 * Throws the following on [execute]:
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found.
 * * [MeasurementNotFoundException] Measurement not found.
 */
class SetCmmsMeasurementIds(private val request: BatchSetCmmsMeasurementIdsRequest) :
  PostgresWriter<List<Measurement>>() {
  override suspend fun TransactionScope.runTransaction(): List<Measurement> {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
        .getByCmmsId(request.cmmsMeasurementConsumerId)
        ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val statement =
      boundStatement(
        """
      UPDATE Measurements SET CmmsMeasurementId = $1, State = $2
      WHERE MeasurementConsumerId = $3 AND CmmsCreateMeasurementRequestId::text = $4
      """
      ) {
        val state = Measurement.State.PENDING
        request.measurementIdsList.forEach {
          addBinding {
            bind("$1", it.cmmsMeasurementId)
            bind("$2", state)
            bind("$3", measurementConsumerId)
            bind("$4", it.cmmsCreateMeasurementRequestId)
          }
        }
      }

    val result = transactionContext.executeStatement(statement)
    if (result.numRowsUpdated < request.measurementIdsList.size) {
      throw MeasurementNotFoundException()
    }

    val createRequestIdMap = mutableMapOf<String, Measurement>()
    MeasurementReader(transactionContext).readMeasurementsByCmmsCreateRequestId(measurementConsumerId, request.measurementIdsList.map { it.cmmsCreateMeasurementRequestId })
      .collect {
        val measurement = it.measurement
        createRequestIdMap.computeIfAbsent(measurement.cmmsCreateMeasurementRequestId) { measurement }
      }

    val measurements = mutableListOf<Measurement>()
    request.measurementIdsList.forEach {
      measurements.add(createRequestIdMap[it.cmmsCreateMeasurementRequestId]!!)
    }

    return measurements
  }
}
