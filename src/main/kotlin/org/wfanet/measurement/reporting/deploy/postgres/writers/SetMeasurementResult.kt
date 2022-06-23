// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.deploy.postgres.writers

import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.measurement
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException

/**
 * Updates the Result column for a Measurement
 *
 * Throws the following on [execute]:
 * * [MeasurementNotFoundException] Measurement not found.
 */
class SetMeasurementResult(private val request: SetMeasurementResultRequest) :
  PostgresWriter<Measurement>() {
  override suspend fun TransactionScope.runTransaction(): Measurement {
    val statement =
      boundStatement(
        """
      UPDATE Measurements
      SET State = $1, Result = $2
      WHERE MeasurementConsumerReferenceId = $3 AND MeasurementReferenceId = $4
      """
      ) {
        bind("$1", Measurement.State.SUCCEEDED_VALUE)
        bind("$2", request.result)
        bind("$3", request.measurementConsumerReferenceId)
        bind("$4", request.measurementReferenceId)
      }

    transactionContext.run {
      val numRowsUpdated = executeStatement(statement).numRowsUpdated
      if (numRowsUpdated == 0) {
        throw MeasurementNotFoundException()
      }
    }

    return measurement {
      measurementConsumerReferenceId = request.measurementConsumerReferenceId
      measurementReferenceId = request.measurementReferenceId
      state = Measurement.State.SUCCEEDED
      result = request.result
    }
  }
}
