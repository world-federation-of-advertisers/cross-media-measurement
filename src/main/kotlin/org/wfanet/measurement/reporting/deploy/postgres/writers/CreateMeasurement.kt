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

import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.reporting.service.internal.MeasurementAlreadyExistsException

/**
 * Inserts a Measurement into the database.
 *
 * Throws the following on [execute]:
 * * [MeasurementAlreadyExistsException] Measurement already exists
 */
class CreateMeasurement(private val request: Measurement) : PostgresWriter<Measurement>() {
  override suspend fun TransactionScope.runTransaction(): Measurement {
    val builder =
      boundStatement(
        """
      INSERT INTO Measurements (MeasurementConsumerReferenceId, MeasurementReferenceId, State)
        VALUES ($1, $2, $3)
      """
      ) {
        bind("$1", request.measurementConsumerReferenceId)
        bind("$2", request.measurementReferenceId)
        bind("$3", Measurement.State.PENDING_VALUE)
      }

    transactionContext.run {
      try {
        executeStatement(builder)
      } catch (e: R2dbcDataIntegrityViolationException) {
        throw MeasurementAlreadyExistsException()
      }
    }

    return request.copy { state = Measurement.State.PENDING }
  }
}
