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

import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumer
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerAlreadyExistsException

/**
 * Inserts a Measurement Consumer into the database.
 *
 * Throws the following on [execute]:
 * * [MeasurementConsumerAlreadyExistsException] MeasurementConsumer already exists
 */
class CreateMeasurementConsumer(private val measurementConsumer: MeasurementConsumer) :
  PostgresWriter<MeasurementConsumer>() {
  override suspend fun TransactionScope.runTransaction(): MeasurementConsumer {
    val internalMeasurementConsumerId = idGenerator.generateInternalId().value

    val statement =
      boundStatement(
        """
      INSERT INTO MeasurementConsumers (MeasurementConsumerId, CmmsMeasurementConsumerId)
        VALUES ($1, $2)
      """
      ) {
        bind("$1", internalMeasurementConsumerId)
        bind("$2", measurementConsumer.cmmsMeasurementConsumerId)
      }

    try {
      transactionContext.executeStatement(statement)
    } catch (e: R2dbcDataIntegrityViolationException) {
      throw MeasurementConsumerAlreadyExistsException()
    }

    return measurementConsumer
  }
}
