// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

/**
 * Creates a measurement in the database.
 *
 * Throw KingdomInternalException with code CERT_SUBJECT_KEY_ID_ALREADY_EXISTS when executed if
 */
class CreateMeasurement(private val measurement: Measurement) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val measurementConsumerId =
      MeasurementConsumerReader()
        .readExternalIdOrNull(
          transactionContext,
          ExternalId(measurement.externalMeasurementConsumerId)
        )
        ?.measurementConsumerId
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
        )

    val existingMeasurement = findExistingMeasurement(measurementConsumerId)
    if (existingMeasurement != null) {
      return existingMeasurement
    }
    // Insert into Requisitions for each EDP

    // Insert into ComputationParticipants for each Duchy

    // Insert this into Measurements
    return createNewMeasurement(measurementConsumerId)
  }

  private suspend fun TransactionScope.createNewMeasurement(
    measurementConsumerId: Long
  ): Measurement {
    val internalMeasurementId = idGenerator.generateInternalId()
    val externalMeasurementId = idGenerator.generateExternalId()
    val externalComputationId = idGenerator.generateExternalId()

    insertMutation("Measurements") {
        set("MeasurementId" to internalMeasurementId.value)
        set("MeasurementConsumerId" to measurementConsumerId)
        set("ExternalMeasurementId" to externalMeasurementId.value)
        set("ExternalComputationId" to externalComputationId.value)
        set("ProvidedMeasurementId" to measurement.providedMeasurementId)
        set("State" to Measurement.State.PENDING_REQUISITION_PARAMS)
        set("MeasurementDetails" to measurement.details)
        setJson("MeasurementDetailsJson" to measurement.details)
        set("CreateTime" to Value.COMMIT_TIMESTAMP)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      }
      .bufferTo(transactionContext)

    return measurement
      .toBuilder()
      .also {
        it.externalMeasurementId = externalMeasurementId.value
        it.externalComputationId = externalComputationId.value
      }
      .build()
  }

  private suspend fun TransactionScope.findExistingMeasurement(
    measurementConsumerId: Long
  ): Measurement? {
    val whereClause =
      """
      WHERE Measurements.MeasurementConsumerId = @measurement_consumer_id
        AND Measurements.ProvidedMeasurementId = @provided_measurement_id
      """.trimIndent()

    return MeasurementReader()
      .withBuilder {
        appendClause(whereClause)
        bind("measurement_consumer_id").to(measurementConsumerId)
        bind("provided_measurement_id").to(measurement.providedMeasurementId)
      }
      .execute(transactionContext)
      .map { it.measurement }
      .singleOrNull()
  }

  override fun ResultScope<Measurement>.buildResult(): Measurement {
    val measurement = checkNotNull(transactionResult)
    return if (measurement.hasCreateTime()) {
      measurement
    } else {
      measurement.toBuilder().apply { createTime = commitTimestamp.toProto() }.build()
    }
  }
}
