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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByComputationException

/**
 * Creates a DuchyMeasurementLogEntry and MeasurementLogEntry in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 * @throws [MeasurementNotFoundByComputationException] Measurement not found
 * @throws [DuchyNotFoundException] Duchy not found
 */
class CreateDuchyMeasurementLogEntry(private val request: CreateDuchyMeasurementLogEntryRequest) :
  SpannerWriter<DuchyMeasurementLogEntry, DuchyMeasurementLogEntry>() {
  data class MeasurementIds(
    val measurementId: InternalId,
    val measurementConsumerId: InternalId,
    val externalMeasurementId: ExternalId,
    val externalMeasurementConsumerId: ExternalId
  )

  override suspend fun TransactionScope.runTransaction(): DuchyMeasurementLogEntry {

    val measurementIds =
      readMeasurementIds()
        ?: throw MeasurementNotFoundByComputationException(
          ExternalId(request.externalComputationId)
        ) {
          "Measurement for external computation ID ${request.externalComputationId} not found"
        }

    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw DuchyNotFoundException(request.externalDuchyId)

    insertMeasurementLogEntry(
      measurementIds.measurementId,
      measurementIds.measurementConsumerId,
      request.measurementLogEntryDetails
    )

    val externalComputationLogEntryId =
      insertDuchyMeasurementLogEntry(
        measurementIds.measurementId,
        measurementIds.measurementConsumerId,
        InternalId(duchyId),
        request.details
      )

    return duchyMeasurementLogEntry {
      this.externalComputationLogEntryId = externalComputationLogEntryId.value
      details = request.details
      externalDuchyId = request.externalDuchyId
      logEntry = measurementLogEntry {
        details = request.measurementLogEntryDetails
        externalMeasurementId = measurementIds.externalMeasurementId.value
        externalMeasurementConsumerId = measurementIds.externalMeasurementConsumerId.value
      }
    }
  }

  fun translateToInternalIds(struct: Struct): MeasurementIds =
    MeasurementIds(
      InternalId(struct.getLong("MeasurementId")),
      InternalId(struct.getLong("MeasurementConsumerId")),
      ExternalId(struct.getLong("ExternalMeasurementId")),
      ExternalId(struct.getLong("ExternalMeasurementConsumerId"))
    )

  private suspend fun TransactionScope.readMeasurementIds(): MeasurementIds? {

    return transactionContext
      .executeQuery(
        Statement.newBuilder(
            """
          SELECT
            Measurements.MeasurementId,
            Measurements.MeasurementConsumerId,
            Measurements.ExternalMeasurementId,
            Measurements.ExternalComputationId,
            MeasurementConsumers.ExternalMeasurementConsumerId,
          FROM Measurements
          JOIN MeasurementConsumers USING (MeasurementConsumerId)
          WHERE ExternalComputationId = ${request.externalComputationId}
          LIMIT 1
        """
              .trimIndent()
          )
          .build()
      )
      .map(::translateToInternalIds)
      .singleOrNull()
  }

  override fun ResultScope<DuchyMeasurementLogEntry>.buildResult(): DuchyMeasurementLogEntry {
    val duchMeasurementLogEntry = checkNotNull(transactionResult)
    return duchMeasurementLogEntry.copy {
      logEntry = duchMeasurementLogEntry.logEntry.copy { createTime = commitTimestamp.toProto() }
    }
  }
}
