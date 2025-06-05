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

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException

/**
 * Creates a DuchyMeasurementLogEntry and MeasurementLogEntry in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementNotFoundByComputationException] Measurement not found.
 * @throws [DuchyNotFoundException] Duchy not found.
 * @throws [MeasurementStateIllegalException] Measurement in invalid state to add log entry.
 */
class CreateDuchyMeasurementLogEntry(private val request: CreateDuchyMeasurementLogEntryRequest) :
  SpannerWriter<DuchyMeasurementLogEntry, DuchyMeasurementLogEntry>() {
  data class MeasurementInfo(
    val measurementId: InternalId,
    val measurementConsumerId: InternalId,
    val externalMeasurementId: ExternalId,
    val externalMeasurementConsumerId: ExternalId,
    val measurementState: Measurement.State,
  )

  override suspend fun TransactionScope.runTransaction(): DuchyMeasurementLogEntry {

    val measurementInfo =
      readMeasurementInfo()
        ?: throw MeasurementNotFoundByComputationException(
          ExternalId(request.externalComputationId)
        ) {
          "Measurement for external computation ID ${request.externalComputationId} not found"
        }

    when (measurementInfo.measurementState) {
      Measurement.State.SUCCEEDED,
      Measurement.State.FAILED,
      Measurement.State.CANCELLED ->
        throw MeasurementStateIllegalException(
          measurementInfo.externalMeasurementConsumerId,
          measurementInfo.externalMeasurementId,
          measurementInfo.measurementState,
        ) {
          "Cannot create log entry for Measurement in terminal state."
        }
      Measurement.State.PENDING_COMPUTATION,
      Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
      Measurement.State.PENDING_REQUISITION_FULFILLMENT,
      Measurement.State.PENDING_REQUISITION_PARAMS -> {}
      Measurement.State.UNRECOGNIZED,
      Measurement.State.STATE_UNSPECIFIED -> error("Unspecified state.")
    }

    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw DuchyNotFoundException(request.externalDuchyId)

    insertMeasurementLogEntry(
      measurementInfo.measurementId,
      measurementInfo.measurementConsumerId,
      request.measurementLogEntryDetails,
    )

    val externalComputationLogEntryId =
      insertDuchyMeasurementLogEntry(
        measurementInfo.measurementId,
        measurementInfo.measurementConsumerId,
        InternalId(duchyId),
        request.details,
      )

    return duchyMeasurementLogEntry {
      this.externalComputationLogEntryId = externalComputationLogEntryId.value
      details = request.details
      externalDuchyId = request.externalDuchyId
      logEntry = measurementLogEntry {
        details = request.measurementLogEntryDetails
        externalMeasurementId = measurementInfo.externalMeasurementId.value
        externalMeasurementConsumerId = measurementInfo.externalMeasurementConsumerId.value
      }
    }
  }

  private fun translateToInternalMeasurementInfo(struct: Struct): MeasurementInfo =
    MeasurementInfo(
      InternalId(struct.getLong("MeasurementId")),
      InternalId(struct.getLong("MeasurementConsumerId")),
      ExternalId(struct.getLong("ExternalMeasurementId")),
      ExternalId(struct.getLong("ExternalMeasurementConsumerId")),
      struct.getProtoEnum("State", Measurement.State::forNumber),
    )

  private suspend fun TransactionScope.readMeasurementInfo(): MeasurementInfo? {
    val baseSql =
      """
      SELECT
        Measurements.MeasurementId,
        Measurements.MeasurementConsumerId,
        Measurements.ExternalMeasurementId,
        Measurements.ExternalComputationId,
        MeasurementConsumers.ExternalMeasurementConsumerId,
        Measurements.State,
      FROM Measurements
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
      WHERE ExternalComputationId = @externalComputationId
      LIMIT 1
      """
        .trimIndent()

    return transactionContext
      .executeQuery(
        statement(baseSql) { bind("externalComputationId").to(request.externalComputationId) },
        Options.tag("writer=$writerName,action=readMeasurementInfo"),
      )
      .map(::translateToInternalMeasurementInfo)
      .singleOrNull()
  }

  override fun ResultScope<DuchyMeasurementLogEntry>.buildResult(): DuchyMeasurementLogEntry {
    val duchyMeasurementLogEntry = checkNotNull(transactionResult)
    return duchyMeasurementLogEntry.copy {
      logEntry = duchyMeasurementLogEntry.logEntry.copy { createTime = commitTimestamp.toProto() }
    }
  }
}
