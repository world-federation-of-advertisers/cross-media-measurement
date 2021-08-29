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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter.TransactionScope

/**
 * Creates a DuchyMeasurementLogEntry and MeasurementLogEntry in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.MEASUREMENT_NOT_FOUND]
 * * [KingdomInternalException.Code.DUCHY_NOT_FOUND]
 */
class CreateDuchyMeasurementLogEntry(private val request: CreateDuchyMeasurementLogEntryRequest) :
  SpannerWriter<DuchyMeasurementLogEntry, DuchyMeasurementLogEntry>() {

  override suspend fun TransactionScope.runTransaction(): DuchyMeasurementLogEntry {

    val measurementResult =
      MeasurementReader(Measurement.View.DEFAULT)
        .readExternalIdOrNull(transactionContext, ExternalId(request.externalComputationId))
        ?: throw KingdomInternalException(KingdomInternalException.Code.MEASUREMENT_NOT_FOUND) {
          "Measurement for external computation ID ${request.externalComputationId} not found"
        }

    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw KingdomInternalException(KingdomInternalException.Code.DUCHY_NOT_FOUND)

    val measurement = measurementResult.measurement
    val measurementId = measurementResult.measurementId
    val measurementConsumerId = measurementResult.measurementConsumerId
    val measurementLogEntry =
      createMeasurementLogEntry(InternalId(measurementId), InternalId(measurementConsumerId))

    return createDuchyMeasurementLogEntry(
      InternalId(measurementId),
      InternalId(measurementConsumerId),
      InternalId(duchyId)
    )
      .copy {
        externalDuchyId = request.externalDuchyId
        logEntry =
          measurementLogEntry.copy {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          }
      }
  }

  private suspend fun TransactionScope.createMeasurementLogEntry(
    measurementId: InternalId,
    measurementConsumerId: InternalId,
  ): MeasurementLogEntry {

    transactionContext.bufferInsertMutation("MeasurementLogEntries") {
      set("MeasurementConsumerId" to measurementConsumerId.value)
      set("MeasurementId" to measurementId.value)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("MeasurementLogDetails" to request.measurementLogEntryDetails)
      setJson("MeasurementLogDetailsJson" to request.measurementLogEntryDetails)
    }

    return measurementLogEntry { details = request.measurementLogEntryDetails }
  }

  private suspend fun TransactionScope.createDuchyMeasurementLogEntry(
    measurementId: InternalId,
    measurementConsumerId: InternalId,
    duchyId: InternalId
  ): DuchyMeasurementLogEntry {
    val externalComputationLogEntryId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("DuchyMeasurementLogEntries") {
      set("MeasurementConsumerId" to measurementConsumerId.value)
      set("MeasurementId" to measurementId.value)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("DuchyId" to duchyId.value)
      set("ExternalComputationLogEntryId" to externalComputationLogEntryId.value)
      set("DuchyMeasurementLogDetails" to request.details)
      setJson("DuchyMeasurementLogDetailsJson" to request.details)
    }

    return duchyMeasurementLogEntry {
      this.externalComputationLogEntryId = externalComputationLogEntryId.value
      details = request.details
    }
  }
  override fun ResultScope<DuchyMeasurementLogEntry>.buildResult(): DuchyMeasurementLogEntry {
    val duchMeasurementLogEntry = checkNotNull(transactionResult)
    return duchMeasurementLogEntry.copy {
      logEntry = duchMeasurementLogEntry.logEntry.copy { createTime = commitTimestamp.toProto() }
    }
  }
}
