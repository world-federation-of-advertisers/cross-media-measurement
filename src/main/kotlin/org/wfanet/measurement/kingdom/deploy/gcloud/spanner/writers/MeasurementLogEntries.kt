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
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryDetails

internal fun SpannerWriter.TransactionScope.insertMeasurementLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
  logDetails: MeasurementLogEntryDetails,
) {

  require(logDetails.logMessage.isNotEmpty())

  transactionContext.bufferInsertMutation("MeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("MeasurementLogDetails").to(logDetails)
  }
}

internal fun SpannerWriter.TransactionScope.insertStateTransitionMeasurementLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
  currentMeasurementState: Measurement.State,
  previousMeasurementState: Measurement.State,
) {

  require(previousMeasurementState != currentMeasurementState)

  transactionContext.bufferInsertMutation("StateTransitionMeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("CurrentMeasurementState").toInt64(currentMeasurementState)
    set("PreviousMeasurementState").toInt64(previousMeasurementState)
  }
}

internal fun SpannerWriter.TransactionScope.insertDuchyMeasurementLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
  duchyId: InternalId,
  logDetails: DuchyMeasurementLogEntryDetails,
): ExternalId {
  val externalComputationLogEntryId = idGenerator.generateExternalId()

  transactionContext.bufferInsertMutation("DuchyMeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("DuchyId" to duchyId)
    set("ExternalComputationLogEntryId" to externalComputationLogEntryId)
    set("DuchyMeasurementLogDetails").to(logDetails)
  }

  return externalComputationLogEntryId
}
