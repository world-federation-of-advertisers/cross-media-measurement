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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry.Details

private val MEASUREMENT_LOG_DETAILS by lazy { Details.getDefaultInstance() }

internal suspend fun SpannerWriter.TransactionScope.createMeasurementStateTransitionLogEntry(
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  nextMeasurementState: Measurement.State,
  previousMeasurementState: Measurement.State = Measurement.State.STATE_UNSPECIFIED
) {
  if (previousMeasurementState != nextMeasurementState) {
    insertMeasurementLogEntry(measurementId, measurementConsumerId)

    insertMeasurementStateTransitionLogEntry(
      measurementId,
      measurementConsumerId,
      previousMeasurementState,
      nextMeasurementState
    )
  }
}

internal fun SpannerWriter.TransactionScope.insertMeasurementLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
) {

  transactionContext.bufferInsertMutation("MeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("MeasurementLogDetails" to MEASUREMENT_LOG_DETAILS)
    setJson("MeasurementLogDetailsJson" to MEASUREMENT_LOG_DETAILS)
  }
}

private fun SpannerWriter.TransactionScope.insertMeasurementStateTransitionLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
  priorMeasurementState: Measurement.State,
  currentMeasurementState: Measurement.State
) {
  transactionContext.bufferInsertMutation("StateTransitionMeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("PriorMeasurementState" to priorMeasurementState)
    set("CurrentMeasurementState" to currentMeasurementState)
  }
}
