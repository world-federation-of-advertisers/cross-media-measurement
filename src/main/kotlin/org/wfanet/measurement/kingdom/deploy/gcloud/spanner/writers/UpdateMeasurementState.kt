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
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry

internal fun SpannerWriter.TransactionScope.updateMeasurementState(
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  nextState: Measurement.State,
  previousState: Measurement.State,
  logDetails: MeasurementLogEntry.Details,
  details: Measurement.Details? = null
) {

  updateMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("State" to nextState)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      if (details != null) {
        set("MeasurementDetails" to details)
        setJson("MeasurementDetailsJson" to details)
      }
    }
    .bufferTo(transactionContext)

  if (nextState == Measurement.State.FAILED) {
    require(logDetails.hasError()) { "$logDetails must have an error when state is FAILED." }
  }

  insertMeasurementLogEntry(measurementId, measurementConsumerId, logDetails)

  insertStateTransitionMeasurementLogEntry(
    measurementId = measurementId,
    measurementConsumerId = measurementConsumerId,
    currentMeasurementState = nextState,
    previousMeasurementState = previousState
  )
}
