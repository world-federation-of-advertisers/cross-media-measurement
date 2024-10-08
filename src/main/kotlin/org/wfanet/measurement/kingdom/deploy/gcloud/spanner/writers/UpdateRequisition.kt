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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

internal fun SpannerWriter.TransactionScope.updateRequisition(
  readResult: RequisitionReader.Result,
  state: Requisition.State,
  details: RequisitionDetails,
  fulfillingDuchyId: InternalId? = null,
) {
  transactionContext.bufferUpdateMutation("Requisitions") {
    set("MeasurementId" to readResult.measurementId.value)
    set("MeasurementConsumerId" to readResult.measurementConsumerId.value)
    set("RequisitionId" to readResult.requisitionId.value)
    set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    set("State").toInt64(state)
    set("RequisitionDetails").to(details)
    if (fulfillingDuchyId != null) {
      set("FulfillingDuchyId" to fulfillingDuchyId)
    }
  }
}

internal suspend fun SpannerWriter.TransactionScope.withdrawRequisitions(
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  excludedRequisitionId: InternalId? = null,
) {
  val keyPrefix = Key.of(measurementConsumerId.value, measurementId.value)
  transactionContext
    .read("Requisitions", KeySet.prefixRange(keyPrefix), listOf("RequisitionId", "State"))
    .collect { row ->
      val requisitionId = row.getInternalId("RequisitionId")
      if (requisitionId == excludedRequisitionId) {
        return@collect
      }

      val requisitionState: Requisition.State =
        row.getProtoEnum("State") {
          Requisition.State.forNumber(it) ?: Requisition.State.UNRECOGNIZED
        }
      when (requisitionState) {
        Requisition.State.PENDING_PARAMS,
        Requisition.State.UNFULFILLED -> {}
        Requisition.State.FULFILLED,
        Requisition.State.REFUSED,
        Requisition.State.WITHDRAWN,
        Requisition.State.STATE_UNSPECIFIED,
        Requisition.State.UNRECOGNIZED -> return@collect
      }

      transactionContext.bufferUpdateMutation("Requisitions") {
        set("MeasurementConsumerId" to measurementConsumerId)
        set("MeasurementId" to measurementId)
        set("RequisitionId" to requisitionId)
        set("State").toInt64(Requisition.State.WITHDRAWN)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }
}
