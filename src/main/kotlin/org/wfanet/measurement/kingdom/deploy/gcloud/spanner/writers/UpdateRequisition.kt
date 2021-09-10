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
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

internal fun SpannerWriter.TransactionScope.updateRequisition(
  readResult: RequisitionReader.Result,
  state: Requisition.State,
  details: Requisition.Details,
  fulfillingDuchyId: InternalId? = null
) {
  transactionContext.bufferUpdateMutation("Requisitions") {
    set("MeasurementId" to readResult.measurementId.value)
    set("MeasurementConsumerId" to readResult.measurementConsumerId.value)
    set("RequisitionId" to readResult.requisitionId.value)
    set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    set("State" to state)
    set("RequisitionDetails" to details)
    if (fulfillingDuchyId != null) {
      set("FulfillingDuchyId" to fulfillingDuchyId.value)
    }
  }
}
