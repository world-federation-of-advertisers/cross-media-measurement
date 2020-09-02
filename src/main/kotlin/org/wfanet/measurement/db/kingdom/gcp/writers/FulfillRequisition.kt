// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.readers.RequisitionReader
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

/**
 * Marks a Requisition in Spanner as fulfilled (with state [RequisitionState.FULFILLED]).
 */
class FulfillRequisition(
  private val externalRequisitionId: ExternalId,
  private val duchyId: String
) : SpannerWriter<Requisition, Requisition>() {
  override suspend fun TransactionScope.runTransaction(): Requisition {
    val readResult = RequisitionReader().readExternalId(transactionContext, externalRequisitionId)

    require(readResult.requisition.state == RequisitionState.UNFULFILLED) {
      "Requisition $externalRequisitionId is not UNFULFILLED: $readResult"
    }

    Mutation.newUpdateBuilder("Requisitions")
      .set("DataProviderId").to(readResult.dataProviderId)
      .set("CampaignId").to(readResult.campaignId)
      .set("RequisitionId").to(readResult.requisitionId)
      .set("State").toProtoEnum(RequisitionState.FULFILLED)
      .set("DuchyId").to(duchyId)
      .build()
      .bufferTo(transactionContext)

    return readResult.requisition.toBuilder()
      .setState(RequisitionState.FULFILLED)
      .setDuchyId(duchyId)
      .build()
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    return checkNotNull(transactionResult)
  }
}
