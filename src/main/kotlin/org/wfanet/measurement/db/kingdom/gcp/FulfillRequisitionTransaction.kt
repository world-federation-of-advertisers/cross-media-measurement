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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

/**
 * Marks a Requisition in Spanner as fulfilled (with state [RequisitionState.FULFILLED]).
 */
class FulfillRequisitionTransaction {
  /**
   * Runs the transaction body.
   *
   * @param[transactionContext] the transaction to use
   * @param[externalRequisitionId] the id of the [Requisition]
   * @throws[IllegalArgumentException] if the [Requisition] doesn't exist
   * @return the existing [Requisition] after being marked as fulfilled
   */
  fun execute(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId,
    duchyId: String
  ): Requisition {
    val readResult = runBlocking(spannerDispatcher()) {
      RequisitionReader().readExternalId(transactionContext, externalRequisitionId)
    }

    require(readResult.requisition.state == RequisitionState.UNFULFILLED) {
      "Requisition $externalRequisitionId is not UNFULFILLED: $readResult"
    }

    val mutation: Mutation =
      Mutation.newUpdateBuilder("Requisitions")
        .set("DataProviderId").to(readResult.dataProviderId)
        .set("CampaignId").to(readResult.campaignId)
        .set("RequisitionId").to(readResult.requisitionId)
        .set("State").toProtoEnum(RequisitionState.FULFILLED)
        .set("DuchyId").to(duchyId)
        .build()
    transactionContext.buffer(mutation)

    return readResult.requisition.toBuilder()
      .setState(RequisitionState.FULFILLED)
      .setDuchyId(duchyId)
      .build()
  }
}
