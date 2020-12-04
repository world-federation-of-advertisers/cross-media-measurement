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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.kingdom.db.RequisitionUpdate
import org.wfanet.measurement.kingdom.db.to
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportRequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

/**
 * [SpannerWriter] for reading a [Requisition] and, if its state is
 * [RequisitionState.UNFULFILLED], updating it to set its state to
 * [RequisitionState.PERMANENTLY_UNAVAILABLE].
 *
 * If its state is not [RequisitionState.UNFULFILLED], no mutation is made.
 */
class RefuseRequisition(
  private val externalRequisitionId: ExternalId,
  private val refusal: RequisitionDetails.Refusal
) : SimpleSpannerWriter<RequisitionUpdate>() {
  override suspend fun TransactionScope.runTransaction(): RequisitionUpdate {
    val readResult = RequisitionReader().readExternalId(transactionContext, externalRequisitionId)
    val requisition = readResult.requisition
    if (requisition.state != RequisitionState.UNFULFILLED) {
      return RequisitionUpdate.noOp(requisition)
    }

    val requisitionDetails = requisition.requisitionDetails.toBuilder().also {
      it.refusal = refusal
    }.build()
    Mutation.newUpdateBuilder("Requisitions")
      .set("DataProviderId").to(readResult.dataProviderId)
      .set("CampaignId").to(readResult.campaignId)
      .set("RequisitionId").to(readResult.requisitionId)
      .set("State").toProtoEnum(RequisitionState.PERMANENTLY_UNAVAILABLE)
      .set("RequisitionDetails").toProtoBytes(requisitionDetails)
      .set("RequisitionDetailsJson").toProtoJson(requisitionDetails)
      .build()
      .bufferTo(transactionContext)

    ReportRequisitionReader
      .readReportsWithAssociatedRequisition(
        transactionContext,
        InternalId(readResult.dataProviderId),
        InternalId(readResult.campaignId),
        InternalId(readResult.requisitionId)
      )
      .collect { markReportFailed(it) }

    return requisition to
      requisition.toBuilder().also {
        it.state = RequisitionState.PERMANENTLY_UNAVAILABLE
        it.requisitionDetails = requisitionDetails
        it.requisitionDetailsJson = requisitionDetails.toJson()
      }.build()
  }

  private fun TransactionScope.markReportFailed(reportReadResult: ReportReader.Result) =
    updateReportState(reportReadResult, Report.ReportState.FAILED)
}
