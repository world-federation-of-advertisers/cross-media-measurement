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
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
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

    ReportReader
      .readReportsWithAssociatedRequisition(
        transactionContext,
        InternalId(readResult.dataProviderId),
        InternalId(readResult.campaignId),
        InternalId(readResult.requisitionId)
      )
      .collect { updateReportDetails(it) }

    return readResult.requisition.toBuilder()
      .setState(RequisitionState.FULFILLED)
      .setDuchyId(duchyId)
      .build()
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    return checkNotNull(transactionResult)
  }

  private fun TransactionScope.updateReportDetails(reportReadResult: ReportReader.Result) {
    val newReportDetails = reportReadResult.report.reportDetails.toBuilder().apply {
      requisitionsBuilderList
        .single { it.externalRequisitionId == externalRequisitionId.value }
        .duchyId = duchyId
    }.build()

    Mutation.newUpdateBuilder("Reports")
      .set("AdvertiserId").to(reportReadResult.advertiserId)
      .set("ReportConfigId").to(reportReadResult.reportConfigId)
      .set("ScheduleId").to(reportReadResult.scheduleId)
      .set("ReportId").to(reportReadResult.reportId)
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .set("ReportDetails").toProtoBytes(newReportDetails)
      .set("ReportDetailsJson").toProtoJson(newReportDetails)
      .build()
      .bufferTo(transactionContext)
  }
}
