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
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.db.RequisitionUpdate
import org.wfanet.measurement.kingdom.db.to
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportRequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

/**
 * [SpannerWriter] for reading a [Requisition] and, if its state is
 * [RequisitionState.UNFULFILLED], updating it to set its state to
 * [RequisitionState.FULFILLED].
 *
 * If its state is not [RequisitionState.UNFULFILLED], no mutation is made.
 */
class FulfillRequisition(
  private val externalRequisitionId: ExternalId,
  private val duchyId: String
) : SimpleSpannerWriter<RequisitionUpdate>() {
  override suspend fun TransactionScope.runTransaction(): RequisitionUpdate {
    val readResult = RequisitionReader().readExternalId(transactionContext, externalRequisitionId)
    val requisition = readResult.requisition
    if (requisition.state != RequisitionState.UNFULFILLED) {
      return RequisitionUpdate.noOp(requisition)
    }

    Mutation.newUpdateBuilder("Requisitions")
      .set("DataProviderId").to(readResult.dataProviderId)
      .set("CampaignId").to(readResult.campaignId)
      .set("RequisitionId").to(readResult.requisitionId)
      .set("State").toProtoEnum(RequisitionState.FULFILLED)
      .set("DuchyId").to(duchyId)
      .build()
      .bufferTo(transactionContext)

    ReportRequisitionReader
      .readReportsWithAssociatedRequisition(
        transactionContext,
        InternalId(readResult.dataProviderId),
        InternalId(readResult.campaignId),
        InternalId(readResult.requisitionId)
      )
      .collect { updateReportDetails(it) }

    return requisition to
      requisition.toBuilder().also {
        it.duchyId = duchyId
        it.state = RequisitionState.FULFILLED
      }.build()
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
