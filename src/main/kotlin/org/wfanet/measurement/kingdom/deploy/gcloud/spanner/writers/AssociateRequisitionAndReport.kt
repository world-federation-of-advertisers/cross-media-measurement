// Copyright 2020 The Cross-Media Measurement Authors
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
import java.util.logging.Logger
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.single
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

class AssociateRequisitionAndReport(
  private val externalRequisitionId: ExternalId,
  private val externalReportId: ExternalId
) : SpannerWriter<Unit, Unit>() {

  override suspend fun TransactionScope.runTransaction() {
    val (reportReadResult, requisitionReadResult) =
      readReportAndRequisition(externalReportId, externalRequisitionId)

    if (reportReadResult.report.alreadyHasRequisition()) {
      logger.info("Requisition $externalRequisitionId already linked to Report $externalReportId")
      return
    }

    Mutation.newInsertBuilder("ReportRequisitions")
      .set("AdvertiserId").to(reportReadResult.advertiserId)
      .set("ReportConfigId").to(reportReadResult.reportConfigId)
      .set("ScheduleId").to(reportReadResult.scheduleId)
      .set("ReportId").to(reportReadResult.reportId)
      .set("DataProviderId").to(requisitionReadResult.dataProviderId)
      .set("CampaignId").to(requisitionReadResult.campaignId)
      .set("RequisitionId").to(requisitionReadResult.requisitionId)
      .build()
      .bufferTo(transactionContext)

    val requisition = requisitionReadResult.requisition

    val newReportDetails = reportReadResult.report.reportDetails.toBuilder().apply {
      addRequisitionsBuilder().apply {
        externalDataProviderId = requisition.externalDataProviderId
        externalCampaignId = requisition.externalCampaignId
        externalRequisitionId = requisition.externalRequisitionId
        duchyId = requisition.duchyId
      }
    }.build()

    Mutation.newUpdateBuilder("Reports")
      .apply {
        set("AdvertiserId").to(reportReadResult.advertiserId)
        set("ReportConfigId").to(reportReadResult.reportConfigId)
        set("ScheduleId").to(reportReadResult.scheduleId)
        set("ReportId").to(reportReadResult.reportId)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        set("ReportDetails").toProtoBytes(newReportDetails)
        set("ReportDetailsJson").toProtoJson(newReportDetails)
        if (requisition.state == RequisitionState.PERMANENTLY_UNAVAILABLE) {
          set("State").toProtoEnum(Report.ReportState.FAILED)
        }
      }
      .build()
      .bufferTo(transactionContext)
  }

  override fun ResultScope<Unit>.buildResult() {
    // Deliberately empty.
  }

  private fun Report.alreadyHasRequisition(): Boolean {
    return reportDetails.requisitionsList.any {
      it.externalRequisitionId == externalRequisitionId.value
    }
  }

  private suspend fun TransactionScope.readReportAndRequisition(
    externalReportId: ExternalId,
    externalRequisitionId: ExternalId
  ): Pair<ReportReader.Result, RequisitionReader.Result> = coroutineScope {
    val reportDeferred = async {
      ReportReader().readExternalId(transactionContext, externalReportId)
    }
    val requisitionDeferred = async { readRequisition(externalRequisitionId) }

    Pair(reportDeferred.await(), requisitionDeferred.await())
  }

  private suspend fun TransactionScope.readRequisition(
    externalRequisitionId: ExternalId
  ): RequisitionReader.Result {
    return RequisitionReader()
      .withBuilder {
        appendClause("WHERE Requisitions.ExternalRequisitionId = @external_requisition_id")
        bind("external_requisition_id").to(externalRequisitionId.value)
      }
      .execute(transactionContext)
      .single()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
