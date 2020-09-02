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
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.single
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.db.kingdom.gcp.readers.RequisitionReader
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

class AssociateRequisitionAndReport(
  private val externalRequisitionId: ExternalId,
  private val externalReportId: ExternalId
) : SpannerWriter<Unit, Unit>() {

  override suspend fun TransactionScope.runTransaction() {
    val (reportReadResult, requisitionReadResult) =
      readReportAndRequisition(externalReportId, externalRequisitionId)

    // This uses an InsertOrUpdate to avoid throwing if it already exists. This can't actually
    // update the row because all columns are part of the PK.
    Mutation.newInsertOrUpdateBuilder("ReportRequisitions")
      .set("AdvertiserId").to(reportReadResult.advertiserId)
      .set("ReportConfigId").to(reportReadResult.reportConfigId)
      .set("ScheduleId").to(reportReadResult.scheduleId)
      .set("ReportId").to(reportReadResult.reportId)
      .set("DataProviderId").to(requisitionReadResult.dataProviderId)
      .set("CampaignId").to(requisitionReadResult.campaignId)
      .set("RequisitionId").to(requisitionReadResult.requisitionId)
      .build()
      .bufferTo(transactionContext)

    if (requisitionReadResult.requisition.state == RequisitionState.PERMANENTLY_UNAVAILABLE) {
      Mutation.newUpdateBuilder("Reports")
        .set("AdvertiserId").to(reportReadResult.advertiserId)
        .set("ReportConfigId").to(reportReadResult.reportConfigId)
        .set("ScheduleId").to(reportReadResult.scheduleId)
        .set("ReportId").to(reportReadResult.reportId)
        .set("State").toProtoEnum(Report.ReportState.FAILED)
        .build()
        .bufferTo(transactionContext)
    }
  }

  override fun ResultScope<Unit>.buildResult() {
    // Deliberately empty.
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
  ): RequisitionReader.Result =
    RequisitionReader()
      .withBuilder {
        appendClause("WHERE Requisitions.ExternalRequisitionId = @external_requisition_id")
        bind("external_requisition_id").to(externalRequisitionId.value)
      }
      .execute(transactionContext)
      .single()
}
