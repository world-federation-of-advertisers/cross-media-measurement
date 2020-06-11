package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause

class AssociateRequisitionAndReportTransaction {
  fun execute(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) = runBlocking {
    val reportFuture = async { readReport(transactionContext, externalReportId) }
    val requisitionFuture = async { readRequisition(transactionContext, externalRequisitionId) }

    val report = reportFuture.await()
    val requisition = requisitionFuture.await()

    // This uses an InsertOrUpdate to avoid crashing if it already exists. This can't actually
    // update the row because the entire thing is part of the PK.
    transactionContext.buffer(
      Mutation.newInsertOrUpdateBuilder("ReportRequisitions")
        .set("AdvertiserId").to(report.advertiserId)
        .set("ReportConfigId").to(report.reportConfigId)
        .set("ScheduleId").to(report.scheduleId)
        .set("ReportId").to(report.reportId)
        .set("DataProviderId").to(requisition.dataProviderId)
        .set("CampaignId").to(requisition.campaignId)
        .set("RequisitionId").to(requisition.requisitionId)
        .build()
    )
  }

  private suspend fun readReport(
    transactionContext: TransactionContext,
    externalReportId: ExternalId
  ): ReportReadResult =
    ReportReader()
      .withBuilder {
        appendClause("WHERE Reports.ExternalReportId = @external_report_id")
        bind("external_report_id").to(externalReportId.value)
      }
      .execute(transactionContext)
      .single()

  private suspend fun readRequisition(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId
  ): RequisitionReadResult =
    RequisitionReader()
      .withBuilder {
        appendClause("WHERE Requisitions.ExternalRequisitionId = @external_requisition_id")
        bind("external_requisition_id").to(externalRequisitionId.value)
      }
      .execute(transactionContext)
      .single()
}
