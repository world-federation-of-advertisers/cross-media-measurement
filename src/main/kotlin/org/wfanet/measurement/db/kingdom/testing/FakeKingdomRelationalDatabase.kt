package org.wfanet.measurement.db.kingdom.testing

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition

class FakeKingdomRelationalDatabase : KingdomRelationalDatabase {
  var writeNewRequisitionFn: (Requisition) -> Requisition = { it }
  var fulfillRequisitionFn: (ExternalId) -> Requisition = { Requisition.getDefaultInstance() }
  var streamRequisitionsFn: (StreamRequisitionsFilter, Long) -> Flow<Requisition> =
    { _, _ -> emptyFlow() }
  var createNextReportFn: (ExternalId) -> Report = { Report.getDefaultInstance() }
  var updateReportStateFn: (ExternalId, ReportState) -> Report =
    { _, _ -> Report.getDefaultInstance() }
  var streamReportsFn: (StreamReportsFilter, Long) -> Flow<Report> = { _, _ -> emptyFlow() }
  var streamReadyReportsFn: (Long) -> Flow<Report> = { emptyFlow() }
  var associateRequisitionToReportFn: (ExternalId, ExternalId) -> Unit = { _, _ -> }

  override suspend fun writeNewRequisition(requisition: Requisition): Requisition =
    writeNewRequisitionFn(requisition)

  override suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition =
    fulfillRequisitionFn(externalRequisitionId)

  override fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> =
    streamRequisitionsFn(filter, limit)

  override fun createNextReport(externalScheduleId: ExternalId): Report =
    createNextReportFn(externalScheduleId)

  override fun updateReportState(externalReportId: ExternalId, state: ReportState): Report =
    updateReportStateFn(externalReportId, state)

  override fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report> =
    streamReportsFn(filter, limit)

  override fun streamReadyReports(limit: Long): Flow<Report> = streamReadyReportsFn(limit)

  override fun associateRequisitionToReport(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) =
    associateRequisitionToReportFn(externalRequisitionId, externalReportId)
}
