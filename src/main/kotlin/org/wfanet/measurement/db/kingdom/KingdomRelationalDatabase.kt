package org.wfanet.measurement.db.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

/**
 * Wrapper interface for the Kingdom's relational database.
 */
interface KingdomRelationalDatabase {
  /**
   * Persists a [Requisition] in the database.
   *
   * If an equivalent [Requisition] already exists, this will return that instead.
   *
   * @param[requisition] the Requisition to save
   * @return the [Requisition] in the database -- old or new
   */
  suspend fun writeNewRequisition(requisition: Requisition): Requisition

  /**
   * Updates the state of a [Requisition] to [RequisitionState.FULFILLED].
   */
  suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition

  /**
   * Streams [Requisition]s.
   */
  fun streamRequisitions(filter: StreamRequisitionsFilter, limit: Long): Flow<Requisition>

  /**
   * Creates the next [Report] for a [ReportConfigSchedule].
   *
   * If the report start window would be in the future, this does nothing.
   */
  fun createNextReport(externalScheduleId: ExternalId): Report

  /**
   * Updates the state of a [Report].
   */
  fun updateReportState(externalReportId: ExternalId, state: ReportState): Report

  /**
   * Streams [Report]s.
   */
  fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report>

  /**
   * Streams [Report]s in state [ReportState.AWAITING_REQUISITION_FULFILLMENT] where all of their
   * [Requisition]s have state [RequisitionState.FULFILLED].
   */
  fun streamReadyReports(limit: Long): Flow<Report>

  /**
   * Associates a [Requisition] and a [Report].
   */
  fun associateRequisitionToReport(externalRequisitionId: ExternalId, externalReportId: ExternalId)

  /** Lists the idealized [RequisitionTemplate]s for a [ReportConfig]. */
  fun listRequisitionTemplates(reportConfigId: ExternalId): Iterable<RequisitionTemplate>
}
