package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.internal.kingdom.Report

/**
 * Manages common operations and queries on [Report]s within the Kingdom, such as creation, state
 * changes, and enumeration.
 *
 * This is the "business layer" abstraction above the data layer.
 */
interface ReportManager {

  /**
   * Creates the next [Report] from a [ReportConfigSchedule].
   *
   * This is idempotent: if there's already a [Report] with a windowEndTime in the past, this
   * returns that without performing any changes.
   *
   * @param[externalScheduleId] the external id of the schedule on which to base the [Report]
   * @return a fully persisted [Report]
   */
  suspend fun createNextReport(externalScheduleId: ExternalId): Report

  /**
   * Sends all [Report]s to the output flow.
   *
   * @param[states] a set of states that the [Report]s must be in
   * @return all [Report]s in that state
   */
  suspend fun streamReports(states: Set<Report.ReportState>): Flow<Report>

  /**
   * Updates the [state] of a [Report].
   *
   * @param[reportId] the id of the report
   * @param[state] the new state for it
   * @throws[IllegalArgumentException] if the state transition is illegal
   * @return the resulting fully persisted [Report]
   */
  suspend fun updateReportState(reportId: ExternalId, state: Report.ReportState): Report

  /**
   * Links a [Report] and a [Requisition].
   *
   * A [Report] cannot be started until it is linked to the right [Requisition]s and they are all
   * fulfilled.
   *
   * @param[reportId] the id of the Report
   * @param[requisitionId] the id of the Requisition
   */
  suspend fun associateToRequisition(reportId: ExternalId, requisitionId: ExternalId)
}
