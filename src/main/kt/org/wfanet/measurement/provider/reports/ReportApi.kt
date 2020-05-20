package org.wfanet.measurement.provider.reports

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.internal.kingdom.Requisition

// TODO(efoxepstein): implement these stubs in different files.
class ScheduledReportConfig
abstract class Report {
  abstract val requisitions: List<Requisition>
}

enum class ReportState { SET_UP }

/**
 * Abstraction to encapsulate ReportConfig, Report, and Requisition internal operations.
 *
 * This is mostly a placeholder until the appropriate APIs are decided on.
 */
interface ReportApi {
  /**
   * Load all ScheduledReportConfigs that are "due" -- i.e. those with a next report time
   * in the past.
   */
  suspend fun streamReadyScheduledReportConfigs(): Flow<ScheduledReportConfig>

  /** Load all Reports in the given state. */
  suspend fun streamReportsInState(state: ReportState): Flow<Report>

  /** Loads all pending Reports with no unsatisfied Requisitions. */
  suspend fun streamFulfilledPendingReports(): Flow<Report>

  /**
   * Creates a report from a ScheduledReportConfig. In a transaction, updates the
   * ReportConfigSchedule and inserts a new Report in a pending state. This does not
   * generate the Metrics necessary for the report.
   */
  suspend fun createReport(reportConfig: ScheduledReportConfig): Unit

  /**
   * Starts a pending Report. This will verify that all Requisitions necessary for the report have
   * been fulfilled. As such, it is an expensive operation.
   */
  suspend fun startReport(report: Report): Unit

  /**
   * Persists a requisition if it does not yet exist. This is idempotent -- if called multiple
   * times, only one database row will be inserted.
   */
  suspend fun maybeAddRequisition(requisition: Requisition): Unit
}
