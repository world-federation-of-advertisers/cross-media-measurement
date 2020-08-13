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

package org.wfanet.measurement.db.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
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
  suspend fun fulfillRequisition(externalRequisitionId: ExternalId, duchyId: String): Requisition

  /**
   * Streams [Requisition]s.
   */
  fun streamRequisitions(filter: StreamRequisitionsFilter, limit: Long): Flow<Requisition>

  /** Returns a Report given its external id. */
  fun getReport(externalId: ExternalId): Report

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
   * Streams [Report]s ordered by ascending update time.
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

  /** Streams [ReportConfigSchedule]s with a nextReportStartTime in the past. */
  fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule>

  /** Appends a ReportLogEntry to a Report. Returns a copy with all fields filled in. */
  fun addReportLogEntry(reportLogEntry: ReportLogEntry): ReportLogEntry

  /**
   * Confirms that [duchyId] is ready to start work on the Report for [externalReportId].
   *
   * @param[externalReportId] the Report
   * @param[duchyId] the stable Duchy identifier
   * @param[externalRequisitionIds] the [Requisition]s for which this Duchy is providing data
   * @throws[IllegalArgumentException] if [externalRequisitionIds] is not exactly what is expected
   **/
  fun confirmDuchyReadiness(
    externalReportId: ExternalId,
    duchyId: String,
    externalRequisitionIds: Set<ExternalId>
  )
}
