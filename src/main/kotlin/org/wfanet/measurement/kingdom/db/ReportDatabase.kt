// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.db

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

/**
 * Abstraction around the Kingdom's relational database for storing [Report] metadata.
 *
 * TODO: with v2alpha API, rename to "MeasurementDatabase".
 */
interface ReportDatabase {
  /** Returns a Report given its external id. */
  suspend fun getReport(externalId: ExternalId): Report

  /**
   * Creates the next [Report] for a ReportConfigSchedule.
   *
   * If the report start window would be in the future, this does nothing.
   */
  suspend fun createNextReport(
    externalScheduleId: ExternalId,
    combinedPublicKeyResourceId: String
  ): Report

  /** Updates the state of a [Report]. */
  suspend fun updateReportState(externalReportId: ExternalId, state: ReportState): Report

  /** Streams [Report]s ordered by ascending update time. */
  fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report>

  /**
   * Streams [Report]s in state [ReportState.AWAITING_REQUISITION_CREATION] where all of their
   * [Requisition]s have state [RequisitionState.FULFILLED].
   */
  fun streamReadyReports(limit: Long): Flow<Report>

  /** Associates a [Requisition] and a [Report]. */
  suspend fun associateRequisitionToReport(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  )

  /** Appends a ReportLogEntry to a Report. Returns a copy with all fields filled in. */
  suspend fun addReportLogEntry(reportLogEntry: ReportLogEntry): ReportLogEntry

  /**
   * Confirms that [duchyId] is ready to start work on the Report for [externalReportId].
   *
   * If all duchies are ready, then the Report is transitioned into state [ReportState.IN_PROGRESS].
   *
   * @param externalReportId the Report
   * @param duchyId the stable Duchy identifier
   * @param externalRequisitionIds the [Requisition]s for which this Duchy is providing data
   * @return the modified Report
   * @throws IllegalArgumentException if [externalRequisitionIds] is not exactly what is expected
   */
  suspend fun confirmDuchyReadiness(
    externalReportId: ExternalId,
    duchyId: String,
    externalRequisitionIds: Set<ExternalId>
  ): Report

  /**
   * Finalizes a [Report].
   *
   * @param externalReportId the Report
   * @param result the end result for the report
   */
  suspend fun finishReport(externalReportId: ExternalId, result: ReportDetails.Result): Report
}
