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

package org.wfanet.measurement.kingdom.daemon

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.Requisition

/**
 * Database abstractions necessary for Kingdom daemons.
 *
 * While the daemons could directly construct and send RPCs, this layer makes the daemons simpler
 * and makes testing behaviors in isolation easier.
 */
interface DaemonDatabaseServicesClient {
  /** Creates the next [Report] for a [ReportConfigSchedule]. */
  suspend fun createNextReport(
    reportConfigSchedule: ReportConfigSchedule,
    combinedPublicKeyResourceId: String
  )

  /**
   * Lists all the [Requisition]s necessary for a [Report].
   *
   * These Requisitions may or may not already exist in storage -- so each should be passed through
   * [createRequisition] to ensure they exist.
   */
  suspend fun buildRequisitionsForReport(report: Report): List<Requisition>

  /** Persists a [Requisition] to the database. */
  suspend fun createRequisition(requisition: Requisition): Requisition

  /** Links a [Requisition] and a [Report] in the database. */
  suspend fun associateRequisitionToReport(requisition: Requisition, report: Report)

  /** Updates the state of [report] in the database to [newState]. */
  suspend fun updateReportState(report: Report, newState: ReportState)

  /**
   * Provides a stream of [Report]s in the given state.
   *
   * The stream may end and will need to be reconnected to pick up on [Report]s newly in [state].
   */
  fun streamReportsInState(state: ReportState): Flow<Report>

  /**
   * Provides a stream of [Report]s where all necessary [Requisition]s are fulfilled.
   *
   * TODO: rename to a better name.
   */
  fun streamReadyReports(): Flow<Report>

  /** Provides a stream of [ReportConfigSchedule]s that need new [Report]s. */
  fun streamReadySchedules(): Flow<ReportConfigSchedule>
}
