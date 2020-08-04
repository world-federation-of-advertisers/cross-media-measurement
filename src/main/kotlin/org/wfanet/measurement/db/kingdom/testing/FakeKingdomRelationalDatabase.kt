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

package org.wfanet.measurement.db.kingdom.testing

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

class FakeKingdomRelationalDatabase : KingdomRelationalDatabase {
  var writeNewRequisitionFn: (Requisition) -> Requisition = { it }
  var fulfillRequisitionFn: (ExternalId) -> Requisition = { Requisition.getDefaultInstance() }
  var streamRequisitionsFn: (StreamRequisitionsFilter, Long) -> Flow<Requisition> =
    { _, _ -> emptyFlow() }
  var getReportFn: (ExternalId) -> Report = { Report.getDefaultInstance() }
  var createNextReportFn: (ExternalId) -> Report = { Report.getDefaultInstance() }
  var updateReportStateFn: (ExternalId, ReportState) -> Report =
    { _, _ -> Report.getDefaultInstance() }
  var streamReportsFn: (StreamReportsFilter, Long) -> Flow<Report> = { _, _ -> emptyFlow() }
  var streamReadyReportsFn: (Long) -> Flow<Report> = { emptyFlow() }
  var associateRequisitionToReportFn: (ExternalId, ExternalId) -> Unit = { _, _ -> }
  var listRequisitionTemplatesFn: (ExternalId) -> Iterable<RequisitionTemplate> = { emptyList() }
  var streamReadySchedulesFn: (Long) -> Flow<ReportConfigSchedule> = { emptyFlow() }
  var addReportLogEntryFn: (ReportLogEntry) -> ReportLogEntry = { it }

  override suspend fun writeNewRequisition(requisition: Requisition): Requisition =
    writeNewRequisitionFn(requisition)

  override suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition =
    fulfillRequisitionFn(externalRequisitionId)

  override fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> =
    streamRequisitionsFn(filter, limit)

  override fun getReport(externalId: ExternalId): Report = getReportFn(externalId)

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

  override fun listRequisitionTemplates(reportConfigId: ExternalId): Iterable<RequisitionTemplate> =
    listRequisitionTemplatesFn(reportConfigId)

  override fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule> =
    streamReadySchedulesFn(limit)

  override fun addReportLogEntry(reportLogEntry: ReportLogEntry): ReportLogEntry =
    addReportLogEntryFn(reportLogEntry)
}
