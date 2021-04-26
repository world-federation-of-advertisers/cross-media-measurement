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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.kingdom.db.ReportDatabase
import org.wfanet.measurement.kingdom.db.StreamReportsFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetReport
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamReadyReports
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamReports
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.AssociateRequisitionAndReport
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ConfirmDuchyReadiness
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateNextReport
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateReportLogEntry
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FinishReport
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateReportState

class SpannerReportDatabase(clock: Clock, idGenerator: IdGenerator, client: AsyncDatabaseClient) :
  ReportDatabase, BaseSpannerDatabase(clock, idGenerator, client) {
  override suspend fun getReport(externalId: ExternalId): Report {
    return GetReport(externalId).executeSingle()
  }

  override suspend fun createNextReport(
    externalScheduleId: ExternalId,
    combinedPublicKeyResourceId: String
  ): Report {
    return CreateNextReport(externalScheduleId, combinedPublicKeyResourceId).execute()
  }

  override suspend fun updateReportState(externalReportId: ExternalId, state: ReportState): Report {
    return UpdateReportState(externalReportId, state).execute()
  }

  override fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report> {
    return StreamReports(filter, limit).execute()
  }

  override fun streamReadyReports(limit: Long): Flow<Report> {
    return StreamReadyReports(limit).execute()
  }

  override suspend fun associateRequisitionToReport(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) {
    AssociateRequisitionAndReport(externalRequisitionId, externalReportId).execute()
  }

  override suspend fun addReportLogEntry(reportLogEntry: ReportLogEntry): ReportLogEntry {
    return CreateReportLogEntry(reportLogEntry).execute()
  }

  override suspend fun confirmDuchyReadiness(
    externalReportId: ExternalId,
    duchyId: String,
    externalRequisitionIds: Set<ExternalId>
  ): Report {
    return ConfirmDuchyReadiness(externalReportId, duchyId, externalRequisitionIds).execute()
  }

  override suspend fun finishReport(
    externalReportId: ExternalId,
    result: ReportDetails.Result
  ): Report {
    return FinishReport(externalReportId, result).execute()
  }
}
