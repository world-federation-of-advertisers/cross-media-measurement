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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.DatabaseClient
import java.time.Clock
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.db.kingdom.gcp.queries.GetReportQuery
import org.wfanet.measurement.db.kingdom.gcp.queries.ReadRequisitionTemplatesQuery
import org.wfanet.measurement.db.kingdom.gcp.queries.StreamReadyReportsQuery
import org.wfanet.measurement.db.kingdom.gcp.queries.StreamReadySchedulesQuery
import org.wfanet.measurement.db.kingdom.gcp.queries.StreamReportsQuery
import org.wfanet.measurement.db.kingdom.gcp.queries.StreamRequisitionsQuery
import org.wfanet.measurement.db.kingdom.gcp.writers.AssociateRequisitionAndReport
import org.wfanet.measurement.db.kingdom.gcp.writers.ConfirmDuchyReadiness
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateAdvertiser
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateCampaign
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateDataProvider
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateNextReport
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateReportConfig
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateReportLogEntry
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateRequisition
import org.wfanet.measurement.db.kingdom.gcp.writers.CreateSchedule
import org.wfanet.measurement.db.kingdom.gcp.writers.FinishReport
import org.wfanet.measurement.db.kingdom.gcp.writers.FulfillRequisition
import org.wfanet.measurement.db.kingdom.gcp.writers.SpannerWriter
import org.wfanet.measurement.db.kingdom.gcp.writers.UpdateReportState
import org.wfanet.measurement.internal.kingdom.Advertiser
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

class GcpKingdomRelationalDatabase(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  lazyClient: () -> DatabaseClient
) : KingdomRelationalDatabase {
  private val client: DatabaseClient by lazy { lazyClient() }

  constructor(
    clock: Clock,
    idGenerator: IdGenerator,
    client: DatabaseClient
  ) : this(clock, idGenerator, { client })

  override suspend fun createRequisition(requisition: Requisition): Requisition {
    return CreateRequisition(requisition).execute()
  }

  override suspend fun fulfillRequisition(
    externalRequisitionId: ExternalId,
    duchyId: String
  ): Requisition {
    return FulfillRequisition(externalRequisitionId, duchyId).execute()
  }

  override fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> {
    return StreamRequisitionsQuery().execute(
      client.singleUse(),
      filter,
      limit
    )
  }

  override fun getReport(externalId: ExternalId): Report {
    return GetReportQuery().execute(client.singleUse(), externalId)
  }

  override fun createNextReport(externalScheduleId: ExternalId): Report {
    return CreateNextReport(externalScheduleId).execute()
  }

  override fun updateReportState(externalReportId: ExternalId, state: ReportState): Report {
    return UpdateReportState(externalReportId, state).execute()
  }

  override fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report> {
    return StreamReportsQuery().execute(client.singleUse(), filter, limit)
  }

  override fun streamReadyReports(limit: Long): Flow<Report> {
    return StreamReadyReportsQuery().execute(client.singleUse(), limit)
  }

  override fun associateRequisitionToReport(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) {
    AssociateRequisitionAndReport(externalRequisitionId, externalReportId).execute()
  }

  override fun listRequisitionTemplates(reportConfigId: ExternalId): Iterable<RequisitionTemplate> {
    return ReadRequisitionTemplatesQuery().execute(client.singleUse(), reportConfigId)
  }

  override fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule> {
    return StreamReadySchedulesQuery().execute(client.singleUse(), limit)
  }

  override fun addReportLogEntry(reportLogEntry: ReportLogEntry): ReportLogEntry {
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

  override fun createDataProvider(): DataProvider {
    return CreateDataProvider().execute()
  }

  override fun createAdvertiser(): Advertiser {
    return CreateAdvertiser().execute()
  }

  override fun createCampaign(
    externalDataProviderId: ExternalId,
    externalAdvertiserId: ExternalId,
    providedCampaignId: String
  ): Campaign {
    return CreateCampaign(externalDataProviderId, externalAdvertiserId, providedCampaignId)
      .execute()
  }

  override fun createReportConfig(
    reportConfig: ReportConfig,
    campaigns: List<ExternalId>
  ): ReportConfig {
    return CreateReportConfig(reportConfig, campaigns).execute()
  }

  override fun createSchedule(schedule: ReportConfigSchedule): ReportConfigSchedule {
    return CreateSchedule(schedule).execute()
  }

  private fun <R> SpannerWriter<*, R>.execute(): R = execute(client, idGenerator, clock)
}
