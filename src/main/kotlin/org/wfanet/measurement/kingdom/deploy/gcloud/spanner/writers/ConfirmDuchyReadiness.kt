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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toSet
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader

/**
 * Confirms a Duchy's readiness for a [Report].
 *
 * If all Duchies are ready, the [Report] is put into state [ReportState.IN_PROGRESS].
 */
class ConfirmDuchyReadiness(
  private val externalReportId: ExternalId,
  private val duchyId: String,
  private val externalRequisitionIds: Set<ExternalId>
) : SimpleSpannerWriter<Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    require(duchyId in DuchyIds.ALL) {
      "Duchy id '$duchyId' not in list of valid duchies: ${DuchyIds.ALL}"
    }

    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    require(reportReadResult.report.state == ReportState.AWAITING_DUCHY_CONFIRMATION) {
      "Report $externalReportId is in wrong state: ${reportReadResult.report.state}"
    }
    if (duchyId in reportReadResult.report.reportDetails.confirmedDuchiesList) {
      return reportReadResult.report
    }

    val requisitions = readRequisitionsForReportAndDuchy(reportReadResult.reportId)
    val expectedIds = requisitions.map { ExternalId(it) }.toSet()
    validateRequisitions(externalRequisitionIds, expectedIds)

    val newReport = reportReadResult.report.toBuilder().apply {
      reportDetailsBuilder.addConfirmedDuchies(duchyId)
      if (reportDetails.confirmedDuchiesCount == DuchyIds.size) {
        state = ReportState.IN_PROGRESS
      }
    }.build()

    updateReport(reportReadResult, newReport)

    return newReport
  }

  private fun TransactionScope.readRequisitionsForReportAndDuchy(
    reportId: Long
  ): Flow<Long> {
    val sql =
      """
      SELECT Requisitions.ExternalRequisitionId
      FROM ReportRequisitions
      JOIN Requisitions USING (DataProviderId, CampaignId, RequisitionId)
      WHERE ReportRequisitions.ReportId = @report_id
        AND Requisitions.DuchyId = @duchy_id
      """.trimIndent()
    val statement =
      Statement.newBuilder(sql)
        .bind("report_id").to(reportId)
        .bind("duchy_id").to(duchyId)
        .build()
    return transactionContext
      .executeQuery(statement)
      .map { it.getLong("ExternalRequisitionId") }
  }

  private fun validateRequisitions(providedIds: Set<ExternalId>, expectedIds: Set<ExternalId>) {
    require(providedIds == expectedIds) {
      """
      Provided Requisitions do not match what's expected:
        - Matching external ids: ${providedIds intersect expectedIds}
        - Missing external ids: ${expectedIds subtract providedIds}
        - Extra external ids: ${providedIds subtract expectedIds}
      """.trimIndent()
    }
  }

  private fun TransactionScope.updateReport(
    reportReadResult: ReportReader.Result,
    newReport: Report
  ) {
    Mutation.newUpdateBuilder("Reports")
      .set("AdvertiserId").to(reportReadResult.advertiserId)
      .set("ReportConfigId").to(reportReadResult.reportConfigId)
      .set("ScheduleId").to(reportReadResult.scheduleId)
      .set("ReportId").to(reportReadResult.reportId)
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .set("ReportDetails").toProtoBytes(newReport.reportDetails)
      .set("ReportDetailsJson").toProtoJson(newReport.reportDetails)
      .set("State").toProtoEnum(newReport.state)
      .build()
      .bufferTo(transactionContext)
  }
}
