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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails

/**
 * Reads [Report] protos from Spanner.
 */
class ReportReader(index: Index = Index.NONE) : SpannerReader<ReportReader.Result>() {
  data class Result(
    val report: Report,
    val advertiserId: Long,
    val reportConfigId: Long,
    val scheduleId: Long,
    val reportId: Long,
    val numRequisitions: Long
  )

  enum class Index(internal val sql: String) {
    NONE(""),
    STATE("@{FORCE_INDEX=ReportsByState}"),
    EXTERNAL_ID("@{FORCE_INDEX=ReportsByExternalId}")
  }

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM Reports${index.sql}
    JOIN Advertisers USING (AdvertiserId)
    JOIN ReportConfigs USING (AdvertiserId, ReportConfigId)
    JOIN ReportConfigSchedules USING (AdvertiserId, ReportConfigId, ScheduleId)
    """.trimIndent()

  override val externalIdColumn: String = "Reports.ExternalReportId"

  override suspend fun translate(struct: Struct): Result {
    return Result(
      report = buildReport(struct),
      advertiserId = struct.getLong("AdvertiserId"),
      reportConfigId = struct.getLong("ReportConfigId"),
      scheduleId = struct.getLong("ScheduleId"),
      reportId = struct.getLong("ReportId"),
      numRequisitions = struct.getLong("NumRequisitions")
    )
  }

  private fun buildReport(struct: Struct): Report = Report.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
    externalReportConfigId = struct.getLong("ExternalReportConfigId")
    externalScheduleId = struct.getLong("ExternalScheduleId")
    externalReportId = struct.getLong("ExternalReportId")

    createTime = struct.getTimestamp("CreateTime").toProto()
    updateTime = struct.getTimestamp("UpdateTime").toProto()

    windowStartTime = struct.getTimestamp("WindowStartTime").toProto()
    windowEndTime = struct.getTimestamp("WindowEndTime").toProto()
    state = struct.getProtoEnum("State", ReportState::forNumber)

    reportDetails = struct.getProtoMessage("ReportDetails", ReportDetails.parser())
    reportDetailsJson = struct.getString("ReportDetailsJson")
  }.build()

  companion object {
    private val SELECT_COLUMNS = listOf(
      "Reports.AdvertiserId",
      "Reports.ReportConfigId",
      "Reports.ScheduleId",
      "Reports.ReportId",
      "Reports.ExternalReportId",
      "Reports.CreateTime",
      "Reports.UpdateTime",
      "Reports.WindowStartTime",
      "Reports.WindowEndTime",
      "Reports.State",
      "Reports.ReportDetails",
      "Reports.ReportDetailsJson",
      "Advertisers.ExternalAdvertiserId",
      "ReportConfigs.ExternalReportConfigId",
      "ReportConfigs.NumRequisitions",
      "ReportConfigSchedules.ExternalScheduleId"
    )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }
}
