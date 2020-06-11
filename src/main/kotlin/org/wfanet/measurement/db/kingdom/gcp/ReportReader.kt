package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getProtoBufMessage
import org.wfanet.measurement.db.gcp.getProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportDetails

/**
 * Reads [Report] protos from Spanner.
 */
class ReportReader : SpannerReader<ReportReadResult>() {
  companion object {
    val SELECT_COLUMNS = listOf(
      "Reports.AdvertiserId",
      "Reports.ReportConfigId",
      "Reports.ScheduleId",
      "Reports.ReportId",
      "Reports.ExternalReportId",
      "Reports.CreateTime",
      "Reports.WindowStartTime",
      "Reports.WindowEndTime",
      "Reports.State",
      "Reports.ReportDetails",
      "Reports.ReportDetailsJson",
      "Advertisers.ExternalAdvertiserId",
      "ReportConfigs.ExternalReportConfigId",
      "ReportConfigSchedules.ExternalScheduleId"
    )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM Reports
    JOIN Advertisers USING (AdvertiserId)
    JOIN ReportConfigs USING (AdvertiserId, ReportConfigId)
    JOIN ReportConfigSchedules USING (AdvertiserId, ReportConfigId, ScheduleId)
    """.trimIndent()

  override suspend fun translate(struct: Struct): ReportReadResult =
    ReportReadResult(
      buildReport(struct),
      struct.getLong("AdvertiserId"),
      struct.getLong("ReportConfigId"),
      struct.getLong("ScheduleId"),
      struct.getLong("ReportId")
    )

  private fun buildReport(struct: Struct): Report = Report.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
    externalReportConfigId = struct.getLong("ExternalReportConfigId")
    externalScheduleId = struct.getLong("ExternalScheduleId")
    externalReportId = struct.getLong("ExternalReportId")

    createTime = struct.getTimestamp("CreateTime").toProto()

    windowStartTime = struct.getTimestamp("WindowStartTime").toProto()
    windowEndTime = struct.getTimestamp("WindowEndTime").toProto()
    state = struct.getProtoEnum("State", Report.ReportState::forNumber)

    reportDetails = struct.getProtoBufMessage("ReportDetails", ReportDetails.parser())
    reportDetailsJson = struct.getString("ReportDetailsJson")
  }.build()
}

data class ReportReadResult(
  val report: Report,
  val advertiserId: Long,
  val reportConfigId: Long,
  val scheduleId: Long,
  val reportId: Long
)
