package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/**
 * Reads [ReportConfigSchedule] protos (and primary key) from Spanner.
 */
class ScheduleReader : SpannerReader<ScheduleReadResult>() {
  override val baseSql: String =
    """
    SELECT Advertisers.ExternalAdvertiserId,
           ReportConfigs.ExternalReportConfigId,
           ReportConfigs.ReportConfigDetails,
           ReportConfigSchedules.ExternalScheduleId,
           ReportConfigSchedules.NextReportStartTime,
           ReportConfigSchedules.RepetitionSpec,
           ReportConfigSchedules.RepetitionSpecJson,
           ReportConfigSchedules.AdvertiserId,
           ReportConfigSchedules.ReportConfigId,
           ReportConfigSchedules.ScheduleId
    FROM ReportConfigSchedules
    JOIN Advertisers USING (AdvertiserId)
    JOIN ReportConfigs USING (AdvertiserId, ReportConfigId)
    """.trimIndent()

  override suspend fun translate(struct: Struct): ScheduleReadResult =
    ScheduleReadResult(
      buildSchedule(struct),
      struct.getLong("AdvertiserId"),
      struct.getLong("ReportConfigId"),
      struct.getLong("ScheduleId"),
      struct.getProtoMessage("ReportConfigDetails", ReportConfigDetails.parser())
    )

  private fun buildSchedule(struct: Struct): ReportConfigSchedule =
    ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
      externalReportConfigId = struct.getLong("ExternalReportConfigId")
      externalScheduleId = struct.getLong("ExternalScheduleId")

      nextReportStartTime = struct.getTimestamp("NextReportStartTime").toProto()
      repetitionSpec = struct.getProtoMessage("RepetitionSpec", RepetitionSpec.parser())
      repetitionSpecJson = struct.getString("RepetitionSpecJson")
    }.build()
}

data class ScheduleReadResult(
  val schedule: ReportConfigSchedule,
  val advertiserId: Long,
  val reportConfigId: Long,
  val scheduleId: Long,
  val reportConfigDetails: ReportConfigDetails
)
