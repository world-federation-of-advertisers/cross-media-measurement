package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getProtoEnum
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfig.ReportConfigState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails

class ReportConfigReader : SpannerReader<ReportConfigReader.Result>() {
  data class Result(
    val reportConfig: ReportConfig,
    val advertiserId: Long,
    val reportConfigId: Long
  )

  override val baseSql: String =
    """
    SELECT
      Advertisers.ExternalAdvertiserId,
      ReportConfigs.AdvertiserId,
      ReportConfigs.ReportConfigId,
      ReportConfigs.ExternalReportConfigId,
      ReportConfigs.NumRequisitions,
      ReportConfigs.State,
      ReportConfigs.ReportConfigDetails,
      ReportConfigs.ReportConfigDetailsJson
    FROM ReportConfigs
    JOIN Advertisers USING (AdvertiserId)
    """.trimIndent()

  override val externalIdColumn: String = "ReportConfigs.ExternalReportConfigId"

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildReportConfig(struct),
      struct.getLong("AdvertiserId"),
      struct.getLong("ReportConfigId")
    )

  private fun buildReportConfig(struct: Struct): ReportConfig = ReportConfig.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
    externalReportConfigId = struct.getLong("ExternalReportConfigId")
    numRequisitions = struct.getLong("NumRequisitions")
    state = struct.getProtoEnum("State", ReportConfigState::forNumber)
    reportConfigDetails =
      struct.getProtoMessage("ReportConfigDetails", ReportConfigDetails.parser())
    reportConfigDetailsJson = struct.getString("ReportConfigDetailsJson")
  }.build()
}
