package org.wfanet.measurement.db.kingdom.gcp.testing

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import java.time.Instant
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.ReportReader
import org.wfanet.measurement.db.kingdom.gcp.RequisitionReader
import org.wfanet.measurement.db.kingdom.gcp.ScheduleReader
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfig.ReportConfigState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails

abstract class KingdomDatabaseTestBase :
  UsingSpannerEmulator("/src/main/db/gcp/measurement_provider.sdl") {

  companion object {
    @JvmStatic
    fun buildRequisitionDetails(sketchConfigId: Long): RequisitionDetails =
      RequisitionDetails.newBuilder().apply {
        metricDefinitionBuilder.sketchBuilder.sketchConfigId = sketchConfigId
      }.build()
  }

  private fun write(mutation: Mutation) {
    databaseClient.write(listOf(mutation))
  }

  // TODO: add AdvertiserDetails proto as input.
  protected fun insertAdvertiser(
    advertiserId: Long,
    externalAdvertiserId: Long
  ) {
    write(
      Mutation.newInsertBuilder("Advertisers")
        .set("AdvertiserId").to(advertiserId)
        .set("ExternalAdvertiserId").to(externalAdvertiserId)
        .set("AdvertiserDetails").to(ByteArray.copyFrom(""))
        .set("AdvertiserDetailsJson").to("irrelevant-advertiser-details-json")
        .build()
    )
  }

  protected fun insertReportConfig(
    advertiserId: Long,
    reportConfigId: Long,
    externalReportConfigId: Long,
    state: ReportConfigState = ReportConfigState.ACTIVE,
    reportConfigDetails: ReportConfigDetails = ReportConfigDetails.getDefaultInstance(),
    numRequisitions: Long = 0
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigs")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("ExternalReportConfigId").to(externalReportConfigId)
        .set("NumRequisitions").to(numRequisitions)
        .set("State").toProtoEnum(state)
        .set("ReportConfigDetails").toProtoBytes(reportConfigDetails)
        .set("ReportConfigDetailsJson").toProtoJson(reportConfigDetails)
        .build()
    )
  }

  protected fun insertReportConfigSchedule(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    externalScheduleId: Long,
    nextReportStartTime: Instant = Instant.EPOCH,
    repetitionSpec: RepetitionSpec = RepetitionSpec.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigSchedules")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("ScheduleId").to(scheduleId)
        .set("ExternalScheduleId").to(externalScheduleId)
        .set("NextReportStartTime").to(nextReportStartTime.toGcpTimestamp())
        .set("RepetitionSpec").toProtoBytes(repetitionSpec)
        .set("RepetitionSpecJson").toProtoJson(repetitionSpec)
        .build()
    )
  }

  protected fun insertReportConfigCampaign(
    advertiserId: Long,
    reportConfigId: Long,
    dataProviderId: Long,
    campaignId: Long
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigCampaigns")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .build()
    )
  }

  protected fun insertReport(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    reportId: Long,
    externalReportId: Long,
    state: ReportState,
    createTime: Instant? = null,
    updateTime: Instant? = null,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    reportDetails: ReportDetails = ReportDetails.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("Reports")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("ScheduleId").to(scheduleId)
        .set("ReportId").to(reportId)
        .set("ExternalReportId").to(externalReportId)
        .set("CreateTime").to(createTime?.toGcpTimestamp() ?: Value.COMMIT_TIMESTAMP)
        .set("UpdateTime").to(updateTime?.toGcpTimestamp() ?: Value.COMMIT_TIMESTAMP)
        .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
        .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
        .set("State").toProtoEnum(state)
        .set("ReportDetails").toProtoBytes(reportDetails)
        .set("ReportDetailsJson").toProtoJson(reportDetails)
        .build()
    )
  }

  protected fun insertDataProvider(
    dataProviderId: Long,
    externalDataProviderId: Long
  ) {
    write(
      Mutation.newInsertBuilder("DataProviders")
        .set("DataProviderId").to(dataProviderId)
        .set("ExternalDataProviderId").to(externalDataProviderId)
        .set("DataProviderDetails").to(ByteArray.copyFrom(""))
        .set("DataProviderDetailsJson").to("")
        .build()
    )
  }

  protected fun insertCampaign(
    dataProviderId: Long,
    campaignId: Long,
    externalCampaignId: Long,
    advertiserId: Long,
    providedCampaignId: String = ""
  ) {
    write(
      Mutation.newInsertBuilder("Campaigns")
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .set("ExternalCampaignId").to(externalCampaignId)
        .set("AdvertiserId").to(advertiserId)
        .set("ProvidedCampaignId").to(providedCampaignId)
        .set("CampaignDetails").to(ByteArray.copyFrom(""))
        .set("CampaignDetailsJson").to("")
        .build()
    )
  }

  protected fun insertRequisition(
    dataProviderId: Long,
    campaignId: Long,
    requisitionId: Long,
    externalRequisitionId: Long,
    createTime: Instant = Instant.EPOCH,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    state: RequisitionState = RequisitionState.UNFULFILLED,
    requisitionDetails: RequisitionDetails = RequisitionDetails.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("Requisitions")
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .set("RequisitionId").to(requisitionId)
        .set("ExternalRequisitionId").to(externalRequisitionId)
        .set("CreateTime").to(createTime.toGcpTimestamp())
        .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
        .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
        .set("State").toProtoEnum(state)
        .set("RequisitionDetails").toProtoBytes(requisitionDetails)
        .set("RequisitionDetailsJson").toProtoJson(requisitionDetails)
        .build()
    )
  }

  protected fun insertReportRequisition(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    reportId: Long,
    dataProviderId: Long,
    campaignId: Long,
    requisitionId: Long
  ) {
    write(
      Mutation.newInsertBuilder("ReportRequisitions")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("ScheduleId").to(scheduleId)
        .set("ReportId").to(reportId)
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .set("RequisitionId").to(requisitionId)
        .build()
    )
  }

  protected fun readAllReportsInSpanner(): List<Report> = runBlocking {
    ReportReader()
      .execute(databaseClient.singleUse())
      .map { it.report }
      .toList()
  }

  protected fun readAllSchedulesInSpanner(): List<ReportConfigSchedule> = runBlocking {
    ScheduleReader()
      .execute(databaseClient.singleUse())
      .map { it.schedule }
      .toList()
  }

  protected fun readAllRequisitionsInSpanner(): List<Requisition> = runBlocking {
    RequisitionReader()
      .execute(databaseClient.singleUse())
      .map { it.requisition }
      .toList()
  }

  // TODO: add helpers for other tables.
}
