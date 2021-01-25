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

package org.wfanet.measurement.kingdom.db.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.Advertiser
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails.Refusal
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

const val DUCHY_ID = "duchy-1"
const val COMBINED_PUBLIC_KEY_RESOURCE_ID = "combined-public-key-1"
const val SKETCH_CONFIG_ID = 123L
const val PROVIDED_CAMPAIGN_ID = "Campaign 1"

val METRIC_DEFINITION: MetricDefinition = MetricDefinition.newBuilder().apply {
  sketchBuilder.apply {
    type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
    sketchConfigId = SKETCH_CONFIG_ID
  }
}.build()
val REFUSAL: Refusal = Refusal.newBuilder().apply {
  justification = Refusal.Justification.COLLECTION_INTERVAL_TOO_DISTANT
  message = "Too old"
}.build()

/** Abstract base class for [KingdomRelationalDatabase] tests. */
@RunWith(JUnit4::class)
abstract class AbstractKingdomRelationalDatabaseTest {
  /** [KingdomRelationalDatabase] instance. */
  abstract val database: KingdomRelationalDatabase

  protected suspend fun buildRequisitionWithParents(): RequisitionWithParents {
    val advertiser = database.createAdvertiser()
    val dataProvider = database.createDataProvider()
    val campaign =
      database.createCampaign(
        ExternalId(dataProvider.externalDataProviderId),
        ExternalId(advertiser.externalAdvertiserId),
        PROVIDED_CAMPAIGN_ID
      )

    val requisition = Requisition.newBuilder().apply {
      externalDataProviderId = campaign.externalDataProviderId
      externalCampaignId = campaign.externalCampaignId
      combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_RESOURCE_ID
      windowStartTimeBuilder.seconds = 100
      windowEndTimeBuilder.seconds = 200

      requisitionDetailsBuilder.apply {
        metricDefinition = METRIC_DEFINITION
      }
    }.build()

    return RequisitionWithParents(
      advertiser = advertiser,
      dataProvider = dataProvider,
      campaign = campaign,
      requisition = requisition
    )
  }

  protected suspend fun createRequisitionWithParents(): RequisitionWithParents {
    val input = buildRequisitionWithParents()
    val requisition = database.createRequisition(input.requisition)
    return RequisitionWithParents(input.advertiser, input.dataProvider, input.campaign, requisition)
  }

  protected suspend fun createReportWithParents(
    advertiserId: ExternalId,
    vararg campaignIds: ExternalId
  ): Report {
    val reportConfig = database.createReportConfig(
      ReportConfig.newBuilder().apply {
        externalAdvertiserId = advertiserId.value
        reportConfigDetailsBuilder.apply {
          reportDurationBuilder.apply {
            unit = TimePeriod.Unit.DAY
            unitValue = 1
          }
          addMetricDefinitions(METRIC_DEFINITION)
        }
      }.build(),
      campaignIds.asList()
    )
    val schedule = database.createSchedule(
      ReportConfigSchedule.newBuilder().apply {
        externalAdvertiserId = reportConfig.externalAdvertiserId
        externalReportConfigId = reportConfig.externalReportConfigId
        repetitionSpecBuilder.apply {
          start = Clock.systemUTC().instant().toProtoTime()
          repetitionPeriod = reportConfig.reportConfigDetails.reportDuration
        }
      }.build()
    )
    return database.createNextReport(
      ExternalId(schedule.externalScheduleId),
      COMBINED_PUBLIC_KEY_RESOURCE_ID
    )
  }

  @Test
  fun `createRequisition returns new Requisition`() = runBlocking {
    val inputRequisition = buildRequisitionWithParents().requisition

    val requisition = database.createRequisition(inputRequisition)

    assertThat(requisition).comparingExpectedFieldsOnly().isEqualTo(inputRequisition)
    assertThat(requisition.externalRequisitionId).isNotEqualTo(0L)
    assertThat(requisition.createTime.seconds).isNotEqualTo(0L)
    assertThat(requisition.providedCampaignId).isEqualTo(PROVIDED_CAMPAIGN_ID)
    assertThat(requisition.state).isEqualTo(RequisitionState.UNFULFILLED)
  }

  @Test
  fun `createRequisition returns existing Requisition`() = runBlocking {
    val inputRequisition = buildRequisitionWithParents().requisition
    val insertedRequisition = database.createRequisition(inputRequisition)

    val requisition = database.createRequisition(insertedRequisition)

    assertThat(requisition).isEqualTo(insertedRequisition)
  }

  @Test
  fun `getRequisition returns inserted Requisition`() = runBlocking {
    val insertedRequisition = createRequisitionWithParents().requisition

    val requisition = database.getRequisition(ExternalId(insertedRequisition.externalRequisitionId))

    assertThat(requisition).isEqualTo(insertedRequisition)
  }

  @Test
  fun `fulfillRequisition returns fulfilled Requisition`() = runBlocking {
    val insertedRequisition = createRequisitionWithParents().requisition
    val externalRequisitionId = ExternalId(insertedRequisition.externalRequisitionId)

    val update = database.fulfillRequisition(externalRequisitionId, DUCHY_ID)

    assertThat(update.original).isEqualTo(insertedRequisition)
    assertThat(update.current.state).isEqualTo(RequisitionState.FULFILLED)
    assertThat(update.current).isEqualTo(database.getRequisition(externalRequisitionId))
  }

  @Test
  fun `fulfillRequisition updates associated Report details`() = runBlocking {
    val (_, _, campaign, requisition) = createRequisitionWithParents()
    val insertedReport =
      createReportWithParents(
        ExternalId(campaign.externalAdvertiserId),
        ExternalId(campaign.externalCampaignId)
      )
    val externalReportId = ExternalId(insertedReport.externalReportId)
    val externalRequisitionId = ExternalId(requisition.externalRequisitionId)
    database.associateRequisitionToReport(externalRequisitionId, externalReportId)

    database.fulfillRequisition(externalRequisitionId, DUCHY_ID)

    val report = database.getReport(externalReportId)
    assertThat(report.reportDetails.requisitionsList).containsExactly(
      ReportDetails.ExternalRequisitionKey.newBuilder().also {
        it.externalCampaignId = requisition.externalCampaignId
        it.externalDataProviderId = requisition.externalDataProviderId
        it.externalRequisitionId = requisition.externalRequisitionId
        it.duchyId = DUCHY_ID
      }.build()
    )
    assertThat(report.updateTime.toInstant()).isGreaterThan(insertedReport.updateTime.toInstant())
  }

  @Test
  fun `refuseRequisition returns refused Requisition`() = runBlocking {
    val insertedRequisition = createRequisitionWithParents().requisition
    val externalRequisitionId = ExternalId(insertedRequisition.externalRequisitionId)

    val update = database.refuseRequisition(externalRequisitionId, REFUSAL)

    assertThat(update.original).isEqualTo(insertedRequisition)
    assertThat(update.current.state).isEqualTo(RequisitionState.PERMANENTLY_UNAVAILABLE)
    assertThat(update.current.requisitionDetails.refusal).isEqualTo(REFUSAL)
    assertThat(update.current).isEqualTo(database.getRequisition(externalRequisitionId))
  }

  @Test
  fun `refuseRequisition is no-op when requisition is fulfilled`() = runBlocking {
    val insertedRequisition = createRequisitionWithParents().requisition
    val externalRequisitionId = ExternalId(insertedRequisition.externalRequisitionId)

    database.fulfillRequisition(externalRequisitionId, DUCHY_ID)
    val update = database.refuseRequisition(externalRequisitionId, REFUSAL)

    assertThat(update.current).isEqualTo(update.original)
    assertThat(update.current).isEqualTo(database.getRequisition(externalRequisitionId))
  }

  @Test
  fun `refuseRequisition marks associated report as failed`() = runBlocking {
    val (_, _, campaign, requisition) = createRequisitionWithParents()
    val insertedReport =
      createReportWithParents(
        ExternalId(campaign.externalAdvertiserId),
        ExternalId(campaign.externalCampaignId)
      )
    val externalReportId = ExternalId(insertedReport.externalReportId)
    val externalRequisitionId = ExternalId(requisition.externalRequisitionId)
    database.associateRequisitionToReport(externalRequisitionId, externalReportId)

    database.refuseRequisition(externalRequisitionId, REFUSAL)

    val report = database.getReport(externalReportId)
    assertThat(report.state).isEqualTo(ReportState.FAILED)
    assertThat(report.updateTime.toInstant()).isGreaterThan(insertedReport.updateTime.toInstant())
  }

  @Test
  fun `associateRequisitionToReport links requisition and report`() = runBlocking<Unit> {
    val (_, _, campaign, requisition) = createRequisitionWithParents()
    val insertedReport =
      createReportWithParents(
        ExternalId(campaign.externalAdvertiserId),
        ExternalId(campaign.externalCampaignId)
      )

    val externalReportId = ExternalId(insertedReport.externalReportId)
    database.associateRequisitionToReport(
      ExternalId(requisition.externalRequisitionId),
      externalReportId
    )

    val report = database.getReport(externalReportId)
    assertThat(report.reportDetails.requisitionsList).containsExactly(
      ReportDetails.ExternalRequisitionKey.newBuilder().apply {
        externalCampaignId = requisition.externalCampaignId
        externalDataProviderId = requisition.externalDataProviderId
        externalRequisitionId = requisition.externalRequisitionId
      }.build()
    )
  }

  data class RequisitionWithParents(
    val advertiser: Advertiser,
    val dataProvider: DataProvider,
    val campaign: Campaign,
    val requisition: Requisition
  )
}
