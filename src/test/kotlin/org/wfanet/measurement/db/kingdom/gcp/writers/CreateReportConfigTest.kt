package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Statement
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.common.testing.FixedIdGenerator
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportConfigReader
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfig.ReportConfigState

private const val ADVERTISER_ID = 1L
private const val EXTERNAL_ADVERTISER_ID = 2L
private const val REPORT_CONFIG_ID = 3L
private const val EXTERNAL_REPORT_CONFIG_ID = 4L
private const val DATA_PROVIDER_ID = 5L
private const val EXTERNAL_DATA_PROVIDER_ID = 6L
private const val CAMPAIGN_ID1 = 7L
private const val EXTERNAL_CAMPAIGN_ID1 = 8L
private const val CAMPAIGN_ID2 = 9L
private const val EXTERNAL_CAMPAIGN_ID2 = 10L

private val REPORT_CONFIG: ReportConfig = ReportConfig.newBuilder().apply {
  externalAdvertiserId = EXTERNAL_ADVERTISER_ID

  reportConfigDetailsBuilder.apply {
    addMetricDefinitionsBuilder().sketchBuilder.sketchConfigId = 1
    addMetricDefinitionsBuilder().sketchBuilder.sketchConfigId = 2
    addMetricDefinitionsBuilder().sketchBuilder.sketchConfigId = 3
  }
}.build()

class CreateReportConfigTest : KingdomDatabaseTestBase() {
  private val idGenerator =
    FixedIdGenerator(InternalId(REPORT_CONFIG_ID), ExternalId(EXTERNAL_REPORT_CONFIG_ID))

  private fun createReportConfig(
    reportConfig: ReportConfig,
    vararg externalCampaignIds: Long
  ): ReportConfig {
    return CreateReportConfig(reportConfig, externalCampaignIds.map(::ExternalId).toList())
      .execute(databaseClient, idGenerator, Clock.systemUTC())
  }

  @Test
  fun success() = runBlocking<Unit> {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID1, EXTERNAL_CAMPAIGN_ID1, ADVERTISER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID2, EXTERNAL_CAMPAIGN_ID2, ADVERTISER_ID)

    val reportConfig =
      createReportConfig(REPORT_CONFIG, EXTERNAL_CAMPAIGN_ID1, EXTERNAL_CAMPAIGN_ID2)

    val expectedReportConfig = REPORT_CONFIG.toBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      state = ReportConfigState.ACTIVE
      numRequisitions = 6
      reportConfigDetailsJson = reportConfigDetails.toJson()
    }.build()

    assertThat(reportConfig)
      .isEqualTo(expectedReportConfig)

    val reportConfigReadResults =
      ReportConfigReader()
        .readExternalIdOrNull(databaseClient.singleUse(), idGenerator.externalId)

    assertNotNull(reportConfigReadResults)
    assertThat(reportConfigReadResults.advertiserId).isEqualTo(ADVERTISER_ID)
    assertThat(reportConfigReadResults.reportConfigId).isEqualTo(idGenerator.internalId.value)

    assertThat(reportConfigReadResults.reportConfig)
      .isEqualTo(expectedReportConfig)

    assertThat(readReportConfigCampaigns())
      .containsExactly(CAMPAIGN_ID1, CAMPAIGN_ID2)
  }

  private fun readReportConfigCampaigns(): List<Long> {
    return databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT CampaignId FROM ReportConfigCampaigns"))
      .asSequence()
      .map { it.getLong(0) }
      .toList()
  }
}
