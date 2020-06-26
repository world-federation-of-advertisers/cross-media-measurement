package org.wfanet.measurement.service.internal.kingdom

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.RandomIdGeneratorImpl
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import java.time.Clock

/**
 * Integration test for Kingdom internal services + Spanner.
 *
 * This minimally tests each RPC method. Edge cases are tested in individual unit tests for the
 * services. This focuses on ensuring that [GcpKingdomRelationalDatabase] integrates with the
 * gRPC services.
 */
class GcpKingdomStorageServerTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val EXTERNAL_ADVERTISER_ID = 2L
    const val REPORT_CONFIG_ID = 3L
    const val EXTERNAL_REPORT_CONFIG_ID = 4L
    const val SCHEDULE_ID = 5L
    const val EXTERNAL_SCHEDULE_ID = 6L
    const val REPORT_ID = 7L
    const val EXTERNAL_REPORT_ID = 8L
    const val DATA_PROVIDER_ID = 9L
    const val EXTERNAL_DATA_PROVIDER_ID = 10L
    const val CAMPAIGN_ID = 11L
    const val EXTERNAL_CAMPAIGN_ID = 12L
    const val REQUISITION_ID = 13L
    const val EXTERNAL_REQUISITION_ID = 14L

    val REPORT_CONFIG_DETAILS: ReportConfigDetails = ReportConfigDetails.newBuilder().apply {
      addMetricDefinitionsBuilder().sketchBuilder.sketchConfigId = 12345
      reportDurationBuilder.apply {
        count = 5
        unit = TimePeriod.Unit.DAY
      }
    }.build()

    val REPETITION_SPEC: RepetitionSpec = RepetitionSpec.newBuilder().apply {
      startBuilder.seconds = 12345
      repetitionPeriodBuilder.apply {
        count = 7
        unit = TimePeriod.Unit.DAY
      }
    }.build()
  }

  private val relationalDatabase = GcpKingdomRelationalDatabase(
    Clock.systemUTC(),
    RandomIdGeneratorImpl(Clock.systemUTC()),
    spanner.client
  )

  @get:Rule
  val grpcTestServer = GrpcTestServerRule {
    buildStorageServices(relationalDatabase)
  }

  private val channel by lazy { grpcTestServer.channel }
  private val reportConfigStorage by lazy { ReportConfigStorageCoroutineStub(channel) }
  private val reportConfigScheduleStorage by lazy {
    ReportConfigScheduleStorageCoroutineStub(channel)
  }
  private val reportStorage by lazy { ReportStorageCoroutineStub(channel) }
  private val requisitionStorage by lazy { RequisitionStorageCoroutineStub(channel) }

  @Before
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(
      ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID,
      reportConfigDetails = REPORT_CONFIG_DETAILS, numRequisitions = 1
    )
    insertReportConfigSchedule(
      ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, EXTERNAL_SCHEDULE_ID,
      repetitionSpec = REPETITION_SPEC
    )
    insertReport(
      ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, REPORT_ID, EXTERNAL_REPORT_ID,
      ReportState.READY_TO_START
    )

    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)

    insertReportConfigCampaign(ADVERTISER_ID, REPORT_CONFIG_ID, DATA_PROVIDER_ID, CAMPAIGN_ID)
  }

  @Test
  fun `reportConfigStorage listRequisitionTemplates`() = runBlocking<Unit> {
    val request = ListRequisitionTemplatesRequest.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
    }.build()

    val expected = ListRequisitionTemplatesResponse.newBuilder().apply {
      addRequisitionTemplatesBuilder().apply {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalCampaignId = EXTERNAL_CAMPAIGN_ID
        requisitionDetailsBuilder.metricDefinition = REPORT_CONFIG_DETAILS.getMetricDefinitions(0)
      }
    }.build()

    val result = reportConfigStorage.listRequisitionTemplates(request)
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `reportConfigScheduleStorage streamReadyReportConfigSchedules`() = runBlocking<Unit> {
    val request = StreamReadyReportConfigSchedulesRequest.getDefaultInstance()

    val expected = ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    }.build()

    val result = reportConfigScheduleStorage.streamReadyReportConfigSchedules(request)
    assertThat(result.toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `reportStorage createNextReport`() = runBlocking<Unit> {
    val request = CreateNextReportRequest.newBuilder().apply {
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    }.build()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    }.build()

    val result = reportStorage.createNextReport(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  // TODO(efoxepstein): add remaining test cases.
}
