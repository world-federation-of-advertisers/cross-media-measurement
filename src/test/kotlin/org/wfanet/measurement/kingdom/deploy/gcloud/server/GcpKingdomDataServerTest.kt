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

package org.wfanet.measurement.kingdom.deploy.gcloud.server

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.common.collect.Range
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import java.time.Instant
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.FinishReportRequest
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerKingdomRelationalDatabase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.kingdom.service.internal.buildDataServices

private const val ADVERTISER_ID = 1L
private const val EXTERNAL_ADVERTISER_ID = 2L
private const val REPORT_CONFIG_ID = 3L
private const val EXTERNAL_REPORT_CONFIG_ID = 4L
private const val SCHEDULE_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID = 7L
private const val EXTERNAL_REPORT_ID = 8L
private const val DATA_PROVIDER_ID = 9L
private const val EXTERNAL_DATA_PROVIDER_ID = 10L
private const val CAMPAIGN_ID = 11L
private const val EXTERNAL_CAMPAIGN_ID = 12L
private const val REQUISITION_ID = 13L
private const val EXTERNAL_REQUISITION_ID = 14L
private const val DUCHY_ID = "some-duchy"

private val REPORT_CONFIG_DETAILS: ReportConfigDetails = ReportConfigDetails.newBuilder().apply {
  addMetricDefinitionsBuilder().sketchBuilder.sketchConfigId = 12345
  reportDurationBuilder.apply {
    count = 5
    unit = TimePeriod.Unit.DAY
  }
}.build()

private val REPETITION_SPEC: RepetitionSpec = RepetitionSpec.newBuilder().apply {
  startBuilder.seconds = 12345
  repetitionPeriodBuilder.apply {
    count = 7
    unit = TimePeriod.Unit.DAY
  }
}.build()

/**
 * Integration test for Kingdom internal services + Spanner.
 *
 * This minimally tests each RPC method. Edge cases are tested in individual unit tests for the
 * services. This focuses on ensuring that [SpannerKingdomRelationalDatabase] integrates with the
 * gRPC services.
 */
class GcpKingdomDataServerTest : KingdomDatabaseTestBase() {
  @get:Rule
  val grpcTestServer = GrpcTestServerRule(logAllRequests = true) {
    val relationalDatabase =
      SpannerKingdomRelationalDatabase(
        Clock.systemUTC(),
        RandomIdGenerator(Clock.systemUTC()),
        databaseClient
      )

    buildDataServices(relationalDatabase)
      .forEach(this::addService)
  }

  @get:Rule
  val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val channel by lazy { grpcTestServer.channel }
  private val reportConfigsStub by lazy { ReportConfigsCoroutineStub(channel) }
  private val reportConfigSchedulesStub by lazy { ReportConfigSchedulesCoroutineStub(channel) }
  private val reportsStub by lazy { ReportsCoroutineStub(channel) }
  private val reportLogEntriesStub by lazy { ReportLogEntriesCoroutineStub(channel) }
  private val requisitionsStub by lazy { RequisitionsCoroutineStub(channel) }

  @Before
  fun populateDatabase() = runBlocking {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      EXTERNAL_REPORT_CONFIG_ID,
      reportConfigDetails = REPORT_CONFIG_DETAILS,
      numRequisitions = 1
    )
    insertReportConfigSchedule(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      EXTERNAL_SCHEDULE_ID,
      repetitionSpec = REPETITION_SPEC
    )
    insertReport(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      EXTERNAL_REPORT_ID,
      ReportState.IN_PROGRESS
    )

    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)

    insertReportConfigCampaign(ADVERTISER_ID, REPORT_CONFIG_ID, DATA_PROVIDER_ID, CAMPAIGN_ID)

    insertRequisition(DATA_PROVIDER_ID, CAMPAIGN_ID, REQUISITION_ID, EXTERNAL_REQUISITION_ID)
  }

  @Test
  fun coverage() {
    val serviceDescriptors = listOf(
      ReportConfigsGrpcKt.serviceDescriptor,
      ReportConfigSchedulesGrpcKt.serviceDescriptor,
      ReportsGrpcKt.serviceDescriptor,
      ReportLogEntriesGrpcKt.serviceDescriptor,
      RequisitionsGrpcKt.serviceDescriptor
    )

    val expectedTests =
      serviceDescriptors
        .flatMap { descriptor ->
          descriptor.methods.map { it.fullMethodName.substringAfterLast('.').replace('/', ' ') }
        }

    val actualTests =
      javaClass
        .methods
        .filter { it.isAnnotationPresent(Test::class.java) }
        .map { it.name }

    assertThat(actualTests)
      .containsAtLeastElementsIn(expectedTests)
  }

  @Test
  fun `ReportConfigs ListRequisitionTemplates`() = runBlocking<Unit> {
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

    val result = reportConfigsStub.listRequisitionTemplates(request)
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `ReportConfigSchedules StreamReadyReportConfigSchedules`() = runBlocking<Unit> {
    val request = StreamReadyReportConfigSchedulesRequest.getDefaultInstance()

    val expected = ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    }.build()

    val result = reportConfigSchedulesStub.streamReadyReportConfigSchedules(request)
    assertThat(result.toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `Reports ConfirmDuchyReadiness`() = runBlocking<Unit> {
    databaseClient.write(
      Mutation.newUpdateBuilder("Reports")
        .set("AdvertiserId").to(ADVERTISER_ID)
        .set("ReportConfigId").to(REPORT_CONFIG_ID)
        .set("ScheduleId").to(SCHEDULE_ID)
        .set("ReportId").to(REPORT_ID)
        .set("State").toProtoEnum(ReportState.AWAITING_DUCHY_CONFIRMATION)
        .build(),
      Mutation.newUpdateBuilder("Requisitions")
        .set("DataProviderId").to(DATA_PROVIDER_ID)
        .set("CampaignId").to(CAMPAIGN_ID)
        .set("RequisitionId").to(REQUISITION_ID)
        .set("DuchyId").to(DUCHY_ID)
        .set("State").toProtoEnum(RequisitionState.FULFILLED)
        .build()
    )

    insertReportRequisition(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID
    )

    val request = ConfirmDuchyReadinessRequest.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      duchyId = DUCHY_ID
      addExternalRequisitionIds(EXTERNAL_REQUISITION_ID)
    }.build()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
      state = ReportState.IN_PROGRESS
    }.build()

    val result = reportsStub.confirmDuchyReadiness(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `Reports GetReport`() = runBlocking<Unit> {
    val request = GetReportRequest.newBuilder().setExternalReportId(EXTERNAL_REPORT_ID).build()
    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportsStub.getReport(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  @Test
  fun `Reports CreateNextReport`() = runBlocking<Unit> {
    val request = CreateNextReportRequest.newBuilder().apply {
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    }.build()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    }.build()

    val result = reportsStub.createNextReport(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  @Test
  fun `Reports FinishReport`() = runBlocking<Unit> {
    val request = FinishReportRequest.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      resultBuilder.apply {
        reach = 12345
        putFrequency(1, 0.2)
        putFrequency(3, 0.8)
      }
    }.build()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
      state = ReportState.SUCCEEDED
      reportDetailsBuilder.resultBuilder.apply {
        reach = 12345
        putFrequency(1, 0.2)
        putFrequency(3, 0.8)
      }
    }.build()

    val result = reportsStub.finishReport(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `Reports StreamReports`() = runBlocking<Unit> {
    val request = StreamReportsRequest.getDefaultInstance()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportsStub.streamReports(request)
    assertThat(result.toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `Reports StreamReadyReports`() = runBlocking<Unit> {
    databaseClient.write(
      Mutation.newUpdateBuilder("Reports")
        .set("AdvertiserId").to(ADVERTISER_ID)
        .set("ReportConfigId").to(REPORT_CONFIG_ID)
        .set("ScheduleId").to(SCHEDULE_ID)
        .set("ReportId").to(REPORT_ID)
        .set("State").toProtoEnum(ReportState.AWAITING_REQUISITION_CREATION)
        .build(),
      Mutation.newUpdateBuilder("Requisitions")
        .set("DataProviderId").to(DATA_PROVIDER_ID)
        .set("CampaignId").to(CAMPAIGN_ID)
        .set("RequisitionId").to(REQUISITION_ID)
        .set("DuchyId").to(DUCHY_ID)
        .set("State").toProtoEnum(RequisitionState.FULFILLED)
        .build()
    )

    insertReportRequisition(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID
    )

    val request = StreamReadyReportsRequest.getDefaultInstance()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportsStub.streamReadyReports(request)
    assertThat(result.toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `Reports UpdateReportState`() = runBlocking<Unit> {
    val request = UpdateReportStateRequest.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      state = ReportState.FAILED
    }.build()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportsStub.updateReportState(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  @Test
  fun `Reports AssociateRequisition`() = runBlocking<Unit> {
    val request = AssociateRequisitionRequest.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
    }.build()

    val expected = AssociateRequisitionResponse.getDefaultInstance()

    val result = reportsStub.associateRequisition(request)
    assertThat(result).isEqualTo(expected)

    val key = Key.of(
      // Report PK
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,

      // Requisition PK
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID
    )

    val spannerResult =
      databaseClient.singleUse()
        .read("ReportRequisitions", KeySet.singleKey(key), listOf("AdvertiserId"))
        .singleOrNull()

    assertThat(spannerResult).isNotNull()
  }

  @Test
  fun `ReportLogEntries CreateReportLogEntry`() = runBlocking<Unit> {
    val request = ReportLogEntry.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      sourceBuilder.duchyBuilder.duchyId = "some-duchy"
    }.build()

    val timeBefore = currentSpannerTimestamp
    val result = reportLogEntriesStub.createReportLogEntry(request)
    val timeAfter = currentSpannerTimestamp

    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(request)

    val createTime = result.createTime.toInstant()
    assertThat(createTime).isIn(Range.open(timeBefore, timeAfter))

    val key = Key.of(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      result.createTime.toGcloudTimestamp()
    )

    val spannerResult = databaseClient.singleUse()
      .read("ReportLogEntries", KeySet.singleKey(key), listOf("AdvertiserId"))
      .singleOrNull()

    assertThat(spannerResult).isNotNull()
  }

  @Test
  fun `Requisitions CreateRequisition`() = runBlocking<Unit> {
    val request = Requisition.newBuilder().apply {
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalCampaignId = EXTERNAL_CAMPAIGN_ID
      combinedPublicKeyResourceId = "some-combined-public-key"
      windowStartTimeBuilder.seconds = 123
      windowEndTimeBuilder.seconds = 456
      state = RequisitionState.UNFULFILLED
    }.build()

    val timeBefore = currentSpannerTimestamp
    val result = requisitionsStub.createRequisition(request)
    val timeAfter = currentSpannerTimestamp

    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(request)

    assertThat(result.createTime.toInstant()).isIn(Range.open(timeBefore, timeAfter))

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .contains(result)
  }

  @Test
  fun `Requisitions FulfillRequisition`() = runBlocking<Unit> {
    val request = FulfillRequisitionRequest.newBuilder().apply {
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      duchyId = DUCHY_ID
    }.build()

    val expected = Requisition.newBuilder().apply {
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      state = RequisitionState.FULFILLED
      duchyId = DUCHY_ID
    }.build()

    val result = requisitionsStub.fulfillRequisition(request)

    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(result)
  }

  @Test
  fun `Requisitions StreamRequisitions`() = runBlocking<Unit> {
    databaseClient.write(
      Mutation.newUpdateBuilder("Requisitions")
        .set("DataProviderId").to(DATA_PROVIDER_ID)
        .set("CampaignId").to(CAMPAIGN_ID)
        .set("RequisitionId").to(REQUISITION_ID)
        .set("CreateTime").to(Instant.now().toGcloudTimestamp())
        .build()
    )

    val request = StreamRequisitionsRequest.newBuilder().apply {
      limit = 12345
    }.build()

    val expected = Requisition.newBuilder().apply {
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalCampaignId = EXTERNAL_CAMPAIGN_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
    }.build()

    val result = requisitionsStub.streamRequisitions(request).toList()
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }
}
