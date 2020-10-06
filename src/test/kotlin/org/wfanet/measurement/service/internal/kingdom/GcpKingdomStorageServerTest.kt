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

package org.wfanet.measurement.service.internal.kingdom

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Statement
import com.google.common.collect.Range
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import kotlin.test.todo
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.gcloud.toGcloudTimestamp
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

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

  @get:Rule
  val grpcTestServer = GrpcTestServerRule {
    val relationalDatabase =
      GcpKingdomRelationalDatabase(
        Clock.systemUTC(),
        RandomIdGenerator(Clock.systemUTC()),
        databaseClient
      )

    buildStorageServices(relationalDatabase).forEach(this::addService)
  }

  private val channel by lazy { grpcTestServer.channel }
  private val reportConfigStorage by lazy { ReportConfigStorageCoroutineStub(channel) }
  private val reportConfigScheduleStorage by lazy {
    ReportConfigScheduleStorageCoroutineStub(channel)
  }
  private val reportStorage by lazy { ReportStorageCoroutineStub(channel) }
  private val reportLogEntryStorage by lazy { ReportLogEntryStorageCoroutineStub(channel) }

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
      ReportConfigStorageGrpcKt.serviceDescriptor,
      ReportConfigScheduleStorageGrpcKt.serviceDescriptor,
      ReportStorageGrpcKt.serviceDescriptor,
      ReportLogEntryStorageGrpcKt.serviceDescriptor,
      RequisitionStorageGrpcKt.serviceDescriptor
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
  fun `ReportConfigStorage ListRequisitionTemplates`() = runBlocking<Unit> {
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
  fun `ReportConfigScheduleStorage StreamReadyReportConfigSchedules`() = runBlocking<Unit> {
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
  fun `ReportStorage GetReport`() = runBlocking<Unit> {
    val request = GetReportRequest.newBuilder().setExternalReportId(EXTERNAL_REPORT_ID).build()
    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportStorage.getReport(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  @Test
  fun `ReportStorage CreateNextReport`() = runBlocking<Unit> {
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

  @Test
  fun `ReportStorage StreamReports`() = runBlocking<Unit> {
    val request = StreamReportsRequest.getDefaultInstance()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportStorage.streamReports(request)
    assertThat(result.toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `ReportStorage StreamReadyReports`() = runBlocking<Unit> {
    databaseClient.readWriteTransaction().execute<Unit> { txn ->
      txn.executeUpdate(
        Statement.newBuilder("UPDATE Reports SET State = @state WHERE ExternalReportId = @id")
          .bind("state").toProtoEnum(ReportState.AWAITING_REQUISITION_CREATION)
          .bind("id").to(EXTERNAL_REPORT_ID)
          .build()
      )

      txn.executeUpdate(
        Statement.newBuilder(
          """
          UPDATE Requisitions
          SET State = @state
          WHERE ExternalRequisitionId = @id
          """
        )
          .bind("state").toProtoEnum(RequisitionState.FULFILLED)
          .bind("id").to(EXTERNAL_REQUISITION_ID)
          .build()
      )
    }

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

    val result = reportStorage.streamReadyReports(request)
    assertThat(result.toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expected)
  }

  @Test
  fun `ReportStorage UpdateReportState`() = runBlocking<Unit> {
    val request = UpdateReportStateRequest.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      state = ReportState.FAILED
    }.build()

    val expected = Report.newBuilder().apply {
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = reportStorage.updateReportState(request)
    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  @Test
  fun `ReportStorage AssociateRequisition`() = runBlocking<Unit> {
    val request = AssociateRequisitionRequest.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
    }.build()

    val expected = AssociateRequisitionResponse.getDefaultInstance()

    val result = reportStorage.associateRequisition(request)
    assertThat(result).isEqualTo(expected)

    val key = Key.of(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID, // Report PK
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID
    ) // Requisition PK

    val spannerResult =
      databaseClient.singleUse()
        .read("ReportRequisitions", KeySet.singleKey(key), listOf("AdvertiserId"))
        .singleOrNull()

    assertThat(spannerResult).isNotNull()
  }

  @Test
  fun `ReportLogEntryStorage CreateReportLogEntry`() = runBlocking<Unit> {
    val request = ReportLogEntry.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
      sourceBuilder.duchyBuilder.duchyId = "some-duchy"
    }.build()

    val timeBefore = currentSpannerTimestamp
    val result = reportLogEntryStorage.createReportLogEntry(request)
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

  // TODO(efoxepstein): add remaining test cases.

  @Test
  fun `ReportStorage ConfirmDuchyReadiness`() = todo {}

  @Test
  fun `ReportStorage FinishReport`() = todo {}

  @Test
  fun `RequisitionStorage CreateRequisition`() = todo {}

  @Test
  fun `RequisitionStorage FulfillRequisition`() = todo {}

  @Test
  fun `RequisitionStorage StreamRequisitions`() = todo {}
}
