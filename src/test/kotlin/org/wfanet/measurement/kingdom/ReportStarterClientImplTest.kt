package org.wfanet.measurement.kingdom

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportConfigStorage
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportStorage
import org.wfanet.measurement.service.internal.kingdom.testing.FakeRequisitionStorage
import org.wfanet.measurement.service.testing.GrpcTestServerRule

class ReportStarterClientImplTest {

  private val fakeReportConfigStorage = FakeReportConfigStorage()
  private val fakeReportStorage = FakeReportStorage()
  private val fakeRequisitionStorage = FakeRequisitionStorage()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(fakeReportConfigStorage, fakeReportStorage, fakeRequisitionStorage)
  }

  private val reportStarterClient: ReportStarterClient by lazy {
    ReportStarterClientImpl(
      ReportConfigStorageCoroutineStub(grpcTestServerRule.channel),
      ReportStorageCoroutineStub(grpcTestServerRule.channel),
      RequisitionStorageCoroutineStub(grpcTestServerRule.channel)
    )
  }

  @Test
  fun createNextReport() = runBlocking<Unit> {
    fakeReportStorage.mocker.mock(FakeReportStorage::createNextReport) {
      Report.getDefaultInstance()
    }

    val schedule =
      ReportConfigSchedule.newBuilder()
        .setExternalScheduleId(12345)
        .build()

    reportStarterClient.createNextReport(schedule)

    val expectedRequest =
      CreateNextReportRequest.newBuilder()
        .setExternalScheduleId(12345)
        .build()

    assertThat(fakeReportStorage.mocker.callsForMethod("createNextReport"))
      .containsExactly(expectedRequest)
  }

  @Test
  fun buildRequisitionsForReport() = runBlocking<Unit> {
    val requisitionTemplate = RequisitionTemplate.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      requisitionDetailsBuilder.metricDefinitionBuilder.sketchBuilder.sketchConfigId = 3
    }.build()

    fakeReportConfigStorage.mocker.mock(FakeReportConfigStorage::listRequisitionTemplates) {
      ListRequisitionTemplatesResponse.newBuilder()
        .addRequisitionTemplates(requisitionTemplate)
        .build()
    }

    val report = Report.newBuilder()
      .setExternalReportConfigId(4)
      .setWindowStartTime(Instant.ofEpochSecond(123).toProtoTime())
      .setWindowEndTime(Instant.ofEpochSecond(456).toProtoTime())
      .build()

    val expectedRequisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      windowStartTime = report.windowStartTime
      windowEndTime = report.windowEndTime
      state = RequisitionState.UNFULFILLED
      requisitionDetails = requisitionTemplate.requisitionDetails
    }.build()

    assertThat(reportStarterClient.buildRequisitionsForReport(report))
      .containsExactly(expectedRequisition)

    val expectedRequest =
      ListRequisitionTemplatesRequest.newBuilder()
        .setExternalReportConfigId(4)
        .build()

    assertThat(fakeReportConfigStorage.mocker.callsForMethod("listRequisitionTemplates"))
      .containsExactly(expectedRequest)
  }

  @Test
  fun createRequisition() {
    // TODO
  }

  @Test
  fun associateRequisitionToReport() {
    // TODO
  }

  @Test
  fun updateReportState() {
    // TODO
  }

  @Test
  fun streamReportsInState() {
    // TODO
  }

  @Test
  fun streamReadyReports() {
    // TODO
  }

  @Test
  fun streamReadySchedules() {
    // TODO
  }
}
