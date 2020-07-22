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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.streamReportsFilter
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ReportStorageServiceTest {

  companion object {
    val REPORT: Report = Report.newBuilder().apply {
      externalAdvertiserId = 1
      externalReportConfigId = 2
      externalScheduleId = 3
      externalReportId = 4
      createTimeBuilder.seconds = 567
      state = ReportState.FAILED
    }.build()
  }

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ReportStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub: ReportStorageGrpcKt.ReportStorageCoroutineStub by lazy {
    ReportStorageGrpcKt.ReportStorageCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun getReport() = runBlocking<Unit> {
    val request = GetReportRequest.newBuilder().setExternalReportId(12345).build()

    var capturedExternalId: ExternalId? = null
    fakeKingdomRelationalDatabase.getReportFn = {
      capturedExternalId = it
      REPORT
    }

    assertThat(stub.getReport(request)).isEqualTo(REPORT)
    assertThat(capturedExternalId).isEqualTo(ExternalId(12345))
  }

  @Test
  fun createNextReport() = runBlocking<Unit> {
    val request: CreateNextReportRequest =
      CreateNextReportRequest.newBuilder().apply {
        externalScheduleId = 12345
      }.build()

    var capturedExternalScheduleId: ExternalId? = null
    fakeKingdomRelationalDatabase.createNextReportFn = {
      capturedExternalScheduleId = it
      REPORT
    }

    assertThat(stub.createNextReport(request))
      .isEqualTo(REPORT)

    assertThat(capturedExternalScheduleId)
      .isEqualTo(ExternalId(12345))
  }

  @Test
  fun updateReportState() = runBlocking<Unit> {
    val request: UpdateReportStateRequest =
      UpdateReportStateRequest.newBuilder().apply {
        externalReportId = REPORT.externalReportId
        state = REPORT.state
      }.build()

    var capturedExternalReportId: ExternalId? = null
    var capturedState: ReportState? = null

    fakeKingdomRelationalDatabase.updateReportStateFn = { externalReportId, reportState ->
      capturedExternalReportId = externalReportId
      capturedState = reportState
      REPORT
    }

    assertThat(stub.updateReportState(request)).isEqualTo(REPORT)
    assertThat(capturedExternalReportId).isEqualTo(ExternalId(REPORT.externalReportId))
    assertThat(capturedState).isEqualTo(REPORT.state)
  }

  @Test
  fun streamReports() = runBlocking<Unit> {
    val request: StreamReportsRequest =
      StreamReportsRequest.newBuilder().apply {
        limit = 10
        filterBuilder.apply {
          addExternalAdvertiserIds(1)
          addExternalReportConfigIds(2)
          addExternalReportConfigIds(3)
          addExternalScheduleIds(4)
          addStates(ReportState.AWAITING_REQUISITION_CREATION)
          updatedAfterBuilder.seconds = 12345
        }
      }.build()

    var capturedFilter: StreamReportsFilter? = null
    var capturedLimit: Long = 0

    fakeKingdomRelationalDatabase.streamReportsFn = { filter, limit ->
      capturedFilter = filter
      capturedLimit = limit
      flowOf(REPORT, REPORT)
    }

    assertThat(stub.streamReports(request).toList())
      .containsExactly(REPORT, REPORT)

    val expectedFilter = streamReportsFilter(
      externalAdvertiserIds = listOf(ExternalId(1)),
      externalReportConfigIds = listOf(ExternalId(2), ExternalId(3)),
      externalScheduleIds = listOf(ExternalId(4)),
      states = listOf(ReportState.AWAITING_REQUISITION_CREATION),
      updatedAfter = Instant.ofEpochSecond(12345)
    )

    assertThat(capturedFilter?.clauses).containsExactlyElementsIn(expectedFilter.clauses)
    assertThat(capturedLimit).isEqualTo(10)
  }

  @Test
  fun streamReadyReports() = runBlocking<Unit> {
    val request: StreamReadyReportsRequest =
      StreamReadyReportsRequest.newBuilder().setLimit(10L).build()

    var capturedLimit: Long = 0

    fakeKingdomRelationalDatabase.streamReadyReportsFn = { limit ->
      capturedLimit = limit
      flowOf(REPORT, REPORT)
    }

    assertThat(stub.streamReadyReports(request).toList())
      .containsExactly(REPORT, REPORT)

    assertThat(capturedLimit).isEqualTo(10)
  }

  @Test
  fun associateRequisition() = runBlocking<Unit> {
    val request = AssociateRequisitionRequest.newBuilder().apply {
      externalReportId = 1
      externalRequisitionId = 2
    }.build()

    var capturedExternalRequisitionId: ExternalId? = null
    var capturedExternalReportId: ExternalId? = null
    fakeKingdomRelationalDatabase.associateRequisitionToReportFn = {
      externalRequisitionId, externalReportId ->
      capturedExternalRequisitionId = externalRequisitionId
      capturedExternalReportId = externalReportId
    }

    assertThat(stub.associateRequisition(request))
      .isEqualTo(AssociateRequisitionResponse.getDefaultInstance())

    assertThat(capturedExternalReportId).isEqualTo(ExternalId(1))
    assertThat(capturedExternalRequisitionId).isEqualTo(ExternalId(2))
  }
}
