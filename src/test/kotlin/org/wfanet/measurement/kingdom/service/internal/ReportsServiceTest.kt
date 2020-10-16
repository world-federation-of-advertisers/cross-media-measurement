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

package org.wfanet.measurement.kingdom.service.internal

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.check
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.streamReportsFilter

private val REPORT: Report = Report.newBuilder().apply {
  externalAdvertiserId = 1
  externalReportConfigId = 2
  externalScheduleId = 3
  externalReportId = 4
  createTimeBuilder.seconds = 567
  reportDetailsBuilder.combinedPublicKeyResourceId = "890"
  state = ReportState.FAILED
}.build()

@RunWith(JUnit4::class)
class ReportsServiceTest {
  private val kingdomRelationalDatabase: KingdomRelationalDatabase = mock()
  private val service = ReportsService(kingdomRelationalDatabase)

  @Test
  fun getReport() = runBlocking<Unit> {
    whenever(kingdomRelationalDatabase.getReport(any())).thenReturn(REPORT)

    val request = GetReportRequest.newBuilder().setExternalReportId(12345).build()

    assertThat(service.getReport(request)).isEqualTo(REPORT)
    verify(kingdomRelationalDatabase).getReport(ExternalId(12345))
  }

  @Test
  fun createNextReport() = runBlocking<Unit> {
    val combinedPublicKeyResourceId = REPORT.reportDetails.combinedPublicKeyResourceId
    whenever(kingdomRelationalDatabase.createNextReport(any(), any())).thenReturn(REPORT)

    val request: CreateNextReportRequest =
      CreateNextReportRequest.newBuilder().apply {
        externalScheduleId = 12345
        this.combinedPublicKeyResourceId = combinedPublicKeyResourceId
      }.build()

    assertThat(service.createNextReport(request)).isEqualTo(REPORT)

    verify(kingdomRelationalDatabase).createNextReport(
      ExternalId(12345),
      combinedPublicKeyResourceId
    )
  }

  @Test
  fun updateReportState() = runBlocking<Unit> {
    whenever(kingdomRelationalDatabase.updateReportState(any(), any())).thenReturn(REPORT)

    val request: UpdateReportStateRequest =
      UpdateReportStateRequest.newBuilder().apply {
        externalReportId = REPORT.externalReportId
        state = REPORT.state
      }.build()

    assertThat(service.updateReportState(request)).isEqualTo(REPORT)

    verify(kingdomRelationalDatabase)
      .updateReportState(ExternalId(REPORT.externalReportId), REPORT.state)
  }

  @Test
  fun streamReports() = runBlocking<Unit> {
    whenever(kingdomRelationalDatabase.streamReports(any(), any()))
      .thenReturn(flowOf(REPORT, REPORT))

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

    assertThat(service.streamReports(request).toList())
      .containsExactly(REPORT, REPORT)

    val expectedFilter = streamReportsFilter(
      externalAdvertiserIds = listOf(ExternalId(1)),
      externalReportConfigIds = listOf(ExternalId(2), ExternalId(3)),
      externalScheduleIds = listOf(ExternalId(4)),
      states = listOf(ReportState.AWAITING_REQUISITION_CREATION),
      updatedAfter = Instant.ofEpochSecond(12345)
    )

    verify(kingdomRelationalDatabase).streamReports(
      check { assertThat(it.clauses).containsExactlyElementsIn(expectedFilter.clauses) },
      eq(10)
    )
  }

  @Test
  fun streamReadyReports() = runBlocking<Unit> {
    whenever(kingdomRelationalDatabase.streamReadyReports(any()))
      .thenReturn(flowOf(REPORT, REPORT))

    val request: StreamReadyReportsRequest =
      StreamReadyReportsRequest.newBuilder().setLimit(10L).build()

    assertThat(service.streamReadyReports(request).toList())
      .containsExactly(REPORT, REPORT)

    verify(kingdomRelationalDatabase).streamReadyReports(10)
  }

  @Test
  fun associateRequisition() = runBlocking<Unit> {
    val request = AssociateRequisitionRequest.newBuilder().apply {
      externalReportId = 1
      externalRequisitionId = 2
    }.build()

    assertThat(service.associateRequisition(request))
      .isEqualTo(AssociateRequisitionResponse.getDefaultInstance())

    verify(kingdomRelationalDatabase)
      .associateRequisitionToReport(ExternalId(2), ExternalId(1))
  }

  @Test
  fun confirmDuchyReadiness() = runBlocking<Unit> {
    whenever(kingdomRelationalDatabase.confirmDuchyReadiness(any(), any(), any()))
      .thenReturn(REPORT)

    val request = ConfirmDuchyReadinessRequest.newBuilder().apply {
      externalReportId = 1
      duchyId = "some-duchy"
      addAllExternalRequisitionIds(listOf(2, 3, 4))
    }.build()

    assertThat(service.confirmDuchyReadiness(request))
      .isEqualTo(REPORT)

    verify(kingdomRelationalDatabase)
      .confirmDuchyReadiness(
        eq(ExternalId(1)),
        eq("some-duchy"),
        check { assertThat(it).containsExactly(ExternalId(2), ExternalId(3), ExternalId(4)) }
      )
  }
}
