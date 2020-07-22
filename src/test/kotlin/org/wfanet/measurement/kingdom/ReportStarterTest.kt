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

package org.wfanet.measurement.kingdom

import java.time.Duration
import java.util.concurrent.CountDownLatch
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.`when` as mockWhen
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.wfanet.measurement.common.testing.FakeThrottler
import org.wfanet.measurement.common.testing.any
import org.wfanet.measurement.common.testing.eq
import org.wfanet.measurement.common.testing.same
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.Requisition

@RunWith(JUnit4::class)
class ReportStarterTest {
  companion object {
    val REPORT: Report = Report.getDefaultInstance()
    val SCHEDULE: ReportConfigSchedule = ReportConfigSchedule.getDefaultInstance()
    val REQUISITION: Requisition = Requisition.getDefaultInstance()
  }

  private val fakeThrottler = FakeThrottler()

  private val reportStarterClient = mock(ReportStarterClient::class.java)
  private val reportStarter = ReportStarter(fakeThrottler, 100, reportStarterClient)

  private fun launchAndCancelWithLatch(latch: CountDownLatch, block: suspend () -> Unit) =
    runBlocking<Unit> {
      withTimeout<Unit>(Duration.ofSeconds(10).toMillis()) {
        val job = launch { block() }
        while (latch.count > 0) {
          delay(200)
        }
        job.cancelAndJoin()
      }
    }

  @Test
  fun createReports() = runBlocking<Unit> {
    mockWhen(reportStarterClient.streamReadySchedules())
      .thenReturn((1..10).map { SCHEDULE }.asFlow())

    val latch = CountDownLatch(15)

    mockWhen(reportStarterClient.createNextReport(any()))
      .then { latch.countDown() }

    launchAndCancelWithLatch(latch) { reportStarter.createReports() }

    verify(reportStarterClient, atLeast(2))
      .streamReadySchedules()

    verify(reportStarterClient, atLeast(15))
      .createNextReport(same(SCHEDULE))
  }

  @Test
  fun createRequisitions() = runBlocking<Unit> {
    mockWhen(reportStarterClient.streamReportsInState(any()))
      .thenReturn(flowOf(REPORT, REPORT, REPORT))
    mockWhen(reportStarterClient.buildRequisitionsForReport(any()))
      .thenReturn(listOf(REQUISITION, REQUISITION))
    mockWhen(reportStarterClient.createRequisition(any()))
      .thenReturn(REQUISITION)

    val latch = CountDownLatch(15)
    mockWhen(reportStarterClient.associateRequisitionToReport(any(), any()))
      .then { latch.countDown() }

    launchAndCancelWithLatch(latch) { reportStarter.createRequisitions() }

    verify(reportStarterClient, atLeast(5))
      .streamReportsInState(ReportState.AWAITING_REQUISITION_CREATION)

    verify(reportStarterClient, atLeast(5))
      .buildRequisitionsForReport(same(REPORT))

    verify(reportStarterClient, atLeast(15))
      .createRequisition(same(REQUISITION))

    verify(reportStarterClient, atLeast(15))
      .associateRequisitionToReport(same(REQUISITION), same(REPORT))
  }

  @Test
  fun startReports() = runBlocking<Unit> {
    mockWhen(reportStarterClient.streamReadyReports())
      .thenReturn(flowOf(REPORT, REPORT, REPORT))

    val latch = CountDownLatch(15)
    mockWhen(reportStarterClient.updateReportState(any(), any()))
      .then { latch.countDown() }

    launchAndCancelWithLatch(latch) { reportStarter.startReports() }

    verify(reportStarterClient, atLeast(5))
      .streamReadyReports()

    verify(reportStarterClient, atLeast(15))
      .updateReportState(same(REPORT), eq(ReportState.READY_TO_START))
  }
}
