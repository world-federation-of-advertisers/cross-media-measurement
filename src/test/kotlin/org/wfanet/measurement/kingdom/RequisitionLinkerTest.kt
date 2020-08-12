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

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.atLeast
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.verify
import java.util.concurrent.CountDownLatch
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.FakeThrottler
import org.wfanet.measurement.common.testing.launchAndCancelWithLatch
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition

private val REPORT: Report = Report.getDefaultInstance()
private val REQUISITION: Requisition = Requisition.getDefaultInstance()

@RunWith(JUnit4::class)
class RequisitionLinkerTest {
  private val reportStarterClient: ReportStarterClient = mock()
  private val daemon = Daemon(FakeThrottler(), 100, reportStarterClient)

  @Test
  fun createRequisitions() = runBlocking<Unit> {
    val latch = CountDownLatch(15)

    reportStarterClient.stub {
      on { streamReportsInState(any()) }
        .thenReturn(flowOf(REPORT, REPORT, REPORT))

      onBlocking { buildRequisitionsForReport(any()) }
        .thenReturn(listOf(REQUISITION, REQUISITION))

      onBlocking { createRequisition(any()) }
        .thenReturn(REQUISITION)

      onBlocking { associateRequisitionToReport(any(), any()) }
        .then { latch.countDown() }
    }

    launchAndCancelWithLatch(latch) { daemon.runRequisitionLinker() }

    verify(reportStarterClient, atLeast(5))
      .streamReportsInState(Report.ReportState.AWAITING_REQUISITION_CREATION)

    verify(reportStarterClient, atLeast(5))
      .buildRequisitionsForReport(same(REPORT))

    verify(reportStarterClient, atLeast(15))
      .createRequisition(same(REQUISITION))

    verify(reportStarterClient, atLeast(15))
      .associateRequisitionToReport(same(REQUISITION), same(REPORT))
  }
}
