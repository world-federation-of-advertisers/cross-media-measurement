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

package org.wfanet.measurement.kingdom.daemon

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.atLeast
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.verify
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.CountDownLatch
import org.wfanet.measurement.common.testing.launchAndCancelWithLatch
import org.wfanet.measurement.common.throttler.testing.FakeThrottler
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

private const val COMBINED_PUBLIC_KEY_ID = "combined-public-key"
private val SCHEDULE: ReportConfigSchedule = ReportConfigSchedule.getDefaultInstance()

@RunWith(JUnit4::class)
class ReportMakerTest {
  private val daemonDatabaseServicesClient: DaemonDatabaseServicesClient = mock()
  private val daemon =
    Daemon(FakeThrottler(), 100, daemonDatabaseServicesClient)

  @Test
  fun createReports() = runBlocking<Unit> {
    val latch = CountDownLatch(15)

    daemonDatabaseServicesClient.stub {
      on { streamReadySchedules() }
        .thenReturn((1..10).map { SCHEDULE }.asFlow())

      onBlocking { createNextReport(any(), any()) }
        .then { latch.countDown() }
    }

    launchAndCancelWithLatch(latch) { daemon.runReportMaker(COMBINED_PUBLIC_KEY_ID) }

    verify(daemonDatabaseServicesClient, atLeast(2))
      .streamReadySchedules()

    verify(daemonDatabaseServicesClient, atLeast(15))
      .createNextReport(same(SCHEDULE), eq(COMBINED_PUBLIC_KEY_ID))
  }
}
