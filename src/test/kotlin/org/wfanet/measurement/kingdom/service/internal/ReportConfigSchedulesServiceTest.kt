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

package org.wfanet.measurement.kingdom.service.internal

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

private val SCHEDULE1 = ReportConfigSchedule.newBuilder().setExternalScheduleId(1).build()
private val SCHEDULE2 = ReportConfigSchedule.newBuilder().setExternalScheduleId(2).build()

@RunWith(JUnit4::class)
class ReportConfigSchedulesServiceTest {
  private val kingdomRelationalDatabase: KingdomRelationalDatabase = mock() {
    on { streamReadySchedules(any()) }
      .thenReturn(flowOf(SCHEDULE1, SCHEDULE2))
  }

  private val service = ReportConfigSchedulesService(kingdomRelationalDatabase)

  @Test
  fun streamReadySchedules() = runBlocking<Unit> {
    val limit = 123L
    val request = StreamReadyReportConfigSchedulesRequest.newBuilder().setLimit(limit).build()

    assertThat(service.streamReadyReportConfigSchedules(request).toList())
      .containsExactly(SCHEDULE1, SCHEDULE2)

    verify(kingdomRelationalDatabase).streamReadySchedules(limit)
  }
}
