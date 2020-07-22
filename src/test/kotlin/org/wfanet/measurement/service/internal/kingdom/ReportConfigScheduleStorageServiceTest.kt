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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ReportConfigScheduleStorageServiceTest {
  companion object {
    val SCHEDULE1 = ReportConfigSchedule.newBuilder().setExternalScheduleId(1).build()
    val SCHEDULE2 = ReportConfigSchedule.newBuilder().setExternalScheduleId(2).build()
  }

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ReportConfigScheduleStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub: ReportConfigScheduleStorageCoroutineStub by lazy {
    ReportConfigScheduleStorageCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun streamReadySchedules() = runBlocking<Unit> {
    val limit = 123L
    val request = StreamReadyReportConfigSchedulesRequest.newBuilder().setLimit(limit).build()

    var capturedLimit: Long? = null
    fakeKingdomRelationalDatabase.streamReadySchedulesFn = {
      capturedLimit = it
      flowOf(SCHEDULE1, SCHEDULE2)
    }

    assertThat(stub.streamReadyReportConfigSchedules(request).toList())
      .containsExactly(SCHEDULE1, SCHEDULE2)

    assertEquals(limit, capturedLimit)
  }
}
