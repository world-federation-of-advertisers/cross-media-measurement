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
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ReportLogEntryStorageServiceTest {

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ReportLogEntryStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub by lazy { ReportLogEntryStorageCoroutineStub(grpcTestServerRule.channel) }

  @Test
  fun success() = runBlocking<Unit> {
    val request = ReportLogEntry.newBuilder().apply {
      externalReportId = 123
    }.build()

    fakeKingdomRelationalDatabase.addReportLogEntryFn = {
      it.toBuilder().apply {
        createTimeBuilder.seconds = 456
      }.build()
    }

    val result = stub.createReportLogEntry(request)
    assertThat(result)
      .isEqualTo(
        ReportLogEntry.newBuilder().apply {
          externalReportId = 123
          createTimeBuilder.seconds = 456
        }.build()
      )
  }
}
