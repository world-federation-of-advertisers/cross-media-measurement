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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

private const val EXTERNAL_REPORT_ID = 123L
private val CREATE_TIME: Timestamp = Timestamp.newBuilder().setSeconds(456).build()

@RunWith(JUnit4::class)
class ReportLogEntriesServiceTest {

  private val kingdomRelationalDatabase: KingdomRelationalDatabase = mock()

  private val service = ReportLogEntriesService(kingdomRelationalDatabase)

  @Test
  fun success() = runBlocking<Unit> {
    whenever(kingdomRelationalDatabase.addReportLogEntry(any()))
      .thenAnswer {
        it.getArgument<ReportLogEntry>(0).toBuilder().setCreateTime(CREATE_TIME).build()
      }

    val request = ReportLogEntry.newBuilder().apply {
      externalReportId = EXTERNAL_REPORT_ID
    }.build()

    val result = service.createReportLogEntry(request)
    assertThat(result)
      .isEqualTo(
        ReportLogEntry.newBuilder().apply {
          externalReportId = EXTERNAL_REPORT_ID
          createTime = CREATE_TIME
        }.build()
      )
  }
}
