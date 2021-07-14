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
import com.google.protobuf.Timestamp
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.kingdom.db.ReportDatabase

private const val EXTERNAL_REPORT_ID = 123L
private val CREATE_TIME: Timestamp = Timestamp.newBuilder().setSeconds(456).build()

@RunWith(JUnit4::class)
class ReportLogEntriesServiceTest {

  private val reportDatabase: ReportDatabase = mock()

  private val service = ReportLogEntriesService(reportDatabase)

  @Test
  fun success() = runBlocking {
    whenever(reportDatabase.addReportLogEntry(any())).thenAnswer {
      it.getArgument<ReportLogEntry>(0).toBuilder().setCreateTime(CREATE_TIME).build()
    }

    val request =
      ReportLogEntry.newBuilder().apply { externalReportId = EXTERNAL_REPORT_ID }.build()

    val result = service.createReportLogEntry(request)
    assertThat(result)
      .isEqualTo(
        ReportLogEntry.newBuilder()
          .apply {
            externalReportId = EXTERNAL_REPORT_ID
            createTime = CREATE_TIME
          }
          .build()
      )
  }
}
