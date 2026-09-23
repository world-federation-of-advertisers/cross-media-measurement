// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.telemetry

import com.google.api.gax.paging.Page
import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.Payload
import com.google.cloud.logging.SourceLocation
import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class GoogleCloudTelemetryTest {
  @Test
  fun `log reader preserves ordinary JSON application message`() = runBlocking {
    val logging = mock<Logging>()
    val page = mock<Page<LogEntry>>()
    val logEntry =
      LogEntry.newBuilder(
          Payload.JsonPayload.of(
            mapOf(
              "message" to "The report worker failed validation",
              "xmm.report.name" to "reports/report-1",
            )
          )
        )
        .setLogName("projects/logging-project/logs/stdout")
        .setTimestamp(NOW)
        .build()
    whenever(page.values).thenReturn(listOf(logEntry))
    whenever(page.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any())).thenReturn(page)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("xmm.report.name"),
        correlationFields = setOf("xmm.report.name"),
      )

    val entries =
      reader.read(listOf("reports/report-1"), NOW.minusSeconds(1), NOW.plusSeconds(1), 100)

    assertThat(entries.single().message).contains("The report worker failed validation")
    assertThat(entries.single().message).contains("xmm.report.name=reports/report-1")
    Unit
  }

  @Test
  fun `log reader excludes split gRPC payload using source context`() = runBlocking {
    val logging = mock<Logging>()
    val correlatedPage = mock<Page<LogEntry>>()
    val grpcContextPage = mock<Page<LogEntry>>()
    val applicationContextPage = mock<Page<LogEntry>>()
    val logName = "projects/logging-project/logs/stdout"
    val grpcSource =
      SourceLocation.newBuilder().setFunction("wfa.measurement.Service.Create").build()
    val applicationSource =
      SourceLocation.newBuilder().setFunction("org.example.Worker.process").build()
    val grpcPreamble =
      LogEntry.newBuilder(Payload.StringPayload.of("[grpc-worker] gRPC request-1 request:"))
        .setLogName(logName)
        .setSourceLocation(grpcSource)
        .setTimestamp(NOW.minusMillis(2))
        .build()
    val grpcContinuation =
      LogEntry.newBuilder(Payload.StringPayload.of("basic_report: \"reports/report-1\""))
        .setLogName(logName)
        .setSourceLocation(grpcSource)
        .setTimestamp(NOW.minusMillis(1))
        .build()
    val applicationError =
      LogEntry.newBuilder(Payload.StringPayload.of("status: failed reports/report-1"))
        .setLogName(logName)
        .setSourceLocation(applicationSource)
        .setTimestamp(NOW)
        .build()
    whenever(correlatedPage.values).thenReturn(listOf(applicationError, grpcContinuation))
    whenever(correlatedPage.hasNextPage()).thenReturn(false)
    whenever(applicationContextPage.values).thenReturn(listOf(applicationError))
    whenever(applicationContextPage.hasNextPage()).thenReturn(false)
    whenever(grpcContextPage.values).thenReturn(listOf(grpcContinuation, grpcPreamble))
    whenever(grpcContextPage.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any()))
      .thenReturn(correlatedPage, applicationContextPage, grpcContextPage)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("event", "xmm.lifecycle.stage"),
        correlationFields = setOf("xmm.report.name"),
      )

    val entries =
      reader.read(listOf("reports/report-1"), NOW.minusSeconds(1), NOW.plusSeconds(1), 100)

    assertThat(entries.map { it.message }).containsExactly("status: failed reports/report-1")
    Unit
  }

  companion object {
    private val NOW = Instant.parse("2026-09-10T12:00:00Z")
  }
}
