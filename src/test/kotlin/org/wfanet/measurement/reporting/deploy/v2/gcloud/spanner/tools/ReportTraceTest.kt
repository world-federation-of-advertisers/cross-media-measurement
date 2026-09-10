/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.common.truth.Truth.assertThat
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Instant
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ReportTraceTest {
  @Test
  fun `main prints entries from logs and traces in chronological order`() {
    val output = StringWriter()
    val error = StringWriter()
    var receivedValues: Collection<String> = emptyList()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = {
          ReportTraceLogReader { correlationValues, _, _, _ ->
            receivedValues = correlationValues
            listOf(
              ReportTraceLogEntry(
                timestamp = Instant.parse("2026-09-10T12:01:00Z"),
                service = "edpa-results-fulfiller",
                severity = "INFO",
                trace = "projects/test/traces/trace-2",
                message = "requisition fulfilled",
              )
            )
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, _, _, _ ->
            listOf(
              ReportTraceLogEntry(
                timestamp = Instant.parse("2026-09-10T12:00:00Z"),
                service = "reporting",
                severity = "TRACE",
                trace = "projects/test/traces/trace-1",
                message = "span measurement created",
              )
            )
          }
        },
        resolverFactory = { _, _ -> error("Resolver should not be used in direct mode") },
        output = PrintWriter(output),
        error = PrintWriter(error),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--report=measurementConsumers/mc-1/reports/report-1",
          "--start-time=2026-09-10T11:00:00Z",
          "--end-time=2026-09-10T13:00:00Z",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(receivedValues).containsExactly("measurementConsumers/mc-1/reports/report-1")
    val rendered = output.toString()
    assertThat(rendered).contains("Report: measurementConsumers/mc-1/reports/report-1")
    assertThat(rendered.indexOf("span measurement created"))
      .isLessThan(rendered.indexOf("requisition fulfilled"))
    assertThat(rendered).contains("trace=trace-2")
    assertThat(error.toString()).isEmpty()
  }
}
