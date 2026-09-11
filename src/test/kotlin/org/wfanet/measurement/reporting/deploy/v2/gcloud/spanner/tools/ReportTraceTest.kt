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
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ReportTraceTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `context before Report creation only correlates by BasicReport`() {
    val context =
      ReportTraceContext(
        basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1",
        basicReportState = "CREATED",
        reportName = "(not created)",
        metricNames = emptyList(),
        measurementNames = emptyList(),
        createTime = Instant.parse("2026-09-10T12:00:00Z"),
      )

    assertThat(context.correlationValues)
      .containsExactly("measurementConsumers/mc-1/basicReports/basic-report-1")
  }

  @Test
  fun `main prints entries from logs and traces in chronological order`() {
    val output = StringWriter()
    val error = StringWriter()
    var receivedValues: Collection<String> = emptyList()
    val readerEndTimes = mutableListOf<Instant>()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = {
          ReportTraceLogReader { correlationValues, _, endTime, _ ->
            receivedValues = correlationValues
            readerEndTimes += endTime
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
          ReportTraceSpanReader { _, _, _, endTime, _ ->
            readerEndTimes += endTime
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
        resolverOverride = null,
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(error),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--report=measurementConsumers/mc-1/reports/report-1",
          "--start-time=2026-09-10T11:00:00Z",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(receivedValues).containsExactly("measurementConsumers/mc-1/reports/report-1")
    assertThat(readerEndTimes).containsExactly(NOW, NOW)
    val rendered = output.toString()
    assertThat(rendered).contains("Report: measurementConsumers/mc-1/reports/report-1")
    assertThat(rendered.indexOf("span measurement created"))
      .isLessThan(rendered.indexOf("requisition fulfilled"))
    assertThat(rendered).contains("trace=trace-2")
    assertThat(error.toString()).isEmpty()
  }

  @Test
  fun `main writes one file per distinct BasicReport`() {
    val output = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("traces").toPath()
    val resolver = BasicReportTraceResolver { key ->
      ReportTraceContext(
        basicReportName = key.toName(),
        basicReportState = "SUCCEEDED",
        reportName =
          "measurementConsumers/${key.cmmsMeasurementConsumerId}/reports/${key.basicReportId}",
        metricNames = emptyList(),
        measurementNames = emptyList(),
        createTime = NOW,
      )
    }
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = resolver,
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--basic-report=measurementConsumers/mc-2/basicReports/report-a",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--output-dir=$outputDirectory",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(outputDirectory.toFile().list()!!.toList())
      .containsExactly("mc-1__report-a.md", "mc-2__report-a.md")
    assertThat(outputDirectory.resolve("mc-1__report-a.md").toFile().readText())
      .contains("BasicReport: measurementConsumers/mc-1/basicReports/report-a")
    assertThat(output.toString()).contains("OK  measurementConsumers/mc-1/basicReports/report-a")
  }

  @Test
  fun `main continues batch after invalid BasicReport`() {
    val output = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("partial-traces").toPath()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver { key ->
            ReportTraceContext(
              basicReportName = key.toName(),
              basicReportState = "SUCCEEDED",
              reportName = "measurementConsumers/mc-1/reports/report-a",
              metricNames = emptyList(),
              measurementNames = emptyList(),
              createTime = NOW,
            )
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=not-a-resource-name",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--output-dir=$outputDirectory",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(1)
    assertThat(outputDirectory.toFile().list()!!.toList())
      .containsExactly("invalid-1.md", "mc-1__report-a.md")
    assertThat(output.toString()).contains("FAILED  not-a-resource-name")
    assertThat(output.toString()).contains("OK  measurementConsumers/mc-1/basicReports/report-a")
  }

  @Test
  fun `buildLogFilters chunks large identifier sets`() {
    val filters =
      ReportTraceOutput.buildLogFilters(
        (1..100).map { "measurementConsumers/mc-1/measurements/${"m".repeat(50)}-$it" },
        Instant.parse("2026-09-10T11:00:00Z"),
        NOW,
      )

    assertThat(filters.size).isGreaterThan(1)
    assertThat(filters.all { it.length <= 20_000 }).isTrue()
  }

  companion object {
    private val NOW: Instant = Instant.parse("2026-09-10T13:00:00Z")
  }
}
