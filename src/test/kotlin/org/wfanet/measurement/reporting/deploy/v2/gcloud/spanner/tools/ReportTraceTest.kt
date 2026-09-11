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

import com.google.cloud.logging.Payload
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
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { correlationValues, _, endTime, _ ->
            receivedValues = correlationValues
            readerEndTimes += endTime
            listOf(
              ReportTraceLogEntry(
                sourceProject = project,
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
          ReportTraceSpanReader { _, _, _, _, endTime, _ ->
            readerEndTimes += endTime
            listOf(
              ReportTraceSpan(
                sourceProject = "test",
                traceId = "trace-1",
                spanId = "span-1",
                parentSpanId = null,
                name = "measurement created",
                service = "reporting",
                startTime = Instant.parse("2026-09-10T12:00:00Z"),
                endTime = Instant.parse("2026-09-10T12:00:01Z"),
                statusCode = "0",
                statusMessage = null,
                attributes = mapOf("xmm.report.name" to "reports/report-1"),
                events = emptyList(),
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
    assertThat(readerEndTimes).containsExactly(NOW, NOW, NOW)
    val rendered = output.toString()
    assertThat(rendered).contains("Report: measurementConsumers/mc-1/reports/report-1")
    assertThat(rendered.indexOf("SPAN [test/reporting] measurement created"))
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
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
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
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
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

  @Test
  fun `render preserves span status hierarchy attributes and events`() {
    val context = reportTraceContext()
    val output =
      ReportTraceOutput.render(
        context,
        spans =
          listOf(
            ReportTraceSpan(
              sourceProject = "kingdom-project",
              traceId = "trace-1",
              spanId = "span-2",
              parentSpanId = "span-1",
              name = "fulfill requisition",
              service = "kingdom",
              startTime = Instant.parse("2026-09-10T12:00:00Z"),
              endTime = Instant.parse("2026-09-10T12:00:01Z"),
              statusCode = "13",
              statusMessage = "failed",
              attributes = mapOf("xmm.requisition.name" to "requisitions/r1"),
              events =
                listOf(
                  ReportTraceEvent(
                    timestamp = Instant.parse("2026-09-10T12:00:00.500Z"),
                    name = "kingdom.requisition.returned",
                    attributes = mapOf("exception.type" to "IllegalStateException"),
                  )
                ),
            )
          ),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("status=13:failed")
    assertThat(output).contains("parent=span-1")
    assertThat(output).contains("xmm.requisition.name=requisitions/r1")
    assertThat(output).contains("EVENT [kingdom-project/kingdom] kingdom.requisition.returned")
    assertThat(output).contains("exception.type=IllegalStateException")
  }

  @Test
  fun `renderLogPayload redacts secrets and omits unapproved fields`() {
    val payload =
      Payload.JsonPayload.of(
        mapOf(
          "message" to "request failed with bearer secret-token",
          "api_key" to "secret-api-key",
          "request" to mapOf("password" to "secret-password"),
          "attributes" to
            mapOf(
              "xmm.report.name" to "measurementConsumers/mc-1/reports/report-1",
              "authorization" to "secret-authorization",
            ),
        )
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeRawPayloads = false)

    assertThat(rendered).contains("xmm.report.name=measurementConsumers/mc-1/reports/report-1")
    assertThat(rendered).contains("[REDACTED]")
    assertThat(rendered).doesNotContain("secret-token")
    assertThat(rendered).doesNotContain("secret-api-key")
    assertThat(rendered).doesNotContain("secret-password")
    assertThat(rendered).doesNotContain("secret-authorization")
  }

  @Test
  fun `renderLogPayload keeps safe fields but omits arbitrary text`() {
    val payload =
      Payload.StringPayload.of(
        "xmm.basic_report.name=measurementConsumers/mc-1/basicReports/br-1 " +
          "xmm.lifecycle.stage=noise_correction token=secret-value arbitrary request body"
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeRawPayloads = false)

    assertThat(rendered)
      .contains("xmm.basic_report.name=measurementConsumers/mc-1/basicReports/br-1")
    assertThat(rendered).contains("xmm.lifecycle.stage=noise_correction")
    assertThat(rendered).doesNotContain("secret-value")
    assertThat(rendered).doesNotContain("arbitrary request body")
  }

  @Test
  fun `main queries every observability project`() {
    val projects = mutableListOf<String>()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          projects += "logging:$project"
          ReportTraceLogReader { _, _, _, _ -> emptyList() }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { project, _, _, _, _, _ ->
            projects += "trace:$project"
            emptyList()
          }
        },
        resolverFactory = { _, _ -> error("Resolver should not be used") },
        resolverOverride = null,
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--observability-project=reporting",
          "--observability-project=kingdom",
          "--report=measurementConsumers/mc-1/reports/report-1",
          "--start-time=2026-09-10T11:00:00Z",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(projects)
      .containsExactly("logging:reporting", "logging:kingdom", "trace:reporting", "trace:kingdom")
      .inOrder()
  }

  @Test
  fun `main uses log trace IDs to discover unlabelled spans in another project`() {
    val output = StringWriter()
    val traceIdsByProject = mutableMapOf<String, Collection<String>>()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { _, _, _, _ ->
            if (project == "reporting") {
              listOf(
                ReportTraceLogEntry(
                  sourceProject = project,
                  timestamp = NOW,
                  service = "reporting",
                  severity = "INFO",
                  trace = "projects/reporting/traces/shared-trace",
                  message = "report result stored",
                )
              )
            } else {
              emptyList()
            }
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { project, _, traceIds, _, _, _ ->
            traceIdsByProject[project] = traceIds
            if (project == "kingdom" && "shared-trace" in traceIds) {
              listOf(
                traceSpan("remote-span", NOW)
                  .copy(sourceProject = project, traceId = "shared-trace", service = "kingdom")
              )
            } else {
              emptyList()
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver should not be used") },
        resolverOverride = null,
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--observability-project=reporting",
          "--observability-project=kingdom",
          "--report=measurementConsumers/mc-1/reports/report-1",
          "--start-time=2026-09-10T11:00:00Z",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(traceIdsByProject["kingdom"]).contains("shared-trace")
    assertThat(output.toString()).contains("SPAN [kingdom/kingdom] remote-span")
  }

  @Test
  fun `main reports truncation as failure by default`() {
    val output = StringWriter()
    val error = StringWriter()
    val span = traceSpan("span-1", Instant.parse("2026-09-10T12:00:00Z"))
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, _, _, _, _ ->
            listOf(span, traceSpan("span-2", Instant.parse("2026-09-10T12:01:00Z")))
          }
        },
        resolverFactory = { _, _ -> error("Resolver should not be used") },
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
          "--limit=1",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(1)
    assertThat(output.toString()).contains("Cloud Trace results were truncated")
    assertThat(error.toString()).contains("could not be generated")
  }

  @Test
  fun `main permits explicitly allowed partial output`() {
    val output = StringWriter()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> error("denied") } },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver should not be used") },
        resolverOverride = null,
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--report=measurementConsumers/mc-1/reports/report-1",
          "--start-time=2026-09-10T11:00:00Z",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString()).contains("Cloud Logging query failed for project test")
    assertThat(output.toString()).contains("No timeline entries were collected")
  }

  private fun traceSpan(spanId: String, startTime: Instant): ReportTraceSpan {
    return ReportTraceSpan(
      sourceProject = "test",
      traceId = "trace-1",
      spanId = spanId,
      parentSpanId = null,
      name = spanId,
      service = "service",
      startTime = startTime,
      endTime = null,
      statusCode = null,
      statusMessage = null,
      attributes = emptyMap(),
      events = emptyList(),
    )
  }

  private fun reportTraceContext(): ReportTraceContext {
    return ReportTraceContext(
      basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1",
      basicReportState = "SUCCEEDED",
      reportName = "measurementConsumers/mc-1/reports/report-1",
      metricNames = emptyList(),
      measurementNames = emptyList(),
      createTime = NOW,
    )
  }

  companion object {
    private val NOW: Instant = Instant.parse("2026-09-10T13:00:00Z")
  }
}
