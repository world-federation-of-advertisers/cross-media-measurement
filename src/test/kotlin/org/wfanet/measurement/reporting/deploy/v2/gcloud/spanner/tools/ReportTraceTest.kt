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
                attributes =
                  mapOf(
                    "xmm.report.name" to "reports/report-1",
                    "xmm.lifecycle.stage" to "report_creation",
                  ),
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
          "--allow-partial",
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
  fun `main marks empty BasicReport artifacts partial`() {
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

    assertThat(exitCode).isEqualTo(1)
    assertThat(outputDirectory.toFile().list()!!.toList())
      .containsExactly("mc-1__report-a.md", "mc-2__report-a.md")
    assertThat(outputDirectory.resolve("mc-1__report-a.md").toFile().readText())
      .contains("BasicReport: measurementConsumers/mc-1/basicReports/report-a")
    assertThat(outputDirectory.resolve("mc-1__report-a.md").toFile().readText())
      .contains("Collection completeness: PARTIAL")
    assertThat(output.toString())
      .contains("PARTIAL  measurementConsumers/mc-1/basicReports/report-a")
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
    assertThat(output.toString())
      .contains("PARTIAL  measurementConsumers/mc-1/basicReports/report-a")
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
  fun `Cloud Trace v1 fixture parses only fields exposed by read API`() {
    val spans =
      parseCloudTraceV1Response(
        "trace-project",
        """
        {
          "traces": [{
            "projectId": "trace-project",
            "traceId": "0123456789abcdef0123456789abcdef",
            "spans": [{
              "spanId": "123",
              "parentSpanId": "100",
              "name": "reporting.metrics.sync_results",
              "startTime": "2026-09-10T12:00:00Z",
              "endTime": "2026-09-10T12:00:01Z",
              "labels": {
                "service.name": "reporting",
                "xmm.lifecycle.stage": "metric_result_sync",
                "xmm.report.name": "measurementConsumers/mc-1/reports/report-1"
              }
            }]
          }]
        }
        """
          .trimIndent(),
      )

    assertThat(spans).hasSize(1)
    assertThat(spans.single().name).isEqualTo("reporting.metrics.sync_results")
    assertThat(spans.single().attributes["xmm.lifecycle.stage"]).isEqualTo("metric_result_sync")
  }

  @Test
  fun `render preserves span hierarchy and readable labels`() {
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
              attributes =
                mapOf(
                  "xmm.requisition.name" to "requisitions/r1",
                  "xmm.lifecycle.stage" to "requisition_creation",
                  "xmm.outcome" to "failed",
                  "exception.type" to "IllegalStateException",
                  "exception.message" to "credential=secret",
                ),
            )
          ),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("parent=span-1")
    assertThat(output).contains("xmm.requisition.name=requisitions/r1")
    assertThat(output).contains("exception.type=IllegalStateException")
    assertThat(output).doesNotContain("credential=secret")
  }

  @Test
  fun `render reports complete direct lifecycle and observed Duchy stage`() {
    val stages =
      listOf(
        "basic_report_creation",
        "report_creation",
        "measurement_creation",
        "requisition_creation",
        "requisition_dispatch",
        "results_fulfillment",
        "kingdom_result_acceptance",
        "metric_result_sync",
        "report_result_assembly",
        "noise_correction",
        "duchy_computation",
      )
    val spans =
      stages.mapIndexed { index, stage ->
        ReportTraceSpan(
          sourceProject = "test",
          traceId = "trace-1",
          spanId = "span-$index",
          parentSpanId = null,
          name = stage,
          service = "test-service",
          startTime = NOW.plusSeconds(index.toLong()),
          endTime = NOW.plusSeconds(index.toLong() + 1),
          attributes = mapOf("xmm.lifecycle.stage" to stage),
        )
      }

    val output =
      ReportTraceOutput.render(
        context = reportTraceContext(),
        spans = spans,
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("Collection completeness: COMPLETE")
    assertThat(output).contains("Execution outcome: SUCCEEDED")
    assertThat(output).contains("| duchy_computation | OBSERVED |")
    assertThat(output).doesNotContain("| basic_report_api_fetch | MISSING |")
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
              "xmm.unrecognized.payload" to "secret-customer-data",
              "authorization" to "secret-authorization",
            ),
        )
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeRawPayloads = false)

    assertThat(rendered).contains("xmm.report.name=measurementConsumers/mc-1/reports/report-1")
    assertThat(rendered).doesNotContain("request failed")
    assertThat(rendered).doesNotContain("secret-token")
    assertThat(rendered).doesNotContain("secret-api-key")
    assertThat(rendered).doesNotContain("secret-password")
    assertThat(rendered).doesNotContain("secret-authorization")
    assertThat(rendered).doesNotContain("secret-customer-data")
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
  fun `renderLogPayload omits credentials JWTs and signed URLs inside message`() {
    val payload =
      Payload.JsonPayload.of(
        mapOf(
          "message" to
            "status=failed password=hunter2 credential=session-secret " +
              "jwt=aaa.bbb.ccc url=https://example.test/object?X-Goog-Signature=secret",
          "event" to "requisition_failed",
        )
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeRawPayloads = false)

    assertThat(rendered).contains("event=requisition_failed")
    assertThat(rendered).contains("status=failed")
    assertThat(rendered).doesNotContain("hunter2")
    assertThat(rendered).doesNotContain("session-secret")
    assertThat(rendered).doesNotContain("aaa.bbb.ccc")
    assertThat(rendered).doesNotContain("X-Goog-Signature")
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
          "--allow-partial",
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
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(traceIdsByProject["kingdom"]).contains("shared-trace")
    assertThat(output.toString()).contains("SPAN [kingdom/kingdom] remote-span")
  }

  @Test
  fun `main pivots back to logs with identifiers discovered from spans`() {
    val output = StringWriter()
    val logQueries = mutableListOf<Collection<String>>()
    val workItemName = "workItems/work-item-1"
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            logQueries += correlationValues.toList()
            if (workItemName in correlationValues) {
              listOf(
                ReportTraceLogEntry(
                  sourceProject = project,
                  timestamp = NOW,
                  service = "results-fulfiller",
                  severity = "ERROR",
                  trace = null,
                  message =
                    "xmm.work_item.name=$workItemName xmm.outcome=failed " +
                      "xmm.error.type=IllegalStateException",
                )
              )
            } else {
              emptyList()
            }
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { project, correlationValues, _, _, _, _ ->
            if (reportName in correlationValues) {
              listOf(
                ReportTraceSpan(
                  sourceProject = project,
                  traceId = "trace-1",
                  spanId = "span-1",
                  parentSpanId = null,
                  name = "work item dispatched",
                  service = "requisition-fetcher",
                  startTime = NOW,
                  endTime = NOW,
                  attributes = mapOf("xmm.work_item.name" to workItemName),
                )
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
          "--project=test",
          "--report=$reportName",
          "--start-time=2026-09-10T11:00:00Z",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(logQueries).containsExactly(listOf(reportName), listOf(workItemName)).inOrder()
    assertThat(output.toString()).contains("xmm.error.type=IllegalStateException")
  }

  @Test
  fun `render separates collection completeness from refused execution outcome`() {
    val context = reportTraceContext().copy(basicReportName = null, basicReportState = null)
    val span =
      traceSpan("span-1", NOW)
        .copy(
          attributes =
            mapOf("xmm.lifecycle.stage" to "results_fulfillment", "xmm.outcome" to "refused")
        )

    val output =
      ReportTraceOutput.render(
        context = context,
        spans = listOf(span),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("Collection completeness: PARTIAL")
    assertThat(output).contains("Execution outcome: REFUSED")
    assertThat(output).contains("| results_fulfillment | REFUSED |")
  }

  @Test
  fun `started lifecycle evidence is not terminally complete`() {
    val stages =
      listOf(
        "basic_report_creation",
        "report_creation",
        "measurement_creation",
        "requisition_creation",
        "requisition_dispatch",
        "results_fulfillment",
        "kingdom_result_acceptance",
        "metric_result_sync",
        "report_result_assembly",
        "noise_correction",
      )
    val spans =
      stages.mapIndexed { index, stage ->
        traceSpan("span-$index", NOW.plusSeconds(index.toLong()))
          .copy(
            attributes =
              mapOf(
                "xmm.lifecycle.stage" to stage,
                "xmm.outcome" to if (stage == "results_fulfillment") "started" else "succeeded",
              )
          )
      }

    val output =
      ReportTraceOutput.render(
        context = reportTraceContext(),
        spans = spans,
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("Collection completeness: PARTIAL")
    assertThat(output).contains("| results_fulfillment | IN_PROGRESS |")
  }

  @Test
  fun `aggregate truncation does not mark an empty project truncated`() {
    val output = StringWriter()
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { project, correlationValues, _, _, _, _ ->
            if (project == "reporting" && reportName in correlationValues) {
              listOf(
                traceSpan("span-1", NOW).copy(sourceProject = project),
                traceSpan("span-2", NOW.plusSeconds(1)).copy(sourceProject = project),
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
          "--report=$reportName",
          "--start-time=2026-09-10T11:00:00Z",
          "--limit=1",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString()).contains("| reporting | Cloud Trace | TRUNCATED | 2 | 1 |")
    assertThat(output.toString()).contains("| kingdom | Cloud Trace | NO_MATCHES | 0 | 0 |")
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
  fun `bounded collection retains newest terminal log evidence and accurate source count`() {
    val output = StringWriter()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { _, _, _, _ ->
            listOf(
              ReportTraceLogEntry(
                project,
                Instant.parse("2026-09-10T12:00:00Z"),
                "reporting",
                "INFO",
                null,
                "xmm.lifecycle.stage=report_creation",
              ),
              ReportTraceLogEntry(
                project,
                Instant.parse("2026-09-10T12:10:00Z"),
                "reporting",
                "ERROR",
                null,
                "xmm.lifecycle.stage=report_result_assembly xmm.outcome=failed",
              ),
            )
          }
        },
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
          "--limit=1",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString()).contains("xmm.outcome=failed")
    assertThat(output.toString()).doesNotContain("LOG INFO")
    assertThat(output.toString()).contains("| test | Cloud Logging | TRUNCATED | 2 | 1 |")
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
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
    assertThat(output.toString()).contains("No matching trace spans or log entries")
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
      attributes = emptyMap(),
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
