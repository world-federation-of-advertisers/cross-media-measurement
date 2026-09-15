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

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.logging.Logging
import com.google.cloud.logging.Payload
import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.io.BufferedWriter
import java.io.PrintWriter
import java.io.StringWriter
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Date
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTracing
import org.wfanet.measurement.common.throttler.Throttler

private fun main(args: Array<String>, dependencies: ReportTraceDependencies): Int = runBlocking {
  runReportTrace(args, dependencies)
}

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
        metricStates = emptyMap(),
        reusedMetricNames = emptySet(),
        unresolvedMetricRequestIds = emptyList(),
        measurementNames = emptyList(),
        reusedMeasurementNames = emptySet(),
        unresolvedMeasurementRequestIds = emptyList(),
        reportResolvedByRequestId = false,
        telemetryRecoveredMeasurementNames = emptyMap(),
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
        output = PrintWriter(BufferedWriter(output)),
        error = PrintWriter(error),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--report=measurementConsumers/mc-1/reports/report-1",
          "--start-time=2026-09-10T11:00:00Z",
          "--allow-partial",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(receivedValues).containsExactly("reports/report-1")
    assertThat(readerEndTimes).containsExactly(NOW, NOW, NOW, NOW, NOW)
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
        metricStates = emptyMap(),
        reusedMetricNames = emptySet(),
        unresolvedMetricRequestIds = emptyList(),
        measurementNames = emptyList(),
        reusedMeasurementNames = emptySet(),
        unresolvedMeasurementRequestIds = emptyList(),
        reportResolvedByRequestId = false,
        telemetryRecoveredMeasurementNames = emptyMap(),
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
              metricStates = emptyMap(),
              reusedMetricNames = emptySet(),
              unresolvedMetricRequestIds = emptyList(),
              measurementNames = emptyList(),
              reusedMeasurementNames = emptySet(),
              unresolvedMeasurementRequestIds = emptyList(),
              reportResolvedByRequestId = false,
              telemetryRecoveredMeasurementNames = emptyMap(),
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
  fun `collection deadline marks one report partial and continues the batch`() {
    val output = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("deadline-traces").toPath()
    val queriedBasicReports = mutableSetOf<String>()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, _, _, _, _ ->
            val basicReport =
              correlationValues.firstOrNull { "/basicReports/" in it }
                ?: return@ReportTraceSpanReader emptyList()
            queriedBasicReports += basicReport
            if (basicReport.endsWith("/report-a")) {
              awaitCancellation()
            }
            emptyList()
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver { key ->
            ReportTraceContext(
              basicReportName = key.toName(),
              basicReportState = "RUNNING",
              reportName =
                "measurementConsumers/${key.cmmsMeasurementConsumerId}/reports/${key.basicReportId}",
              metricNames = emptyList(),
              metricStates = emptyMap(),
              reusedMetricNames = emptySet(),
              unresolvedMetricRequestIds = emptyList(),
              measurementNames = emptyList(),
              reusedMeasurementNames = emptySet(),
              unresolvedMeasurementRequestIds = emptyList(),
              reportResolvedByRequestId = false,
              telemetryRecoveredMeasurementNames = emptyMap(),
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
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-b",
          "--output-dir=$outputDirectory",
          "--collection-deadline=PT0.05S",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(queriedBasicReports)
      .containsExactly(
        "measurementConsumers/mc-1/basicReports/report-a",
        "measurementConsumers/mc-1/basicReports/report-b",
      )
    assertThat(outputDirectory.resolve("mc-1__report-a.md").toFile().readText())
      .contains("Telemetry collection exceeded the per-report deadline")
    assertThat(outputDirectory.resolve("mc-1__report-b.md").toFile().readText())
      .contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `collection caps correlation values and trace IDs per report`() {
    val outputDirectory = temporaryFolder.newFolder("capped-traces").toPath()
    val logQueryValues = mutableListOf<Collection<String>>()
    val traceQueryIds = mutableListOf<Collection<String>>()
    val context =
      reportTraceContext()
        .copy(
          metricNames =
            listOf(
              "measurementConsumers/mc-1/metrics/metric-1",
              "measurementConsumers/mc-1/metrics/metric-2",
            )
        )
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            logQueryValues += correlationValues
            listOf("trace-1", "trace-2", "trace-3").mapIndexed { index, traceId ->
              ReportTraceLogEntry(
                sourceProject = project,
                timestamp = NOW.plusSeconds(index.toLong()),
                service = "test",
                severity = "INFO",
                trace = "projects/test/traces/$traceId",
                message = "",
              )
            }
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, traceIds, _, _, _ ->
            traceQueryIds += traceIds
            emptyList()
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=${context.basicReportName}",
          "--output-dir=$outputDirectory",
          "--max-correlation-values=2",
          "--max-trace-ids=2",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(logQueryValues.flatten().distinct()).hasSize(2)
    assertThat(traceQueryIds.flatten().distinct()).containsExactly("trace-1", "trace-2")
    val artifact = outputDirectory.toFile().listFiles().single().readText()
    assertThat(artifact).contains("| collector | Correlation values | TRUNCATED |")
    assertThat(artifact).contains("| collector | Trace IDs | TRUNCATED |")
    assertThat(artifact).contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `main stops BasicReport batch when resolver is cancelled`() {
    val output = StringWriter()
    val error = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("cancelled-traces").toPath()
    var resolverCalls = 0
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver {
            resolverCalls++
            throw CancellationException("resolver cancelled")
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(error),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-b",
          "--output-dir=$outputDirectory",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(1)
    assertThat(resolverCalls).isEqualTo(1)
    assertThat(outputDirectory.toFile().list()!!.toList()).isEmpty()
    assertThat(output.toString()).doesNotContain("report-b")
    assertThat(error.toString()).contains("resolver cancelled")
  }

  @Test
  fun `report creation failure before Report linkage matches BasicReport`() {
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "FAILED",
          reportName = "(not created)",
          measurementNames = emptyList(),
        )
    val failure =
      failedLifecycleSpan(
        "report_creation",
        mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        ReportTraceRouteResolution.unresolved(
          measurementNames = emptyList(),
          topology = ReportTraceTopology.notSupplied(),
          status = "NOT_ATTEMPTED",
          note = "",
        ),
        listOf(failure),
        emptyList(),
      )

    val stage = coverage.single { it.name == "report_creation" }
    assertThat(stage.status).isEqualTo("FAILED")
    assertThat(stage.resource).contains(checkNotNull(context.basicReportName))
    assertThat(stage.resource).doesNotContain("(not created)")
  }

  @Test
  fun `recoveredMeasurementNames accepts unique successful creation evidence`() {
    val requestId = "measurement-request-1"
    val measurementName = "measurementConsumers/mc-1/measurements/measurement-2"
    val context =
      reportTraceContext()
        .copy(measurementNames = emptyList(), unresolvedMeasurementRequestIds = listOf(requestId))
    val span =
      lifecycleSpan(
          "measurement_creation",
          mapOf(
            "xmm.measurement.request_id" to requestId,
            "xmm.measurement.name" to measurementName,
          ),
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "measurement_creation",
              "xmm.outcome" to "accepted",
              "xmm.measurement.request_id" to requestId,
              "xmm.measurement.name" to measurementName,
            )
        )

    assertThat(ReportTraceOutput.recoveredMeasurementNames(context, listOf(span), emptyList()))
      .containsExactly(measurementName, requestId)
  }

  @Test
  fun `stale WorkItem delivery does not satisfy processing stage`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val staleDelivery =
      lifecycleSpan(
          "work_item_processing",
          mapOf(
            "xmm.requisition.name" to requisitionName,
            "xmm.work_item.name" to "workItems/results-fulfiller-1",
          ),
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "work_item_processing",
              "xmm.outcome" to "stale_delivery",
              "xmm.requisition.name" to requisitionName,
              "xmm.work_item.name" to "workItems/results-fulfiller-1",
            )
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(staleDelivery),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "work_item_processing" }.status)
      .isEqualTo("IN_PROGRESS")
  }

  @Test
  fun `failed unexpected refusal remains failed`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.DIRECT_EDP,
        )
        .withRequisitionState("UNFULFILLED", measurementState = "PENDING")
    val failedRefusal =
      failedLifecycleSpan("requisition_refusal", mapOf("xmm.requisition.name" to requisitionName))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(failedRefusal),
        emptyList(),
      )

    val stage = coverage.single { it.name == "requisition_refusal" }
    assertThat(stage.status).isEqualTo("FAILED")
    assertThat(stage.evidence).contains("unexpected for the final route")
  }

  @Test
  fun `main collects telemetry when BasicReport resolution fails`() {
    val output = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("resolver-failure").toPath()
    val basicReportName = "measurementConsumers/mc-1/basicReports/report-a"
    var loggingCorrelationValues: Collection<String> = emptyList()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            loggingCorrelationValues = correlationValues
            listOf(
              ReportTraceLogEntry(
                sourceProject = project,
                timestamp = NOW,
                service = "reporting",
                severity = "ERROR",
                trace = null,
                message =
                  "xmm.basic_report.name=$basicReportName " +
                    "xmm.lifecycle.stage=report_creation xmm.outcome=failed",
              )
            )
          }
        },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { error("Reporting database unavailable") },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(loggingCorrelationValues).containsExactly(basicReportName)
    val artifact = outputDirectory.resolve("mc-1__report-a.md").toFile().readText()
    assertThat(artifact).contains("Collection completeness: PARTIAL")
    assertThat(artifact).contains("Resource resolution")
    assertThat(artifact).contains("IllegalStateException")
    assertThat(artifact).contains("xmm.lifecycle.stage=report_creation")
  }

  @Test
  fun `main collects telemetry when Kingdom route resolution fails`() {
    val output = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("kingdom-resolver-failure").toPath()
    val basicReportName = "measurementConsumers/mc-1/basicReports/report-a"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { _, _, _, _ ->
            listOf(
              ReportTraceLogEntry(
                sourceProject = project,
                timestamp = NOW,
                service = "reporting",
                severity = "INFO",
                trace = null,
                message =
                  "xmm.basic_report.name=$basicReportName " +
                    "xmm.lifecycle.stage=report_creation xmm.outcome=succeeded",
              )
            )
          }
        },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver { key ->
            reportTraceContext().copy(basicReportName = key.toName())
          },
        routeResolverOverride = ReportTraceRouteResolver { _, _ -> error("Kingdom unavailable") },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    val artifact = outputDirectory.resolve("mc-1__report-a.md").toFile().readText()
    assertThat(artifact).contains("Collection completeness: PARTIAL")
    assertThat(artifact).contains("| kingdom | Route resolution | FAILED |")
    assertThat(artifact)
      .contains(
        "| duchy_computation | measurementConsumers/mc-1/measurements/measurement-1 | " +
          "UNKNOWN |"
      )
    assertThat(artifact).contains("xmm.lifecycle.stage=report_creation")
  }

  @Test
  fun `main marks Reporting resolution partial when descendants are unresolved`() {
    val outputDirectory = temporaryFolder.newFolder("partial-reporting-resolution").toPath()
    val basicReportName = "measurementConsumers/mc-1/basicReports/report-a"
    val context =
      reportTraceContext()
        .copy(
          basicReportName = basicReportName,
          unresolvedMetricRequestIds = listOf("create-metric-request-2"),
          unresolvedMeasurementRequestIds = listOf("create-measurement-request-2"),
        )
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, _, _, _, _ ->
            listOf(lifecycleSpan("basic_report_creation", "xmm.basic_report.name", basicReportName))
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        routeResolverOverride =
          ReportTraceRouteResolver { _, _ ->
            routeResolution(
              context,
              ReportTraceMeasurementRouteKind.DIRECT,
              "dataProviders/direct/requisitions/requisition-1",
              ReportTraceRequisitionRouteKind.DIRECT_EDP,
            )
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    val artifact = outputDirectory.resolve("mc-1__report-a.md").toFile().readText()
    assertThat(artifact).contains("| reporting | Resource resolution | PARTIAL |")
    assertThat(artifact).contains("Metric request: create-metric-request-2 [UNRESOLVED]")
    assertThat(artifact).contains("Measurement request: create-measurement-request-2 [UNRESOLVED]")
    assertThat(artifact).contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `buildLogFilters chunks large identifier sets`() {
    val filters =
      ReportTraceOutput.buildLogFilters(
        (1..100).map { "measurementConsumers/mc-1/measurements/${"m".repeat(50)}-$it" },
        Instant.parse("2026-09-10T11:00:00Z"),
        NOW,
        includeGrpcPayloads = false,
      )

    assertThat(filters.size).isGreaterThan(1)
    assertThat(filters.all { it.length <= 20_000 }).isTrue()
  }

  @Test
  fun `buildLogFilters excludes verbose gRPC payloads at query time`() {
    val filter =
      ReportTraceOutput.buildLogFilters(
          listOf("measurementConsumers/mc-1/basicReports/report-1"),
          Instant.parse("2026-09-10T11:00:00Z"),
          NOW,
          includeGrpcPayloads = false,
        )
        .single()

    assertThat(filter).contains("NOT (textPayload =~")
    assertThat(filter).contains("jsonPayload.message =~")
    assertThat(filter).contains("gRPC([[:space:]]+client)?")
    assertThat(filter).contains("resource.labels.container_name =~ \".*api-server.*\"")
    assertThat(filter).contains("^[[:space:]]*[a-z][a-z0-9_.-]*:[[:space:]]")
  }

  @Test
  fun `buildLogFilters includes verbose gRPC payloads when requested`() {
    val filter =
      ReportTraceOutput.buildLogFilters(
          listOf("measurementConsumers/mc-1/basicReports/report-1"),
          Instant.parse("2026-09-10T11:00:00Z"),
          NOW,
          includeGrpcPayloads = true,
        )
        .single()

    assertThat(filter).doesNotContain("NOT (textPayload =~")
    assertThat(filter).doesNotContain("jsonPayload.message =~")
  }

  @Test
  fun `Logging options charge reads to observability project`() {
    val options =
      buildReportTraceLoggingOptions(
        "observability-project",
        GoogleCredentials.create(AccessToken("token", Date(Long.MAX_VALUE))),
      )

    assertThat(options.projectId).isEqualTo("observability-project")
    assertThat(options.quotaProjectId).isEqualTo("observability-project")
    assertThat((options.credentials as GoogleCredentials).quotaProjectId)
      .isEqualTo("observability-project")
  }

  @Test
  fun `Cloud Trace v1 fixture parses only fields exposed by read API`() {
    val spans =
      GoogleCloudReportTraceSpanReader.parseResponse(
        project = "trace-project",
        body =
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
        fallbackTraceId = null,
      )

    assertThat(spans).hasSize(1)
    assertThat(spans.single().name).isEqualTo("reporting.metrics.sync_results")
    assertThat(spans.single().attributes["xmm.lifecycle.stage"]).isEqualTo("metric_result_sync")
  }

  @Test
  fun `Cloud Trace reader retains failure outside chronological read limit`() = runBlocking {
    val successfulResponse = mock<HttpResponse<String>>()
    whenever(successfulResponse.statusCode()).thenReturn(200)
    whenever(successfulResponse.body())
      .thenReturn(
        """
        {
          "traces": [{
            "projectId": "trace-project",
            "traceId": "11111111111111111111111111111111",
            "spans": [
              {
                "spanId": "1",
                "name": "old-success",
                "startTime": "2026-09-10T12:00:00Z",
                "endTime": "2026-09-10T12:00:01Z",
                "labels": {"xmm.outcome": "succeeded"}
              },
              {
                "spanId": "2",
                "name": "newer-success",
                "startTime": "2026-09-10T12:00:01Z",
                "endTime": "2026-09-10T12:00:02Z",
                "labels": {"xmm.outcome": "succeeded"}
              }
            ]
          }]
        }
        """
          .trimIndent()
      )
    val failureResponse = mock<HttpResponse<String>>()
    whenever(failureResponse.statusCode()).thenReturn(200)
    whenever(failureResponse.body())
      .thenReturn(
        """
        {
          "traces": [{
            "projectId": "trace-project",
            "traceId": "22222222222222222222222222222222",
            "spans": [{
              "spanId": "3",
              "name": "late-failure",
              "startTime": "2026-09-10T12:00:02Z",
              "endTime": "2026-09-10T12:00:03Z",
              "labels": {"xmm.outcome": "failed_validation"}
            }]
          }]
        }
        """
          .trimIndent()
      )
    val httpClient = mock<HttpClient>()
    whenever(httpClient.send(any<HttpRequest>(), any<HttpResponse.BodyHandler<String>>()))
      .thenReturn(successfulResponse, failureResponse)
    val reader =
      GoogleCloudReportTraceSpanReader(
        GoogleCredentials.create(AccessToken("token", Date(Long.MAX_VALUE))),
        httpClient,
      )

    val spans =
      reader.read(
        project = "trace-project",
        correlationValues =
          listOf(
            "measurementConsumers/mc-1/basicReports/report-1",
            "measurementConsumers/mc-1/basicReports/report-2",
          ),
        traceIds = emptyList(),
        startTime = Instant.parse("2026-09-10T11:00:00Z"),
        endTime = Instant.parse("2026-09-10T13:00:00Z"),
        limit = 1,
      )

    assertThat(spans.map { it.name }).containsExactly("newer-success", "late-failure").inOrder()
  }

  @Test
  fun `Cloud Trace reader bounds concurrent requests`() = runBlocking {
    val response = mock<HttpResponse<String>>()
    whenever(response.statusCode()).thenReturn(200)
    whenever(response.body()).thenReturn("{\"traces\":[]}")
    val httpClient = mock<HttpClient>()
    val activeRequests = AtomicInteger()
    val maximumActiveRequests = AtomicInteger()
    val firstTwoRequestsStarted = CountDownLatch(2)
    whenever(httpClient.send(any<HttpRequest>(), any<HttpResponse.BodyHandler<String>>()))
      .thenAnswer {
        val active = activeRequests.incrementAndGet()
        maximumActiveRequests.accumulateAndGet(active) { current, update -> maxOf(current, update) }
        firstTwoRequestsStarted.countDown()
        check(firstTwoRequestsStarted.await(5, TimeUnit.SECONDS))
        activeRequests.decrementAndGet()
        response
      }
    val reader =
      GoogleCloudReportTraceSpanReader(
        GoogleCredentials.create(AccessToken("token", Date(Long.MAX_VALUE))),
        httpClient,
        maxConcurrency = 2,
        requestThrottlerFactory = { SerializingThrottler() },
      )

    reader.read(
      project = "trace-project",
      correlationValues =
        listOf(
          "measurementConsumers/mc-1/basicReports/report-1",
          "measurementConsumers/mc-1/basicReports/report-2",
          "measurementConsumers/mc-1/basicReports/report-3",
        ),
      traceIds = emptyList(),
      startTime = Instant.parse("2026-09-10T11:00:00Z"),
      endTime = Instant.parse("2026-09-10T13:00:00Z"),
      limit = 100,
    )

    assertThat(maximumActiveRequests.get()).isEqualTo(2)
  }

  @Test
  fun `Cloud Trace reader charges list and get requests to shared quota`() = runBlocking {
    val successfulResponse = mock<HttpResponse<String>>()
    whenever(successfulResponse.statusCode()).thenReturn(200)
    whenever(successfulResponse.body()).thenReturn("{\"traces\":[]}")
    val httpClient = mock<HttpClient>()
    val requests = mutableListOf<HttpRequest>()
    whenever(httpClient.send(any<HttpRequest>(), any<HttpResponse.BodyHandler<String>>()))
      .thenAnswer { invocation ->
        requests += invocation.getArgument<HttpRequest>(0)
        successfulResponse
      }
    val requestThrottler = RecordingThrottler()
    val reader =
      GoogleCloudReportTraceSpanReader(
        GoogleCredentials.create(AccessToken("token", Date(Long.MAX_VALUE))),
        httpClient,
        maxConcurrency = 1,
        requestThrottlerFactory = { requestThrottler },
      )

    val spans =
      reader.read(
        project = "trace-project",
        correlationValues =
          listOf(
            "measurementConsumers/mc-1/basicReports/report-1",
            "measurementConsumers/mc-1/basicReports/report-2",
          ),
        traceIds = listOf("trace-1"),
        startTime = Instant.parse("2026-09-10T11:00:00Z"),
        endTime = Instant.parse("2026-09-10T13:00:00Z"),
        limit = 100,
      )

    assertThat(spans).isEmpty()
    assertThat(requestThrottler.invocationCount).isEqualTo(51)
    assertThat(
        requests.map { request -> request.headers().firstValue("x-goog-user-project").orElse(null) }
      )
      .containsExactly("trace-project", "trace-project", "trace-project")
    Unit
  }

  @Test
  fun `Cloud Logging reader throttles list requests`() = runBlocking {
    val logging = mock<Logging>()
    whenever(logging.listLogEntries(any(), any(), any()))
      .thenThrow(IllegalStateException("stop after request starts"))
    val throttler = RecordingThrottler()
    val reader =
      GoogleCloudReportTraceLogReader(
        project = "logging-project",
        logging = logging,
        includeGrpcPayloads = false,
        requestThrottler = throttler,
      )

    val failure =
      runCatching {
          reader.read(
            correlationValues = listOf("measurementConsumers/mc-1/basicReports/report-1"),
            startTime = Instant.parse("2026-09-10T11:00:00Z"),
            endTime = Instant.parse("2026-09-10T13:00:00Z"),
            limit = 100,
          )
        }
        .exceptionOrNull()

    assertThat(failure).isInstanceOf(IllegalStateException::class.java)
    assertThat(throttler.invocationCount).isEqualTo(1)
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
                  "xmm.lifecycle.stage" to "requisition_available",
                  "xmm.outcome" to "failed",
                  "exception.type" to "IllegalStateException",
                  "exception.message" to "credential=secret",
                ),
            )
          ),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("parent=span-1")
    assertThat(output).contains("xmm.requisition.name=requisitions/r1")
    assertThat(output).contains("exception.type=IllegalStateException")
    assertThat(output).doesNotContain("credential=secret")
  }

  @Test
  fun `render reports complete direct lifecycle per expected child`() {
    val context =
      reportTraceContext().copy(metricNames = listOf("measurementConsumers/mc-1/metrics/metric-1"))
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context = context,
        measurementRoute = ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName = requisitionName,
        requisitionRoute = ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val stageResources =
      listOf(
        "basic_report_creation" to ("xmm.basic_report.name" to context.basicReportName!!),
        "report_creation" to ("xmm.report.name" to context.reportName),
        "metric_creation" to ("xmm.metric.name" to context.metricNames.single()),
        "measurement_creation" to ("xmm.measurement.name" to context.measurementNames.single()),
        "measurement_linkage" to ("xmm.measurement.name" to context.measurementNames.single()),
        "requisition_available" to ("xmm.requisition.name" to requisitionName),
        "kingdom_requisition_result_acceptance" to ("xmm.requisition.name" to requisitionName),
        "kingdom_measurement_sync" to ("xmm.measurement.name" to context.measurementNames.single()),
        "metric_result_sync" to ("xmm.metric.name" to context.metricNames.single()),
        "report_result_assembly" to ("xmm.report.name" to context.reportName),
        "noise_correction" to ("xmm.basic_report.name" to context.basicReportName!!),
        "processed_result_writeback" to ("xmm.basic_report.name" to context.basicReportName!!),
      )
    val spans =
      stageResources.mapIndexed { index, (stage, resource) ->
        ReportTraceSpan(
          sourceProject = "test",
          traceId = "trace-1",
          spanId = "span-$index",
          parentSpanId = null,
          name = stage,
          service = "test-service",
          startTime = NOW.plusSeconds(index.toLong()),
          endTime = NOW.plusSeconds(index.toLong() + 1),
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to stage,
              "xmm.outcome" to "succeeded",
              resource.first to resource.second,
            ),
        )
      } +
        lifecycleSpan("basic_report_api_fetch", "xmm.basic_report.name", context.basicReportName!!)
          .copy(
            attributes =
              mapOf(
                "xmm.lifecycle.stage" to "basic_report_api_fetch",
                "xmm.outcome" to "started",
                "xmm.basic_report.name" to context.basicReportName,
              )
          )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = spans,
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Collection completeness: COMPLETE")
    assertThat(output).contains("Execution outcome: SUCCEEDED")
    assertThat(output)
      .contains("| duchy_computation | ${context.measurementNames.single()} | NOT_APPLICABLE |")
    assertThat(output).contains("| requisition_dispatch | $requisitionName | NOT_APPLICABLE |")
    assertThat(output)
      .contains("| basic_report_api_fetch | ${context.basicReportName} | OPTIONAL |")
  }

  @Test
  fun `one child evidence does not satisfy another expected child`() {
    val metric1 = "measurementConsumers/mc-1/metrics/metric-1"
    val metric2 = "measurementConsumers/mc-1/metrics/metric-2"
    val context = reportTraceContext().copy(metricNames = listOf(metric1, metric2))
    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context = context,
        routeResolution =
          ReportTraceRouteResolution.unresolved(
            context.measurementNames,
            ReportTraceTopology.notSupplied(),
            "FAILED",
            "test",
          ),
        spans =
          listOf(
            lifecycleSpan(
              stage = "metric_creation",
              resourceAttribute = "xmm.metric.name",
              resource = metric1,
            )
          ),
        logEntries = emptyList(),
      )

    assertThat(coverage.single { it.name == "metric_creation" && it.resource == metric1 }.status)
      .isEqualTo("SUCCEEDED")
    assertThat(coverage.single { it.name == "metric_creation" && it.resource == metric2 }.status)
      .isEqualTo("MISSING")
  }

  @Test
  fun `unresolved Metric request remains visible when another Metric resolves`() {
    val resolvedMetric = "measurementConsumers/mc-1/metrics/metric-1"
    val unresolvedRequestId = "create-metric-request-2"
    val context =
      reportTraceContext()
        .copy(
          metricNames = listOf(resolvedMetric),
          unresolvedMetricRequestIds = listOf(unresolvedRequestId),
        )
    val spans = listOf(lifecycleSpan("metric_creation", "xmm.metric.name", resolvedMetric))
    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context = context,
        routeResolution =
          ReportTraceRouteResolution.unresolved(
            context.measurementNames,
            ReportTraceTopology.notSupplied(),
            "FAILED",
            "test",
          ),
        spans = spans,
        logEntries = emptyList(),
      )

    assertThat(
        coverage
          .single {
            it.name == "metric_creation" && it.resource == "Metric request $unresolvedRequestId"
          }
          .status
      )
      .isEqualTo("UNKNOWN")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `Metric creation failure resolves an unresolved request by request ID`() {
    val requestId = "create-metric-request-2"
    val context =
      reportTraceContext()
        .copy(metricNames = emptyList(), unresolvedMetricRequestIds = listOf(requestId))
    val spans =
      listOf(failedLifecycleSpan("metric_creation", mapOf("xmm.metric.request_id" to requestId)))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context = context,
        routeResolution =
          ReportTraceRouteResolution.unresolved(
            context.measurementNames,
            ReportTraceTopology.notSupplied(),
            "FAILED",
            "test",
          ),
        spans = spans,
        logEntries = emptyList(),
      )

    assertThat(
        coverage.single {
          it.name == "metric_creation" && it.resource == "Metric request $requestId"
        }
      )
      .isEqualTo(
        ReportTraceLifecycleStage(
          name = "metric_creation",
          resource = "Metric request $requestId",
          status = "FAILED",
          evidence = "span metric_creation xmm.outcome=failed xmm.error.type=IllegalStateException",
          correlationValues = setOf(requestId),
        )
      )
  }

  @Test
  fun `unresolved Measurement request remains visible when another Measurement resolves`() {
    val unresolvedRequestId = "create-measurement-request-2"
    val context =
      reportTraceContext().copy(unresolvedMeasurementRequestIds = listOf(unresolvedRequestId))
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val spans =
      listOf(
        lifecycleSpan(
          "measurement_creation",
          "xmm.measurement.name",
          context.measurementNames.single(),
        )
      )
    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context = context,
        routeResolution = routeResolution,
        spans = spans,
        logEntries = emptyList(),
      )

    assertThat(
        coverage
          .single {
            it.name == "measurement_creation" &&
              it.resource == "Measurement request $unresolvedRequestId"
          }
          .status
      )
      .isEqualTo("UNKNOWN")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `Measurement creation failure resolves an unresolved request by request ID`() {
    val requestId = "create-measurement-request-2"
    val context =
      reportTraceContext()
        .copy(measurementNames = emptyList(), unresolvedMeasurementRequestIds = listOf(requestId))
    val spans =
      listOf(
        failedLifecycleSpan(
          "measurement_creation",
          mapOf("xmm.measurement.request_id" to requestId),
        )
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context = context,
        routeResolution =
          ReportTraceRouteResolution.unresolved(
            context.measurementNames,
            ReportTraceTopology.notSupplied(),
            "FAILED",
            "test",
          ),
        spans = spans,
        logEntries = emptyList(),
      )

    assertThat(
        coverage.single {
          it.name == "measurement_creation" && it.resource == "Measurement request $requestId"
        }
      )
      .isEqualTo(
        ReportTraceLifecycleStage(
          name = "measurement_creation",
          resource = "Measurement request $requestId",
          status = "FAILED",
          evidence =
            "span measurement_creation xmm.outcome=failed xmm.error.type=IllegalStateException",
          correlationValues = setOf(requestId),
        )
      )
  }

  @Test
  fun `lifecycle correlates bare WorkItem ID to canonical dispatch name`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val workItemId = "results-fulfiller-group-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val spans =
      listOf(
        lifecycleSpan(
          "requisition_dispatch",
          mapOf(
            "xmm.requisition.name" to requisitionName,
            "xmm.edpa.group_id" to "group-1",
            "xmm.work_item.name" to "workItems/$workItemId",
          ),
        ),
        lifecycleSpan("work_item_processing", mapOf("xmm.work_item.name" to workItemId)),
      )

    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(
        coverage.single { it.name == "work_item_processing" && it.resource == requisitionName }
      )
      .isEqualTo(
        ReportTraceLifecycleStage(
          name = "work_item_processing",
          resource = requisitionName,
          status = "SUCCEEDED",
          evidence = "span work_item_processing xmm.outcome=succeeded",
          correlationValues = setOf(requisitionName),
        )
      )
  }

  @Test
  fun `lifecycle uses completion time when an overlapping duplicate finishes first`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val workItemName = "workItems/group-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val original =
      lifecycleSpan(
          "work_item_processing",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.work_item.name" to workItemName),
        )
        .copy(startTime = NOW, endTime = NOW.plusSeconds(10))
    val duplicate =
      lifecycleSpan(
          "work_item_processing",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.work_item.name" to workItemName),
        )
        .copy(
          startTime = NOW.plusSeconds(5),
          endTime = NOW.plusSeconds(6),
          attributes = original.attributes + (ReportTraceAttributes.OUTCOME_STRING to "in_progress"),
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(original, duplicate),
        emptyList(),
      )

    assertThat(
        coverage.single { it.name == "work_item_processing" && it.resource == requisitionName }
      )
      .isEqualTo(
        ReportTraceLifecycleStage(
          name = "work_item_processing",
          resource = requisitionName,
          status = "SUCCEEDED",
          evidence =
            "span work_item_processing xmm.outcome=succeeded, " +
              "span work_item_processing xmm.outcome=in_progress",
          correlationValues = setOf(requisitionName),
        )
      )
  }

  @Test
  fun `execution outcome uses Report span completion time`() {
    val context = reportTraceContext().copy(basicReportState = null)
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val completedLater =
      lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName)
        .copy(
          startTime = NOW,
          endTime = NOW.plusSeconds(10),
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "report_result_assembly",
              "xmm.outcome" to "succeeded",
              "xmm.report.name" to context.reportName,
              "xmm.report.state" to "SUCCEEDED",
            ),
        )
    val startedLater =
      lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName)
        .copy(
          startTime = NOW.plusSeconds(5),
          endTime = NOW.plusSeconds(6),
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "report_result_assembly",
              "xmm.outcome" to "in_progress",
              "xmm.report.name" to context.reportName,
              "xmm.report.state" to "RUNNING",
            ),
        )

    assertThat(
        ReportTraceOutput.executionOutcome(
          context,
          routeResolution,
          listOf(completedLater, startedLater),
          emptyList(),
        )
      )
      .isEqualTo(ReportTraceExecutionOutcome.SUCCEEDED)
  }

  @Test
  fun `foreign report failure does not change execution outcome`() {
    val context = reportTraceContext().copy(basicReportState = null)
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val targetReportSucceeded =
      traceSpan("target-report", NOW)
        .copy(
          attributes =
            mapOf(
              "xmm.report.name" to context.reportName,
              "xmm.report.state" to "SUCCEEDED",
              "xmm.outcome" to "succeeded",
            )
        )
    val foreignMeasurementFailed =
      traceSpan("foreign-measurement", NOW.plusSeconds(1))
        .copy(
          attributes =
            mapOf(
              "xmm.measurement.name" to
                "measurementConsumers/mc-1/measurements/foreign-measurement",
              "xmm.measurement.state" to "FAILED",
              "xmm.outcome" to "failed",
            )
        )

    assertThat(
        ReportTraceOutput.executionOutcome(
          context,
          routeResolution,
          listOf(targetReportSucceeded, foreignMeasurementFailed),
          emptyList(),
        )
      )
      .isEqualTo(ReportTraceExecutionOutcome.SUCCEEDED)
  }

  @Test
  fun `correlation discovery rejects resources belonging only to another report`() {
    val targetMeasurement = "measurementConsumers/mc-1/measurements/measurement-1"
    val targetWorkItem = "workItems/target-work-item"
    val foreignMeasurement = "measurementConsumers/mc-1/measurements/foreign-measurement"
    val foreignComputation = "computations/foreign-computation"
    val spans =
      listOf(
        traceSpan("target", NOW)
          .copy(
            attributes =
              mapOf(
                "xmm.measurement.name" to targetMeasurement,
                "xmm.work_item.name" to targetWorkItem,
              )
          ),
        traceSpan("foreign", NOW.plusSeconds(1))
          .copy(
            attributes =
              mapOf(
                "xmm.measurement.name" to foreignMeasurement,
                "xmm.computation.name" to foreignComputation,
              )
          ),
      )

    val discovered =
      ReportTraceOutput.discoveredCorrelationValues(spans, emptyList(), setOf(targetMeasurement))

    assertThat(discovered).containsExactly(targetMeasurement, targetWorkItem)
  }

  @Test
  fun `span retention preserves all recognized failure outcomes`() {
    val failureOutcomes =
      listOf("failed", "failed_validation", "report_failed", "failure", "error", "refused")
    val failures =
      failureOutcomes.mapIndexed { index, outcome ->
        traceSpan("failure-$index", NOW.plusSeconds(index.toLong()))
          .copy(attributes = mapOf("xmm.outcome" to outcome))
      }
    val newerSuccess =
      traceSpan("newer-success", NOW.plusSeconds(100))
        .copy(attributes = mapOf("xmm.outcome" to "succeeded"))

    val retained = retainReportTraceSpans(failures + newerSuccess, failureOutcomes.size)

    assertThat(retained.map { it.attributes.getValue("xmm.outcome") })
      .containsExactlyElementsIn(failureOutcomes)
  }

  @Test
  fun `render reports complete MPC and EDPA lifecycle per expected child`() {
    val context =
      reportTraceContext().copy(metricNames = listOf("measurementConsumers/mc-1/metrics/metric-1"))
    val measurementName = context.measurementNames.single()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        requisitionName,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val groupId = "group-1"
    val workItemName = "workItems/results-fulfiller-$groupId"
    val computationName = "computations/computation-1"
    val commonStageResources =
      listOf(
        "basic_report_creation" to mapOf("xmm.basic_report.name" to context.basicReportName!!),
        "report_creation" to mapOf("xmm.report.name" to context.reportName),
        "metric_creation" to mapOf("xmm.metric.name" to context.metricNames.single()),
        "measurement_creation" to mapOf("xmm.measurement.name" to measurementName),
        "measurement_linkage" to mapOf("xmm.measurement.name" to measurementName),
        "requisition_available" to mapOf("xmm.requisition.name" to requisitionName),
        "requisition_dispatch" to
          mapOf(
            "xmm.requisition.name" to requisitionName,
            "xmm.edpa.group_id" to groupId,
            "xmm.work_item.name" to workItemName,
          ),
        "work_item_processing" to mapOf("xmm.work_item.name" to workItemName),
        "results_fulfillment" to
          mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to groupId),
        "duchy_requisition_acceptance" to
          mapOf("xmm.requisition.name" to requisitionName, "xmm.duchy.id" to "worker1"),
        "duchy_requisition_kingdom_fulfillment" to
          mapOf("xmm.requisition.name" to requisitionName, "xmm.duchy.id" to "worker1"),
        "kingdom_computation_result_acceptance" to mapOf("xmm.computation.name" to computationName),
        "kingdom_measurement_sync" to mapOf("xmm.measurement.name" to measurementName),
        "metric_result_sync" to mapOf("xmm.metric.name" to context.metricNames.single()),
        "report_result_assembly" to mapOf("xmm.report.name" to context.reportName),
        "noise_correction" to mapOf("xmm.basic_report.name" to context.basicReportName!!),
        "processed_result_writeback" to mapOf("xmm.basic_report.name" to context.basicReportName!!),
      )
    val duchyStageResources =
      listOf("aggregator", "worker1").flatMap { duchyId ->
        listOf("duchy_computation", "duchy_stage_attempt").map { stage ->
          stage to
            mapOf(
              "xmm.measurement.name" to measurementName,
              "xmm.computation.name" to computationName,
              "xmm.duchy.id" to duchyId,
            )
        }
      }

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans =
          (commonStageResources + duchyStageResources).map { (stage, attributes) ->
            lifecycleSpan(stage, attributes)
          } +
            traceSpan("results_fulfiller.process_group", NOW.minusSeconds(3))
              .copy(attributes = mapOf("xmm.edpa.group_id" to groupId)) +
            lifecycleSpan(
                "results_fulfillment",
                mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to groupId),
              )
              .copy(
                startTime = NOW.minusSeconds(2),
                endTime = NOW.minusSeconds(1),
                attributes =
                  mapOf(
                    "xmm.lifecycle.stage" to "results_fulfillment",
                    "xmm.outcome" to "prepared",
                    "xmm.requisition.name" to requisitionName,
                    "xmm.edpa.group_id" to groupId,
                  ),
              ),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Collection completeness: COMPLETE")
    assertThat(output)
      .contains("| duchy_computation | $measurementName @ duchy worker1 | SUCCEEDED |")
    assertThat(output).contains("| results_fulfillment | $requisitionName | SUCCEEDED |")
  }

  @Test
  fun `already fulfilled Requisition is a successful ResultsFulfiller terminal outcome`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val alreadyCompleted =
      lifecycleSpan(
          "results_fulfillment",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to "group-1"),
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "results_fulfillment",
              "xmm.outcome" to "already_completed",
              "xmm.requisition.name" to requisitionName,
              "xmm.requisition.state" to "FULFILLED",
              "xmm.edpa.group_id" to "group-1",
            )
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(alreadyCompleted),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "results_fulfillment" }.status).isEqualTo("SUCCEEDED")
  }

  @Test
  fun `reused Metric and Measurement do not require historical telemetry`() {
    val metricName = "measurementConsumers/mc-1/metrics/shared-metric"
    val context =
      reportTraceContext()
        .copy(
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "SUCCEEDED"),
          reusedMetricNames = setOf(metricName),
          reusedMeasurementNames = reportTraceContext().measurementNames.toSet(),
        )
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val spans =
      listOf(
        lifecycleSpan(
          "basic_report_creation",
          "xmm.basic_report.name",
          checkNotNull(context.basicReportName),
        ),
        lifecycleSpan("report_creation", "xmm.report.name", context.reportName),
        lifecycleSpan("metric_result_sync", "xmm.metric.name", metricName),
        lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName),
        lifecycleSpan(
          "noise_correction",
          "xmm.basic_report.name",
          checkNotNull(context.basicReportName),
        ),
        lifecycleSpan(
          "processed_result_writeback",
          "xmm.basic_report.name",
          checkNotNull(context.basicReportName),
        ),
      )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = spans,
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Collection completeness: COMPLETE")
    assertThat(output).contains("- Metric: $metricName [REUSED]")
    assertThat(output).contains("- Measurement: ${context.measurementNames.single()} [REUSED]")
    assertThat(output).contains("| metric_creation | $metricName | REUSED |")
    assertThat(output)
      .contains("| measurement_creation | ${context.measurementNames.single()} | REUSED |")
    assertThat(output).contains("| metric_result_sync | $metricName | SUCCEEDED |")
    assertThat(output).contains("| report_result_assembly | ${context.reportName} | SUCCEEDED |")
  }

  @Test
  fun `direct EDP refusal can have complete telemetry coverage`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "REPORT_CREATED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "FAILED"),
        )
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                state = "FAILED",
                requisitions =
                  listOf(
                    baseRoute.measurementRoutes
                      .single()
                      .requisitions
                      .single()
                      .copy(state = "REFUSED")
                  ),
              )
          )
      )
    val spans = refusalPropagationSpans(context, metricName, requisitionName)
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.single { it.name == "kingdom_requisition_refusal_acceptance" }.status)
      .isEqualTo("REFUSED")
    assertThat(coverage.single { it.name == "kingdom_requisition_result_acceptance" }.status)
      .isEqualTo("SKIPPED_AFTER_REFUSAL")
    assertThat(coverage.single { it.name == "requisition_dispatch" }.status)
      .isEqualTo("NOT_APPLICABLE")
    assertThat(coverage.single { it.name == "noise_correction" }.status)
      .isEqualTo("SKIPPED_AFTER_REFUSAL")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `EDPA refusal skips unstarted dispatch and fulfillment stages`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "REPORT_CREATED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "FAILED"),
        )
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                state = "FAILED",
                requisitions =
                  listOf(
                    baseRoute.measurementRoutes
                      .single()
                      .requisitions
                      .single()
                      .copy(state = "REFUSED")
                  ),
              )
          )
      )
    val alreadyTerminal =
      lifecycleSpan(
          "results_fulfillment",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to "group-1"),
        )
        .copy(
          startTime = NOW.plusSeconds(2),
          endTime = NOW.plusSeconds(3),
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "results_fulfillment",
              "xmm.outcome" to "already_completed",
              "xmm.requisition.name" to requisitionName,
              "xmm.requisition.state" to "REFUSED",
              "xmm.edpa.group_id" to "group-1",
            ),
        )
    val spans =
      refusalPropagationSpans(context, metricName, requisitionName) +
        refusalOriginSpan(
          requisitionName,
          ReportTraceAttributes.REQUISITION_FETCHER_REFUSAL_ORIGIN,
        ) +
        alreadyTerminal
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(
        coverage
          .filter {
            it.name in setOf("requisition_dispatch", "work_item_processing", "results_fulfillment")
          }
          .map { it.status }
      )
      .containsExactly("SKIPPED_AFTER_REFUSAL", "SKIPPED_AFTER_REFUSAL", "SKIPPED_AFTER_REFUSAL")
    assertThat(coverage.single { it.name == "requisition_refusal" }.status).isEqualTo("REFUSED")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `accepted Requisition refusal skips report assembly when it was not reached`() {
    val context = reportTraceContext().copy(basicReportState = "REPORT_CREATED")
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                state = "FAILED",
                requisitions =
                  listOf(
                    baseRoute.measurementRoutes
                      .single()
                      .requisitions
                      .single()
                      .copy(state = "REFUSED")
                  ),
              )
          )
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, emptyList(), emptyList())

    assertThat(coverage.single { it.name == "report_result_assembly" }.status)
      .isEqualTo("SKIPPED_AFTER_REFUSAL")
  }

  @Test
  fun `lifecycleCoverage preserves failed RequisitionFetcher attempt when refusal was accepted`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "REPORT_CREATED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "FAILED"),
        )
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.EDPA,
        )
        .withRequisitionState("REFUSED", measurementState = "FAILED")
    val failedRefusal =
      failedLifecycleSpan(
        "requisition_refusal",
        mapOf(
          "xmm.requisition.name" to requisitionName,
          ReportTraceAttributes.REFUSAL_ORIGIN_STRING to
            ReportTraceAttributes.REQUISITION_FETCHER_REFUSAL_ORIGIN,
        ),
      )
    val spans = refusalPropagationSpans(context, metricName, requisitionName) + failedRefusal

    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(
        coverage
          .filter {
            it.name in setOf("requisition_dispatch", "work_item_processing", "results_fulfillment")
          }
          .map { it.status }
      )
      .containsExactly("SKIPPED_AFTER_REFUSAL", "SKIPPED_AFTER_REFUSAL", "SKIPPED_AFTER_REFUSAL")
    assertThat(coverage.single { it.name == "requisition_refusal" }.status).isEqualTo("FAILED")
    assertThat(coverage.single { it.name == "kingdom_requisition_refusal_acceptance" }.status)
      .isEqualTo("REFUSED")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `EDPA refusal with unknown origin leaves processing stages unknown`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "REPORT_CREATED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "FAILED"),
        )
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.EDPA,
        )
        .withRequisitionState("REFUSED", measurementState = "FAILED")
    val spans = refusalPropagationSpans(context, metricName, requisitionName)

    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(
        coverage
          .filter {
            it.name in
              setOf(
                "requisition_refusal",
                "requisition_dispatch",
                "work_item_processing",
                "results_fulfillment",
              )
          }
          .map { it.status }
      )
      .containsExactly("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `ResultsFulfiller refusal requires dispatch and WorkItem evidence`() {
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val context = reportTraceContext().copy(basicReportState = "REPORT_CREATED")
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.EDPA,
        )
        .withRequisitionState("REFUSED", measurementState = "FAILED")
    val workItemName = "workItems/work-item-1"
    val alreadyTerminal =
      lifecycleSpan(
          "results_fulfillment",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to "group-1"),
        )
        .copy(
          startTime = NOW.plusSeconds(2),
          endTime = NOW.plusSeconds(3),
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "results_fulfillment",
              "xmm.outcome" to "already_completed",
              "xmm.requisition.name" to requisitionName,
              "xmm.requisition.state" to "REFUSED",
              "xmm.edpa.group_id" to "group-1",
            ),
        )
    val spans =
      listOf(
        lifecycleSpan(
          "requisition_dispatch",
          mapOf(
            "xmm.requisition.name" to requisitionName,
            "xmm.edpa.group_id" to "group-1",
            "xmm.work_item.name" to workItemName,
          ),
        ),
        lifecycleSpan(
          "work_item_processing",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.work_item.name" to workItemName),
        ),
        refusalOriginSpan(requisitionName, ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN),
        lifecycleSpan(
            "results_fulfillment",
            mapOf(
              "xmm.requisition.name" to requisitionName,
              "xmm.edpa.group_id" to "group-1",
              ReportTraceAttributes.REFUSAL_ORIGIN_STRING to
                ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN,
            ),
          )
          .copy(
            attributes =
              mapOf(
                "xmm.lifecycle.stage" to "results_fulfillment",
                "xmm.outcome" to "refused",
                "xmm.requisition.name" to requisitionName,
                "xmm.edpa.group_id" to "group-1",
                ReportTraceAttributes.REFUSAL_ORIGIN_STRING to
                  ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN,
              )
          ),
        refusalAcceptanceSpan(requisitionName, NOW.plusSeconds(1)),
        alreadyTerminal,
      )

    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.single { it.name == "requisition_dispatch" }.status).isEqualTo("SUCCEEDED")
    assertThat(coverage.single { it.name == "work_item_processing" }.status).isEqualTo("SUCCEEDED")
    assertThat(coverage.single { it.name == "results_fulfillment" }.status).isEqualTo("REFUSED")
    assertThat(coverage.single { it.name == "requisition_refusal" }.status).isEqualTo("REFUSED")
    assertThat(coverage.single { it.name == "kingdom_requisition_refusal_acceptance" }.status)
      .isEqualTo("REFUSED")
  }

  @Test
  fun `failed ResultsFulfiller refusal RPC does not make execution refused`() {
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val context = reportTraceContext().copy(basicReportState = "REPORT_CREATED")
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.EDPA,
        )
        .withRequisitionState("UNFULFILLED", measurementState = "PENDING")
    val resultRefusal =
      refusalOriginSpan(requisitionName, ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN)
        .copy(
          name = "results_fulfillment",
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "results_fulfillment",
              "xmm.outcome" to "refused",
              "xmm.requisition.name" to requisitionName,
              "xmm.edpa.group_id" to "group-1",
              ReportTraceAttributes.REFUSAL_ORIGIN_STRING to
                ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN,
            ),
        )
    val failedRefusalRpc =
      failedLifecycleSpan(
        "requisition_refusal",
        mapOf(
          "xmm.requisition.name" to requisitionName,
          ReportTraceAttributes.REFUSAL_ORIGIN_STRING to
            ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN,
        ),
      )

    val outcome =
      ReportTraceOutput.executionOutcome(
        context,
        routeResolution,
        listOf(resultRefusal, failedRefusalRpc),
        emptyList(),
      )

    assertThat(outcome).isEqualTo(ReportTraceExecutionOutcome.IN_PROGRESS)
  }

  @Test
  fun `failed direct Measurement skips result work that was not reached`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "FAILED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "FAILED"),
        )
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                state = "FAILED",
                requisitions =
                  listOf(
                    baseRoute.measurementRoutes
                      .single()
                      .requisitions
                      .single()
                      .copy(state = "UNFULFILLED")
                  ),
              )
          )
      )
    val spans =
      failurePropagationSpans(context, metricName, requisitionName) +
        failedLifecycleSpan(
          "kingdom_measurement_sync",
          mapOf("xmm.measurement.name" to context.measurementNames.single()),
        )
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.single { it.name == "kingdom_requisition_result_acceptance" }.status)
      .isEqualTo("SKIPPED_AFTER_FAILURE")
    assertThat(coverage.single { it.name == "duchy_computation" }.status)
      .isEqualTo("NOT_APPLICABLE")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `failed MPC Measurement keeps Duchy failure evidence required`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "FAILED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "FAILED"),
        )
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                state = "FAILED",
                requisitions =
                  listOf(
                    baseRoute.measurementRoutes
                      .single()
                      .requisitions
                      .single()
                      .copy(state = "FULFILLED")
                  ),
              )
          )
      )
    val measurementName = context.measurementNames.single()
    val computationName = "computations/computation-1"
    val duchySpans =
      routeResolution.measurementRoutes.single().duchyIds.flatMap { duchyId ->
        listOf(
          lifecycleSpan(
            "duchy_computation",
            mapOf(
              "xmm.measurement.name" to measurementName,
              "xmm.computation.name" to computationName,
              "xmm.duchy.id" to duchyId,
            ),
          ),
          failedLifecycleSpan(
            "duchy_stage_attempt",
            mapOf(
              "xmm.measurement.name" to measurementName,
              "xmm.computation.name" to computationName,
              "xmm.duchy.id" to duchyId,
            ),
          ),
        )
      }
    val spans =
      failurePropagationSpans(context, metricName, requisitionName) +
        failedLifecycleSpan(
          "kingdom_measurement_sync",
          mapOf("xmm.measurement.name" to measurementName),
        ) +
        lifecycleSpan(
          "duchy_requisition_acceptance",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.duchy.id" to "worker1"),
        ) +
        lifecycleSpan(
          "duchy_requisition_kingdom_fulfillment",
          mapOf("xmm.requisition.name" to requisitionName, "xmm.duchy.id" to "worker1"),
        ) +
        duchySpans
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.filter { it.name == "duchy_stage_attempt" }.map { it.status })
      .containsExactly("FAILED", "FAILED")
    assertThat(coverage.single { it.name == "kingdom_computation_result_acceptance" }.status)
      .isEqualTo("SKIPPED_AFTER_FAILURE")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `Report failure skips post-processing without reducing collection completeness`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "FAILED",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "SUCCEEDED"),
        )
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val spans =
      successfulDirectUpstreamSpans(context, metricName, requisitionName) +
        failedLifecycleSpan(
          "report_result_assembly",
          mapOf("xmm.report.name" to context.reportName, "xmm.report.state" to "FAILED"),
        )
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.single { it.name == "report_result_assembly" }.status).isEqualTo("FAILED")
    assertThat(coverage.single { it.name == "noise_correction" }.status)
      .isEqualTo("SKIPPED_AFTER_FAILURE")
    assertThat(coverage.single { it.name == "processed_result_writeback" }.status)
      .isEqualTo("SKIPPED_AFTER_FAILURE")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `BasicReport failure before Report creation skips the unresolved downstream graph`() {
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "FAILED",
          reportName = "(not created)",
          metricNames = emptyList(),
          measurementNames = emptyList(),
        )
    val basicReportFailure =
      failedLifecycleSpan(
        "basic_report_creation",
        mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
      )
    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        ReportTraceRouteResolution.unresolved(
          emptyList(),
          ReportTraceTopology.notSupplied(),
          "NO_INPUT",
          "No Measurement was created",
        ),
        listOf(basicReportFailure),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "basic_report_creation" }.status).isEqualTo("FAILED")
    assertThat(coverage.single { it.name == "report_creation" }.status)
      .isEqualTo("SKIPPED_AFTER_FAILURE")
    assertThat(
        coverage.filter { it.resource.startsWith("(unresolved") }.map { it.status }.distinct()
      )
      .containsExactly("SKIPPED_AFTER_FAILURE")
    assertThat(
        ReportTraceOutput.artifactStatus(
          listOf(basicReportFailure),
          emptyList(),
          emptyList(),
          coverage,
        )
      )
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
  }

  @Test
  fun `successful BasicReport remains partial without processed result writeback`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(metricNames = listOf(metricName), metricStates = mapOf(metricName to "SUCCEEDED"))
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val spans =
      successfulDirectUpstreamSpans(context, metricName, requisitionName) +
        lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName) +
        lifecycleSpan(
          "noise_correction",
          "xmm.basic_report.name",
          checkNotNull(context.basicReportName),
        )
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.single { it.name == "processed_result_writeback" }.status)
      .isEqualTo("MISSING")
    assertThat(coverage.single { it.name == "basic_report_available" }.status)
      .isEqualTo("SUCCEEDED")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `nonterminal BasicReport requires final writeback and availability`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(
          basicReportState = "UNPROCESSED_RESULTS_READY",
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "SUCCEEDED"),
        )
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val spans =
      successfulDirectUpstreamSpans(context, metricName, requisitionName) +
        lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName) +
        lifecycleSpan(
          "noise_correction",
          "xmm.basic_report.name",
          checkNotNull(context.basicReportName),
        )

    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(coverage.single { it.name == "processed_result_writeback" }.status)
      .isEqualTo("MISSING")
    assertThat(coverage.single { it.name == "basic_report_available" }.status).isEqualTo("MISSING")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `diagnostic requisition lookup does not satisfy requisition availability`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val diagnosticLookup =
      traceSpan("wfa.measurement.api.v2alpha.Requisitions/ListRequisitions", NOW)
        .copy(
          attributes =
            mapOf(
              "xmm.measurement.name" to context.measurementNames.single(),
              "xmm.requisition.name" to requisitionName,
            )
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(diagnosticLookup),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "requisition_available" }.status).isEqualTo("MISSING")
  }

  @Test
  fun `durable computation success without accepted telemetry remains unknown`() {
    val context = reportTraceContext()
    val measurementName = context.measurementNames.single()
    val computationName = "computations/computation-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val duchyEvidence =
      lifecycleSpan(
        "duchy_computation",
        mapOf(
          "xmm.measurement.name" to measurementName,
          "xmm.computation.name" to computationName,
          "xmm.duchy.id" to "aggregator",
        ),
      )
    val failedAcceptance =
      lifecycleSpan(
          "kingdom_computation_result_acceptance",
          mapOf("xmm.computation.name" to computationName),
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "kingdom_computation_result_acceptance",
              "xmm.outcome" to "failed",
              "xmm.computation.name" to computationName,
            )
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(duchyEvidence, failedAcceptance),
        emptyList(),
      )

    val acceptance = coverage.single { it.name == "kingdom_computation_result_acceptance" }
    assertThat(acceptance.resource).isEqualTo(measurementName)
    assertThat(acceptance.status).isEqualTo("UNKNOWN")
    assertThat(acceptance.evidence).contains("no matching accepted telemetry")
    assertThat(acceptance.evidence).contains("Measurement correlated by computation")
  }

  @Test
  fun `durable direct fulfillment success survives later rejected duplicate`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val accepted =
      lifecycleSpan(
          "kingdom_requisition_result_acceptance",
          "xmm.requisition.name",
          requisitionName,
        )
        .copy(startTime = NOW)
    val rejectedDuplicate =
      failedLifecycleSpan(
          "kingdom_requisition_result_acceptance",
          mapOf("xmm.requisition.name" to requisitionName),
        )
        .copy(startTime = NOW.plusSeconds(1))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(accepted, rejectedDuplicate),
        emptyList(),
      )

    val acceptance = coverage.single { it.name == "kingdom_requisition_result_acceptance" }
    assertThat(acceptance.status).isEqualTo("SUCCEEDED")
    assertThat(acceptance.evidence).contains("span kingdom_requisition_result_acceptance")
  }

  @Test
  fun `durable direct fulfillment without accepted telemetry remains unknown`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val rejectedAttempt =
      failedLifecycleSpan(
        "kingdom_requisition_result_acceptance",
        mapOf("xmm.requisition.name" to requisitionName),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(rejectedAttempt),
        emptyList(),
      )

    val acceptance = coverage.single { it.name == "kingdom_requisition_result_acceptance" }
    assertThat(acceptance.status).isEqualTo("UNKNOWN")
    assertThat(acceptance.evidence).contains("no matching accepted telemetry")
  }

  @Test
  fun `durable direct fulfillment uses successful retry after failed attempt`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val failedAttempt =
      failedLifecycleSpan(
          "kingdom_requisition_result_acceptance",
          mapOf("xmm.requisition.name" to requisitionName),
        )
        .copy(startTime = NOW)
    val successfulRetry =
      lifecycleSpan(
          "kingdom_requisition_result_acceptance",
          "xmm.requisition.name",
          requisitionName,
        )
        .copy(startTime = NOW.plusSeconds(1))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(failedAttempt, successfulRetry),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "kingdom_requisition_result_acceptance" }.status)
      .isEqualTo("SUCCEEDED")
  }

  @Test
  fun `durable computation success survives later rejected duplicate`() {
    val context = reportTraceContext()
    val measurementName = context.measurementNames.single()
    val computationName = "computations/computation-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val identity =
      mapOf("xmm.measurement.name" to measurementName, "xmm.computation.name" to computationName)
    val accepted =
      lifecycleSpan("kingdom_computation_result_acceptance", identity).copy(startTime = NOW)
    val rejectedDuplicate =
      failedLifecycleSpan("kingdom_computation_result_acceptance", identity)
        .copy(startTime = NOW.plusSeconds(1))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(accepted, rejectedDuplicate),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "kingdom_computation_result_acceptance" }.status)
      .isEqualTo("SUCCEEDED")
  }

  @Test
  fun `durable computation uses successful retry after failed attempt`() {
    val context = reportTraceContext()
    val measurementName = context.measurementNames.single()
    val computationName = "computations/computation-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val identity =
      mapOf("xmm.measurement.name" to measurementName, "xmm.computation.name" to computationName)
    val failedAttempt =
      failedLifecycleSpan("kingdom_computation_result_acceptance", identity).copy(startTime = NOW)
    val successfulRetry =
      lifecycleSpan("kingdom_computation_result_acceptance", identity)
        .copy(startTime = NOW.plusSeconds(1))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(failedAttempt, successfulRetry),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "kingdom_computation_result_acceptance" }.status)
      .isEqualTo("SUCCEEDED")
  }

  @Test
  fun `durable refusal success survives later rejected duplicate`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.DIRECT_EDP,
        )
        .withRequisitionState("REFUSED", measurementState = "FAILED")
    val accepted = refusalAcceptanceSpan(requisitionName, NOW)
    val rejectedDuplicate =
      failedLifecycleSpan(
          "kingdom_requisition_refusal_acceptance",
          mapOf("xmm.requisition.name" to requisitionName),
        )
        .copy(startTime = NOW.plusSeconds(1))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(accepted, rejectedDuplicate),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "kingdom_requisition_refusal_acceptance" }.status)
      .isEqualTo("REFUSED")
  }

  @Test
  fun `durable refusal uses successful retry after failed attempt`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.DIRECT_EDP,
        )
        .withRequisitionState("REFUSED", measurementState = "FAILED")
    val failedAttempt =
      failedLifecycleSpan(
          "kingdom_requisition_refusal_acceptance",
          mapOf("xmm.requisition.name" to requisitionName),
        )
        .copy(startTime = NOW)
    val successfulRetry = refusalAcceptanceSpan(requisitionName, NOW.plusSeconds(1))

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(failedAttempt, successfulRetry),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "kingdom_requisition_refusal_acceptance" }.status)
      .isEqualTo("REFUSED")
  }

  @Test
  fun `EDPA evidence is unexpected for direct EDP Requisition`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val dispatch =
      lifecycleSpan(
        "requisition_dispatch",
        mapOf(
          "xmm.requisition.name" to requisitionName,
          "xmm.edpa.group_id" to "group-1",
          "xmm.work_item.name" to "workItems/results-fulfiller-group-1",
        ),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, listOf(dispatch), emptyList())

    assertThat(coverage.single { it.name == "requisition_dispatch" }.status).isEqualTo("UNEXPECTED")
    assertThat(
        ReportTraceOutput.artifactStatus(listOf(dispatch), emptyList(), emptyList(), coverage)
      )
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `Duchy evidence is unexpected for direct Measurement`() {
    val context = reportTraceContext()
    val measurementName = context.measurementNames.single()
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val duchyEvidence =
      lifecycleSpan(
        "duchy_computation",
        mapOf("xmm.measurement.name" to measurementName, "xmm.duchy.id" to "aggregator"),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(duchyEvidence),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "duchy_computation" }.status).isEqualTo("UNEXPECTED")
  }

  @Test
  fun `EDPA lifecycle is partial when its WorkItem evidence is missing`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.MPC,
          requisitionName,
          ReportTraceRequisitionRouteKind.EDPA,
        )
        .withRequisitionState("UNFULFILLED", measurementState = "PENDING")
    val dispatch =
      lifecycleSpan(
        "requisition_dispatch",
        mapOf(
          "xmm.requisition.name" to requisitionName,
          "xmm.edpa.group_id" to "group-1",
          "xmm.work_item.name" to "workItems/results-fulfiller-group-1",
        ),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, listOf(dispatch), emptyList())

    assertThat(
        coverage
          .single { it.name == "work_item_processing" && it.resource == requisitionName }
          .status
      )
      .isEqualTo("MISSING")
    assertThat(
        ReportTraceOutput.artifactStatus(listOf(dispatch), emptyList(), emptyList(), coverage)
      )
      .isEqualTo(ReportTraceArtifactStatus.PARTIAL)
  }

  @Test
  fun `dispatch evidence is evaluated independently for each Requisition`() {
    val context = reportTraceContext()
    val requisition1 = "dataProviders/edpa/requisitions/requisition-1"
    val requisition2 = "dataProviders/edpa/requisitions/requisition-2"
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        requisition1,
        ReportTraceRequisitionRouteKind.EDPA,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                requisitions =
                  baseRoute.measurementRoutes.single().requisitions +
                    ReportTraceRequisitionRoute(
                      name = requisition2,
                      state = "FULFILLED",
                      dataProvider = "dataProviders/edpa",
                      route = ReportTraceRequisitionRouteKind.EDPA,
                    )
              )
          )
      )
    val dispatch =
      lifecycleSpan(
        "requisition_dispatch",
        mapOf(
          "xmm.requisition.name" to requisition1,
          "xmm.edpa.group_id" to "group-1",
          "xmm.work_item.name" to "workItems/results-fulfiller-group-1",
        ),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, listOf(dispatch), emptyList())

    assertThat(
        coverage.single { it.name == "requisition_dispatch" && it.resource == requisition1 }.status
      )
      .isEqualTo("SUCCEEDED")
    assertThat(
        coverage.single { it.name == "requisition_dispatch" && it.resource == requisition2 }.status
      )
      .isEqualTo("MISSING")
  }

  @Test
  fun `one Duchy participant does not satisfy another expected participant`() {
    val context = reportTraceContext()
    val measurementName = context.measurementNames.single()
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.MPC,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val aggregatorEvidence =
      lifecycleSpan(
        "duchy_computation",
        mapOf("xmm.measurement.name" to measurementName, "xmm.duchy.id" to "aggregator"),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(aggregatorEvidence),
        emptyList(),
      )

    assertThat(
        coverage
          .single { it.name == "duchy_computation" && it.resource.endsWith("duchy aggregator") }
          .status
      )
      .isEqualTo("SUCCEEDED")
    assertThat(
        coverage
          .single { it.name == "duchy_computation" && it.resource.endsWith("duchy worker1") }
          .status
      )
      .isEqualTo("MISSING")
  }

  @Test
  fun `MPC operation is unknown when Duchy evidence lacks Measurement identity`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.MPC,
          requisitionName,
          ReportTraceRequisitionRouteKind.DIRECT_EDP,
        )
        .withRequisitionState("FULFILLED", measurementState = "COMPUTING")
    val unscopedDuchySpan =
      traceSpan("duchy", NOW)
        .copy(
          attributes =
            mapOf("xmm.lifecycle.stage" to "duchy_computation", "xmm.outcome" to "succeeded")
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(unscopedDuchySpan),
        emptyList(),
      )

    assertThat(coverage.filter { it.name == "duchy_computation" }.map { it.status })
      .containsExactly("UNKNOWN", "UNKNOWN")
    assertThat(coverage.filter { it.name == "duchy_computation" }.map { it.evidence })
      .containsExactly(
        "Stage evidence did not identify this resource",
        "Stage evidence did not identify this resource",
      )
  }

  @Test
  fun `direct operation is unknown when Duchy evidence lacks Measurement identity`() {
    val context = reportTraceContext()
    val routeResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        "dataProviders/direct/requisitions/requisition-1",
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val unscopedDuchySpan =
      traceSpan("duchy", NOW)
        .copy(
          attributes =
            mapOf("xmm.lifecycle.stage" to "duchy_computation", "xmm.outcome" to "succeeded")
        )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        listOf(unscopedDuchySpan),
        emptyList(),
      )

    assertThat(coverage.single { it.name == "duchy_computation" }.status).isEqualTo("UNKNOWN")
  }

  @Test
  fun `render marks route-specific stages not applicable for direct EDP path`() {
    val context = reportTraceContext()
    val routeResolution =
      ReportTraceRouteResolution(
        status = "SUCCESS",
        note = "",
        topology =
          ReportTraceTopology(
            routes =
              mapOf(
                "dataProviders/edpa" to ReportTraceRequisitionRouteKind.EDPA,
                "dataProviders/direct" to ReportTraceRequisitionRouteKind.DIRECT_EDP,
              ),
            provenance = "operator-provided --topology-config-file (2 DataProvider routes)",
          ),
        measurementRoutes =
          listOf(
            ReportTraceMeasurementRoute(
              name = "measurementConsumers/mc-1/measurements/measurement-1",
              state = "SUCCEEDED",
              protocol = "DIRECT",
              route = ReportTraceMeasurementRouteKind.DIRECT,
              duchyIds = emptyList(),
              duchyParticipantsResolved = true,
              requisitions =
                listOf(
                  ReportTraceRequisitionRoute(
                    name = "dataProviders/direct/requisitions/requisition-1",
                    state = "FULFILLED",
                    dataProvider = "dataProviders/direct",
                    route = ReportTraceRequisitionRouteKind.DIRECT_EDP,
                  )
                ),
              requisitionsResolved = true,
            )
          ),
        warnings = emptyList(),
      )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = emptyList(),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output)
      .contains("| duchy_computation | ${context.measurementNames.single()} | NOT_APPLICABLE |")
    assertThat(output)
      .contains(
        "| requisition_dispatch | dataProviders/direct/requisitions/requisition-1 | " +
          "NOT_APPLICABLE |"
      )
    assertThat(output)
      .contains(
        "| results_fulfillment | dataProviders/direct/requisitions/requisition-1 | " +
          "NOT_APPLICABLE |"
      )
    assertThat(output).contains("| DIRECT | DIRECT |")
    assertThat(output).contains("| FULFILLED | dataProviders/direct | DIRECT_EDP |")
    assertThat(output).contains("- DataProvider: dataProviders/direct [DIRECT_EDP]")
    assertThat(output).doesNotContain("- DataProvider: dataProviders/edpa [EDPA]")
  }

  @Test
  fun `render promotes application errors and warning refusal details`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val errorMessage =
      "Failed to write report\njava.lang.IllegalStateException: database unavailable"
    val warningMessage =
      "Refusing Requisition $requisitionName\n" +
        "org.wfanet.measurement.dataprovider.UnfulfillableRequisitionException: " +
        "PopulationSpec is invalid\n" +
        "\tat example.Fulfiller.validate(Fulfiller.kt:10)\n" +
        "Caused by: org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException: " +
        "Not all population fields are set\n" +
        "  Population field Common.gender not set in subpopulations[0]"
    val logEntries =
      listOf(
        ReportTraceLogEntry("test", NOW, "reporting", "ERROR", null, errorMessage),
        ReportTraceLogEntry(
          "test",
          NOW.plusSeconds(1),
          "population-fulfiller",
          "WARNING",
          null,
          warningMessage,
        ),
      )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution =
          routeResolution(
            context,
            ReportTraceMeasurementRouteKind.DIRECT,
            requisitionName,
            ReportTraceRequisitionRouteKind.DIRECT_EDP,
          ),
        spans = emptyList(),
        logEntries = logEntries,
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output.indexOf("## Errors")).isLessThan(output.indexOf("## Resolved resource chain"))
    assertThat(output.indexOf("## Warnings"))
      .isLessThan(output.indexOf("## Resolved resource chain"))
    val diagnostics = output.substringBefore("## Resolved resource chain")
    assertThat(diagnostics).contains("Failed to write report java.lang.IllegalStateException")
    assertThat(diagnostics).contains("Refusing Requisition $requisitionName")
    assertThat(diagnostics)
      .contains("org.wfanet.measurement.dataprovider.UnfulfillableRequisitionException")
    assertThat(diagnostics).contains("PopulationSpecValidationException")
    assertThat(diagnostics).contains("Population field Common.gender not set")
    assertThat(diagnostics).doesNotContain("at example.Fulfiller.validate")
  }

  @Test
  fun `render includes Kingdom refusal details and distinguishes undiscovered telemetry`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val baseRouteResolution =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val measurementRoute = baseRouteResolution.measurementRoutes.single()
    val routeResolution =
      baseRouteResolution.copy(
        measurementRoutes =
          listOf(
            measurementRoute.copy(
              state = "FAILED",
              requisitions =
                listOf(
                  measurementRoute.requisitions
                    .single()
                    .copy(
                      state = "REFUSED",
                      refusalJustification = "SPEC_INVALID",
                      refusalMessage = "EventGroup is unsupported | check configuration",
                    )
                ),
            )
          )
      )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = emptyList(),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("- Requisitions: none discovered from telemetry")
    assertThat(output).contains("| Refusal justification | Refusal message |")
    assertThat(output)
      .contains("| DIRECT_EDP | SPEC_INVALID | EventGroup is unsupported \\| check configuration |")
  }

  @Test
  fun `Kingdom terminal child state overrides transitional BasicReport state`() {
    val context = reportTraceContext().copy(basicReportState = "REPORT_CREATED")
    val routeResolution =
      ReportTraceRouteResolution(
        status = "SUCCESS",
        note = "",
        topology = ReportTraceTopology.notSupplied(),
        measurementRoutes =
          listOf(
            ReportTraceMeasurementRoute(
              name = context.measurementNames.single(),
              state = "FAILED",
              protocol = "DIRECT",
              route = ReportTraceMeasurementRouteKind.DIRECT,
              duchyIds = emptyList(),
              duchyParticipantsResolved = true,
              requisitions = emptyList(),
              requisitionsResolved = true,
            )
          ),
        warnings = emptyList(),
      )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = emptyList(),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Execution outcome: FAILED")
  }

  @Test
  fun `main includes Kingdom Requisitions in initial correlation set`() {
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val topologyConfigFile = temporaryFolder.newFile("report-trace-topology.textproto")
    topologyConfigFile.writeText(
      """
      data_provider_routes {
        data_provider: "dataProviders/edpa"
        route: EDPA
      }
      """
        .trimIndent()
    )
    var loggingCorrelationValues: Collection<String> = emptyList()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            loggingCorrelationValues = correlationValues
            emptyList()
          }
        },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver { key ->
            reportTraceContext().copy(basicReportName = key.toName())
          },
        routeResolverOverride =
          ReportTraceRouteResolver { measurementNames, topology ->
            assertThat(measurementNames)
              .containsExactly("measurementConsumers/mc-1/measurements/measurement-1")
            assertThat(topology.routes)
              .containsExactly("dataProviders/edpa", ReportTraceRequisitionRouteKind.EDPA)
            ReportTraceRouteResolution(
              status = "SUCCESS",
              note = "",
              topology = topology,
              measurementRoutes =
                listOf(
                  ReportTraceMeasurementRoute(
                    name = measurementNames.single(),
                    state = "SUCCEEDED",
                    protocol = "DIRECT",
                    route = ReportTraceMeasurementRouteKind.DIRECT,
                    duchyIds = emptyList(),
                    duchyParticipantsResolved = true,
                    requisitions =
                      listOf(
                        ReportTraceRequisitionRoute(
                          name = requisitionName,
                          state = "FULFILLED",
                          dataProvider = "dataProviders/edpa",
                          route = ReportTraceRequisitionRouteKind.EDPA,
                        )
                      ),
                    requisitionsResolved = true,
                  )
                ),
              warnings = emptyList(),
            )
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--topology-config-file=$topologyConfigFile",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(loggingCorrelationValues).contains(requisitionName)
  }

  @Test
  fun `main renders complete mixed direct and MPC lifecycle per child`() {
    val basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1"
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val directMetricName = "measurementConsumers/mc-1/metrics/direct-metric"
    val mpcMetricName = "measurementConsumers/mc-1/metrics/mpc-metric"
    val directMeasurementName = "measurementConsumers/mc-1/measurements/direct-measurement"
    val mpcMeasurementName = "measurementConsumers/mc-1/measurements/mpc-measurement"
    val directMeasurementRequisitionName =
      "dataProviders/direct/requisitions/direct-measurement-requisition"
    val mpcDirectRequisitionName = "dataProviders/direct/requisitions/mpc-direct-requisition"
    val edpaRequisitionName = "dataProviders/edpa/requisitions/edpa-requisition"
    val workItemName = "workItems/results-fulfiller-group-1"
    val groupId = "group-1"
    val computationName = "computations/mpc-computation"
    val duchyIds = listOf("aggregator", "worker1")
    val context =
      ReportTraceContext(
        basicReportName = basicReportName,
        basicReportState = "SUCCEEDED",
        reportName = reportName,
        metricNames = listOf(directMetricName, mpcMetricName),
        metricStates = mapOf(directMetricName to "SUCCEEDED", mpcMetricName to "SUCCEEDED"),
        reusedMetricNames = emptySet(),
        unresolvedMetricRequestIds = emptyList(),
        measurementNames = listOf(directMeasurementName, mpcMeasurementName),
        reusedMeasurementNames = emptySet(),
        unresolvedMeasurementRequestIds = emptyList(),
        reportResolvedByRequestId = false,
        telemetryRecoveredMeasurementNames = emptyMap(),
        createTime = NOW,
      )
    val routeResolution =
      ReportTraceRouteResolution(
        status = "SUCCESS",
        note = "",
        topology = ReportTraceTopology.notSupplied(),
        measurementRoutes =
          listOf(
            ReportTraceMeasurementRoute(
              name = directMeasurementName,
              state = "SUCCEEDED",
              protocol = "DIRECT",
              route = ReportTraceMeasurementRouteKind.DIRECT,
              duchyIds = emptyList(),
              duchyParticipantsResolved = true,
              requisitions =
                listOf(
                  ReportTraceRequisitionRoute(
                    name = directMeasurementRequisitionName,
                    state = "FULFILLED",
                    dataProvider = "dataProviders/direct",
                    route = ReportTraceRequisitionRouteKind.DIRECT_EDP,
                  )
                ),
              requisitionsResolved = true,
            ),
            ReportTraceMeasurementRoute(
              name = mpcMeasurementName,
              state = "SUCCEEDED",
              protocol = "HONEST_MAJORITY_SHARE_SHUFFLE",
              route = ReportTraceMeasurementRouteKind.MPC,
              duchyIds = duchyIds,
              duchyParticipantsResolved = true,
              requisitions =
                listOf(
                  ReportTraceRequisitionRoute(
                    name = mpcDirectRequisitionName,
                    state = "FULFILLED",
                    dataProvider = "dataProviders/direct",
                    route = ReportTraceRequisitionRouteKind.DIRECT_EDP,
                  ),
                  ReportTraceRequisitionRoute(
                    name = edpaRequisitionName,
                    state = "FULFILLED",
                    dataProvider = "dataProviders/edpa",
                    route = ReportTraceRequisitionRouteKind.EDPA,
                  ),
                ),
              requisitionsResolved = true,
            ),
          ),
        warnings = emptyList(),
      )
    val commonStages =
      listOf(
        "basic_report_creation" to mapOf("xmm.basic_report.name" to basicReportName),
        "report_creation" to mapOf("xmm.report.name" to reportName),
        "metric_creation" to mapOf("xmm.metric.name" to directMetricName),
        "metric_creation" to mapOf("xmm.metric.name" to mpcMetricName),
        "metric_result_sync" to mapOf("xmm.metric.name" to directMetricName),
        "metric_result_sync" to mapOf("xmm.metric.name" to mpcMetricName),
        "measurement_creation" to mapOf("xmm.measurement.name" to directMeasurementName),
        "measurement_linkage" to mapOf("xmm.measurement.name" to directMeasurementName),
        "kingdom_measurement_sync" to mapOf("xmm.measurement.name" to directMeasurementName),
        "measurement_creation" to mapOf("xmm.measurement.name" to mpcMeasurementName),
        "measurement_linkage" to mapOf("xmm.measurement.name" to mpcMeasurementName),
        "kingdom_measurement_sync" to mapOf("xmm.measurement.name" to mpcMeasurementName),
        "requisition_available" to
          mapOf("xmm.requisition.name" to directMeasurementRequisitionName),
        "kingdom_requisition_result_acceptance" to
          mapOf("xmm.requisition.name" to directMeasurementRequisitionName),
        "requisition_available" to mapOf("xmm.requisition.name" to mpcDirectRequisitionName),
        "duchy_requisition_acceptance" to
          mapOf("xmm.requisition.name" to mpcDirectRequisitionName, "xmm.duchy.id" to "worker1"),
        "duchy_requisition_kingdom_fulfillment" to
          mapOf("xmm.requisition.name" to mpcDirectRequisitionName, "xmm.duchy.id" to "worker1"),
        "requisition_available" to mapOf("xmm.requisition.name" to edpaRequisitionName),
        "requisition_dispatch" to
          mapOf(
            "xmm.requisition.name" to edpaRequisitionName,
            "xmm.edpa.group_id" to groupId,
            "xmm.work_item.name" to workItemName,
          ),
        "work_item_processing" to
          mapOf("xmm.work_item.name" to workItemName.removePrefix("workItems/")),
        "results_fulfillment" to
          mapOf("xmm.requisition.name" to edpaRequisitionName, "xmm.edpa.group_id" to groupId),
        "results_fulfillment" to mapOf("xmm.edpa.group_id" to groupId),
        "duchy_requisition_acceptance" to
          mapOf("xmm.requisition.name" to edpaRequisitionName, "xmm.duchy.id" to "worker1"),
        "duchy_requisition_kingdom_fulfillment" to
          mapOf("xmm.requisition.name" to edpaRequisitionName, "xmm.duchy.id" to "worker1"),
        "kingdom_computation_result_acceptance" to
          mapOf(
            "xmm.measurement.name" to mpcMeasurementName,
            "xmm.computation.name" to computationName,
          ),
        "report_result_assembly" to mapOf("xmm.report.name" to reportName),
        "noise_correction" to mapOf("xmm.basic_report.name" to basicReportName),
        "processed_result_writeback" to mapOf("xmm.basic_report.name" to basicReportName),
      )
    val duchyStages =
      duchyIds.flatMap { duchyId ->
        listOf("duchy_computation", "duchy_stage_attempt").map { stage ->
          stage to
            mapOf(
              "xmm.measurement.name" to mpcMeasurementName,
              "xmm.computation.name" to computationName,
              "xmm.duchy.id" to duchyId,
            )
        }
      }
    val spans =
      (commonStages + duchyStages).mapIndexed { index, (stage, attributes) ->
        lifecycleSpan(stage, attributes)
          .copy(
            traceId = "mixed-trace",
            spanId = "mixed-span-$index",
            startTime = NOW.plusSeconds(index.toLong()),
            endTime = NOW.plusSeconds(index.toLong() + 1),
          )
      }
    val outputDirectory = temporaryFolder.newFolder("mixed-routes").toPath()
    val topologyConfigFile = temporaryFolder.newFile("mixed-routes-topology.textproto")
    topologyConfigFile.writeText(
      """
      data_provider_routes {
        data_provider: "dataProviders/direct"
        route: DIRECT_EDP
      }
      data_provider_routes {
        data_provider: "dataProviders/edpa"
        route: EDPA
      }
      """
        .trimIndent()
    )
    var spansReturned = false
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, _, _, _, _ ->
            if (spansReturned) {
              emptyList()
            } else {
              spansReturned = true
              spans
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        routeResolverOverride =
          ReportTraceRouteResolver { measurementNames, topology ->
            assertThat(measurementNames)
              .containsExactly(directMeasurementName, mpcMeasurementName)
              .inOrder()
            assertThat(topology.routes)
              .containsExactly(
                "dataProviders/direct",
                ReportTraceRequisitionRouteKind.DIRECT_EDP,
                "dataProviders/edpa",
                ReportTraceRequisitionRouteKind.EDPA,
              )
            routeResolution.copy(topology = topology)
          },
        clock = Clock.fixed(NOW.plusSeconds(60), ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--topology-config-file=$topologyConfigFile",
          "--output-dir=$outputDirectory",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    val artifact = outputDirectory.toFile().listFiles().single().readText()
    assertThat(artifact).contains("Collection completeness: COMPLETE")
    assertThat(artifact).contains("Execution outcome: SUCCEEDED")
    assertThat(artifact).contains("| $directMeasurementName | SUCCEEDED | DIRECT | DIRECT |")
    assertThat(artifact)
      .contains("| $mpcMeasurementName | SUCCEEDED | HONEST_MAJORITY_SHARE_SHUFFLE | MPC |")
    assertThat(artifact).contains("| duchy_computation | $directMeasurementName | NOT_APPLICABLE |")
    for (duchyId in duchyIds) {
      assertThat(artifact)
        .contains("| duchy_computation | $mpcMeasurementName @ duchy $duchyId | SUCCEEDED |")
      assertThat(artifact)
        .contains("| duchy_stage_attempt | $mpcMeasurementName @ duchy $duchyId | SUCCEEDED |")
    }
    assertThat(artifact)
      .contains("| kingdom_computation_result_acceptance | $mpcMeasurementName | SUCCEEDED |")
    assertThat(artifact)
      .contains(
        "| kingdom_computation_result_acceptance | $directMeasurementName | NOT_APPLICABLE |"
      )
    assertThat(artifact)
      .contains(
        "| kingdom_requisition_result_acceptance | $directMeasurementRequisitionName | " +
          "SUCCEEDED |"
      )
    assertThat(artifact)
      .contains("| kingdom_requisition_result_acceptance | $edpaRequisitionName | NOT_APPLICABLE |")
    assertThat(artifact)
      .contains(
        "| kingdom_requisition_result_acceptance | $mpcDirectRequisitionName | " +
          "NOT_APPLICABLE |"
      )
    for (stage in listOf("requisition_dispatch", "work_item_processing", "results_fulfillment")) {
      assertThat(artifact).contains("| $stage | $edpaRequisitionName | SUCCEEDED |")
      assertThat(artifact)
        .contains("| $stage | $directMeasurementRequisitionName | NOT_APPLICABLE |")
      assertThat(artifact).contains("| $stage | $mpcDirectRequisitionName | NOT_APPLICABLE |")
    }
    for (stage in listOf("duchy_requisition_acceptance", "duchy_requisition_kingdom_fulfillment")) {
      assertThat(artifact).contains("| $stage | $edpaRequisitionName | SUCCEEDED |")
      assertThat(artifact).contains("| $stage | $mpcDirectRequisitionName | SUCCEEDED |")
      assertThat(artifact)
        .contains("| $stage | $directMeasurementRequisitionName | NOT_APPLICABLE |")
    }

    val incompleteCoverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        spans.filterNot {
          it.attributes["xmm.lifecycle.stage"] == "results_fulfillment" ||
            (it.attributes["xmm.lifecycle.stage"] == "duchy_requisition_acceptance" &&
              it.attributes["xmm.requisition.name"] == mpcDirectRequisitionName) ||
            (it.attributes["xmm.lifecycle.stage"] == "measurement_linkage" &&
              it.attributes["xmm.measurement.name"] == mpcMeasurementName)
        },
        emptyList(),
      )
    assertThat(
        incompleteCoverage
          .single { it.name == "results_fulfillment" && it.resource == edpaRequisitionName }
          .status
      )
      .isEqualTo("MISSING")
    assertThat(
        incompleteCoverage
          .single {
            it.name == "results_fulfillment" && it.resource == directMeasurementRequisitionName
          }
          .status
      )
      .isEqualTo("NOT_APPLICABLE")
    assertThat(
        incompleteCoverage
          .single {
            it.name == "duchy_requisition_acceptance" && it.resource == mpcDirectRequisitionName
          }
          .status
      )
      .isEqualTo("MISSING")
    assertThat(
        incompleteCoverage
          .single {
            it.name == "duchy_requisition_acceptance" && it.resource == edpaRequisitionName
          }
          .status
      )
      .isEqualTo("SUCCEEDED")
    assertThat(
        incompleteCoverage
          .single { it.name == "measurement_linkage" && it.resource == mpcMeasurementName }
          .status
      )
      .isEqualTo("MISSING")
    assertThat(
        incompleteCoverage
          .single { it.name == "measurement_linkage" && it.resource == directMeasurementName }
          .status
      )
      .isEqualTo("SUCCEEDED")
  }

  @Test
  fun `main resolves Kingdom route after recovering Measurement name from telemetry`() {
    val requestId = "measurement-request-1"
    val measurementName = "measurementConsumers/mc-1/measurements/measurement-2"
    val outputDirectory = temporaryFolder.newFolder("recovered-route").toPath()
    val topologyConfigFile = temporaryFolder.newFile("recovered-route-topology.textproto")
    topologyConfigFile.writeText(
      """
      data_provider_routes {
        data_provider: "dataProviders/direct"
        route: DIRECT_EDP
      }
      """
        .trimIndent()
    )
    val routeInputs = mutableListOf<List<String>>()
    val spanCorrelationInputs = mutableListOf<Collection<String>>()
    val creationSpan =
      lifecycleSpan(
          "measurement_creation",
          mapOf(
            "xmm.measurement.request_id" to requestId,
            "xmm.measurement.name" to measurementName,
          ),
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "measurement_creation",
              "xmm.outcome" to "accepted",
              "xmm.measurement.request_id" to requestId,
              "xmm.measurement.name" to measurementName,
            )
        )
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, _, _, _, _ ->
            spanCorrelationInputs += correlationValues
            if (requestId in correlationValues) listOf(creationSpan) else emptyList()
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver { key ->
            reportTraceContext()
              .copy(
                basicReportName = key.toName(),
                measurementNames = emptyList(),
                unresolvedMeasurementRequestIds = listOf(requestId),
              )
          },
        routeResolverOverride =
          ReportTraceRouteResolver { measurementNames, topology ->
            routeInputs.add(measurementNames.toList())
            if (measurementNames.isEmpty()) {
              ReportTraceRouteResolution.unresolved(
                measurementNames = emptyList(),
                topology = topology,
                status = "PARTIAL",
                note = "Measurement not linked yet",
              )
            } else {
              ReportTraceRouteResolution(
                status = "SUCCESS",
                note = "",
                topology = topology,
                measurementRoutes =
                  listOf(
                    ReportTraceMeasurementRoute(
                      name = measurementName,
                      state = "PENDING",
                      protocol = "DIRECT",
                      route = ReportTraceMeasurementRouteKind.DIRECT,
                      duchyIds = emptyList(),
                      duchyParticipantsResolved = true,
                      requisitions = emptyList(),
                      requisitionsResolved = true,
                    )
                  ),
                warnings = emptyList(),
              )
            }
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--topology-config-file=$topologyConfigFile",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(routeInputs).containsExactly(emptyList<String>(), listOf(measurementName)).inOrder()
    assertThat(spanCorrelationInputs.any { requestId in it }).isTrue()
    val artifact = outputDirectory.toFile().listFiles().single().readText()
    assertThat(artifact).contains("$measurementName [TELEMETRY_RECOVERED from request $requestId]")
    assertThat(artifact).contains("Kingdom resolution: SUCCESS")
    assertThat(artifact).doesNotContain("Measurement requests do not have Kingdom Measurement IDs")
    assertThat(artifact).doesNotContain("No Kingdom Measurement names were resolved from Reporting")
    assertThat(artifact).doesNotContain("Measurement not linked yet")
  }

  @Test
  fun `lifecycleCoverage evaluates span emitted by ReportTracing`() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    val spanExporter = InMemorySpanExporter.create()
    val openTelemetry =
      OpenTelemetrySdk.builder()
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
    try {
      val context = reportTraceContext()
      val measurementName = context.measurementNames.single()
      ReportTracing.recordFailure(
        spanName = "reporting.measurement.create_failed",
        attributes =
          Attributes.builder()
            .put(ReportTraceAttributes.LIFECYCLE_STAGE, "measurement_creation")
            .put(ReportTraceAttributes.MEASUREMENT_NAME, measurementName)
            .build(),
        error = IllegalStateException("creation failed"),
      )
      val exportedSpan = spanExporter.finishedSpanItems.single()
      val collectedSpan =
        ReportTraceSpan(
          sourceProject = "test",
          traceId = exportedSpan.traceId,
          spanId = exportedSpan.spanId,
          parentSpanId = exportedSpan.parentSpanId,
          name = exportedSpan.name,
          service = "reporting",
          startTime = NOW,
          endTime = NOW.plusSeconds(1),
          attributes =
            listOf(
                ReportTraceAttributes.LIFECYCLE_STAGE,
                ReportTraceAttributes.MEASUREMENT_NAME,
                ReportTraceAttributes.OUTCOME,
                ReportTraceAttributes.ERROR_TYPE,
              )
              .mapNotNull { key -> exportedSpan.attributes.get(key)?.let { key.key to it } }
              .toMap(),
        )
      val routeResolution =
        routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          "dataProviders/direct/requisitions/requisition-1",
          ReportTraceRequisitionRouteKind.DIRECT_EDP,
        )

      val coverage =
        ReportTraceOutput.lifecycleCoverage(
          context,
          routeResolution,
          listOf(collectedSpan),
          emptyList(),
        )

      val stage =
        coverage.single { it.name == "measurement_creation" && it.resource == measurementName }
      assertThat(stage.status).isEqualTo("FAILED")
      assertThat(stage.evidence).contains("reporting.measurement.create_failed")
    } finally {
      openTelemetry.close()
      GlobalOpenTelemetry.resetForTest()
      Instrumentation.resetForTest()
    }
  }

  @Test
  fun `structured errors render for every direct EDPA lifecycle stage`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val context =
      reportTraceContext()
        .copy(metricNames = listOf(metricName), metricStates = mapOf(metricName to "RUNNING"))
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.DIRECT,
          requisitionName,
          ReportTraceRequisitionRouteKind.EDPA,
        )
        .withRequisitionState("UNFULFILLED", measurementState = "PENDING")
    val workItemName = "workItems/results-fulfiller-group-1"
    val stageAttributes =
      linkedMapOf(
        "basic_report_creation" to
          mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
        "basic_report_api_fetch" to
          mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
        "report_creation" to mapOf("xmm.report.name" to context.reportName),
        "metric_creation" to mapOf("xmm.metric.name" to metricName),
        "measurement_creation" to
          mapOf("xmm.measurement.name" to context.measurementNames.single()),
        "measurement_linkage" to mapOf("xmm.measurement.name" to context.measurementNames.single()),
        "requisition_available" to mapOf("xmm.requisition.name" to requisitionName),
        "requisition_dispatch" to
          mapOf(
            "xmm.requisition.name" to requisitionName,
            "xmm.edpa.group_id" to "group-1",
            "xmm.work_item.name" to workItemName,
          ),
        "work_item_processing" to
          mapOf("xmm.requisition.name" to requisitionName, "xmm.work_item.name" to workItemName),
        "results_fulfillment" to
          mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to "group-1"),
        "kingdom_requisition_result_acceptance" to mapOf("xmm.requisition.name" to requisitionName),
        "kingdom_measurement_sync" to
          mapOf("xmm.measurement.name" to context.measurementNames.single()),
        "metric_result_sync" to mapOf("xmm.metric.name" to metricName),
        "report_result_assembly" to mapOf("xmm.report.name" to context.reportName),
        "noise_correction" to
          mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
        "processed_result_writeback" to
          mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
      )
    val logEntries =
      stageAttributes.entries.mapIndexed { index, (stage, attributes) ->
        failedLifecycleLog(stage, attributes, index)
      }
    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, emptyList(), logEntries)
    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = emptyList(),
        logEntries = logEntries,
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    for (stage in stageAttributes.keys) {
      assertThat(coverage.filter { it.name == stage }.map { it.status }.distinct())
        .containsExactly("FAILED")
      assertThat(output).contains("xmm.lifecycle.stage=$stage")
    }
    assertThat(output).contains("xmm.error.type=TestFailure")
    assertThat(output).contains("xmm.error.code=grpc.UNAVAILABLE")
    assertThat(coverage.first { it.status == "FAILED" }.evidence)
      .contains("xmm.error.code=grpc.UNAVAILABLE")
    assertThat(output).contains("LOG ERROR")
  }

  @Test
  fun `Reporting lifecycle logs retain per-child coverage without spans`() {
    val metricNames =
      listOf(
        "measurementConsumers/mc-1/metrics/metric-1",
        "measurementConsumers/mc-1/metrics/metric-2",
      )
    val measurementNames =
      listOf(
        "measurementConsumers/mc-1/measurements/measurement-1",
        "measurementConsumers/mc-1/measurements/measurement-2",
      )
    val context =
      reportTraceContext().copy(metricNames = metricNames, measurementNames = measurementNames)
    val routeResolution =
      ReportTraceRouteResolution(
        status = "SUCCESS",
        note = "",
        topology =
          ReportTraceTopology(
            routes = emptyMap(),
            provenance = "operator-provided --topology-config-file (0 DataProvider routes)",
          ),
        measurementRoutes =
          measurementNames.map { measurementName ->
            ReportTraceMeasurementRoute(
              name = measurementName,
              state = "SUCCEEDED",
              protocol = "DIRECT",
              route = ReportTraceMeasurementRouteKind.DIRECT,
              duchyIds = emptyList(),
              duchyParticipantsResolved = true,
              requisitions = emptyList(),
              requisitionsResolved = true,
            )
          },
        warnings = emptyList(),
      )
    val logEntries =
      listOf(
        successfulLifecycleLog(
          "metric_result_sync",
          mapOf("xmm.metric.name" to metricNames.first()),
          0,
        ),
        successfulLifecycleLog(
          "kingdom_measurement_sync",
          mapOf("xmm.measurement.name" to measurementNames.first()),
          1,
        ),
        successfulLifecycleLog(
          "report_result_assembly",
          mapOf("xmm.report.name" to context.reportName),
          2,
        ),
      )

    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, emptyList(), logEntries)

    assertThat(
        coverage.filter { it.name == "metric_result_sync" }.associate { it.resource to it.status }
      )
      .containsExactly(metricNames[0], "SUCCEEDED", metricNames[1], "MISSING")
    assertThat(
        coverage
          .filter { it.name == "kingdom_measurement_sync" }
          .associate { it.resource to it.status }
      )
      .containsExactly(measurementNames[0], "SUCCEEDED", measurementNames[1], "MISSING")
    assertThat(coverage.single { it.name == "report_result_assembly" }.status)
      .isEqualTo("SUCCEEDED")
  }

  @Test
  fun `structured errors render for every MPC and Duchy lifecycle stage`() {
    val context = reportTraceContext()
    val measurementName = context.measurementNames.single()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val routeResolution =
      routeResolution(
          context,
          ReportTraceMeasurementRouteKind.MPC,
          requisitionName,
          ReportTraceRequisitionRouteKind.DIRECT_EDP,
        )
        .withRequisitionState("FULFILLED", measurementState = "COMPUTING")
    val computationName = "computations/computation-1"
    val logEntries =
      routeResolution.measurementRoutes.single().duchyIds.flatMapIndexed { index, duchyId ->
        listOf(
          failedLifecycleLog(
            "duchy_computation",
            mapOf(
              "xmm.measurement.name" to measurementName,
              "xmm.computation.name" to computationName,
              "xmm.duchy.id" to duchyId,
            ),
            index * 2,
          ),
          failedLifecycleLog(
            "duchy_stage_attempt",
            mapOf(
              "xmm.measurement.name" to measurementName,
              "xmm.computation.name" to computationName,
              "xmm.duchy.id" to duchyId,
            ),
            index * 2 + 1,
          ),
        )
      } +
        failedLifecycleLog(
          "kingdom_computation_result_acceptance",
          mapOf(
            "xmm.measurement.name" to measurementName,
            "xmm.computation.name" to computationName,
          ),
          10,
        )
    val coverage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, emptyList(), logEntries)
    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = emptyList(),
        logEntries = logEntries,
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(coverage.filter { it.name == "duchy_computation" }.map { it.status })
      .containsExactly("FAILED", "FAILED")
    assertThat(coverage.filter { it.name == "duchy_stage_attempt" }.map { it.status })
      .containsExactly("FAILED", "FAILED")
    assertThat(coverage.single { it.name == "kingdom_computation_result_acceptance" }.status)
      .isEqualTo("FAILED")
    assertThat(output).contains("xmm.lifecycle.stage=duchy_computation")
    assertThat(output).contains("xmm.lifecycle.stage=duchy_stage_attempt")
    assertThat(output).contains("xmm.lifecycle.stage=kingdom_computation_result_acceptance")
    assertThat(output).contains("xmm.error.type=TestFailure")
  }

  @Test
  fun `durable refusal without accepted telemetry is attributed but remains unknown`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/direct/requisitions/requisition-1"
    val baseRoute =
      routeResolution(
        context,
        ReportTraceMeasurementRouteKind.DIRECT,
        requisitionName,
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
      )
    val routeResolution =
      baseRoute.copy(
        measurementRoutes =
          listOf(
            baseRoute.measurementRoutes
              .single()
              .copy(
                requisitions =
                  listOf(
                    baseRoute.measurementRoutes
                      .single()
                      .requisitions
                      .single()
                      .copy(state = "REFUSED")
                  )
              )
          )
      )
    val refusalError =
      failedLifecycleLog(
        "kingdom_requisition_refusal_acceptance",
        mapOf("xmm.requisition.name" to requisitionName),
        0,
      )
    val coverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        emptyList(),
        listOf(refusalError),
      )

    val stage = coverage.single { it.name == "kingdom_requisition_refusal_acceptance" }
    assertThat(stage.resource).isEqualTo(requisitionName)
    assertThat(stage.status).isEqualTo("UNKNOWN")
    assertThat(stage.evidence).contains("no matching accepted telemetry")
    assertThat(stage.evidence).contains("log kingdom")
  }

  @Test
  fun `renderLogPayload keeps non-gRPC JSON payload`() {
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

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeGrpcPayloads = false)

    assertThat(rendered).contains(payload.toString())
    assertThat(rendered).contains("xmm.report.name=measurementConsumers/mc-1/reports/report-1")
  }

  @Test
  fun `renderLogPayload keeps non-gRPC string payload`() {
    val payload =
      Payload.StringPayload.of(
        "xmm.basic_report.name=measurementConsumers/mc-1/basicReports/br-1 " +
          "xmm.lifecycle.stage=noise_correction token=secret-value arbitrary request body"
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeGrpcPayloads = false)

    assertThat(rendered).isEqualTo(payload.data)
  }

  @Test
  fun `renderLogPayload keeps non-gRPC fields without redaction`() {
    val payload =
      Payload.JsonPayload.of(
        mapOf(
          "message" to
            "status=failed password=hunter2 credential=session-secret " +
              "jwt=aaa.bbb.ccc url=https://example.test/object?X-Goog-Signature=secret",
          "event" to "requisition_failed",
        )
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeGrpcPayloads = false)

    assertThat(rendered).contains(payload.toString())
  }

  @Test
  fun `renderLogPayload keeps complete application exception chain`() {
    val payload =
      Payload.StringPayload.of(
        "Refusing Requisition dataProviders/dp-1/requisitions/r1\n" +
          "org.example.UnfulfillableRequisitionException: PopulationSpec is invalid\n" +
          "\tat org.example.Fulfiller.process(Fulfiller.kt:42)\n" +
          "Caused by: org.example.PopulationSpecValidationException: " +
          "Not all population fields are set: gender, age_group, us_state"
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeGrpcPayloads = false)

    assertThat(rendered).isEqualTo(payload.data)
  }

  @Test
  fun `renderLogPayload omits verbose gRPC payload mislabeled as error`() {
    val payload =
      Payload.StringPayload.of(
        "INFO: [grpc-worker] gRPC trace-id request: " +
          "Metadata(x-api-key=secret-key) report: \"reports/report-1\""
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeGrpcPayloads = false)

    assertThat(rendered).isNull()
  }

  @Test
  fun `renderLogPayload includes verbose gRPC payload when requested`() {
    val payload =
      Payload.StringPayload.of(
        "INFO: [grpc-worker] gRPC trace-id request: " +
          "Metadata(x-api-key=secret-key) report: \"reports/report-1\""
      )

    val rendered = ReportTraceOutput.renderLogPayload(payload, includeGrpcPayloads = true)

    assertThat(rendered).isEqualTo(payload.data)
  }

  @Test
  fun `renderLogPayload omits split gRPC protobuf continuation from API server`() {
    val payload = Payload.StringPayload.of("    work_item: \"workItems/work-item-1\"")

    val rendered =
      ReportTraceOutput.renderLogPayload(
        payload,
        includeGrpcPayloads = false,
        service = "secure-computation-api-server-container",
      )

    assertThat(rendered).isNull()
  }

  @Test
  fun `renderLogPayload keeps similar application message outside API server`() {
    val message = "work_item: processing started"
    val payload = Payload.StringPayload.of(message)

    val rendered =
      ReportTraceOutput.renderLogPayload(
        payload,
        includeGrpcPayloads = false,
        service = "results-fulfiller",
      )

    assertThat(rendered).isEqualTo(message)
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
  fun `main expands identifiers and trace IDs discovered from logs`() {
    val output = StringWriter()
    val logQueries = mutableListOf<Collection<String>>()
    val traceIdQueries = mutableListOf<Collection<String>>()
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val workItemName = "workItems/work-item-1"
    val computationName = "computations/computation-1"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            logQueries += correlationValues.toList()
            when {
              reportName in correlationValues ->
                listOf(
                  ReportTraceLogEntry(
                    sourceProject = project,
                    timestamp = NOW,
                    service = "requisition-fetcher",
                    severity = "INFO",
                    trace = null,
                    message = "xmm.work_item.name=$workItemName",
                  )
                )
              workItemName in correlationValues ->
                listOf(
                  ReportTraceLogEntry(
                    sourceProject = project,
                    timestamp = NOW.plusSeconds(1),
                    service = "results-fulfiller",
                    severity = "INFO",
                    trace = "projects/test/traces/trace-2",
                    message = "xmm.computation.name=$computationName",
                  )
                )
              computationName in correlationValues ->
                listOf(
                  ReportTraceLogEntry(
                    sourceProject = project,
                    timestamp = NOW.plusSeconds(2),
                    service = "duchy",
                    severity = "ERROR",
                    trace = null,
                    message =
                      "xmm.computation.name=$computationName xmm.outcome=failed " +
                        "xmm.error.type=IllegalStateException",
                  )
                )
              else -> emptyList()
            }
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { project, _, traceIds, _, _, _ ->
            traceIdQueries += traceIds.toList()
            if ("trace-2" in traceIds) {
              listOf(
                traceSpan("duchy-span", NOW.plusSeconds(2))
                  .copy(sourceProject = project, traceId = "trace-2", service = "duchy")
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
    assertThat(logQueries)
      .containsExactly(listOf(reportName), listOf(workItemName), listOf(computationName))
      .inOrder()
    assertThat(traceIdQueries.flatten()).contains("trace-2")
    assertThat(output.toString()).contains("xmm.error.type=IllegalStateException")
  }

  @Test
  fun `render separates collection completeness from refused execution outcome`() {
    val context = reportTraceContext().copy(basicReportName = null, basicReportState = null)
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val span =
      traceSpan("span-1", NOW)
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "results_fulfillment",
              "xmm.outcome" to "refused",
              "xmm.requisition.name" to requisitionName,
              "xmm.edpa.group_id" to "group-1",
            )
        )

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution =
          routeResolution(
              context,
              ReportTraceMeasurementRouteKind.DIRECT,
              requisitionName,
              ReportTraceRequisitionRouteKind.EDPA,
            )
            .withRequisitionState("REFUSED", measurementState = "SUCCEEDED"),
        spans = listOf(span),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Collection completeness: PARTIAL")
    assertThat(output).contains("Execution outcome: REFUSED")
    assertThat(output).contains("| results_fulfillment | $requisitionName | REFUSED |")
  }

  @Test
  fun `render lets terminal Report failure override transitional BasicReport state`() {
    val context = reportTraceContext().copy(basicReportState = "REPORT_CREATED")
    val reportFailure =
      traceSpan("report-failed", NOW)
        .copy(
          attributes =
            mapOf(
              "xmm.report.name" to context.reportName,
              "xmm.report.state" to "FAILED",
              "xmm.lifecycle.stage" to "report_result_assembly",
              "xmm.outcome" to "failed",
            )
        )

    val output =
      ReportTraceOutput.render(
        context = context,
        spans = listOf(reportFailure),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Execution outcome: FAILED")
  }

  @Test
  fun `render uses latest durable Report state after a retry`() {
    val context = reportTraceContext().copy(basicReportName = null, basicReportState = null)
    val failedAttempt =
      traceSpan("failed-attempt", NOW)
        .copy(
          attributes =
            mapOf(
              "xmm.report.name" to context.reportName,
              "xmm.report.state" to "FAILED",
              "xmm.lifecycle.stage" to "report_result_assembly",
              "xmm.outcome" to "failed",
            )
        )
    val successfulRetry =
      traceSpan("successful-retry", NOW.plusSeconds(1))
        .copy(
          attributes =
            mapOf(
              "xmm.report.name" to context.reportName,
              "xmm.report.state" to "SUCCEEDED",
              "xmm.lifecycle.stage" to "report_result_assembly",
              "xmm.outcome" to "succeeded",
            )
        )

    val output =
      ReportTraceOutput.render(
        context = context,
        spans = listOf(failedAttempt, successfulRetry),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Execution outcome: SUCCEEDED")
    assertThat(output).contains("| report_result_assembly | ${context.reportName} | SUCCEEDED |")
  }

  @Test
  fun `render does not promote one successful child to report success`() {
    val context = reportTraceContext().copy(basicReportName = null, basicReportState = null)
    val childSuccess =
      traceSpan("metric-success", NOW)
        .copy(
          attributes =
            mapOf(
              "xmm.metric.name" to "measurementConsumers/mc-1/metrics/metric-1",
              "xmm.metric.state" to "SUCCEEDED",
              "xmm.lifecycle.stage" to "metric_result_sync",
              "xmm.outcome" to "succeeded",
            )
        )

    val output =
      ReportTraceOutput.render(
        context = context,
        spans = listOf(childSuccess),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Execution outcome: UNKNOWN")
  }

  @Test
  fun `started lifecycle evidence is not terminally complete`() {
    val context = reportTraceContext()
    val requisitionName = "dataProviders/edpa/requisitions/requisition-1"
    val stages =
      listOf(
        "basic_report_creation",
        "report_creation",
        "metric_creation",
        "measurement_creation",
        "requisition_available",
        "requisition_dispatch",
        "results_fulfillment",
        "kingdom_requisition_result_acceptance",
        "kingdom_measurement_sync",
        "metric_result_sync",
        "report_result_assembly",
        "noise_correction",
        "processed_result_writeback",
      )
    val spans =
      stages.mapIndexed { index, stage ->
        traceSpan("span-$index", NOW.plusSeconds(index.toLong()))
          .copy(
            attributes =
              mapOf(
                "xmm.lifecycle.stage" to stage,
                "xmm.outcome" to if (stage == "results_fulfillment") "started" else "succeeded",
              ) +
                if (stage == "results_fulfillment") {
                  mapOf("xmm.requisition.name" to requisitionName, "xmm.edpa.group_id" to "group-1")
                } else {
                  emptyMap()
                }
          )
      }

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution =
          routeResolution(
            context,
            ReportTraceMeasurementRouteKind.DIRECT,
            requisitionName,
            ReportTraceRequisitionRouteKind.EDPA,
          ),
        spans = spans,
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeGrpcPayloads = false,
      )

    assertThat(output).contains("Collection completeness: PARTIAL")
    assertThat(output).contains("| results_fulfillment | $requisitionName | IN_PROGRESS |")
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
  fun `post-merge truncation is partial when every query batch is below limit`() {
    val output = StringWriter()
    val basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1"
    val metricName = "measurementConsumers/mc-1/metrics/shared-metric"
    val workItemName = "workItems/discovered-work-item"
    val context =
      reportTraceContext()
        .copy(
          metricNames = listOf(metricName),
          metricStates = mapOf(metricName to "SUCCEEDED"),
          reusedMetricNames = setOf(metricName),
          reusedMeasurementNames = reportTraceContext().measurementNames.toSet(),
        )
    val initialSpans =
      listOf(
          lifecycleSpan(
            "basic_report_creation",
            "xmm.basic_report.name",
            checkNotNull(context.basicReportName),
          ),
          lifecycleSpan("report_creation", "xmm.report.name", context.reportName),
          lifecycleSpan("metric_result_sync", "xmm.metric.name", metricName),
          lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName),
          lifecycleSpan(
            "noise_correction",
            "xmm.basic_report.name",
            checkNotNull(context.basicReportName),
          ),
          lifecycleSpan(
            "processed_result_writeback",
            "xmm.basic_report.name",
            checkNotNull(context.basicReportName),
          ),
          traceSpan("discovery", NOW).copy(attributes = mapOf("xmm.work_item.name" to workItemName)),
        )
        .mapIndexed { index, span ->
          val startTime = NOW.plusSeconds(100L + index)
          span.copy(
            spanId = "initial-$index",
            traceId = "initial-trace",
            startTime = startTime,
            endTime = startTime.plusSeconds(1),
          )
        }
    val expansionSpans =
      listOf(
        traceSpan("expansion-1", NOW.minusSeconds(2)),
        traceSpan("expansion-2", NOW.minusSeconds(1)),
      )
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, traceIds, _, _, _ ->
            when {
              traceIds.isNotEmpty() -> emptyList()
              basicReportName in correlationValues -> initialSpans
              workItemName in correlationValues -> expansionSpans
              else -> emptyList()
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        routeResolverOverride =
          ReportTraceRouteResolver { _, _ ->
            routeResolution(
              context,
              ReportTraceMeasurementRouteKind.DIRECT,
              "dataProviders/direct/requisitions/requisition-1",
              ReportTraceRequisitionRouteKind.DIRECT_EDP,
            )
          },
        clock = Clock.fixed(NOW.plusSeconds(200), ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--limit=7",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString()).contains("| collector | Merged Cloud Trace | TRUNCATED | 9 | 7 |")
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
    assertThat(output.toString()).doesNotContain("Cloud Trace results were truncated for project")
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
  fun `main retains log evidence when primary Cloud Trace query fails`() {
    val output = StringWriter()
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { _, _, _, _ ->
            listOf(
              ReportTraceLogEntry(
                sourceProject = project,
                timestamp = NOW,
                service = "reporting",
                severity = "ERROR",
                trace = null,
                message =
                  "xmm.report.name=$reportName " +
                    "xmm.lifecycle.stage=report_result_assembly xmm.outcome=failed",
              )
            )
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, _, _, _, _ ->
            error("trace denied; credential=secret-value")
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
    assertThat(output.toString())
      .contains(
        "Cloud Trace query failed for project test: " +
          "IllegalStateException: trace denied; credential=[REDACTED]"
      )
    assertThat(output.toString()).doesNotContain("secret-value")
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
    assertThat(output.toString()).contains("xmm.outcome=failed")
    assertThat(output.toString()).contains("| test | Cloud Trace | FAILED |")
  }

  @Test
  fun `main reports Cloud Trace ID expansion failure without discarding primary span`() {
    val output = StringWriter()
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, traceIds, _, _, _ ->
            when {
              reportName in correlationValues -> listOf(traceSpan("primary-span", NOW))
              "trace-1" in traceIds -> error("trace ID denied")
              else -> emptyList()
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
    assertThat(output.toString())
      .contains("Cloud Trace ID lookup failed for project test: IllegalStateException")
    assertThat(output.toString()).contains("SPAN [test/service] primary-span")
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `fallback queries only resources with incomplete lifecycle coverage`() {
    val outputDirectory = temporaryFolder.newFolder("targeted-fallback").toPath()
    val basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1"
    val coveredMetricName = "measurementConsumers/mc-1/metrics/covered-metric"
    val missingMetricName = "measurementConsumers/mc-1/metrics/missing-metric"
    val discoveredWorkItemName = "workItems/discovered-work-item"
    val context =
      reportTraceContext()
        .copy(
          basicReportName = basicReportName,
          metricNames = listOf(coveredMetricName, missingMetricName),
        )
    val logQueryValues = mutableListOf<Set<String>>()
    val traceQueryValues = mutableListOf<Set<String>>()
    val traceQueryIds = mutableListOf<Set<String>>()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            logQueryValues += correlationValues.toSet()
            if (coveredMetricName in correlationValues) {
              listOf(
                ReportTraceLogEntry(
                  sourceProject = "test",
                  timestamp = NOW,
                  service = "reporting",
                  severity = "INFO",
                  trace = null,
                  message =
                    "xmm.lifecycle.stage=metric_creation " +
                      "xmm.metric.name=$coveredMetricName xmm.outcome=succeeded " +
                      "evidence=covered-log-only",
                )
              )
            } else {
              emptyList()
            }
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, traceIds, _, _, _ ->
            traceQueryValues += correlationValues.toSet()
            traceQueryIds += traceIds.toSet()
            when {
              basicReportName in correlationValues && traceIds.isEmpty() ->
                listOf(
                  lifecycleSpan("metric_creation", "xmm.metric.name", coveredMetricName),
                  lifecycleSpan("metric_result_sync", "xmm.metric.name", coveredMetricName),
                )
              missingMetricName in correlationValues ->
                listOf(
                  lifecycleSpan("metric_creation", "xmm.metric.name", missingMetricName),
                  lifecycleSpan("metric_result_sync", "xmm.metric.name", missingMetricName)
                    .copy(
                      traceId = "fallback-trace",
                      attributes =
                        mapOf(
                          "xmm.lifecycle.stage" to "metric_result_sync",
                          "xmm.metric.name" to missingMetricName,
                          "xmm.work_item.name" to discoveredWorkItemName,
                          "xmm.outcome" to "succeeded",
                        ),
                    ),
                )
              else -> emptyList()
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        routeResolverOverride =
          ReportTraceRouteResolver { _, _ ->
            routeResolution(
              context,
              ReportTraceMeasurementRouteKind.DIRECT,
              "dataProviders/direct/requisitions/requisition-1",
              ReportTraceRequisitionRouteKind.DIRECT_EDP,
            )
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(logQueryValues.first())
      .containsAtLeast(basicReportName, coveredMetricName, missingMetricName)
    assertThat(traceQueryValues.first()).containsExactly(basicReportName)
    assertThat(traceQueryValues.drop(1).flatten()).contains(missingMetricName)
    assertThat(traceQueryValues.drop(1).flatten()).doesNotContain(coveredMetricName)
    assertThat(traceQueryValues.flatten()).contains(discoveredWorkItemName)
    assertThat(traceQueryIds.flatten()).contains("fallback-trace")
    val artifact = outputDirectory.toFile().listFiles().single().readText()
    assertThat(artifact).contains("evidence=covered-log-only")
    assertThat(artifact).contains("| metric_creation | $missingMetricName | SUCCEEDED |")
    assertThat(artifact).contains("| metric_result_sync | $missingMetricName | SUCCEEDED |")
  }

  @Test
  fun `correlation value cap applies across expansion rounds`() {
    val outputDirectory = temporaryFolder.newFolder("capped-expansion").toPath()
    val context = reportTraceContext()
    val discoveredWorkItemName = "workItems/discovered-work-item"
    val traceQueryValues = mutableListOf<Set<String>>()
    val logQueryValues = mutableListOf<Set<String>>()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            logQueryValues += correlationValues.toSet()
            emptyList()
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, _, _, _, _ ->
            traceQueryValues += correlationValues.toSet()
            if (checkNotNull(context.basicReportName) in correlationValues) {
              listOf(
                lifecycleSpan("report_creation", "xmm.report.name", context.reportName),
                lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName),
                traceSpan("discovery", NOW)
                  .copy(attributes = mapOf("xmm.work_item.name" to discoveredWorkItemName)),
              )
            } else {
              emptyList()
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        routeResolverOverride =
          ReportTraceRouteResolver { _, _ ->
            routeResolution(
              context,
              ReportTraceMeasurementRouteKind.DIRECT,
              "dataProviders/direct/requisitions/requisition-1",
              ReportTraceRequisitionRouteKind.DIRECT_EDP,
            )
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=${context.basicReportName}",
          "--output-dir=$outputDirectory",
          "--max-correlation-values=2",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat((logQueryValues + traceQueryValues).flatten().distinct()).hasSize(2)
    assertThat((logQueryValues + traceQueryValues).flatten()).doesNotContain(discoveredWorkItemName)
    assertThat(outputDirectory.toFile().listFiles().single().readText())
      .contains("| collector | Correlation values | TRUNCATED |")
  }

  @Test
  fun `unresolved Measurement request ID is used directly for fallback`() {
    val outputDirectory = temporaryFolder.newFolder("request-id-fallback").toPath()
    val requestId = "measurement-request-id"
    val recoveredMeasurementName = "measurementConsumers/mc-1/measurements/measurement-1"
    val context =
      reportTraceContext()
        .copy(measurementNames = emptyList(), unresolvedMeasurementRequestIds = listOf(requestId))
    val traceQueryValues = mutableListOf<Set<String>>()
    val routeResolverInputs = mutableListOf<List<String>>()
    val measurementLifecycleStages =
      listOf(
        "measurement_creation",
        "measurement_linkage",
        "kingdom_measurement_sync",
        "duchy_computation",
        "duchy_stage_attempt",
        "kingdom_computation_result_acceptance",
      )
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, _, _, _, _ ->
            traceQueryValues += correlationValues.toSet()
            when {
              checkNotNull(context.basicReportName) in correlationValues ->
                measurementLifecycleStages.map { stage ->
                  lifecycleSpan(stage, "xmm.measurement.request_id", requestId)
                }
              requestId in correlationValues ->
                listOf(
                  lifecycleSpan(
                    "measurement_creation",
                    mapOf(
                      "xmm.measurement.request_id" to requestId,
                      "xmm.measurement.name" to recoveredMeasurementName,
                    ),
                  )
                )
              else -> emptyList()
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        routeResolverOverride =
          ReportTraceRouteResolver { measurementNames, topology ->
            routeResolverInputs += measurementNames.toList()
            ReportTraceRouteResolution.unresolved(
              measurementNames = measurementNames,
              topology = topology,
              status = "SUCCESS",
              note = "",
            )
          },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(StringWriter()),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=${context.basicReportName}",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(traceQueryValues.drop(1).flatten()).contains(requestId)
    assertThat(routeResolverInputs)
      .containsExactly(emptyList<String>(), listOf(recoveredMeasurementName))
      .inOrder()
    val artifact = outputDirectory.toFile().listFiles().single().readText()
    assertThat(artifact)
      .contains("Measurement $recoveredMeasurementName was recovered from telemetry")
  }

  @Test
  fun `main reports Cloud Trace fallback query failure`() {
    val output = StringWriter()
    val context = reportTraceContext()
    val basicReportName = checkNotNull(context.basicReportName)
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, _, _, _, _ ->
            if (basicReportName in correlationValues) {
              emptyList()
            } else {
              error("fallback denied")
            }
          }
        },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride = BasicReportTraceResolver { context },
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
        output = PrintWriter(output),
        error = PrintWriter(StringWriter()),
      )

    val exitCode =
      main(
        arrayOf(
          "--project=test",
          "--basic-report=$basicReportName",
          "--start-time=2026-09-10T11:00:00Z",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString())
      .contains("Cloud Trace fallback query failed for project test: IllegalStateException")
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `main retains evidence when Cloud Logging correlation expansion fails`() {
    val output = StringWriter()
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val workItemName = "workItems/discovered-work-item"
    var logReads = 0
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { project, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            logReads++
            if (workItemName in correlationValues) {
              error("expansion denied")
            }
            listOf(
              ReportTraceLogEntry(
                sourceProject = project,
                timestamp = NOW.minusSeconds(1),
                service = "reporting",
                severity = "INFO",
                trace = null,
                message = "xmm.report.name=$reportName primary-evidence",
              )
            )
          }
        },
        spanReaderFactory = {
          ReportTraceSpanReader { _, correlationValues, traceIds, _, _, _ ->
            if (traceIds.isEmpty() && reportName in correlationValues) {
              listOf(
                traceSpan("primary-span", NOW)
                  .copy(attributes = mapOf("xmm.work_item.name" to workItemName))
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
    assertThat(logReads).isEqualTo(2)
    assertThat(output.toString()).contains("primary-evidence")
    assertThat(output.toString())
      .contains("Cloud Logging correlation-expansion query failed for project test")
    assertThat(output.toString()).contains("| test | Cloud Logging | PARTIAL | 1 | 1 |")
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `main retains healthy project spans when another Cloud Trace project fails`() {
    val output = StringWriter()
    val reportName = "measurementConsumers/mc-1/reports/report-1"
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { project, _, _, _, _, _ ->
            if (project == "broken") {
              error("trace unavailable")
            }
            listOf(traceSpan("healthy-span", NOW).copy(sourceProject = project))
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
          "--observability-project=broken",
          "--observability-project=healthy",
          "--report=$reportName",
          "--start-time=2026-09-10T11:00:00Z",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString())
      .contains("Cloud Trace query failed for project broken: IllegalStateException")
    assertThat(output.toString()).contains("SPAN [healthy/service] healthy-span")
    assertThat(output.toString()).contains("Collection completeness: PARTIAL")
  }

  @Test
  fun `Cloud Trace quota exhaustion fails even when partial output is allowed`() {
    val output = StringWriter()
    val error = StringWriter()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ -> ReportTraceLogReader { _, _, _, _ -> emptyList() } },
        spanReaderFactory = {
          ReportTraceSpanReader { _, _, _, _, _, _ -> error("Cloud Trace API returned HTTP 429") }
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
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(1)
    assertThat(output.toString()).isEmpty()
    assertThat(error.toString())
      .contains("Telemetry collection aborted because a read quota was exhausted")
  }

  @Test
  fun `Cloud Logging quota exhaustion writes failed artifact and continues batch`() {
    val output = StringWriter()
    val outputDirectory = temporaryFolder.newFolder("quota-exhausted-traces").toPath()
    val dependencies =
      ReportTraceDependencies(
        logReaderFactory = { _, _ ->
          ReportTraceLogReader { correlationValues, _, _, _ ->
            if (correlationValues.any { it.endsWith("/report-a") }) {
              throw Status.RESOURCE_EXHAUSTED.asRuntimeException()
            }
            emptyList()
          }
        },
        spanReaderFactory = { ReportTraceSpanReader { _, _, _, _, _, _ -> emptyList() } },
        resolverFactory = { _, _ -> error("Resolver factory should not be used") },
        resolverOverride =
          BasicReportTraceResolver { key ->
            ReportTraceContext(
              basicReportName = key.toName(),
              basicReportState = "RUNNING",
              reportName =
                "measurementConsumers/${key.cmmsMeasurementConsumerId}/reports/${key.basicReportId}",
              metricNames = emptyList(),
              metricStates = emptyMap(),
              reusedMetricNames = emptySet(),
              unresolvedMetricRequestIds = emptyList(),
              measurementNames = emptyList(),
              reusedMeasurementNames = emptySet(),
              unresolvedMeasurementRequestIds = emptyList(),
              reportResolvedByRequestId = false,
              telemetryRecoveredMeasurementNames = emptyMap(),
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
          "--basic-report=measurementConsumers/mc-1/basicReports/report-a",
          "--basic-report=measurementConsumers/mc-1/basicReports/report-b",
          "--output-dir=$outputDirectory",
          "--allow-partial",
          "--spanner-ready-timeout=PT10S",
        ),
        dependencies,
      )

    assertThat(exitCode).isEqualTo(1)
    assertThat(outputDirectory.resolve("mc-1__report-a.md").toFile().readText())
      .contains("Collection completeness: FAILED")
    assertThat(outputDirectory.resolve("mc-1__report-a.md").toFile().readText())
      .doesNotContain("Collection completeness: PARTIAL")
    assertThat(outputDirectory.resolve("mc-1__report-b.md").toFile().readText())
      .contains("Collection completeness: PARTIAL")
    assertThat(output.toString())
      .contains("FAILED  measurementConsumers/mc-1/basicReports/report-a")
    assertThat(output.toString())
      .contains("PARTIAL  measurementConsumers/mc-1/basicReports/report-b")
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

  private fun lifecycleSpan(
    stage: String,
    resourceAttribute: String,
    resource: String,
  ): ReportTraceSpan = lifecycleSpan(stage, mapOf(resourceAttribute to resource))

  private fun lifecycleSpan(
    stage: String,
    producerAttributes: Map<String, String>,
  ): ReportTraceSpan {
    return ReportTraceSpan(
      sourceProject = "test",
      traceId = "trace-1",
      spanId = "$stage-${producerAttributes.hashCode()}",
      parentSpanId = null,
      name = stage,
      service = "test-service",
      startTime = NOW,
      endTime = NOW.plusSeconds(1),
      attributes =
        mapOf("xmm.lifecycle.stage" to stage, "xmm.outcome" to "succeeded") + producerAttributes,
    )
  }

  private fun failedLifecycleSpan(
    stage: String,
    producerAttributes: Map<String, String>,
  ): ReportTraceSpan {
    return lifecycleSpan(stage, producerAttributes)
      .copy(
        attributes =
          mapOf(
            "xmm.lifecycle.stage" to stage,
            "xmm.outcome" to "failed",
            "xmm.error.type" to "IllegalStateException",
          ) + producerAttributes
      )
  }

  private fun refusalOriginSpan(requisitionName: String, origin: String): ReportTraceSpan {
    return lifecycleSpan(
        "requisition_refusal",
        mapOf(
          "xmm.requisition.name" to requisitionName,
          ReportTraceAttributes.REFUSAL_ORIGIN_STRING to origin,
        ),
      )
      .copy(
        attributes =
          mapOf(
            "xmm.lifecycle.stage" to "requisition_refusal",
            "xmm.outcome" to "refused",
            "xmm.requisition.name" to requisitionName,
            ReportTraceAttributes.REFUSAL_ORIGIN_STRING to origin,
          )
      )
  }

  private fun refusalAcceptanceSpan(requisitionName: String, startTime: Instant): ReportTraceSpan {
    return traceSpan("kingdom-refusal-acceptance", startTime)
      .copy(
        attributes =
          mapOf(
            "xmm.lifecycle.stage" to "kingdom_requisition_refusal_acceptance",
            "xmm.outcome" to "refused",
            "xmm.requisition.name" to requisitionName,
          )
      )
  }

  private fun failedLifecycleLog(
    stage: String,
    producerAttributes: Map<String, String>,
    secondsAfterNow: Int,
  ): ReportTraceLogEntry {
    val message =
      (mapOf(
          "xmm.lifecycle.stage" to stage,
          "xmm.outcome" to "failed",
          "xmm.error.type" to "TestFailure",
          "xmm.error.code" to "grpc.UNAVAILABLE",
        ) + producerAttributes)
        .entries
        .joinToString(" ") { (name, value) -> "$name=$value" }
    return ReportTraceLogEntry(
      sourceProject = "test",
      timestamp = NOW.plusSeconds(secondsAfterNow.toLong()),
      service =
        when {
          stage.startsWith("kingdom_") -> "kingdom"
          stage.startsWith("duchy_") -> "duchy"
          stage in setOf("requisition_dispatch", "work_item_processing", "results_fulfillment") ->
            "edpa"
          else -> "reporting"
        },
      severity = "ERROR",
      trace = "projects/test/traces/trace-$secondsAfterNow",
      message = message,
    )
  }

  private fun successfulLifecycleLog(
    stage: String,
    producerAttributes: Map<String, String>,
    secondsAfterNow: Int,
  ): ReportTraceLogEntry {
    val message =
      (mapOf("xmm.lifecycle.stage" to stage, "xmm.outcome" to "succeeded") + producerAttributes)
        .entries
        .joinToString(" ") { (name, value) -> "$name=$value" }
    return ReportTraceLogEntry(
      sourceProject = "test",
      timestamp = NOW.plusSeconds(secondsAfterNow.toLong()),
      service = "reporting",
      severity = "INFO",
      trace = null,
      message = message,
    )
  }

  private fun successfulDirectUpstreamSpans(
    context: ReportTraceContext,
    metricName: String,
    requisitionName: String,
  ): List<ReportTraceSpan> {
    return listOf(
      lifecycleSpan(
        "basic_report_creation",
        "xmm.basic_report.name",
        checkNotNull(context.basicReportName),
      ),
      lifecycleSpan("report_creation", "xmm.report.name", context.reportName),
      lifecycleSpan("metric_creation", "xmm.metric.name", metricName),
      lifecycleSpan(
        "measurement_creation",
        "xmm.measurement.name",
        context.measurementNames.single(),
      ),
      lifecycleSpan(
        "measurement_linkage",
        "xmm.measurement.name",
        context.measurementNames.single(),
      ),
      lifecycleSpan("requisition_available", "xmm.requisition.name", requisitionName),
      lifecycleSpan(
        "kingdom_requisition_result_acceptance",
        "xmm.requisition.name",
        requisitionName,
      ),
      lifecycleSpan(
        "kingdom_measurement_sync",
        "xmm.measurement.name",
        context.measurementNames.single(),
      ),
      lifecycleSpan("metric_result_sync", "xmm.metric.name", metricName),
    )
  }

  private fun failurePropagationSpans(
    context: ReportTraceContext,
    metricName: String,
    requisitionName: String,
  ): List<ReportTraceSpan> {
    return listOf(
      lifecycleSpan(
        "basic_report_creation",
        "xmm.basic_report.name",
        checkNotNull(context.basicReportName),
      ),
      lifecycleSpan("report_creation", "xmm.report.name", context.reportName),
      lifecycleSpan("metric_creation", "xmm.metric.name", metricName),
      lifecycleSpan(
        "measurement_creation",
        "xmm.measurement.name",
        context.measurementNames.single(),
      ),
      lifecycleSpan(
        "measurement_linkage",
        "xmm.measurement.name",
        context.measurementNames.single(),
      ),
      lifecycleSpan("requisition_available", "xmm.requisition.name", requisitionName),
      failedLifecycleSpan("metric_result_sync", mapOf("xmm.metric.name" to metricName)),
      failedLifecycleSpan(
        "report_result_assembly",
        mapOf("xmm.report.name" to context.reportName, "xmm.report.state" to "FAILED"),
      ),
    )
  }

  private fun refusalPropagationSpans(
    context: ReportTraceContext,
    metricName: String,
    requisitionName: String,
  ): List<ReportTraceSpan> {
    return failurePropagationSpans(context, metricName, requisitionName).filterNot {
      it.attributes["xmm.lifecycle.stage"] == "report_result_assembly"
    } +
      lifecycleSpan(
          "kingdom_requisition_refusal_acceptance",
          "xmm.requisition.name",
          requisitionName,
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "kingdom_requisition_refusal_acceptance",
              "xmm.outcome" to "refused",
              "xmm.requisition.name" to requisitionName,
            )
        ) +
      lifecycleSpan(
          "kingdom_measurement_sync",
          "xmm.measurement.name",
          context.measurementNames.single(),
        )
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "kingdom_measurement_sync",
              "xmm.outcome" to "refused",
              "xmm.measurement.name" to context.measurementNames.single(),
            )
        ) +
      lifecycleSpan("report_result_assembly", "xmm.report.name", context.reportName)
        .copy(
          attributes =
            mapOf(
              "xmm.lifecycle.stage" to "report_result_assembly",
              "xmm.outcome" to "refused",
              "xmm.report.name" to context.reportName,
            )
        )
  }

  private fun routeResolution(
    context: ReportTraceContext,
    measurementRoute: ReportTraceMeasurementRouteKind,
    requisitionName: String,
    requisitionRoute: ReportTraceRequisitionRouteKind,
  ): ReportTraceRouteResolution {
    val dataProvider = requisitionName.substringBefore("/requisitions/")
    return ReportTraceRouteResolution(
      status = "SUCCESS",
      note = "",
      topology =
        ReportTraceTopology(
          routes = mapOf(dataProvider to requisitionRoute),
          provenance = "operator-provided --topology-config-file (1 DataProvider route)",
        ),
      measurementRoutes =
        listOf(
          ReportTraceMeasurementRoute(
            name = context.measurementNames.single(),
            state = "SUCCEEDED",
            protocol =
              if (measurementRoute == ReportTraceMeasurementRouteKind.DIRECT) "DIRECT"
              else "HONEST_MAJORITY_SHARE_SHUFFLE",
            route = measurementRoute,
            duchyIds =
              if (measurementRoute == ReportTraceMeasurementRouteKind.MPC) {
                listOf("aggregator", "worker1")
              } else {
                emptyList()
              },
            duchyParticipantsResolved = true,
            requisitions =
              listOf(
                ReportTraceRequisitionRoute(
                  name = requisitionName,
                  state = "FULFILLED",
                  dataProvider = dataProvider,
                  route = requisitionRoute,
                )
              ),
            requisitionsResolved = true,
          )
        ),
      warnings = emptyList(),
    )
  }

  private fun ReportTraceRouteResolution.withRequisitionState(
    requisitionState: String,
    measurementState: String,
  ): ReportTraceRouteResolution {
    val measurementRoute = measurementRoutes.single()
    return copy(
      measurementRoutes =
        listOf(
          measurementRoute.copy(
            state = measurementState,
            requisitions =
              listOf(measurementRoute.requisitions.single().copy(state = requisitionState)),
          )
        )
    )
  }

  private fun reportTraceContext(): ReportTraceContext {
    return ReportTraceContext(
      basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1",
      basicReportState = "SUCCEEDED",
      reportName = "measurementConsumers/mc-1/reports/report-1",
      metricNames = emptyList(),
      metricStates = emptyMap(),
      reusedMetricNames = emptySet(),
      unresolvedMetricRequestIds = emptyList(),
      measurementNames = listOf("measurementConsumers/mc-1/measurements/measurement-1"),
      reusedMeasurementNames = emptySet(),
      unresolvedMeasurementRequestIds = emptyList(),
      reportResolvedByRequestId = false,
      telemetryRecoveredMeasurementNames = emptyMap(),
      createTime = NOW,
    )
  }

  private class RecordingThrottler : Throttler {
    var invocationCount = 0

    override suspend fun <T> onReady(block: suspend () -> T): T {
      invocationCount++
      return block()
    }
  }

  private class SerializingThrottler : Throttler {
    private val mutex = Mutex()

    override suspend fun <T> onReady(block: suspend () -> T): T {
      return mutex.withLock { block() }
    }
  }

  companion object {
    private val NOW: Instant = Instant.parse("2026-09-10T13:00:00Z")
  }
}
