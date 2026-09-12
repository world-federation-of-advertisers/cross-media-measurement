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
        metricStates = emptyMap(),
        reusedMetricNames = emptySet(),
        measurementNames = emptyList(),
        reusedMeasurementNames = emptySet(),
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
        metricStates = emptyMap(),
        reusedMetricNames = emptySet(),
        measurementNames = emptyList(),
        reusedMeasurementNames = emptySet(),
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
              measurementNames = emptyList(),
              reusedMeasurementNames = emptySet(),
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
        includeRawPayloads = false,
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
      }

    val output =
      ReportTraceOutput.render(
        context = context,
        routeResolution = routeResolution,
        spans = spans,
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("Collection completeness: COMPLETE")
    assertThat(output).contains("Execution outcome: SUCCEEDED")
    assertThat(output)
      .contains("| duchy_computation | ${context.measurementNames.single()} | NOT_APPLICABLE |")
    assertThat(output).contains("| requisition_dispatch | $requisitionName | NOT_APPLICABLE |")
    assertThat(output).doesNotContain("| basic_report_api_fetch | MISSING |")
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
          },
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
      )

    assertThat(output).contains("Collection completeness: COMPLETE")
    assertThat(output)
      .contains("| duchy_computation | $measurementName @ duchy worker1 | SUCCEEDED |")
    assertThat(output).contains("| results_fulfillment | $requisitionName | SUCCEEDED |")
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
        includeRawPayloads = false,
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
    val spans = refusalPropagationSpans(context, metricName, requisitionName)
    val coverage = ReportTraceOutput.lifecycleCoverage(context, routeResolution, spans, emptyList())

    assertThat(
        coverage
          .filter {
            it.name in setOf("requisition_dispatch", "work_item_processing", "results_fulfillment")
          }
          .map { it.status }
      )
      .containsExactly("SKIPPED_AFTER_REFUSAL", "SKIPPED_AFTER_REFUSAL", "SKIPPED_AFTER_REFUSAL")
    assertThat(ReportTraceOutput.artifactStatus(spans, emptyList(), emptyList(), coverage))
      .isEqualTo(ReportTraceArtifactStatus.COMPLETE)
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
  fun `failed Kingdom computation acceptance is correlated through Duchy evidence`() {
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
    assertThat(acceptance.status).isEqualTo("FAILED")
    assertThat(acceptance.evidence).contains("Measurement correlated by computation")
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
        includeRawPayloads = false,
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
        includeRawPayloads = false,
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
        includeRawPayloads = false,
      )

    for (stage in stageAttributes.keys) {
      assertThat(coverage.filter { it.name == stage }.map { it.status }.distinct())
        .containsExactly("FAILED")
      assertThat(output).contains("xmm.lifecycle.stage=$stage")
    }
    assertThat(output).contains("xmm.error.type=TestFailure")
    assertThat(output).contains("LOG ERROR")
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
        includeRawPayloads = false,
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
  fun `structured Kingdom refusal error is attributed to the exact Requisition`() {
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
    assertThat(stage.status).isEqualTo("FAILED")
    assertThat(stage.evidence).isEqualTo("log kingdom")
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
          ),
        spans = listOf(span),
        logEntries = emptyList(),
        sourceStatuses = emptyList(),
        warnings = emptyList(),
        includeRawPayloads = false,
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
        includeRawPayloads = false,
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
        includeRawPayloads = false,
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
        includeRawPayloads = false,
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
        includeRawPayloads = false,
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

  private fun reportTraceContext(): ReportTraceContext {
    return ReportTraceContext(
      basicReportName = "measurementConsumers/mc-1/basicReports/basic-report-1",
      basicReportState = "SUCCEEDED",
      reportName = "measurementConsumers/mc-1/reports/report-1",
      metricNames = emptyList(),
      metricStates = emptyMap(),
      reusedMetricNames = emptySet(),
      measurementNames = listOf("measurementConsumers/mc-1/measurements/measurement-1"),
      reusedMeasurementNames = emptySet(),
      createTime = NOW,
    )
  }

  companion object {
    private val NOW: Instant = Instant.parse("2026-09-10T13:00:00Z")
  }
}
