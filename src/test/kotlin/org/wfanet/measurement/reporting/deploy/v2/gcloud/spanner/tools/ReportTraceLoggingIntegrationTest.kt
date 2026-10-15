/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTraceLogging

@RunWith(JUnit4::class)
class ReportTraceLoggingIntegrationTest {
  @Test
  fun `lifecycle coverage parses a real structured lifecycle log`() {
    val metricName = "measurementConsumers/mc-1/metrics/metric-1"
    val records = mutableListOf<LogRecord>()
    val logger =
      Logger.getAnonymousLogger().apply {
        useParentHandlers = false
        addHandler(recordingHandler(records))
      }
    ReportTraceLogging.log(
      logger,
      "reporting.metric.result_synchronized",
      ReportTraceAttributes.LIFECYCLE_STAGE_STRING to "metric_result_sync",
      ReportTraceAttributes.METRIC_NAME_STRING to metricName,
      ReportTraceAttributes.METRIC_STATE_STRING to "SUCCEEDED",
      ReportTraceAttributes.OUTCOME_STRING to "succeeded",
    )
    val context =
      ReportTraceContext(
        basicReportName = null,
        basicReportState = null,
        reportName = "measurementConsumers/mc-1/reports/report-1",
        metricNames = listOf(metricName),
        metricStates = mapOf(metricName to "SUCCEEDED"),
        reusedMetricNames = emptySet(),
        unresolvedMetricRequestIds = emptyList(),
        measurementNames = emptyList(),
        reusedMeasurementNames = emptySet(),
        unresolvedMeasurementRequestIds = emptyList(),
        reportResolvedByRequestId = false,
        telemetryRecoveredMeasurementNames = emptyMap(),
        createTime = null,
      )
    val routeResolution =
      ReportTraceRouteResolution(
        status = "SUCCESS",
        note = "",
        topology = ReportTraceTopology(emptyMap(), "test"),
        measurementRoutes = emptyList(),
        warnings = emptyList(),
      )
    val logEntry =
      ReportTraceLogEntry(
        sourceProject = "test",
        timestamp = Instant.parse("2026-09-15T00:00:00Z"),
        service = "reporting",
        severity = "INFO",
        trace = null,
        message = records.single().message,
      )

    val stage =
      ReportTraceOutput.lifecycleCoverage(context, routeResolution, emptyList(), listOf(logEntry))
        .single { it.name == "metric_result_sync" }

    assertThat(stage.resource).isEqualTo(metricName)
    assertThat(stage.status).isEqualTo("SUCCEEDED")
    assertThat(stage.evidence).contains("log reporting")
  }

  private fun recordingHandler(records: MutableList<LogRecord>): Handler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        records += record
      }

      override fun flush() {}

      override fun close() {}
    }
}
