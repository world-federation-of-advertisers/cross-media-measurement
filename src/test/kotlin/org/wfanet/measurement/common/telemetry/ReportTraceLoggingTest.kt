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

package org.wfanet.measurement.common.telemetry

import com.google.common.truth.Truth.assertThat
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ReportTraceLoggingTest {
  @Test
  fun `log writes one payload-free line with safe fields`() {
    val records = mutableListOf<LogRecord>()
    val handler = recordingHandler(records)
    val logger = Logger.getAnonymousLogger().apply { addHandler(handler) }

    ReportTraceLogging.log(
      logger,
      "reporting.metric.result_synchronized",
      ReportTraceAttributes.LIFECYCLE_STAGE_STRING to "metric_result_sync",
      ReportTraceAttributes.METRIC_NAME_STRING to "measurementConsumers/mc/metrics/metric-1",
      ReportTraceAttributes.OUTCOME_STRING to "failed with newline\nand spaces",
      ReportTraceAttributes.ERROR_CODE_STRING to null,
    )

    assertThat(records).hasSize(1)
    assertThat(records.single().level).isEqualTo(Level.INFO)
    assertThat(records.single().message)
      .isEqualTo(
        "event=reporting.metric.result_synchronized " +
          "xmm.lifecycle.stage=metric_result_sync " +
          "xmm.metric.name=measurementConsumers/mc/metrics/metric-1 " +
          "xmm.outcome=failed_with_newline_and_spaces"
      )
  }

  @Test
  fun `log rejects fields outside the report trace allowlist`() {
    val logger = Logger.getAnonymousLogger()

    assertFailsWith<IllegalArgumentException> {
      ReportTraceLogging.log(
        logger,
        "reporting.metric.result_synchronized",
        "authorization" to "secret",
      )
    }
  }

  @Test
  fun `log rejects invalid event names`() {
    val logger = Logger.getAnonymousLogger()

    for (event in listOf("", "event with spaces", "event\nwith-newline")) {
      assertFailsWith<IllegalArgumentException> { ReportTraceLogging.log(logger, event) }
    }
  }

  @Test
  fun `log omits null and blank field values`() {
    val records = mutableListOf<LogRecord>()
    val logger = Logger.getAnonymousLogger().apply { addHandler(recordingHandler(records)) }

    ReportTraceLogging.log(
      logger,
      "reporting.metric.result_synchronized",
      ReportTraceAttributes.METRIC_NAME_STRING to null,
      ReportTraceAttributes.OUTCOME_STRING to "",
      ReportTraceAttributes.ERROR_CODE_STRING to "   ",
    )

    assertThat(records.single().message).isEqualTo("event=reporting.metric.result_synchronized")
  }

  @Test
  fun `log bounds field values to 1000 characters`() {
    val records = mutableListOf<LogRecord>()
    val logger = Logger.getAnonymousLogger().apply { addHandler(recordingHandler(records)) }

    ReportTraceLogging.log(
      logger,
      "reporting.metric.result_synchronized",
      ReportTraceAttributes.METRIC_NAME_STRING to "m".repeat(1001),
    )

    val metricName = records.single().message.substringAfter("xmm.metric.name=")
    assertThat(metricName).hasLength(1000)
  }

  @Test
  fun `log retains requested severity and throwable`() {
    val records = mutableListOf<LogRecord>()
    val logger = Logger.getAnonymousLogger().apply { addHandler(recordingHandler(records)) }
    val error = IllegalStateException("required blob is missing")

    ReportTraceLogging.log(
      logger,
      Level.SEVERE,
      error,
      "edp_aggregator.results_fulfiller.group_failed",
      ReportTraceAttributes.LIFECYCLE_STAGE_STRING to "results_fulfillment",
      ReportTraceAttributes.OUTCOME_STRING to "failed",
    )

    assertThat(records).hasSize(1)
    assertThat(records.single().level).isEqualTo(Level.SEVERE)
    assertThat(records.single().thrown).isSameInstanceAs(error)
    assertThat(records.single().message)
      .isEqualTo(
        "event=edp_aggregator.results_fulfiller.group_failed " +
          "xmm.lifecycle.stage=results_fulfillment xmm.outcome=failed"
      )
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
