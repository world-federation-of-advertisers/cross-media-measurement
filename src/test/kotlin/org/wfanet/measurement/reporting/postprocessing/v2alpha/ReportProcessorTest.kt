/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.reporting.postprocessing.v2alpha

import com.google.common.truth.Truth.assertThat
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.v2alpha.Report

data class MetricReport(
  val cumulativeMeasurements: Map<Set<String>, List<Long>>,
  val totalMeasurements: Map<Set<String>, Long>,
)

@RunWith(JUnit4::class)
class ReportProcessorTest {
  @Test
  fun `run correct report with custom policy successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentCumulativeMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentCumulativeMeasurements()).isTrue()
  }

  @Test
  fun `run correct report with unique reach and incremental reach successfully`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_incremental_reach_small.json")
        .toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentCumulativeMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentCumulativeMeasurements()).isTrue()
  }

  @Test
  fun `run correct report successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_large.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentCumulativeMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentCumulativeMeasurements()).isTrue()
  }

  @Test
  fun `run correct report throws RuntimeException when cumulative measurements have different length`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_invalid_cumulative_measurements.json")
        .toFile()
    val reportAsJson = reportFile.readText()

    val exception =
      assertFailsWith<RuntimeException> { ReportProcessor.processReportJson(reportAsJson) }

    assertThat(exception).hasMessageThat().contains("must have the same length")
  }

  @Test
  fun `run correct report throws RuntimeException when difference measurements have invalid variance`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve(
          "sample_report_with_invalid_difference_measurement_variance.json"
        )
        .toFile()
    val reportAsJson = reportFile.readText()

    val exception =
      assertFailsWith<RuntimeException> { ReportProcessor.processReportJson(reportAsJson) }

    assertThat(exception).hasMessageThat().contains("The variance of the difference measurement")
  }

  companion object {
    private val POLICIES = listOf("ami", "mrc", "custom")
    private val TEST_DATA_RUNTIME_DIR: Path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "reporting",
          "postprocessing",
          "v2alpha",
        )
      )!!

    private fun ReportSummary.toMetricReport(measurementPolicy: String): MetricReport {
      val cumulativeMeasurements: MutableMap<Set<String>, List<Long>> = mutableMapOf()
      val totalMeasurements: MutableMap<Set<String>, Long> = mutableMapOf()

      // Processes cumulative measurements.
      for (entry in measurementDetailsList) {
        if (entry.setOperation == "cumulative") {
          val dataProviders = entry.dataProvidersList.toSortedSet()
          val measurements = entry.measurementResultsList.map { result -> result.reach }
          if (entry.measurementPolicy == measurementPolicy) {
            cumulativeMeasurements[dataProviders] = measurements
          }
        }
      }

      // Processes total union measurements.
      for (entry in measurementDetailsList) {
        if (entry.setOperation == "union" && !entry.isCumulative) {
          val measurements = entry.measurementResultsList.map { result -> result.reach }
          if (entry.measurementPolicy == measurementPolicy) {
            totalMeasurements[entry.dataProvidersList.toSortedSet()] = measurements[0]
          }
        }
      }

      return MetricReport(cumulativeMeasurements, totalMeasurements)
    }

    private fun ReportSummary.toReportByPolicy(): Map<String, MetricReport> {
      return POLICIES.associateWith { policy -> this.toMetricReport(policy) }
    }

    private fun MetricReport.hasConsistentCumulativeMeasurements(): Boolean {
      if (cumulativeMeasurements.isEmpty()) {
        return true
      }
      for ((edpCombination, measurements) in cumulativeMeasurements) {
        if (measurements.zipWithNext().any { (a, b) -> a > b }) {
          return false
        }
        if (edpCombination in totalMeasurements) {
          if (measurements[measurements.size - 1] > totalMeasurements[edpCombination]!!) {
            return false
          }
        }
      }
      return true
    }

    private fun Report.hasConsistentCumulativeMeasurements(): Boolean {
      return this.toReportSummaries().all {
        it.toReportByPolicy().values.all { metricReport ->
          metricReport.hasConsistentCumulativeMeasurements()
        }
      }
    }
  }
}
