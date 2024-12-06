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
  fun `run correct report with unique reach successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_small.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isEqualTo(false)

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isEqualTo(true)
  }

  @Test
  fun `run correct report successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_large.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isEqualTo(false)

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isEqualTo(true)
  }

  companion object {
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

      for (entry in measurementDetailsList) {
        if (entry.setOperation == "difference" && entry.uniqueReachTarget != "") {
          val subset =
            entry.dataProvidersList.filter { it != entry.uniqueReachTarget }.toSortedSet()
          val measurements = entry.measurementResultsList.map { result -> result.reach }

          if (entry.measurementPolicy == measurementPolicy) {
            val supersetMeasurement = totalMeasurements[entry.dataProvidersList.toSortedSet()]!!
            if (subset !in totalMeasurements) {
              totalMeasurements[subset] = supersetMeasurement - measurements[0]
            }
          }
        }
      }

      return MetricReport(cumulativeMeasurements, totalMeasurements)
    }

    private fun ReportSummary.toReportByPolicy(): Map<String, MetricReport> {
      val metricReportByPolicy: MutableMap<String, MetricReport> = mutableMapOf()
      metricReportByPolicy["ami"] = this.toMetricReport("ami")
      metricReportByPolicy["mrc"] = this.toMetricReport("mrc")
      return metricReportByPolicy
    }

    private fun MetricReport.hasConsistentMeasurements(): Boolean {
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

    private fun Report.hasConsistentMeasurements(): Boolean {
      this.toReportSummaries().forEach {
        val metricReportByPolicy = it.toReportByPolicy()
        if (
          !metricReportByPolicy["ami"]!!.hasConsistentMeasurements() ||
            !metricReportByPolicy["mrc"]!!.hasConsistentMeasurements()
        ) {
          return false
        }
      }
      return true
    }
  }
}
