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
import kotlin.math.abs
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.v2alpha.Report

data class MetricReport(
  val cumulativeMeasurements: Map<Set<String>, List<Long>>,
  val totalMeasurements: Map<Set<String>, Long>,
  val kreach: Map<Set<String>, Map<Int, Long>>,
  val impression: Map<Set<String>, Long>,
)

@RunWith(JUnit4::class)
class ReportProcessorTest {
  @Test
  fun `run correct report with custom policy successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct report with unique reach and incremental reach successfully`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_incremental_reach_small.json")
        .toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct report successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_large.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  companion object {
    private val TOLERANCE: Double = 1.0
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
      val kreach: MutableMap<Set<String>, Map<Int, Long>> = mutableMapOf()
      val impression: MutableMap<Set<String>, Long> = mutableMapOf()

      // Processes cumulative measurements.
      for (entry in measurementDetailsList) {
        if (entry.measurementPolicy == measurementPolicy) {
          if (entry.setOperation == "cumulative") {
            val measurements = entry.measurementResultsList.map { result -> result.reach.value }
            cumulativeMeasurements[entry.dataProvidersList.toSortedSet()] = measurements
          }
        }
      }

      // Processes total measurements.
      for (entry in measurementDetailsList) {
        if (entry.measurementPolicy == measurementPolicy) {
          if (entry.setOperation == "union" && !entry.isCumulative) {
            entry.measurementResultsList.map { result ->
              if (result.hasReachAndFrequency()) {
                totalMeasurements[entry.dataProvidersList.toSortedSet()] =
                  result.reachAndFrequency.reach.value
                kreach[entry.dataProvidersList.toSortedSet()] =
                  result.reachAndFrequency.frequency.binsList
                    .map { bin -> bin.label.toInt() to bin.value }
                    .toMap()
                    .toMutableMap()
              } else if (result.hasImpressionCount()) {
                impression[entry.dataProvidersList.toSortedSet()] = result.impressionCount.value
              }
            }
          }
        }
      }

      return MetricReport(cumulativeMeasurements, totalMeasurements, kreach, impression)
    }

    private fun ReportSummary.toReportByPolicy(): Map<String, MetricReport> {
      return POLICIES.associateWith { policy -> this.toMetricReport(policy) }
    }

    private fun MetricReport.hasConsistentMeasurements(): Boolean {
      if (cumulativeMeasurements.isEmpty()) {
        return true
      }

      // Verifies that cumulative measurements are consistent.
      for ((_, measurements) in cumulativeMeasurements) {
        if (measurements.any { it < 0 }) {
          return false
        }
        if (measurements.zipWithNext().any { (a, b) -> a > b }) {
          return false
        }
      }

      // Verifies that last cumulative reach equals to the total reach.
      for (edpCombination in totalMeasurements.keys.intersect(cumulativeMeasurements.keys)) {
        if (
          !fuzzyEqual(
            cumulativeMeasurements[edpCombination]!![
                cumulativeMeasurements[edpCombination]!!.size - 1]
              .toDouble(),
            totalMeasurements[edpCombination]!!.toDouble(),
            TOLERANCE,
          )
        ) {
          return false
        }
      }

      // Verifies that total reach equals to sum of kreach.
      for (edpCombination in totalMeasurements.keys.intersect(kreach.keys)) {
        val kreachSum = kreach[edpCombination]!!.values.sumOf { it }
        if (
          !fuzzyEqual(
            totalMeasurements[edpCombination]!!.toDouble(),
            kreachSum.toDouble(),
            kreach[edpCombination]!!.size * TOLERANCE,
          )
        ) {
          return false
        }
      }

      // Verifies that the relationship between kreach and impression holds.
      for (edpCombination in impression.keys.intersect(kreach.keys)) {
        val kreachWeightedSum =
          kreach[edpCombination]!!.entries.sumOf { (key, value) -> key * value }
        if (
          !fuzzyLessEqual(
            kreachWeightedSum.toDouble(),
            impression[edpCombination]!!.toDouble(),
            TOLERANCE,
          )
        ) {
          return false
        }
      }

      // Verifies that the relationship between impression holds.
      for (edpCombination in impression.keys) {
        if (edpCombination.size > 1) {
          val singleEdpCombinations = edpCombination.map { setOf(it) }
          val sumOfImpressions =
            impression.entries.sumOf { (key, value) ->
              if (key in singleEdpCombinations) value else 0
            }
          if (
            !fuzzyEqual(
              sumOfImpressions.toDouble(),
              impression[edpCombination]!!.toDouble(),
              edpCombination.size * TOLERANCE,
            )
          ) {
            return false
          }
        }
      }

      return true
    }

    private fun Report.hasConsistentMeasurements(): Boolean {
      val reportSummaries = this.toReportSummaries()

      for (reportSummary in reportSummaries) {
        val metricReports = reportSummary.toReportByPolicy()

        if (!metricReports.values.all { it -> it.hasConsistentMeasurements() }) {
          return false
        }

        if (metricReports.containsKey("ami") && metricReports.containsKey("mrc")) {
          if (!metricReportsAreConsistent(metricReports["ami"]!!, metricReports["mrc"]!!)) {
            return false
          }
        }
      }

      return true
    }

    private fun metricReportsAreConsistent(parent: MetricReport, child: MetricReport): Boolean {
      for (edpCombination in
        parent.cumulativeMeasurements.keys.intersect(child.cumulativeMeasurements.keys)) {
        if (
          !child.cumulativeMeasurements[edpCombination]!!
            .zip(parent.cumulativeMeasurements[edpCombination]!!)
            .all { (a, b) -> fuzzyLessEqual(a.toDouble(), b.toDouble(), TOLERANCE) }
        ) {
          return false
        }
      }

      for (edpCombination in
        parent.totalMeasurements.keys.intersect(child.totalMeasurements.keys)) {
        if (
          !fuzzyLessEqual(
            child.totalMeasurements[edpCombination]!!.toDouble(),
            parent.totalMeasurements[edpCombination]!!.toDouble(),
            TOLERANCE,
          )
        ) {
          return false
        }
      }

      for (edpCombination in parent.impression.keys.intersect(child.impression.keys)) {
        if (
          !fuzzyLessEqual(
            child.impression[edpCombination]!!.toDouble(),
            parent.impression[edpCombination]!!.toDouble(),
            TOLERANCE,
          )
        ) {
          return false
        }
      }

      return true
    }

    private fun fuzzyEqual(val1: Double, val2: Double, tolerance: Double): Boolean {
      return abs(val1 - val2) < tolerance
    }

    private fun fuzzyLessEqual(val1: Double, val2: Double, tolerance: Double): Boolean {
      return val1 < val2 + tolerance
    }
  }
}
