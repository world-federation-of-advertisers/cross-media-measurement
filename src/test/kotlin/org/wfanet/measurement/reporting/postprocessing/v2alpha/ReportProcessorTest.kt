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
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

data class MetricReport(
  val cumulativeMeasurements: Map<Set<String>, List<Long>>,
  val totalMeasurements: Map<Set<String>, Long>,
  val kreach: Map<Set<String>, Map<Int, Long>>,
  val impression: Map<Set<String>, Long>,
)

class TestStorageFactory(private val inMemoryStorageClient: InMemoryStorageClient) :
  ReportProcessor.StorageFactory {
  override fun createStorageClient(projectId: String, bucketName: String): StorageClient {
    return inMemoryStorageClient
  }
}

@RunWith(JUnit4::class)
class ReportProcessorTest {
  private lateinit var inMemoryStorageClient: InMemoryStorageClient
  private lateinit var testFactory: TestStorageFactory

  @Before
  fun setUp() {
    inMemoryStorageClient = InMemoryStorageClient()
    testFactory = TestStorageFactory(inMemoryStorageClient)

    // Swap the factory in ReportProcessor.
    ReportProcessor.setTestStorageFactory(testFactory)
  }

  @After
  fun tearDown() {
    ReportProcessor.resetToGcsStorageFactory()
  }

  @Test
  fun `report post processing output has INTERNAL_ERROR issue when noise correction throws exception`() =
    runBlocking {
      // The sample report has cumulative impression count measurement. This causes the noise
      // correction to throw an exception as only cumulative reach measurement is supported.
      val reportFile =
        TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_invalid_cumulative_measurement_type.json")
          .toFile()
      val reportAsJson = reportFile.readText()

      val reportProcessingOutput: ReportProcessingOutput =
        ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")

      // Verify that the output contains the INTERNAL_ERROR issue.
      assertThat(reportProcessingOutput.reportPostProcessorLog.issuesList)
        .contains(ReportPostProcessorLog.ReportPostProcessorIssue.INTERNAL_ERROR)
      assertThat(reportProcessingOutput.reportPostProcessorLog.results).hasSize(1)
      assertThat(
          reportProcessingOutput.reportPostProcessorLog.results.values.first().status.statusCode
        )
        .isEqualTo(ReportPostProcessorStatus.StatusCode.INTERNAL_ERROR)
      assertThat(reportProcessingOutput.reportPostProcessorLog.results.values.first().errorMessage)
        .contains("Cumulative measurements must be reach measurements.")

      val expectedBlobKey = "20241213/20241213102410_c8f5ab1b95b44c0691f44111700054c3.textproto"

      // Verify that the log is written to the storage.
      assertThat(inMemoryStorageClient.contents).containsKey(expectedBlobKey)

      assertThat(
          ReportPostProcessorLog.parseFrom(
            inMemoryStorageClient.getBlob(expectedBlobKey)!!.read().flatten()
          )
        )
        .isEqualTo(reportProcessingOutput.reportPostProcessorLog)
    }

  @Test
  fun `run correct report with logging with custom policy successfully`() = runBlocking {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val reportProcessingOutput: ReportProcessingOutput =
      ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")
    val updatedReport =
      ReportConversion.getReportFromJsonString(reportProcessingOutput.updatedReportJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()

    val expectedBlobKey = "20241213/20241213102410_c8f5ab1b95b44c0691f44111700054c3.textproto"
    assertThat(inMemoryStorageClient.contents).containsKey(expectedBlobKey)

    assertThat(
        ReportPostProcessorLog.parseFrom(
          inMemoryStorageClient.getBlob(expectedBlobKey)!!.read().flatten()
        )
      )
      .isEqualTo(reportProcessingOutput.reportPostProcessorLog)
  }

  @Test
  fun `run correct report with logging without cumulative measurements successfully`() =
    runBlocking {
      val reportFile =
        TEST_DATA_RUNTIME_DIR.resolve("sample_report_without_cumulative_measurements.json").toFile()
      val reportAsJson = reportFile.readText()

      val report = ReportConversion.getReportFromJsonString(reportAsJson)
      assertThat(report.hasConsistentMeasurements()).isFalse()

      val reportProcessingOutput: ReportProcessingOutput =
        ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")
      val updatedReport =
        ReportConversion.getReportFromJsonString(reportProcessingOutput.updatedReportJson)
      assertThat(updatedReport.hasConsistentMeasurements()).isTrue()

      val expectedBlobKey = "20250620/20250620111829_e250ee4dd864ce99f1fe1df77944b48.textproto"
      assertThat(inMemoryStorageClient.contents).containsKey(expectedBlobKey)

      assertThat(
          ReportPostProcessorLog.parseFrom(
            inMemoryStorageClient.getBlob(expectedBlobKey)!!.read().flatten()
          )
        )
        .isEqualTo(reportProcessingOutput.reportPostProcessorLog)
    }

  @Test
  fun `run correct report with logging with demographic slicing successfully`() = runBlocking {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_demographic_slicing.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val reportProcessingOutput: ReportProcessingOutput =
      ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")
    val updatedReport =
      ReportConversion.getReportFromJsonString(reportProcessingOutput.updatedReportJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()

    val expectedBlobKey = "20250206/20250206144635_bd39d48654554a83ba9c8534a5bb7502.textproto"

    assertThat(inMemoryStorageClient.contents).containsKey(expectedBlobKey)

    assertThat(
        ReportPostProcessorLog.parseFrom(
          inMemoryStorageClient.getBlob(expectedBlobKey)!!.read().flatten()
        )
      )
      .isEqualTo(reportProcessingOutput.reportPostProcessorLog)
  }

  @Test
  fun `run correct report with logging with unique reach and incremental reach successfully`() =
    runBlocking {
      val reportFile =
        TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_incremental_reach_small.json")
          .toFile()
      val reportAsJson = reportFile.readText()

      val report = ReportConversion.getReportFromJsonString(reportAsJson)
      assertThat(report.hasConsistentMeasurements()).isFalse()

      val reportProcessingOutput: ReportProcessingOutput =
        ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")
      val updatedReport =
        ReportConversion.getReportFromJsonString(reportProcessingOutput.updatedReportJson)
      assertThat(updatedReport.hasConsistentMeasurements()).isTrue()

      val expectedBlobKey = "20240913/20240913151951_a9c1a2b3fc74ebf8c5ab81d7763aa70.textproto"

      assertThat(inMemoryStorageClient.contents).containsKey(expectedBlobKey)

      assertThat(
          ReportPostProcessorLog.parseFrom(
            inMemoryStorageClient.getBlob(expectedBlobKey)!!.read().flatten()
          )
        )
        .isEqualTo(reportProcessingOutput.reportPostProcessorLog)
    }

  @Test
  fun `run correct report without logging with custom policy successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct report without cumulative measurements successfully`() = runBlocking {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_without_cumulative_measurements.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val reportProcessingOutput: ReportProcessingOutput =
      ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")
    val updatedReport =
      ReportConversion.getReportFromJsonString(reportProcessingOutput.updatedReportJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct reach only report successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_reach_only_report.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct report without whole campaign reach successfully`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_without_whole_campaign_reach.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct report without logging with unique reach and incremental reach successfully`() {
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
  fun `run correct report without logging successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_large.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isFalse()

    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isTrue()
  }

  @Test
  fun `run correct report without logging with demographic slicing successfully`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_demographic_slicing.json").toFile()
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
      // Verifies that cumulative measurements are consistent.
      if (cumulativeMeasurements.isNotEmpty()) {
        for ((_, measurements) in cumulativeMeasurements) {
          if (measurements.any { it < 0 }) {
            return false
          }
          if (measurements.zipWithNext().any { (a, b) -> a > b }) {
            return false
          }
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

      // Verifies that the relationship between total reach and impression holds.
      for (edpCombination in impression.keys.intersect(totalMeasurements.keys)) {
        if (
          !fuzzyLessEqual(
            totalMeasurements[edpCombination]!!.toDouble(),
            impression[edpCombination]!!.toDouble(),
            TOLERANCE,
          )
        ) {
          return false
        }
      }

      // Verifies that the relationship between kreach and impression holds.
      for (edpCombination in impression.keys.intersect(kreach.keys)) {
        val kReachByEdpCombination: Map<Int, Long> = kreach.getValue(edpCombination)
        val kreachWeightedSum: Long =
          kReachByEdpCombination.entries.sumOf { (key, value) -> key * value }
        val totalWeight: Int = kReachByEdpCombination.entries.sumOf { (key, _) -> key }
        if (
          !fuzzyLessEqual(
            kreachWeightedSum.toDouble(),
            impression[edpCombination]!!.toDouble(),
            totalWeight * TOLERANCE,
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
