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

@RunWith(JUnit4::class)
class ReportProcessorTest {
  @Test
  fun `run correct report with unique reach successfully`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_small.json").toFile()
    val reportAsJson = reportFile.readText()

    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    val updatedReportAsJson = ReportProcessor.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
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

    private fun Report.hasConsistentMeasurements(): Boolean {
      this.toReportSummaries().forEach {
        it.measurementDetailsList.map { measurementDetail ->
          if (measurementDetail.isCumulative) {
            val reachMeasurements =
              measurementDetail.measurementResultsList.map { result -> result.reach }.toList()
            if (reachMeasurements.size >= 2) {
              for (i in 0 until (reachMeasurements.size - 1)) {
                if (reachMeasurements[i] > reachMeasurements[i + 1] + 1) {
                  print(reachMeasurements[i])
                  print(reachMeasurements[i + 1])
                  return false
                }
              }
            }
          }
        }
      }
      return true
    }
  }
}
