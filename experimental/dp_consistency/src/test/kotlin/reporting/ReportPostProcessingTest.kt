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

package org.wfanet.measurement.reporting.postprocessing

import com.google.common.truth.Truth.assertThat
import java.io.File
import java.nio.file.Files
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.reporting.measurementDetail
import org.wfanet.measurement.reporting.v2alpha.Report

@RunWith(JUnit4::class)
class ReportPostProcessingTest {
  @Test
  fun `run correct report successfully`() {
    val reportAsJson =
      Files.readString(
        File("experimental/dp_consistency/src/test/kotlin/reporting/sample_report.json").toPath()
      )
    val report = ReportConversion.getReportFromJsonString(reportAsJson)
    assertThat(report.hasConsistentMeasurements()).isEqualTo(false)
    val updatedReportAsJson = ReportPostProcessing.processReportJson(reportAsJson)
    val updatedReport = ReportConversion.getReportFromJsonString(updatedReportAsJson)
    assertThat(updatedReport.hasConsistentMeasurements()).isEqualTo(true)
  }

  companion object {
    private fun Report.hasConsistentMeasurements(): Boolean {
      this.toReportSummaries().forEach {
        it.measurementDetailsList.map { measurementDetail ->
          if (measurementDetail.isCumulative) {
            val reachMeasurements =
              measurementDetail.measurementResultsList.map { result -> result.reach }.toList()
            if (reachMeasurements.size >= 2) {
              for (i in 0 until (reachMeasurements.size - 1)) {
                if (reachMeasurements[i] > reachMeasurements[i + 1]) {
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
