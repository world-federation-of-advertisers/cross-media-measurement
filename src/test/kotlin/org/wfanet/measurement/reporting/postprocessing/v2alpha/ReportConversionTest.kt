// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.postprocessing.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.measurementResult

@RunWith(JUnit4::class)
class ReportConversionTest {
  @Test
  fun `report as json string is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_small.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)
    val expectedReportSummary = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/LkJlb3UZkY8"
        measurementResults += measurementResult {
          reach = 24129432
          standardDeviation = 1.0
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/00"
        }
        measurementResults += measurementResult {
          reach = 29152165
          standardDeviation = 1.0
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/01"
        }
        measurementResults += measurementResult {
          reach = 31474050
          standardDeviation = 1.0
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/02"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        isCumulative = false
        dataProviders += "dataProviders/LkJlb3UZkY8"
        measurementResults += measurementResult {
          reach = 1000
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/TjyUnormbAg/metrics/ami/union/00"
        }
      }
    }
    assertThat(reportSummary).hasSize(1)
    assertThat(reportSummary[0]).isEqualTo(expectedReportSummary)
  }

  @Test
  fun `report with unsuccessful state fails to be converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("report_with_unspecified_state.json").toFile()
    val reportAsJson = reportFile.readText()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportConversion.convertJsontoReportSummaries(reportAsJson)
      }

    assertThat(exception).hasMessageThat().contains("not supported")
  }

  @Test
  fun `report with failed measurement fails to be converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("report_with_failed_measurement.json").toFile()
    val reportAsJson = reportFile.readText()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportConversion.convertJsontoReportSummaries(reportAsJson)
      }

    assertThat(exception).hasMessageThat().contains("not supported")
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
  }
}
