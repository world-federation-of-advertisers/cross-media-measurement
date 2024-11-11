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
  fun `report without unique reach is successfully converted to report summary proto`() {
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
  fun `report with unique reach is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_small.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)

    val expectedReportSummary = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "dataProviders/LkJlb3UZkY8"
        measurementResults += measurementResult {
          reach = 74640
          standardDeviation = 102032.8580350049
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_LkJlb3UZkY8"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/LkJlb3UZkY8"
        measurementResults += measurementResult {
          reach = 30000
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_LkJlb3UZkY8"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "dataProviders/LkJlb3UZkY8"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        dataProviders += "dataProviders/a0tmyormbgQ"
        uniqueReachTarget = "dataProviders/LkJlb3UZkY8"
        measurementResults += measurementResult {
          reach = 2000
          standardDeviation = 262192.75285658165
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_LkJlb3UZkY8"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "dataProviders/LkJlb3UZkY8"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        measurementResults += measurementResult {
          reach = 400
          standardDeviation = 230564.3972774748
          metric = "measurementConsumers/TjyUnormbAg/metrics/a3c0f4fda-c0c9-41f0-bb8e-a88f1354181f"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "dataProviders/LkJlb3UZkY8"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        dataProviders += "dataProviders/a0tmyormbgQ"
        uniqueReachTarget = "dataProviders/TbsgjIrmbj0"
        measurementResults += measurementResult {
          reach = 300
          standardDeviation = 261177.24408350687
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_TbsgjIrmbj0"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          reach = 187439
          standardDeviation = 102065.46555734947
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_a0tmyormbgQ"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_a0tmyormbgQ"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "dataProviders/LkJlb3UZkY8"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        dataProviders += "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          reach = 91199
          standardDeviation = 137993.02905314422
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/all_edps"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/LkJlb3UZkY8"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        dataProviders += "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          reach = 48300
          standardDeviation = 184559.25807765796
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/all_edps"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_TbsgjIrmbj0"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/TbsgjIrmbj0"
        measurementResults += measurementResult {
          reach = 189700
          standardDeviation = 137776.9714846423
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_TbsgjIrmbj0"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "dataProviders/LkJlb3UZkY8"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        dataProviders += "dataProviders/a0tmyormbgQ"
        uniqueReachTarget = "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 261663.2405567259
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_a0tmyormbgQ"
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
