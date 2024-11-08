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
          metric = "measurementConsumers/TjyUnormbAg/metrics/a0cca6039-8ba0-4bc9-81e8-81469f8840d3"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/LkJlb3UZkY8"
        measurementResults += measurementResult {
          reach = 0
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/TjyUnormbAg/metrics/a611cd145-8618-4fba-9109-7aabea17e2c3"
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
          reach = 200
          standardDeviation = 262192.75285658165
          metric = "measurementConsumers/TjyUnormbAg/metrics/a1302b107-ddaa-4736-9357-eef6f7f83d78"
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
          metric = "measurementConsumers/TjyUnormbAg/metrics/a71098bcd-4d84-4fa8-adc5-47dbb368615e"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          reach = 187439
          standardDeviation = 102065.46555734947
          metric = "measurementConsumers/TjyUnormbAg/metrics/af5a0dfb9-09b3-4a1a-a6b0-b7d81cafc241"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "dataProviders/a0tmyormbgQ"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/TjyUnormbAg/metrics/a022f5100-05b2-4563-8045-3076f0ee67d9"
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
          metric = "measurementConsumers/TjyUnormbAg/metrics/a1d51b95e-194b-490d-bc7e-69e948ba25d4"
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
          metric = "measurementConsumers/TjyUnormbAg/metrics/a4ce3d32a-5ea0-4658-bd90-2cfdfe899182"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "dataProviders/TbsgjIrmbj0"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/TjyUnormbAg/metrics/a0d9cee90-5ba4-41b2-9ca7-da72cb818e31"
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
          metric = "measurementConsumers/TjyUnormbAg/metrics/a6e09e1ad-6e39-4e10-8385-b417447c4539"
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
          metric = "measurementConsumers/TjyUnormbAg/metrics/adf76f93b-618b-455d-8433-e1115b8f01b8"
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
