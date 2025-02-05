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
import com.google.protobuf.util.JsonFormat
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.FrequencyResultKt.binResult
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.frequencyResult
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.impressionCountResult
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.measurementResult
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.reachAndFrequencyResult
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.reachResult

@RunWith(JUnit4::class)
class ReportConversionTest {
  @Test
  fun `report with custom measurement policy is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)

    println(JsonFormat.printer().preservingProtoFieldNames().print(reportSummary[0]))

    // A custom total campaign measurement that contains impression and reach and frequency.
    val unionCustomEdp1Edp2MeasurementDetail = measurementDetail {
      measurementPolicy = "custom"
      setOperation = "union"
      dataProviders += "edp1"
      dataProviders += "edp2"
      leftHandSideTargets += "edp1"
      leftHandSideTargets += "edp2"
      measurementResults += measurementResult {
        impressionCount = impressionCountResult {
          value = 239912
          standardDeviation = 506550.02723539365
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a0175804e-d71b-4a18-a198-b4d9dc69221b"
      }
      measurementResults += measurementResult {
        reachAndFrequency = reachAndFrequencyResult {
          reach = reachResult {
            value = 92459
            standardDeviation = 145777.467021918
          }
          frequency = frequencyResult {
            bins += binResult {
              label = "5"
              value = 40199
              standardDeviation = 80625.84487265335
            }
            bins += binResult {
              label = "2"
              value = 52259
              standardDeviation = 96293.33072911394
            }
            bins += binResult {
              label = "1"
              standardDeviation = 49832.8343667827
            }
            bins += binResult {
              label = "3"
              standardDeviation = 49832.8343667827
            }
            bins += binResult {
              label = "4"
              standardDeviation = 49832.8343667827
            }
          }
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/adacfb57a-fe7b-44b5-9c29-022be610a407"
      }
    }

    // A custom cumulative measurements.
    val cummulativeCustomEdp1Edp2MeasurementDetail = measurementDetail {
      measurementPolicy = "custom"
      setOperation = "cumulative"
      isCumulative = true
      dataProviders += "edp1"
      dataProviders += "edp2"
      leftHandSideTargets += "edp1"
      leftHandSideTargets += "edp2"
      measurementResults += measurementResult {
        reach = reachResult {
          value = 18000
          standardDeviation = 185589.5021572231
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a400e54b3-95d1-4056-b92f-f978615a05c3"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 92700
          standardDeviation = 191025.0129033726
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aebc1632a-3676-4fea-b22f-78486f0c48d7"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 163700
          standardDeviation = 196286.4317309566
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a61e13352-b0ff-41eb-879b-dfa7a458d232"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 19100
          standardDeviation = 185668.79847492787
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aa0605786-63a4-4fe1-bbc1-42717bf17ff6"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 127200
          standardDeviation = 193570.0395894004
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/accc56b11-d361-4291-a00b-cef502b50d74"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 224400
          standardDeviation = 200858.04694133752
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/affcf4c2b-d2ec-4083-9db8-8659c6bd5c67"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 100
          standardDeviation = 184302.26284602462
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a6932cc4a-d367-43b8-be1c-9b8d7f9e4c93"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 100
          standardDeviation = 184302.26284602462
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ae345574e-76ab-4da6-8b86-8e232453f413"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 100
          standardDeviation = 184302.26284602462
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a1cfe162d-cfac-443e-b71f-c75cf569200c"
      }
      measurementResults += measurementResult {
        reach = reachResult {
          value = 100
          standardDeviation = 184302.26284602462
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a7645a53f-960f-44d7-a13e-8e388ed53f6b"
      }
    }

    // Verifies that reportSummary contains the above two protos for custom measurements where
    // reachResult, reachAndFrequencyResult, and impressionCountResult are parsed correctly.
    assertThat(reportSummary[0].measurementDetailsList)
      .containsAtLeast(
        unionCustomEdp1Edp2MeasurementDetail,
        cummulativeCustomEdp1Edp2MeasurementDetail,
      )
  }

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
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 24129432
            standardDeviation = 1.0
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/00"
        }
        measurementResults += measurementResult {
          reach = reachResult {
            value = 29152165
            standardDeviation = 1.0
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/01"
        }
        measurementResults += measurementResult {
          reach = reachResult {
            value = 31474050
            standardDeviation = 1.0
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/02"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        isCumulative = false
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 1000
              standardDeviation = 102011.27564649425
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                value = 100
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "2"
                value = 200
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "3"
                value = 300
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "4"
                value = 400
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "5"
                standardDeviation = 106176.70203773731
              }
            }
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/ami/union/00"
        }
      }
    }
    assertThat(reportSummary).hasSize(1)
    assertThat(reportSummary[0]).isEqualTo(expectedReportSummary)
  }

  @Test
  fun `report with unique reach is successfully converted to report summary proto`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_incremental_reach_small.json")
        .toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)

    val expectedReportSummary = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 74640
              standardDeviation = 102032.8580350049
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                value = 74640
                standardDeviation = 108362.92537282953
              }
              bins += binResult {
                label = "2"
                standardDeviation = 36494.10194217629
              }
            }
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_edp2"
        }
        measurementResults += measurementResult {
          impressionCount = impressionCountResult { standardDeviation = 1432701.2690834093 }
          metric = "measurementConsumers/TjyUnormbAg/metrics/aee3941f8-5be9-4a2e-b771-d32ad3e7e6ba"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 30000
            standardDeviation = 137708.79990420336
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_edp2"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        uniqueReachTarget = "edp2"
        leftHandSideTargets += "edp2"
        rightHandSideTargets += "edp1"
        rightHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 2000
            standardDeviation = 262192.75285658165
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_edp2"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 400
            standardDeviation = 230564.3972774748
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/a3c0f4fda-c0c9-41f0-bb8e-a88f1354181f"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        uniqueReachTarget = "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        rightHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 300
            standardDeviation = 261177.24408350687
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_edp1"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp3"
        measurementResults += measurementResult {
          impressionCount = impressionCountResult { standardDeviation = 1432701.2690834093 }
          metric = "measurementConsumers/TjyUnormbAg/metrics/a6db34b4f-a6a8-4478-8263-ddefd446a975"
        }
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 187439
              standardDeviation = 102065.46555734947
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                value = 187439
                standardDeviation = 119219.35283812648
              }
              bins += binResult {
                label = "2"
                standardDeviation = 61610.83372024118
              }
            }
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_edp3"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp3"
        measurementResults += measurementResult {
          reach = reachResult { standardDeviation = 137708.79990420336 }
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_edp3"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        leftHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 91199
              standardDeviation = 137993.02905314422
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "2"
                value = 79431
                standardDeviation = 129325.07135157812
              }
              bins += binResult {
                label = "1"
                value = 11767
                standardDeviation = 50960.5941803963
              }
            }
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/all_edps"
        }
        measurementResults += measurementResult {
          impressionCount = impressionCountResult { standardDeviation = 2481511.3901208746 }
          metric = "measurementConsumers/TjyUnormbAg/metrics/aa7c347f9-0ea1-4e7b-85eb-c87969d8dbc2"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        leftHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 48300
            standardDeviation = 184559.25807765796
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/all_edps"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult { standardDeviation = 102011.27564649425 }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "2"
                standardDeviation = 106176.70203773731
              }
            }
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_edp1"
        }
        measurementResults += measurementResult {
          impressionCount = impressionCountResult {
            value = 3270106
            standardDeviation = 1432964.0978411257
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/a22025b98-9854-4fb7-87ee-e0c6f90519ae"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 189700
            standardDeviation = 137776.9714846423
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_edp1"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        uniqueReachTarget = "edp3"
        leftHandSideTargets += "edp3"
        rightHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 100
            standardDeviation = 261663.2405567259
          }
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_edp3"
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
