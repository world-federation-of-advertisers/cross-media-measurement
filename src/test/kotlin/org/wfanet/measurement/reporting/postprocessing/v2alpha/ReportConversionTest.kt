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
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.FrequencyResultKt.binResult
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.frequencyResult
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.impressionCountResult
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.measurementResult
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.reachAndFrequencyResult
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.reachResult
import org.wfanet.measurement.internal.reporting.postprocessing.measurementDetail
import org.wfanet.measurement.internal.reporting.postprocessing.reportSummary

@RunWith(JUnit4::class)
class ReportConversionTest {
  @Test
  fun `report with custom measurement policy is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)

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
              value = 40199.565217391304
              standardDeviation = 80625.84487265335
            }
            bins += binResult {
              label = "2"
              value = 52259.43478260869
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

    // Measurement with NaN values in k reach result.
    val unionCustomEdp2MeasurementDetail = measurementDetail {
      measurementPolicy = "custom"
      setOperation = "union"
      dataProviders += "edp2"
      measurementResults += measurementResult {
        reachAndFrequency = reachAndFrequencyResult {
          reach = reachResult { standardDeviation = 102011.27564649425 }
          frequency = frequencyResult {
            bins += binResult {
              label = "1"
              standardDeviation = 102011.27564649425
            }
            bins += binResult {
              label = "2"
              standardDeviation = 102011.27564649425
            }
            bins += binResult {
              label = "3"
              standardDeviation = 29448.118727440287
            }
            bins += binResult {
              label = "4"
              standardDeviation = 29448.118727440287
            }
            bins += binResult {
              label = "5"
              standardDeviation = 29448.118727440287
            }
          }
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a410a1c14-1214-449a-b08e-2df1b292dc4a"
      }
      measurementResults += measurementResult {
        impressionCount = impressionCountResult {
          value = 70833
          standardDeviation = 358181.0108199463
        }
        metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8133cd9a-1803-41b4-a82a-01025c1c01e6"
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
        unionCustomEdp2MeasurementDetail,
        cummulativeCustomEdp1Edp2MeasurementDetail,
      )
    assertThat(reportSummary[0].demographicGroupsList)
      .isEqualTo(listOf("FEMALE_AGE_GROUP2", "MALE_AGE_GROUP2"))
    assertThat(reportSummary[0].population).isEqualTo(159377L)
  }

  @Test
  fun `report without unique reach is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_small.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)
    val expectedReportSummary = reportSummary {
      demographicGroups += "-"
      population = 500000
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
                value = 100.0
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "2"
                value = 200.0
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "3"
                value = 300.0
                standardDeviation = 29448.118727440287
              }
              bins += binResult {
                label = "4"
                value = 400.0
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
                value = 74640.0
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
                value = 187439.0
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
                value = 79431.3870967742
                standardDeviation = 129325.07135157812
              }
              bins += binResult {
                label = "1"
                value = 11767.612903225807
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
      demographicGroups += "FEMALE_AGE_GROUP2"
      population = 0
    }
    assertThat(reportSummary).hasSize(1)
    assertThat(reportSummary[0]).isEqualTo(expectedReportSummary)
  }

  @Test
  fun `report with demographic slicing is successfully converted to report summary proto`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_demographic_slicing.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummaries = ReportConversion.convertJsontoReportSummaries(reportAsJson)

    // Verifies that there are 6 report summaries that match 6 demographic groups in the report.
    assertThat(reportSummaries).hasSize(6)

    // Verifies that each report summary has 8 measurements.
    reportSummaries.forEach { reportSummary ->
      assertThat(reportSummary.measurementDetailsList).hasSize(8)
    }

    // Verifies the report summary for the demographic group [common.sex==1, common.age_group==2].
    val expectedReportSummary12 = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 4091069
            standardDeviation = 9420.159895996094
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a55c10a65-8de3-46c9-980a-de54b9565b5f"
        }
        measurementResults += measurementResult {
          reach = reachResult {
            value = 4116486
            standardDeviation = 9420.159895996094
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a820f0601-8e2e-4221-94a5-0d9d1d477c49"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 4075739
              standardDeviation = 103183.18151361225
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                value = 3450.033742465593
                standardDeviation = 3253.2458892873115
              }
              bins += binResult {
                label = "2"
                value = 1725.0168712327966
                standardDeviation = 3238.0460772496767
              }
              bins += binResult {
                label = "3"
                standardDeviation = 3223.3558207024994
              }
              bins += binResult {
                label = "4"
                standardDeviation = 3223.3558207024994
              }
              bins += binResult {
                label = "5"
                value = 4070563.9493863015
                standardDeviation = 103254.58360196998
              }
            }
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ab5b4f513-624f-4e64-87aa-dcfee2a08cd4"
        }
        measurementResults += measurementResult {
          impressionCount = impressionCountResult {
            value = 47516912
            standardDeviation = 361974.60188177926
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ab6b41793-c7e6-4da0-92d0-fef36980f170"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        dataProviders += "edp1"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 4101959
            standardDeviation = 103123.07369317865
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a5e6e4318-4c8b-4be8-84da-51ab05ad42a1"
        }
        measurementResults += measurementResult {
          reach = reachResult {
            value = 4098766
            standardDeviation = 103123.07369317865
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ab80afd6f-782e-44d8-a579-c6c7ee4662f8"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        dataProviders += "edp1"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          impressionCount = impressionCountResult {
            value = 65152282
            standardDeviation = 510226.05412966275
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a1bf0e6be-17ea-4af0-bfe1-85b885280513"
        }
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 4073399
              standardDeviation = 102490.16730804376
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                value = 659.9998379731919
                standardDeviation = 3223.469791075696
              }
              bins += binResult {
                label = "2"
                value = 4259.9989541906025
                standardDeviation = 3223.469791075696
              }
              bins += binResult {
                label = "3"
                value = 4859.9988068935045
                standardDeviation = 3223.469791075696
              }
              bins += binResult {
                label = "5"
                value = 4063619.0024009426
                standardDeviation = 102946.78989273963
              }
              bins += binResult {
                label = "4"
                standardDeviation = 3223.469791075696
              }
            }
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a69f8ba0b-95b4-4d33-8ac7-835633e10a72"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 1666066
            standardDeviation = 9420.159895996094
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ac636b58f-1af1-40c8-afeb-fbb4fa7c5217"
        }
        measurementResults += measurementResult {
          reach = reachResult {
            value = 1654276
            standardDeviation = 9420.159895996094
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ac7c15fa0-d3d2-4f56-905a-312bfd171aff"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        measurementResults += measurementResult {
          impressionCount = impressionCountResult {
            value = 17671264
            standardDeviation = 359592.937507083
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4dbe3175-e047-4dfa-964a-00022fbae865"
        }
        measurementResults += measurementResult {
          reachAndFrequency = reachAndFrequencyResult {
            reach = reachResult {
              value = 1717079
              standardDeviation = 102506.62424766447
            }
            frequency = frequencyResult {
              bins += binResult {
                label = "1"
                standardDeviation = 3228.0602405038294
              }
              bins += binResult {
                label = "2"
                value = 2360.587605368836
                standardDeviation = 3248.2700501747786
              }
              bins += binResult {
                label = "3"
                value = 931.8108968561196
                standardDeviation = 3235.3231393200517
              }
              bins += binResult {
                label = "4"
                standardDeviation = 3228.0602405038294
              }
              bins += binResult {
                label = "5"
                value = 1713786.601497775
                standardDeviation = 102513.74351444335
              }
            }
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/af1c4be36-cc5a-4239-99e3-482e31f5b64e"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        uniqueReachTarget = "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 2514100
            standardDeviation = 102490.16730804376
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a0978c883-62d8-4f53-8b15-124609732cdd"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        uniqueReachTarget = "edp2"
        leftHandSideTargets += "edp2"
        rightHandSideTargets += "edp1"
        measurementResults += measurementResult {
          reach = reachResult {
            value = 284600
            standardDeviation = 102490.16730804376
          }
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aff225199-571a-4e16-b33d-a71cbbff8a70"
        }
      }
      demographicGroups += "MALE_AGE_GROUP2"
      population = 75427
    }
    assertThat(reportSummaries).contains(expectedReportSummary12)
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
