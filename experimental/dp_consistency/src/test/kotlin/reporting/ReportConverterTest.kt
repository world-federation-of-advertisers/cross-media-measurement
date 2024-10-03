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

package experimental.dp_consistency.src.test.kotlin.reporting

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import experimental.dp_consistency.src.main.kotlin.reporting.ReportConversion
import experimental.dp_consistency.src.main.proto.reporting.MeasurementDetailKt.measurementResult
import experimental.dp_consistency.src.main.proto.reporting.measurementDetail
import experimental.dp_consistency.src.main.proto.reporting.reportSummary
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ReportConverterTest {
  @Test
  fun `report as json string is successfully converted to report summary proto`() {
    val reportSummary = ReportConversion.convertJsontoReportSummaries(REPORT_SAMPLE)
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
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportConversion.convertJsontoReportSummaries(REPORT_WITH_UNSPECIFIED_STATE)
      }

    assertThat(exception).hasMessageThat().contains("not supported")
  }

  @Test
  fun `report with failed measurement fails to be converted to report summary proto`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportConversion.convertJsontoReportSummaries(REPORT_WITH_FAILED_MEASUREMENT)
      }

    assertThat(exception).hasMessageThat().contains("not supported")
  }

  companion object {
    private val REPORT_WITH_UNSPECIFIED_STATE =
      """
      {
        "name": "Sample report",
        "reportingMetricEntries": [
          {
            "key": "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48",
            "value": {
              "metricCalculationSpecs": [
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union"
              ]
            }
          }
        ],
        "state": "STATE_UNSPECIFIED",
        "metricCalculationResults": [],
        "reportSchedule": "",
        "tags": {
          "campaign_name": "dc5s1",
          "media_types": "DISPLAY VIDEO",
          "report_name": "DC5SEP1_incremental",
          "start_date": "2024-01-01T00:00:00Z",
          "end_date": "2024-01-08T10:00:00Z",
          "brands": "'Cloud' 'Global'",
          "target": "'dataProviders/LkJlb3UZkY8' 'dataProviders/TbsgjIrmbj0'",
          "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48": "{measured_entity=dataProviders/LkJlb3UZkY8, viewability=, set_operation=union,incrementality,cumulative,marked unions for reach only, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=dataProviders/LkJlb3UZkY8, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=DISPLAY, anchor=dataProviders/TbsgjIrmbj0, primitive_egroup_id=aDnfoB_curE, measurement_entities=dataProviders/LkJlb3UZkY8, measurement_policy_incrementality=AMI, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=AMI}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union": "{metrics=impressions,reach, grouping=-, cumulative=false, common_filter=-, set_operation=union, metric_frequency=-}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative": "{metrics=reach, grouping=-, cumulative=true, common_filter=-, set_operation=cumulative, metric_frequency=weekly1}"
        },
        "reportingInterval": {
          "reportStart": {
            "year": 2024,
            "month": 1,
            "day": 1,
            "hours": 0,
            "minutes": 0,
            "seconds": 0,
            "nanos": 0,
            "utcOffset": "0s"
          },
          "reportEnd": {
            "year": 2024,
            "month": 1,
            "day": 8
          }
        }
      }
      """

    private val REPORT_WITH_FAILED_MEASUREMENT =
      """
      {
        "name": "Sample report",
        "reportingMetricEntries": [
          {
            "key": "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48",
            "value": {
              "metricCalculationSpecs": [
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union"
              ]
            }
          }
        ],
        "state": "SUCCEEDED",
        "metricCalculationResults": [
          {
            "displayName": "METRICS_reach_CUMULATIVE_true",
            "reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48",
            "resultAttributes": [
              {
                "groupingPredicates": [],
                "metricSpec": {},
                "timeInterval": {
                  "startTime": "2024-03-11T00:00:00Z",
                  "endTime": "2024-03-17T00:00:00Z"
                },
                "metricResult": {
                  "reach": {
                    "value": "24129432",
                    "univariateStatistics": {
                      "standardDeviation": 1.0
                    }
                  },
                  "cmmsMeasurements": [
                    "measurementConsumers/TjyUnormbAg/measurements/Fjz-heBVF-A"
                  ]
                },
                "filter": "",
                "metric": "measurementConsumers/TjyUnormbAg/metrics/total/ami/00",
                "state": "FAILED"
              }
            ],
            "metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative"
          }
        ],
        "reportSchedule": "",
        "tags": {
          "campaign_name": "dc5s1",
          "media_types": "DISPLAY VIDEO",
          "report_name": "DC5SEP1_incremental",
          "start_date": "2024-01-01T00:00:00Z",
          "end_date": "2024-01-08T10:00:00Z",
          "brands": "'Cloud' 'Global'",
          "target": "'dataProviders/LkJlb3UZkY8' 'dataProviders/TbsgjIrmbj0'",
          "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48": "{measured_entity=dataProviders/LkJlb3UZkY8, viewability=, set_operation=union,incrementality,cumulative,marked unions for reach only, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=dataProviders/LkJlb3UZkY8, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=DISPLAY, anchor=dataProviders/TbsgjIrmbj0, primitive_egroup_id=aDnfoB_curE, measurement_entities=dataProviders/LkJlb3UZkY8, measurement_policy_incrementality=AMI, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=AMI}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union": "{metrics=impressions,reach, grouping=-, cumulative=false, common_filter=-, set_operation=union, metric_frequency=-}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative": "{metrics=reach, grouping=-, cumulative=true, common_filter=-, set_operation=cumulative, metric_frequency=weekly1}"
        },
        "reportingInterval": {
          "reportStart": {
            "year": 2024,
            "month": 1,
            "day": 1,
            "hours": 0,
            "minutes": 0,
            "seconds": 0,
            "nanos": 0,
            "utcOffset": "0s"
          },
          "reportEnd": {
            "year": 2024,
            "month": 1,
            "day": 8
          }
        }
      }
      """
    private val REPORT_SAMPLE =
      """
      {
        "name": "Sample report",
        "reportingMetricEntries": [
          {
            "key": "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48",
            "value": {
              "metricCalculationSpecs": [
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union"
              ]
            }
          }
        ],
        "state": "SUCCEEDED",
        "metricCalculationResults": [
          {
            "displayName": "METRICS_reach_CUMULATIVE_true",
            "reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48",
            "resultAttributes": [
              {
                "groupingPredicates": [],
                "metricSpec": {},
                "timeInterval": {
                  "startTime": "2024-03-11T00:00:00Z",
                  "endTime": "2024-03-17T00:00:00Z"
                },
                "metricResult": {
                  "reach": {
                    "value": "24129432",
                    "univariateStatistics": {
                      "standardDeviation": 1.0
                    }
                  },
                  "cmmsMeasurements": [
                    "measurementConsumers/TjyUnormbAg/measurements/Fjz-heBVF-A"
                  ]
                },
                "filter": "",
                "metric": "measurementConsumers/TjyUnormbAg/metrics/total/ami/00",
                "state": "SUCCEEDED"
              },
              {
                "groupingPredicates": [],
                "metricSpec": {},
                "timeInterval": {
                  "startTime": "2024-03-11T00:00:00Z",
                  "endTime": "2024-03-24T00:00:00Z"
                },
                "metricResult": {
                  "reach": {
                    "value": "29152165",
                    "univariateStatistics": {
                      "standardDeviation": 1.0
                    }
                  },
                  "cmmsMeasurements": [
                    "measurementConsumers/TjyUnormbAg/measurements/Fjz-heBVF-A"
                  ]
                },
                "filter": "",
                "metric": "measurementConsumers/TjyUnormbAg/metrics/total/ami/01",
                "state": "SUCCEEDED"
              },
              {
                "groupingPredicates": [],
                "metricSpec": {},
                "timeInterval": {
                  "startTime": "2024-03-11T00:00:00Z",
                  "endTime": "2024-03-31T00:00:00Z"
                },
                "metricResult": {
                  "reach": {
                    "value": "31474050",
                    "univariateStatistics": {
                      "standardDeviation": 1.0
                    }
                  },
                  "cmmsMeasurements": [
                    "measurementConsumers/TjyUnormbAg/measurements/Fjz-heBVF-A"
                  ]
                },
                "filter": "",
                "metric": "measurementConsumers/TjyUnormbAg/metrics/total/ami/02",
                "state": "SUCCEEDED"
              }
            ],
            "metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative"
          },
          {
            "displayName": "METRICS_reach_CUMULATIVE_true",
            "reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48",
            "resultAttributes": [
              {
                "groupingPredicates": [],
                "metricSpec": {
                  "reachAndFrequency": {
                    "maximumFrequency": 5,
                    "multipleDataProviderParams": {
                      "reachPrivacyParams": {
                        "epsilon": 0.0033,
                        "delta": 1e-12
                      },
                      "frequencyPrivacyParams": {
                        "epsilon": 0.115,
                        "delta": 1e-12
                      },
                      "vidSamplingInterval": {
                        "start": 0.16,
                        "width": 0.016666668
                      }
                    },
                    "singleDataProviderParams": {
                      "reachPrivacyParams": {
                        "epsilon": 0.0033,
                        "delta": 1e-12
                      },
                      "frequencyPrivacyParams": {
                        "epsilon": 0.115,
                        "delta": 1e-12
                      },
                      "vidSamplingInterval": {
                        "start": 0.16,
                        "width": 0.016666668
                      }
                    }
                  }
                },
                "timeInterval": {
                  "startTime": "2023-05-28T00:00:00Z",
                  "endTime": "2023-06-11T00:00:00Z"
                },
                "metricResult": {
                  "reachAndFrequency": {
                    "reach": {
                      "value": "1000",
                      "univariateStatistics": {
                        "standardDeviation": 102011.27564649425
                      }
                    },
                    "frequencyHistogram": {
                      "bins": [
                        {
                          "label": "1",
                          "binResult": {
                            "value": 100
                          },
                          "resultUnivariateStatistics": {
                            "standardDeviation": 29448.118727440287
                          },
                          "relativeUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          },
                          "kPlusUnivariateStatistics": {
                            "standardDeviation": 102011.27564649425
                          },
                          "relativeKPlusUnivariateStatistics": {
                            "standardDeviation": 0.0
                          }
                        },
                        {
                          "label": "2",
                          "binResult": {
                            "value": 200
                          },
                          "resultUnivariateStatistics": {
                            "standardDeviation": 29448.118727440287
                          },
                          "relativeUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          },
                          "kPlusUnivariateStatistics": {
                            "standardDeviation": 106176.70203773731
                          },
                          "relativeKPlusUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          }
                        },
                        {
                          "label": "3",
                          "binResult": {
                            "value": 300
                          },
                          "resultUnivariateStatistics": {
                            "standardDeviation": 29448.118727440287
                          },
                          "relativeUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          },
                          "kPlusUnivariateStatistics": {
                            "standardDeviation": 106176.70203773731
                          },
                          "relativeKPlusUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          }
                        },
                        {
                          "label": "4",
                          "binResult": {
                            "value": 400
                          },
                          "resultUnivariateStatistics": {
                            "standardDeviation": 29448.118727440287
                          },
                          "relativeUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          },
                          "kPlusUnivariateStatistics": {
                            "standardDeviation": 106176.70203773731
                          },
                          "relativeKPlusUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          }
                        },
                        {
                          "label": "5",
                          "binResult": {
                            "value": 0
                          },
                          "resultUnivariateStatistics": {
                            "standardDeviation": 106176.70203773731
                          },
                          "relativeUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          },
                          "kPlusUnivariateStatistics": {
                            "standardDeviation": 106176.70203773731
                          },
                          "relativeKPlusUnivariateStatistics": {
                            "standardDeviation": 0.28867513459481289
                          }
                        }
                      ]
                    }
                  },
                  "cmmsMeasurements": [
                    "measurementConsumers/TjyUnormbAg/measurements/G0UtIeBVF48"
                  ]
                },
                "filter": "",
                "metric": "measurementConsumers/TjyUnormbAg/metrics/ami/union/00",
                "state": "SUCCEEDED"
              }
            ],
            "metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union"
          }
        ],
        "reportSchedule": "",
        "tags": {
          "campaign_name": "dc5s1",
          "media_types": "DISPLAY VIDEO",
          "report_name": "DC5SEP1_incremental",
          "start_date": "2024-01-01T00:00:00Z",
          "end_date": "2024-01-08T10:00:00Z",
          "brands": "'Cloud' 'Global'",
          "target": "'dataProviders/LkJlb3UZkY8' 'dataProviders/TbsgjIrmbj0'",
          "measurementConsumers/TjyUnormbAg/reportingSets/f4fee2bcb73a4cf08a6bd1ad33a61e48": "{measured_entity=dataProviders/LkJlb3UZkY8, viewability=, set_operation=union,incrementality,cumulative,marked unions for reach only, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=dataProviders/LkJlb3UZkY8, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=DISPLAY, anchor=dataProviders/TbsgjIrmbj0, primitive_egroup_id=aDnfoB_curE, measurement_entities=dataProviders/LkJlb3UZkY8, measurement_policy_incrementality=AMI, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=AMI}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/union": "{metrics=impressions,reach, grouping=-, cumulative=false, common_filter=-, set_operation=union, metric_frequency=-}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/ami/cumulative": "{metrics=reach, grouping=-, cumulative=true, common_filter=-, set_operation=cumulative, metric_frequency=weekly1}"
        },
        "reportingInterval": {
          "reportStart": {
            "year": 2024,
            "month": 1,
            "day": 1,
            "hours": 0,
            "minutes": 0,
            "seconds": 0,
            "nanos": 0,
            "utcOffset": "0s"
          },
          "reportEnd": {
            "year": 2024,
            "month": 1,
            "day": 8
          }
        }
      }
      """
  }
}
