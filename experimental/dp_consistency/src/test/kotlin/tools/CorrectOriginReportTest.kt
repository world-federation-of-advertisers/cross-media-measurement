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

package experimental.dp_consistency.src.test.kotlin.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.copy
import com.google.type.interval
import experimental.dp_consistency.src.main.kotlin.tools.CorrectOriginReport
import experimental.dp_consistency.src.main.kotlin.tools.getReportFromJsonString
import experimental.dp_consistency.src.main.kotlin.tools.toReportSummaries
import experimental.dp_consistency.src.main.proto.reporting.measurementDetail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachResult
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt.MetricCalculationResultKt.resultAttribute
import org.wfanet.measurement.reporting.v2alpha.ReportKt.metricCalculationResult
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.univariateStatistics

@RunWith(JUnit4::class)
class CorrectOriginReportTest {
  @Test
  fun `run correct report successfully`() {
    val reportTemplate = getReportFromJsonString(REPORT_TEMPLATE)
    val report =
      reportTemplate
        .updateReportWithCumulativeReachMeasurements(
          "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp1",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
          listOf(
            6333L,
            3585L,
            7511L,
            1037L,
            0L,
            10040L,
            0L,
            2503L,
            7907L,
            0L,
            0L,
            0L,
            0L,
            1729L,
            0L,
            1322L,
          ),
        )
        .updateReportWithCumulativeReachMeasurements(
          "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp1",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
          listOf(0L, 2196L, 2014L, 0L, 129L, 0L, 2018L, 81L, 0L, 0L, 288L, 0L, 0L, 0L, 0L, 0L),
        )
        .updateReportWithCumulativeReachMeasurements(
          "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp2",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
          listOf(
            24062000L,
            29281000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
          ),
        )
        .updateReportWithCumulativeReachMeasurements(
          "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp2",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
          listOf(
            24062000L,
            29281000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
            31569000L,
          ),
        )
        .updateReportWithCumulativeReachMeasurements(
          "measurementConsumers/TjyUnormbAg/reportingSets/ami/union",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
          listOf(
            24129432L,
            29152165L,
            31474050L,
            31352346L,
            31685183L,
            31425302L,
            31655739L,
            31643458L,
            31438532L,
            31600739L,
            31386917L,
            31785206L,
            31627169L,
            31453865L,
            31582783L,
            31806702L,
          ),
        )
        .updateReportWithCumulativeReachMeasurements(
          "measurementConsumers/TjyUnormbAg/reportingSets/mrc/union",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
          listOf(
            24299684L,
            29107595L,
            31680517L,
            31513613L,
            32127776L,
            31517198L,
            31786057L,
            31225783L,
            31237872L,
            31901620L,
            31720183L,
            31263524L,
            31775635L,
            31917650L,
            31478465L,
            31784354L,
          ),
        )
    assertThat(report.hasConsistentMeasurements()).isEqualTo(false)
    val reportCorrector: CorrectOriginReport = CorrectOriginReport()
    val reportAsJson = JsonFormat.printer().print(report)
    val correctedReportAsJson = reportCorrector.correctReport(reportAsJson)
    val correctedReport = getReportFromJsonString(correctedReportAsJson)
    assertThat(correctedReport.hasConsistentMeasurements()).isEqualTo(true)
  }

  companion object {
    val reportStartTimestampInSeconds = 1678502400L // "2024-03-11T00:00:00Z".
    val measurementFrequencyInSeconds = 604800L // 7 days has 604800 seconds.
    val REPORT_TEMPLATE =
      """
      {
      	"name": "measurementConsumers/TjyUnormbAg/reports/c1acdfb43b3476998977072c89efcc9",
      	"reportingMetricEntries": [
      		{
      			"key": "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp1",
      			"value": {
      				"metricCalculationSpecs": [
      					"measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      				]
      			}
      		},
      		{
      			"key": "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp2",
      			"value": {
      				"metricCalculationSpecs": [
      					"measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      				]
      			}
      		},
      		{
      			"key": "measurementConsumers/TjyUnormbAg/reportingSets/ami/union",
      			"value": {
      				"metricCalculationSpecs": [
      					"measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      				]
      			}
      		},
          {
      			"key": "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp1",
      			"value": {
      				"metricCalculationSpecs": [
      					"measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      				]
      			}
      		},
      		{
      			"key": "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp2",
      			"value": {
      				"metricCalculationSpecs": [
      					"measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      				]
      			}
      		},
      		{
      			"key": "measurementConsumers/TjyUnormbAg/reportingSets/mrc/union",
      			"value": {
      				"metricCalculationSpecs": [
      					"measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative",
                "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      				]
      			}
      		}
      	],
      	"state": "SUCCEEDED",
      	"metricCalculationResults": [
      		{
      			"displayName": "METRICS_frequency_CUMULATIVE_false",
      			"reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/ami/union",
      			"resultAttributes": [
      				{
      					"groupingPredicates": [],
      					"metricSpec": {},
      					"timeInterval": {
      						"startTime": "2024-03-11T00:00:00Z",
      						"endTime": "2024-06-30T00:00:00Z"
      					},
      					"metricResult": {
      						"reachAndFrequency": {
      							"reach": {
      								"value": "31477620",
      								"univariateStatistics": {
      									"standardDeviation": 1.0
      								}
      							},
      							"frequencyHistogram": {
      								"bins": [
      									{
      										"label": "1",
      										"binResult": {
      											"value": "6917769"
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
      											"value": "4048567"
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
      											"value": "2996233"
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
      											"value": "2051362"
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
      											"value": "15463689"
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
      					"metric": "measurementConsumers/TjyUnormbAg/metrics/ami/union/",
      					"state": "SUCCEEDED"
      				}
      			],
      			"metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      		},
          {
      			"displayName": "METRICS_frequency_CUMULATIVE_false",
      			"reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/mrc/union",
      			"resultAttributes": [
      				{
      					"groupingPredicates": [],
      					"metricSpec": {},
      					"timeInterval": {
      						"startTime": "2024-03-11T00:00:00Z",
      						"endTime": "2024-06-30T00:00:00Z"
      					},
      					"metricResult": {
      						"reachAndFrequency": {
      							"reach": {
      								"value": "31542065",
      								"univariateStatistics": {
      									"standardDeviation": 1.0
      								}
      							},
      							"frequencyHistogram": {
      								"bins": [
      									{
      										"label": "1",
      										"binResult": {
      											"value": "6685968"
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
      											"value": "4089515"
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
      											"value": "3214415"
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
      											"value": "1940715"
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
      											"value": "15611452"
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
      					"metric": "measurementConsumers/TjyUnormbAg/metrics/mrc/union",
      					"state": "SUCCEEDED"
      				}
      			],
      			"metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      		},
          {
      			"displayName": "METRICS_frequency_CUMULATIVE_false",
      			"reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp1",
      			"resultAttributes": [
      				{
      					"groupingPredicates": [],
      					"metricSpec": {},
      					"timeInterval": {
      						"startTime": "2024-03-11T00:00:00Z",
      						"endTime": "2024-06-30T00:00:00Z"
      					},
      					"metricResult": {
      						"reachAndFrequency": {
      							"reach": {
      								"value": "1",
      								"univariateStatistics": {
      									"standardDeviation": 1.0
      								}
      							},
      							"frequencyHistogram": {
      								"bins": [
      									{
      										"label": "1",
      										"binResult": {
      											"value": "1"
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
      											"value": "0"
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
      											"value": "0"
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
      											"value": "0"
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
      											"value": "0"
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
      					"metric": "measurementConsumers/TjyUnormbAg/metrics/ami/edp1",
      					"state": "SUCCEEDED"
      				}
      			],
      			"metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      		},
          {
      			"displayName": "METRICS_frequency_CUMULATIVE_false",
      			"reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp1",
      			"resultAttributes": [
      				{
      					"groupingPredicates": [],
      					"metricSpec": {},
      					"timeInterval": {
      						"startTime": "2024-03-11T00:00:00Z",
      						"endTime": "2024-06-30T00:00:00Z"
      					},
      					"metricResult": {
      						"reachAndFrequency": {
      							"reach": {
      								"value": "1",
      								"univariateStatistics": {
      									"standardDeviation": 1.0
      								}
      							},
      							"frequencyHistogram": {
      								"bins": [
      									{
      										"label": "1",
      										"binResult": {
      											"value": "1"
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
      											"value": "0"
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
      											"value": "0"
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
      											"value": "0"
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
      											"value": "0"
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
      					"metric": "measurementConsumers/TjyUnormbAg/metrics/mrc/edp1",
      					"state": "SUCCEEDED"
      				}
      			],
      			"metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      		},
          {
      			"displayName": "METRICS_frequency_CUMULATIVE_false",
      			"reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp2",
      			"resultAttributes": [
      				{
      					"groupingPredicates": [],
      					"metricSpec": {},
      					"timeInterval": {
      						"startTime": "2024-03-11T00:00:00Z",
      						"endTime": "2024-06-30T00:00:00Z"
      					},
      					"metricResult": {
      						"reachAndFrequency": {
      							"reach": {
      								"value": "31569000",
      								"univariateStatistics": {
      									"standardDeviation": 102011.27564649425
      								}
      							},
      							"frequencyHistogram": {
      								"bins": [
      									{
      										"label": "1",
      										"binResult": {
      											"value": "6966000"
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
      											"value": "4029000"
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
      											"value": "3084000"
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
      											"value": "2043000"
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
      											"value": "15463689"
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
      					"metric": "measurementConsumers/TjyUnormbAg/metrics/ami/edp2",
      					"state": "SUCCEEDED"
      				}
      			],
      			"metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      		},
          {
      			"displayName": "METRICS_frequency_CUMULATIVE_false",
      			"reportingSet": "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp2",
      			"resultAttributes": [
      				{
      					"groupingPredicates": [],
      					"metricSpec": {},
      					"timeInterval": {
      						"startTime": "2024-03-11T00:00:00Z",
      						"endTime": "2024-06-30T00:00:00Z"
      					},
      					"metricResult": {
      						"reachAndFrequency": {
      							"reach": {
      								"value": "31569000",
      								"univariateStatistics": {
      									"standardDeviation": 102011.27564649425
      								}
      							},
      							"frequencyHistogram": {
      								"bins": [
      									{
      										"label": "1",
      										"binResult": {
      											"value": "6966000"
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
      											"value": "4029000"
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
      											"value": "3084000"
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
      											"value": "2043000"
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
      											"value": "15463689"
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
      					"metric": "measurementConsumers/TjyUnormbAg/metrics/mrc/edp2",
      					"state": "SUCCEEDED"
      				}
      			],
      			"metricCalculationSpec": "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union"
      		}
      	],
      	"createTime": "2024-07-23T16:03:10.726577Z",
      	"reportSchedule": "",
      	"tags": {
      		"brands": "'DOVE SELF ESTEEM PROJECT' 'Juci' 'Juci' 'LYNX EPIC FRESH' 'POT NOODLE'",
      		"campaign_name": "DC-3edp-cross-3m",
      		"end_date": "2024-06-30T00:00:00Z",
      		"filter_display_viewability_mrc": "display.viewable_50_percent_plus",
      		"filter_video_viewability_mrc": "video.viewable_100_percent",
      		"media_types": "DISPLAY OTHER VIDEO",
      		"report_name": "DC-3edp-cross-3media",
      		"start_date": "2024-03-11T00:00:00Z",
      		"target": "'edp1' 'edp2'",
          "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp1": "{measured_entity=edp1, viewability=, set_operation=union,cumulative, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=edp1, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=VIDEO, anchor=, primitive_egroup_id=HnCIGKw-5bQ, measurement_entities=, measurement_policy_incrementality=, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=AMI}",
          "measurementConsumers/TjyUnormbAg/reportingSets/ami/edp2": "{measured_entity=edp2, viewability=, set_operation=union,cumulative, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=edp2, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=VIDEO, anchor=, primitive_egroup_id=HnCIGKw-5bQ, measurement_entities=, measurement_policy_incrementality=, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=AMI}",
          "measurementConsumers/TjyUnormbAg/reportingSets/ami/union": "{measured_entity=edp1,edp2, viewability=, set_operation=union,cumulative, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=edp1,edp2, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=VIDEO, anchor=, primitive_egroup_id=HnCIGKw-5bQ, measurement_entities=, measurement_policy_incrementality=, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=AMI}",
          "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp1": "{measured_entity=edp1, viewability=, set_operation=union,cumulative, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=edp1, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=VIDEO, anchor=, primitive_egroup_id=HnCIGKw-5bQ, measurement_entities=, measurement_policy_incrementality=, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=MRC}",
          "measurementConsumers/TjyUnormbAg/reportingSets/mrc/edp2": "{measured_entity=edp2, viewability=, set_operation=union,cumulative, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=edp2, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=VIDEO, anchor=, primitive_egroup_id=HnCIGKw-5bQ, measurement_entities=, measurement_policy_incrementality=, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=MRC}",
          "measurementConsumers/TjyUnormbAg/reportingSets/mrc/union": "{measured_entity=edp1,edp2, viewability=, set_operation=union,cumulative, position_in_incrementality=, version=1.0, lhs_reporting_set_ids=, rhs_reporting_set_ids=, target=edp1,edp2, incrementality_type=ENTITY, video_completion=, channels=, unique_Reach_Target=, media_type=VIDEO, anchor=, primitive_egroup_id=HnCIGKw-5bQ, measurement_entities=, measurement_policy_incrementality=, campaign_id=0568606a-910b-4a36-a439-b38fc66fc4ae, measurement_policy=MRC}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/cumulative": "{metrics=reach, grouping=-, cumulative=true, common_filter=-, set_operation=cumulative, metric_frequency=weekly1}",
          "measurementConsumers/TjyUnormbAg/metricCalculationSpecs/union": "{metrics=frequency, grouping=-, cumulative=false, common_filter=-, set_operation=union, metric_frequency=-}"
      	},
      	"reportingInterval": {
      		"reportStart": {
      			"year": 2024,
      			"month": 3,
      			"day": 11,
      			"hours": 0,
      			"minutes": 0,
      			"seconds": 0,
      			"nanos": 0,
      			"utcOffset": "0s"
      		},
      		"reportEnd": {
      			"year": 2024,
      			"month": 6,
      			"day": 30
      		}
      	}
      }
    """

    private fun Report.updateReportWithCumulativeReachMeasurements(
      reportingSetName: String,
      measurementSpecName: String,
      reachMeasurements: List<Long>,
    ): Report {
      val result = metricCalculationResult {
        displayName = "METRICS_reach_CUMULATIVE_true"
        reportingSet = reportingSetName
        metricCalculationSpec = measurementSpecName
        resultAttributes +=
          reachMeasurements
            .withIndex()
            .map { (index, element) ->
              resultAttribute {
                metric = (reportingSetName + String.format("_cumulative_%05d", index))
                timeInterval = interval {
                  startTime = timestamp { seconds = reportStartTimestampInSeconds }
                  endTime = timestamp {
                    seconds =
                      reportStartTimestampInSeconds + measurementFrequencyInSeconds * (index + 1)
                  }
                }
                metricResult = metricResult {
                  reach = reachResult {
                    value = element
                    univariateStatistics = univariateStatistics { standardDeviation = 1.0 }
                  }
                }
                state = Metric.State.SUCCEEDED
              }
            }
            .toList()
      }

      return this.copy { metricCalculationResults += result }
    }

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
