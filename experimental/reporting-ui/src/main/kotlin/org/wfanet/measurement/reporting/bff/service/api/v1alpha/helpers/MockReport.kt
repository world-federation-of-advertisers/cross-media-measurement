// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.bff.service.api.v1alpha

import com.google.protobuf.Timestamp
import io.grpc.Status
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.impressionCountResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachAndFrequencyResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.histogramResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.bin
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.binResult
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt.metricCalculationResult
import org.wfanet.measurement.reporting.v2alpha.ReportKt.reportingMetricEntry
import org.wfanet.measurement.reporting.v2alpha.ReportKt.MetricCalculationResultKt.resultAttribute
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import com.google.type.interval

class Temp(
    val reportingSet: String,
    val helpers: List<Helper>,
    val cumulative: Boolean,
) {}

class GenerateMockReport() {
    companion object {
        fun GenerateReport(): Report {
            val reportingSets = listOf(
                Temp(edp1_cumulative_reportingSet, edp1_cumulative_data, edp1_cumulative_iscumulative),
                Temp(edp2_cumulative_reportingSet, edp2_cumulative_data, edp2_cumulative_iscumulative),
                Temp(edp3_cumulative_reportingSet, edp3_cumulative_data, edp3_cumulative_iscumulative),
                Temp(union_cumulative_reportingSet, union_cumulative_data, union_cumulative_iscumulative),

                Temp(edp1_unique_cumulative_reportingSet, edp1_unique_cumulative_data, edp1_unique_cumulative_iscumulative),
                Temp(edp2_unique_cumulative_reportingSet, edp2_unique_cumulative_data, edp2_unique_cumulative_iscumulative),
                Temp(edp3_unique_cumulative_reportingSet, edp3_unique_cumulative_data, edp3_unique_cumulative_iscumulative),

                Temp(edp1_non_cumulative_reportingSet, edp1_non_cumulative_data, edp1_non_cumulative_iscumulative),
                Temp(edp2_non_cumulative_reportingSet, edp2_non_cumulative_data, edp2_non_cumulative_iscumulative),
                Temp(edp3_non_cumulative_reportingSet, edp3_non_cumulative_data, edp3_non_cumulative_iscumulative),
                // Temp(union_non_cumulative_reportingSet, union_non_cumulative_data, union_non_cumulative_iscumulative),

                // Temp(edp1_unique_non_cumulative_reportingSet, edp1_unique_non_cumulative_data, edp1_unique_non_cumulative_iscumulative),
                // Temp(edp2_unique_non_cumulative_reportingSet, edp2_unique_non_cumulative_data, edp2_unique_non_cumulative_iscumulative),
                // Temp(edp3_unique_non_cumulative_reportingSet, edp3_unique_non_cumulative_data, edp3_unique_non_cumulative_iscumulative),
            )
            val dates = edp1_cumulative_data.groupBy{Pair(it.start, it.end)}.keys

            return report {
                name = "Fake Report"
                state = Report.State.SUCCEEDED
                for (rs in reportingSets) {
                    reportingMetricEntries += reportingMetricEntry {
                        key = rs.reportingSet
                    }
                }
                timeIntervals = timeIntervals {
                    for (date in dates) {
                        timeIntervals += interval {
                            startTime = Instant.ofEpochSecond(date.first).toProtoTime()
                            endTime = Instant.ofEpochSecond(date.second).toProtoTime()
                        }
                    }
                }
                tags.put("ui.halo-cmm.org", "1")
                for (rs in reportingSets) {
                    metricCalculationResults += metricCalculationResult {
                        reportingSet = rs.reportingSet
                        cumulative = rs.cumulative
                        for (dataPoint in rs.helpers) {
                            resultAttributes += resultAttribute {
                                for (group in dataPoint.groups) {
                                    groupingPredicates += group
                                }
                                timeInterval = interval {
                                    startTime = Instant.ofEpochSecond(dataPoint.start).toProtoTime()
                                    endTime = Instant.ofEpochSecond(dataPoint.end).toProtoTime()
                                }
                                metricResult = metricResult {
                                    if (dataPoint.reach != null && dataPoint.frequencies != null) {
                                        reachAndFrequency = reachAndFrequencyResult {
                                            reach = reachResult {
                                                value = dataPoint.reach!!
                                            }
                                            frequencyHistogram = histogramResult {
                                                for (bin in dataPoint.frequencies!!) {
                                                    bins += bin {
                                                        label = bin.label
                                                        binResult = binResult {
                                                            value = bin.value
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (dataPoint.reach != null && dataPoint.frequencies == null) {
                                        reach = reachResult {
                                            value = dataPoint.reach!!
                                        }
                                    }
                                    if (dataPoint.impression != null) {
                                        impressionCount = impressionCountResult {
                                            value = dataPoint.impression!!
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
