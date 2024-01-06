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

class GenerateMockReport() {
    companion object {
        fun GenerateReport(): Report {
            val reportingSets = mapOf(
                union_data[0].reportingSet to union_data,
                edp1_data[0].reportingSet to edp1_data,
                edp2_data[0].reportingSet to edp2_data,
                edp3_data[0].reportingSet to edp3_data,
                edp1_unique_data[0].reportingSet to edp1_unique_data,
            )
            val dates = union_data.groupBy{Pair(it.start, it.end)}.keys

            return report {
                name = "Fake Report"
                state = Report.State.SUCCEEDED
                for (rs in reportingSets.keys) {
                    reportingMetricEntries += reportingMetricEntry {
                        key = rs
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
                // Union MCR
                for (rs in reportingSets) {
                    metricCalculationResults += metricCalculationResult {
                        reportingSet = rs.key
                        for (dataPoint in rs.value) {
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
