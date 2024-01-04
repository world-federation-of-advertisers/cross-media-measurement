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
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.Report.DemographicMetricsByTimeInterval
import org.wfanet.measurement.reporting.bff.v1alpha.Report.DemographicMetricsByTimeInterval.DemoBucket
import org.wfanet.measurement.reporting.bff.v1alpha.ReportView
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.demographicMetricsByTimeInterval
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.DemographicMetricsByTimeIntervalKt.demoBucket
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.DemographicMetricsByTimeIntervalKt.DemoBucketKt.sourceMetrics
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.DemographicMetricsByTimeIntervalKt.DemoBucketKt.SourceMetricsKt.impressionCountResult
import org.wfanet.measurement.reporting.bff.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.report
import org.wfanet.measurement.reporting.v2alpha.Report as BackendReport
import org.wfanet.measurement.reporting.v2alpha.Report.MetricCalculationResult
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt as BackendReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.report as backendReport
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.MetricResult.ResultCase
import com.google.type.interval

class ReportsService(private val backendReportsStub: BackendReportsGrpcKt.ReportsCoroutineStub) :
  ReportsGrpcKt.ReportsCoroutineImplBase() {
  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    // TODO(@bdomen-ggl): Still working on UX for pagination, so holding off for now.
    // Will hold off on internally looping the request until it becomes an issue (eg. no reports
    // returned)
    if (request.pageSize != 0 || request.pageToken.isNotEmpty()) {
      throw Status.UNIMPLEMENTED.withDescription("PageSize and PageToken not implemented yet")
        .asRuntimeException()
    }

    val backendRequest = listReportsRequest {
      parent = request.parent
      pageSize = 1000
    }

    val resp = backendReportsStub.listReports(backendRequest)

    // if (resp.nextPageToken.isNotEmpty()) {
    //   logger.warning { "Additional ListReport items. Not Loopping through additional pages." }
    // }

    // val view =
    //   if (request.view == ReportView.REPORT_VIEW_UNSPECIFIED) ReportView.REPORT_VIEW_BASIC
    //   else request.view
    // val results = listReportsResponse {
    //   resp.reportsList
    //     // .filter { it.tagsMap.containsKey("ui.halo-cmm.org") }
    //     .forEach { reports += it.toBffReport(view) }
    // }
    val results = listReportsResponse {
      reports += report{
        name = "TESTREPORT"
        reportId = "TESTREPORT"
        state = Report.State.SUCCEEDED
      }
    }
    return results
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    // val backendRequest = getReportRequest { name = request.name }
    val backendRequest = getReportRequest { name = "measurementConsumers/VCTqwV_vFXw/reports/ui-report-non-cumulative-non-uniq-6" }

    val resp = backendReportsStub.getReport(backendRequest)

    val view =
      if (request.view == ReportView.REPORT_VIEW_UNSPECIFIED) ReportView.REPORT_VIEW_FULL
      else request.view

    val result = resp.toBffReport(view)
    return result
  }

  private fun BackendReport.toBffReport(view: ReportView): Report {
    val source = this

    return when (view) {
      ReportView.REPORT_VIEW_BASIC -> source.toBasicReport()
      ReportView.REPORT_VIEW_FULL -> source.toFullReport()
      else ->
        throw Status.INVALID_ARGUMENT.withDescription("View type must be specified")
          .asRuntimeException()
    }
  }

  private fun BackendReport.toBasicReport(): Report {
    val source = this

    return report {
      reportId = source.name
      name = source.name
      state = source.state.toBffState()
    }
  }

  private fun capNumber(num: Double): Double {
    if (num.isNaN() || num < 0) {
      return 0.0;
    } else {
      return num;
    }
  }

  private fun capNumber(num: Long): Long {
    if (num < 0) {
      return 0L;
    } else {
      return num;
    }
  }

  private fun BackendReport.toFullReport(): Report {
    val source = this

    val demoMap = mapOf(
      "person.gender == 1" to "Male",
      "person.gender == 2" to "Female",
      "person.age_group == 1" to "18-35",
      "person.age_group == 2" to "36-54",
      "person.age_group == 3" to "55+",
    )

    // Map the result attributes to the reporting sets so we can set the RS name later
    val rsByMcr = mutableMapOf<BackendReport.MetricCalculationResult.ResultAttribute, String>()
    for (mcr in source.metricCalculationResultsList) {
      val rsName = mcr.reportingSet // TODO: This will come from display_name on the RS, will also need to fetch the reporting set to get this
      for (ra in mcr.resultAttributesList) {
        rsByMcr[ra.toString()] = rsName
      }
    }

    val ras = source.metricCalculationResultsList.map{it.resultAttributesList}.flatten()
    val timeGroup = ras.groupBy{it.timeInterval}

    // Build report...
    val rep = report {
      name = source.name
      reportId = source.name
      state = source.state.toBffState()

      // By time...
      // Result Attributes grouped by interval
      for(time in timeGroup) {
        timeInterval += demographicMetricsByTimeInterval {
          timeInterval = interval {
            startTime = time.key.startTime
            endTime = time.key.endTime
          }

          // By demo...
          // Result Attributes from the time group, grouped by grouping predicates
          val demoGroup = time.value.groupBy{it.groupingPredicatesList.map{demoMap[it]}.joinToString(prefix = "", postfix = "", separator = ";")}
          for (demo in demoGroup) {
            demoBucket += demoBucket {
              demoCategoryName = demo.key

              // By source...
              // Result Attributes from the time group and grouping predicate
              // The Result Attributes come multiple times in some cases (frequency and impressions are separate)
              // so they need to be grouped and merged.
              val pubGroup = demo.value.groupBy{rsByMcr[it]!!}
              for (pub in pubGroup) {
                val pubName = pub.key
                val metrs = sourceMetrics {
                  sourceName = pubName
                  for (resultAttribute in pub.value) {
                    val oneOfCase = resultAttribute.metricResult.getResultCase()
                    when(oneOfCase) {
                      ResultCase.REACH -> {
                        reach = resultAttribute.metricResult.reach.value
                      }
                      ResultCase.REACH_AND_FREQUENCY -> {
                        reach = capNumber(resultAttribute.metricResult.reachAndFrequency.reach.value)
                        val bins = resultAttribute.metricResult.reachAndFrequency.frequencyHistogram.binsList.sortedBy{it.label.toInt()}.reversed()
                        var runningCount = 0.0
                        for (bin in bins) {
                          runningCount = runningCount + capNumber(bin.binResult.value)
                          frequencyHistogram[bin.label.toInt()] = runningCount
                        }
                      }
                      ResultCase.IMPRESSION_COUNT -> {
                        impressionCount = impressionCountResult {
                          count = capNumber(resultAttribute.metricResult.impressionCount.value)
                          standardDeviation = resultAttribute.metricResult.impressionCount.univariateStatistics.standardDeviation
                        }
                      }
                      else -> println("error")
                    }
                  }
                }
                if (!pubName.contains("union")) {
                  perPublisherSource += metrs
                } else {
                  unionSource = metrs
                }
              }
            }
          }
        }
      }
    }

    return rep
  }

  private fun BackendReport.State.toBffState(): Report.State {
    val source = this
    return when (source) {
      BackendReport.State.STATE_UNSPECIFIED -> Report.State.STATE_UNSPECIFIED
      BackendReport.State.RUNNING -> Report.State.RUNNING
      BackendReport.State.SUCCEEDED -> Report.State.SUCCEEDED
      BackendReport.State.FAILED -> Report.State.FAILED
      else -> Report.State.STATE_UNSPECIFIED
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
