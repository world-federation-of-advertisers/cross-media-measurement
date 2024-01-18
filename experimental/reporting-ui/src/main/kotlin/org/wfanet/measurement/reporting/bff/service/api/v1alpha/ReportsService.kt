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

package org.wfanet.measurement.reporting.bff.service.api.v1alpha

import com.google.type.interval
import io.grpc.Status
import java.util.logging.Logger
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.DemographicMetricsByTimeIntervalKt.DemoBucketKt.SourceMetricsKt.impressionCountResult
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.DemographicMetricsByTimeIntervalKt.DemoBucketKt.sourceMetrics
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.DemographicMetricsByTimeIntervalKt.demoBucket
import org.wfanet.measurement.reporting.bff.v1alpha.ReportKt.demographicMetricsByTimeInterval
import org.wfanet.measurement.reporting.bff.v1alpha.ReportView
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.bff.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.report
import org.wfanet.measurement.reporting.v2alpha.MetricResult.ResultCase
import org.wfanet.measurement.reporting.v2alpha.Report as BackendReport
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt as BackendReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest

val UNIQUE_TYPE_TAG = "unique"
val UNION_TYPE_TAG = "union"
val ID_TAG = "ui.halo-cmm.org/reporting_set_id"
val TYPE_TAG = "ui.halo-cmm.org/reporting_set_type"
val DISPLAY_NAME_TAG = "ui.halo-cmm.org/display_name"

class ReportsService(
  private val backendReportsStub: BackendReportsGrpcKt.ReportsCoroutineStub,
  private val reportingSetsService: ReportingSetsService,
) : ReportsGrpcKt.ReportsCoroutineImplBase() {
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

    if (resp.nextPageToken.isNotEmpty()) {
      logger.warning { "Additional ListReport items. Not Loopping through additional pages." }
    }

    val view =
      if (request.view == ReportView.REPORT_VIEW_UNSPECIFIED) ReportView.REPORT_VIEW_BASIC
      else request.view
    val results = listReportsResponse {
      resp.reportsList
        .filter { it.tagsMap.containsKey("ui.halo-cmm.org") }
        .forEach { reports += it.toBffReport(view) }
    }

    return results
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val backendRequest = getReportRequest { name = request.name }
    val resp = backendReportsStub.getReport(backendRequest)

    if (!resp.tagsMap.containsKey("ui.halo-cmm.org")) {
      throw Status.INVALID_ARGUMENT.withDescription("Not a supported UI report")
        .asRuntimeException()
    }

    val view =
      if (request.view == ReportView.REPORT_VIEW_UNSPECIFIED) ReportView.REPORT_VIEW_FULL
      else request.view

    val result = resp.toBffReport(view)
    return result
  }

  private suspend fun BackendReport.toBffReport(view: ReportView): Report {
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
      reportId = source.name.substring(source.name.lastIndexOf("/") + 1)
      name = source.name
      state = source.state.toBffState()
    }
  }

  private fun Double.capNumber(): Double {
    val source = this
    if (source.isNaN() || source < 0) {
      return 0.0
    } else {
      return source
    }
  }

  private fun Long.capNumber(): Long {
    val source = this
    if (source < 0) {
      return 0L
    } else {
      return source
    }
  }

  val DEMOGRAPHIC_CODE_TO_STRING =
    mapOf(
      "person.gender == 1" to "Male",
      "person.gender == 2" to "Female",
      "person.age_group == 1" to "18-35",
      "person.age_group == 2" to "36-54",
      "person.age_group == 3" to "55+",
    )

  private suspend fun BackendReport.toFullReport(): Report {
    val source = this

    // Get Reporting Sets from Reporting Metric Entries
    //  Need to get both the individual and unique reporting sets
    //  to map them to a single result later.
    // TODO(@bdomen-ggl): Probably want to add get reporting set to ensure we don't have to paginate
    //  and to make the list more manageable as lots of reporting sets get created.
    val measurementConsumerName =
      source.name.substring(0, source.name.indexOf("/", source.name.indexOf("/")))
    val reportingSetsRequest = ListReportingSetsRequest(measurementConsumerName, 1000)
    val reportingSets = reportingSetsService.ListReportingSets(reportingSetsRequest)

    // Build the BFF report from the Backend report
    val rep = report {
      reportId = source.name
      name =
        if (source.tagsMap.containsKey(DISPLAY_NAME_TAG)) source.tagsMap[DISPLAY_NAME_TAG]!!
        else source.name
      state = source.state.toBffState()

      // Go through each time Interval...
      for (sourceTimeInterval in source.timeIntervals.timeIntervalsList) {
        timeInterval += demographicMetricsByTimeInterval {
          timeInterval = interval {
            startTime = sourceTimeInterval.startTime
            endTime = sourceTimeInterval.endTime
          }

          // Go through each metric calcualtion result
          // Stop here instead of flattening so pairing the reporting sets is easy
          demoBucket += demoBucket {
            for (metricCalculationResult in source.metricCalculationResultsList) {
              // Get and inspect the related reporting set: individual, unique, or union
              // 1. There must be a matching reporting set but maybe we didn't paginate enough
              // 2. Unique sets will be paired later, so don't process them
              // 3. If it's not a union, there should be a paired reporting set (the complement)
              val reportingSetName = metricCalculationResult.reportingSet
              val matchingReportingSet = reportingSets.find { it.name == reportingSetName }
              if (matchingReportingSet == null) {
                throw Status.INVALID_ARGUMENT.withDescription("Reporting Set missing")
                  .asRuntimeException()
              }

              val reportingSetDisplayName = matchingReportingSet.displayName
              val isUnique = matchingReportingSet.tagsMap[TYPE_TAG] == UNIQUE_TYPE_TAG
              if (isUnique) continue

              val isUnion = matchingReportingSet.tagsMap[TYPE_TAG] == UNION_TYPE_TAG
              var individualAndUnique = listOf(metricCalculationResult)
              if (!isUnion) {
                val rsPair =
                  reportingSets.find {
                    it.tagsMap[TYPE_TAG] == UNIQUE_TYPE_TAG &&
                      it.tagsMap[ID_TAG] == reportingSetName
                  }
                val metricResult =
                  source.metricCalculationResultsList.find { it.reportingSet == rsPair?.name }
                if (metricResult != null) individualAndUnique += metricResult
              }

              // Get all the result attributes filtered by the time interval and grouped by the
              // grouping predicate (ie. demo category)
              val resultAttributesList =
                individualAndUnique
                  .map { it.resultAttributesList }
                  .flatten()
                  .filter {
                    "${it.timeInterval.startTime.seconds}|${it.timeInterval.endTime.seconds}" ==
                      "${sourceTimeInterval.startTime.seconds}|${sourceTimeInterval.endTime.seconds}"
                  }
                  .groupBy {
                    it.groupingPredicatesList
                      .map { DEMOGRAPHIC_CODE_TO_STRING[it] }
                      .joinToString(prefix = "", postfix = "", separator = ";")
                  }

              for (resultAttributesByGroup in resultAttributesList) {
                demoCategoryName = resultAttributesByGroup.key

                val metrics = sourceMetrics {
                  cumulative = metricCalculationResult.cumulative
                  // Each result attribute will have one metric spec
                  // The metric spec could be of different types, so we'll go through each one
                  // and add it to the appropriate field.
                  // We have already grouped by time and metric calculation spec (ie. reporting set)
                  // so all these result attributes fall into the right bucket.
                  for (resultAttribute in resultAttributesByGroup.value) {
                    sourceName = reportingSetDisplayName

                    val oneOfCase = resultAttribute.metricResult.getResultCase()
                    when (oneOfCase) {
                      ResultCase.IMPRESSION_COUNT -> {
                        impressionCount = impressionCountResult {
                          count = resultAttribute.metricResult.impressionCount.value.capNumber()
                          standardDeviation =
                            resultAttribute.metricResult.impressionCount.univariateStatistics
                              .standardDeviation
                        }
                      }
                      ResultCase.REACH_AND_FREQUENCY -> {
                        reach =
                          resultAttribute.metricResult.reachAndFrequency.reach.value.capNumber()
                        val bins =
                          resultAttribute.metricResult.reachAndFrequency.frequencyHistogram.binsList
                            .sortedBy { it.label.toInt() }
                            .reversed()
                        var runningCount = 0.0
                        for (bin in bins) {
                          runningCount = runningCount + bin.binResult.value.capNumber()
                          frequencyHistogram[bin.label.toInt()] = runningCount
                        }
                      }
                      ResultCase.REACH -> {
                        uniqueReach = resultAttribute.metricResult.reach.value
                      }
                      else -> {
                        logger.warning { oneOfCase.toString() + ": Not a supported result case." }
                      }
                    }
                  }
                }

                if (isUnion) {
                  unionSource += metrics
                } else {
                  perPublisherSource += metrics
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
