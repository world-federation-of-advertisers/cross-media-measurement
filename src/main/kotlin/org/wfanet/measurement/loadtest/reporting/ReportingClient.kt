// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.reporting

import com.google.protobuf.duration
import com.google.protobuf.timestamp
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.impressionCountParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.reportingSet

/** A client performing reporting operations. */
class ReportingClient(
  private val mcName: String,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val reportingSetsClient: ReportingSetsCoroutineStub,
  private val reportsClient: ReportsCoroutineStub,
) {
  /** A sequence of operations done to verify a report. */
  suspend fun execute(runId: String) {
    val createdReport = createReport(runId)
    val reports = listReports()
    assert(reports.reportsList.size >= 1)
    val completedReport = getReport(createdReport.name)
    var sum = 0.0
    completedReport.result.scalarTable.columnsList.forEach { column ->
      column.setOperationsList.forEach { sum += it }
    }
    assert(sum == 200.0)
  }

  private suspend fun listEventGroups(): ListEventGroupsResponse {
    return eventGroupsClient.listEventGroups(
      listEventGroupsRequest { parent = "$mcName/dataProviders/-" }
    )
  }

  suspend fun createReportingSet(runId: String): ReportingSet {
    val eventGroupsList = listEventGroups().eventGroupsList
    return reportingSetsClient.createReportingSet(
      createReportingSetRequest {
        parent = mcName
        reportingSet = reportingSet {
          displayName = "reporting-set-$runId"
          eventGroups += eventGroupsList.map { it.name }
        }
      }
    )
  }

  private suspend fun listReportingSets(): ListReportingSetsResponse {
    return reportingSetsClient.listReportingSets(listReportingSetsRequest { parent = mcName })
  }

  private suspend fun createReport(runId: String): Report {
    val eventGroupsList = listEventGroups().eventGroupsList
    val reportingSets = listReportingSets().reportingSetsList
    assert(reportingSets.size >= 3)
    return reportsClient.createReport(
      createReportRequest {
        parent = mcName
        report = report {
          measurementConsumer = mcName
          reportIdempotencyKey = runId
          eventGroupUniverse = eventGroupUniverse {
            eventGroupsList.forEach { eventGroupEntries += eventGroupEntry { key = it.name } }
          }
          periodicTimeInterval = periodicTimeInterval {
            startTime = timestamp { seconds = 100 }
            increment = duration { seconds = 5 }
            intervalCount = 2
          }
          metrics += metric {
            impressionCount = impressionCountParams { maximumFrequencyPerUser = 5 }
            setOperations += namedSetOperation {
              uniqueName = "set-operation"
              setOperation = setOperation {
                type = SetOperation.Type.UNION
                lhs = operand {
                  operation = setOperation {
                    type = SetOperation.Type.UNION
                    lhs = operand { reportingSet = reportingSets[0].name }
                    rhs = operand { reportingSet = reportingSets[1].name }
                  }
                }
                rhs = operand { reportingSet = reportingSets[2].name }
              }
            }
          }
        }
      }
    )
  }

  private suspend fun getReport(reportName: String): Report {
    return reportsClient.getReport(getReportRequest { name = reportName })
  }

  private suspend fun listReports(): ListReportsResponse {
    return reportsClient.listReports(listReportsRequest { parent = mcName })
  }
}
