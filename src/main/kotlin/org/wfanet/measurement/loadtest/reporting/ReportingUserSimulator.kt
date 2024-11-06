/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.reporting

import com.google.common.truth.Truth.assertThat
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import java.util.logging.Logger
import kotlinx.coroutines.delay
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet

/** Simulator for Reporting operations on the Reporting public API. */
class ReportingUserSimulator(
  private val measurementConsumerName: String,
  private val apiAuthenticationKey: String,
  private val eventGroupsClient: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val reportingSetsClient: ReportingSetsGrpcKt.ReportingSetsCoroutineStub,
  private val metricCalculationSpecsClient:
    MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub,
  private val reportsClient: ReportsGrpcKt.ReportsCoroutineStub,
) {
  suspend fun testCreateReport(runId: String) {
    logger.info("Creating report...")

    val eventGroup = listEventGroups().first()
    val createdPrimitiveReportingSet = createPrimitiveReportingSet(eventGroup)
    val createdMetricCalculationSpec = createMetricCalculationSpec()

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2024
            month = 1
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2024
            month = 1
            day = 4
          }
        }
    }

    val createdReport =
      reportsClient
        .withAuthenticationKey(apiAuthenticationKey)
        .createReport(
          createReportRequest {
            parent = measurementConsumerName
            this.report = report
            reportId = "a-$runId"
          }
        )

    val completedReport = pollForCompletedReport(createdReport.name)

    assertThat(completedReport.state).isEqualTo(Report.State.SUCCEEDED)
    logger.info("Report creation succeeded")
  }

  private suspend fun listEventGroups(): List<EventGroup> {
    return eventGroupsClient
      .withAuthenticationKey(apiAuthenticationKey)
      .listEventGroups(
        listEventGroupsRequest {
          parent = measurementConsumerName
          pageSize = 1000
        }
      )
      .eventGroupsList
  }

  private suspend fun createPrimitiveReportingSet(eventGroup: EventGroup): ReportingSet {
    val primitiveReportingSet = reportingSet {
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    return reportingSetsClient
      .withAuthenticationKey(apiAuthenticationKey)
      .createReportingSet(
        createReportingSetRequest {
          parent = measurementConsumerName
          reportingSet = primitiveReportingSet
          reportingSetId = "a-123"
        }
      )
  }

  private suspend fun createMetricCalculationSpec(): MetricCalculationSpec {
    return metricCalculationSpecsClient
      .withAuthenticationKey(apiAuthenticationKey)
      .createMetricCalculationSpec(
        createMetricCalculationSpecRequest {
          parent = measurementConsumerName
          metricCalculationSpecId = "a-123"
          metricCalculationSpec = metricCalculationSpec {
            displayName = "union reach"
            metricSpecs += metricSpec {
              reach =
                MetricSpecKt.reachParams {
                  singleDataProviderParams = MetricSpecKt.samplingAndPrivacyParams {
                    privacyParams = MetricSpecKt.differentialPrivacyParams {  }
                  }
                  multipleDataProviderParams = MetricSpecKt.samplingAndPrivacyParams {
                    privacyParams = MetricSpecKt.differentialPrivacyParams {  }
                  }
                }
            }
            metricFrequencySpec =
              MetricCalculationSpecKt.metricFrequencySpec {
                weekly =
                  MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                    dayOfWeek = DayOfWeek.WEDNESDAY
                  }
              }
            trailingWindow =
              MetricCalculationSpecKt.trailingWindow {
                count = 1
                increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
              }
          }
        }
      )
  }

  private suspend fun pollForCompletedReport(reportName: String): Report {
    while (true) {
      val retrievedReport =
        reportsClient
          .withAuthenticationKey(apiAuthenticationKey)
          .getReport(getReportRequest { name = reportName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
