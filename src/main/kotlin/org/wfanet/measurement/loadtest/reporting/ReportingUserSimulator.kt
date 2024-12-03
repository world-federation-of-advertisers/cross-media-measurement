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
import io.grpc.StatusException
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.time.delay
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.coerceAtMost
import org.wfanet.measurement.loadtest.config.TestIdentifiers
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
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
  private val dataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val reportingSetsClient: ReportingSetsGrpcKt.ReportingSetsCoroutineStub,
  private val metricCalculationSpecsClient:
    MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub,
  private val reportsClient: ReportsGrpcKt.ReportsCoroutineStub,
  private val initialResultPollingDelay: Duration = Duration.ofSeconds(1),
  private val maximumResultPollingDelay: Duration = Duration.ofMinutes(1),
) {
  suspend fun testCreateReport(runId: String) {
    logger.info("Creating report...")

    val eventGroup =
      listEventGroups()
        .filter {
          it.eventGroupReferenceId.startsWith(
            TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
          )
        }
        .firstOrNull {
          getDataProvider(it.cmmsDataProvider).capabilities.honestMajorityShareShuffleSupported
        } ?: listEventGroups().first()
    val createdPrimitiveReportingSet = createPrimitiveReportingSet(eventGroup, runId)
    val createdMetricCalculationSpec = createMetricCalculationSpec(runId)

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
      try {
        reportsClient.createReport(
          createReportRequest {
            parent = measurementConsumerName
            this.report = report
            reportId = "a-$runId"
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating Report", e)
      }

    val completedReport = pollForCompletedReport(createdReport.name)

    assertThat(completedReport.state).isEqualTo(Report.State.SUCCEEDED)
    logger.info("Report creation succeeded")
  }

  private suspend fun listEventGroups(): List<EventGroup> {
    try {
      return buildList {
        var response: ListEventGroupsResponse = ListEventGroupsResponse.getDefaultInstance()
        do {
          response =
            eventGroupsClient.listEventGroups(
              listEventGroupsRequest {
                parent = measurementConsumerName
                pageToken = response.nextPageToken
              }
            )
          addAll(response.eventGroupsList)
        } while (response.nextPageToken.isNotEmpty())
      }
    } catch (e: StatusException) {
      throw Exception("Error listing EventGroups", e)
    }
  }

  private suspend fun getDataProvider(dataProviderName: String): DataProvider {
    try {
      return dataProvidersClient.getDataProvider(getDataProviderRequest { name = dataProviderName })
    } catch (e: StatusException) {
      throw Exception("Error getting DataProvider $dataProviderName", e)
    }
  }

  private suspend fun createPrimitiveReportingSet(
    eventGroup: EventGroup,
    runId: String,
  ): ReportingSet {
    val primitiveReportingSet = reportingSet {
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    try {
      return reportingSetsClient.createReportingSet(
        createReportingSetRequest {
          parent = measurementConsumerName
          reportingSet = primitiveReportingSet
          reportingSetId = "a-$runId"
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error creating ReportingSet", e)
    }
  }

  private suspend fun createMetricCalculationSpec(runId: String): MetricCalculationSpec {
    try {
      return metricCalculationSpecsClient.createMetricCalculationSpec(
        createMetricCalculationSpecRequest {
          parent = measurementConsumerName
          metricCalculationSpecId = "a-$runId"
          metricCalculationSpec = metricCalculationSpec {
            displayName = "union reach"
            metricSpecs += metricSpec {
              reach =
                MetricSpecKt.reachParams {
                  singleDataProviderParams =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams = MetricSpecKt.differentialPrivacyParams {}
                    }
                  multipleDataProviderParams =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams = MetricSpecKt.differentialPrivacyParams {}
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
    } catch (e: StatusException) {
      throw Exception("Error creating MetricCalculationSpec", e)
    }
  }

  private suspend fun pollForCompletedReport(reportName: String): Report {
    val backoff =
      ExponentialBackoff(initialDelay = initialResultPollingDelay, randomnessFactor = 0.0)
    var attempt = 1
    while (true) {
      val retrievedReport =
        try {
          reportsClient.getReport(getReportRequest { name = reportName })
        } catch (e: StatusException) {
          throw Exception("Error getting Report", e)
        }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> {
          val resultPollingDelay =
            backoff.durationForAttempt(attempt).coerceAtMost(maximumResultPollingDelay)
          logger.info {
            "Report not completed yet. Waiting for ${resultPollingDelay.seconds} seconds."
          }
          delay(resultPollingDelay)
          attempt++
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
