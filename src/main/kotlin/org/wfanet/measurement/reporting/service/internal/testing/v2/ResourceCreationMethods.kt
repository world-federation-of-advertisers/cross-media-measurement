/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.protobuf.timestamp
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.createReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.reportSchedule
import org.wfanet.measurement.internal.reporting.v2.reportingSet

suspend fun createReportingSet(
  cmmsMeasurementConsumerId: String,
  reportingSetsService: ReportingSetsCoroutineImplBase,
  externalReportingSetId: String = "external-reporting-set-id",
  cmmsDataProviderId: String = "data-provider-id",
  cmmsEventGroupId: String = "event-group-id",
): ReportingSet {
  val reportingSet = reportingSet {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    primitive =
      ReportingSetKt.primitive {
        eventGroupKeys +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            this.cmmsDataProviderId = cmmsDataProviderId
            this.cmmsEventGroupId = cmmsEventGroupId
          }
      }
  }
  return reportingSetsService.createReportingSet(
    createReportingSetRequest {
      this.reportingSet = reportingSet
      this.externalReportingSetId = externalReportingSetId
    }
  )
}

suspend fun createMeasurementConsumer(
  cmmsMeasurementConsumerId: String,
  measurementConsumersService: MeasurementConsumersCoroutineImplBase,
) {
  measurementConsumersService.createMeasurementConsumer(
    measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
  )
}

suspend fun createMetricCalculationSpec(
  cmmsMeasurementConsumerId: String,
  metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
  externalMetricCalculationSpecId: String = "external-metric-calculation-spec-id",
): MetricCalculationSpec {
  val metricCalculationSpec = metricCalculationSpec {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    details =
      MetricCalculationSpecKt.details {
        displayName = "display"
        metricSpecs += metricSpec {
          reach =
            MetricSpecKt.reachParams {
              multipleDataProviderParams =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
            }
        }
        groupings += MetricCalculationSpecKt.grouping { predicates += "age > 10" }
        filter = "filter"
        metricFrequencySpec =
          MetricCalculationSpecKt.metricFrequencySpec {
            daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
          }
        trailingWindow =
          MetricCalculationSpecKt.trailingWindow {
            count = 2
            increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
          }
      }
  }
  return metricCalculationSpecsService.createMetricCalculationSpec(
    createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      this.externalMetricCalculationSpecId = externalMetricCalculationSpecId
    }
  )
}

suspend fun createReportSchedule(
  cmmsMeasurementConsumerId: String,
  reportingSet: ReportingSet,
  metricCalculationSpec: MetricCalculationSpec,
  reportSchedulesService: ReportSchedulesCoroutineImplBase,
  externalReportScheduleId: String = "report-schedule-123",
): ReportSchedule {
  val reportSchedule = reportSchedule {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    state = ReportSchedule.State.ACTIVE
    details =
      ReportScheduleKt.details {
        displayName = "display"
        description = "description"
        reportTemplate = report {
          reportingMetricEntries[reportingSet.externalReportingSetId] =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecReportingMetrics +=
                ReportKt.metricCalculationSpecReportingMetrics {
                  externalMetricCalculationSpecId =
                    metricCalculationSpec.externalMetricCalculationSpecId
                }
            }
        }
        eventStart = dateTime {
          year = 2023
          month = 10
          day = 1
          hours = 6
          timeZone = timeZone { id = "America/New_York" }
        }
        eventEnd = date {
          year = 2024
          month = 12
          day = 1
        }
        frequency = ReportScheduleKt.frequency { daily = ReportScheduleKt.FrequencyKt.daily {} }
        reportWindow =
          ReportScheduleKt.reportWindow {
            trailingWindow =
              ReportScheduleKt.ReportWindowKt.trailingWindow {
                count = 1
                increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
              }
          }
      }
    nextReportCreationTime = timestamp { seconds = 200 }
  }

  return reportSchedulesService.createReportSchedule(
    createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      this.externalReportScheduleId = externalReportScheduleId
    }
  )
}
