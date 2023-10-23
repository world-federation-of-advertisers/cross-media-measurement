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

import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingSet

suspend fun createReportingSet(
  cmmsMeasurementConsumerId: String,
  reportingSetsService: ReportingSetsCoroutineImplBase,
  externalReportingSetId: String = "external-reporting-set-id",
  cmmsDataProviderId: String = "data-provider-id",
  cmmsEventGroupId: String = "event-group-id"
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
  externalMetricCalculationSpecId: String = "external-metric-calculation-spec-id"
): MetricCalculationSpec {
  val metricCalculationSpec = metricCalculationSpec {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    details =
      MetricCalculationSpecKt.details {
        displayName = "display"
        metricSpecs += metricSpec {
          reach =
            MetricSpecKt.reachParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 1.0
                  delta = 2.0
                }
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        groupings += MetricCalculationSpecKt.grouping { predicates += "age > 10" }
        filter = "filter"
        cumulative = false
      }
  }
  return metricCalculationSpecsService.createMetricCalculationSpec(
    createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      this.externalMetricCalculationSpecId = externalMetricCalculationSpecId
    }
  )
}
