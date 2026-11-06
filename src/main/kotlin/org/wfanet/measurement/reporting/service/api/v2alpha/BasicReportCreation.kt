/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.internal.reporting.v2.BasicReportDetails
import org.wfanet.measurement.internal.reporting.v2.ReportingInterval
import org.wfanet.measurement.reporting.v2alpha.Report

/** Freezes the validated Report inputs, without creating metrics or measurements. */
fun Report.toCreationPlan(): BasicReportDetails.ReportCreationPlan {
  val plan = BasicReportDetails.ReportCreationPlan.newBuilder()
  for (entry in reportingMetricEntriesList) {
    plan.addReportingMetricEntries(
      BasicReportDetails.ReportCreationPlan.ReportingMetricEntry.newBuilder()
        .setExternalReportingSetId(
          requireNotNull(ReportingSetKey.fromName(entry.key)).reportingSetId
        )
        .addAllExternalMetricCalculationSpecIds(
          entry.value.metricCalculationSpecsList.map {
            requireNotNull(MetricCalculationSpecKey.fromName(it)).metricCalculationSpecId
          }
        )
    )
  }
  plan.setReportingInterval(
    ReportingInterval.newBuilder()
      .setReportStart(reportingInterval.reportStart)
      .setReportEnd(reportingInterval.reportEnd)
  )
  return plan.build()
}

/** Restores the same immutable inputs after a timeout, lost response, or job restart. */
fun BasicReportDetails.ReportCreationPlan.toReport(
  cmmsMeasurementConsumerId: String,
  basicReportName: String,
): Report {
  val result = Report.newBuilder().setBasicReport(basicReportName)
  for (entry in reportingMetricEntriesList) {
    result.addReportingMetricEntries(
      Report.ReportingMetricEntry.newBuilder()
        .setKey(ReportingSetKey(cmmsMeasurementConsumerId, entry.externalReportingSetId).toName())
        .setValue(
          Report.ReportingMetricCalculationSpec.newBuilder()
            .addAllMetricCalculationSpecs(
              entry.externalMetricCalculationSpecIdsList.map {
                MetricCalculationSpecKey(cmmsMeasurementConsumerId, it).toName()
              }
            )
        )
    )
  }
  result.setReportingInterval(
    Report.ReportingInterval.newBuilder()
      .setReportStart(reportingInterval.reportStart)
      .setReportEnd(reportingInterval.reportEnd)
  )
  return result.build()
}
