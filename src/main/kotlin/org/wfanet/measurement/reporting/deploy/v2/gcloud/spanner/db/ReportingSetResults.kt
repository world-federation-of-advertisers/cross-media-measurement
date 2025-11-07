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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult

object ReportingSetResults {
  const val CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID = -1L
}

suspend fun AsyncDatabaseClient.ReadContext.reportingSetResultExists(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
): Boolean {
  return readRow(
    "ReportingSetResults",
    Key.of(measurementConsumerId, reportResultId, reportingSetResultId),
    listOf("ReportResultId"),
  ) != null
}

fun AsyncDatabaseClient.TransactionContext.insertReportingSetResult(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  externalReportingSetId: String,
  vennDiagramRegionType: ReportResult.VennDiagramRegionType,
  impressionQualificationFilterId: Long?,
  metricFrequencySpec: MetricFrequencySpec,
  metricFrequencySpecFingerprint: Long,
  groupingDimensionFingerprint: Long,
  eventFilters: Collection<EventFilter>,
  filterFingerprint: Long?,
  populationSize: Int,
) {
  bufferInsertMutation("ReportingSetResults") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ReportingSetResultId").to(reportingSetResultId)
    set("ExternalReportingSetId").to(externalReportingSetId)
    set("VennDiagramRegionType").to(vennDiagramRegionType)
    set("ImpressionQualificationFilterId")
      .to(
        impressionQualificationFilterId
          ?: ReportingSetResults.CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID
      )
    set("MetricFrequencySpec").to(metricFrequencySpec)
    set("MetricFrequencySpecFingerprint").to(metricFrequencySpecFingerprint)
    set("GroupingDimensionFingerprint").to(groupingDimensionFingerprint)
    set("EventFilters").toProtoMessageArray(eventFilters, EventFilter.getDescriptor())
    set("FilterFingerprint").to(filterFingerprint)
    set("PopulationSize").to(populationSize.toLong())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}
