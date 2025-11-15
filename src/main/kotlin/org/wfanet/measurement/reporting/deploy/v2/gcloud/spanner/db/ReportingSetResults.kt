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
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues
import org.wfanet.measurement.reporting.service.internal.GroupingDimensions
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

object ReportingSetResults {
  const val CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID = -1L
}

data class ReportingSetResultResult(
  val measurementConsumerId: Long,
  val reportingSetId: Long,
  val reportingSetResultId: Long,
  val reportingSetResult: ReportingSetResult,
)

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

suspend fun AsyncDatabaseClient.ReadContext.reportingSetResultExistsByExternalId(
  measurementConsumerId: Long,
  reportResultId: Long,
  externalReportingSetResultId: Long,
): Boolean {
  return readRowUsingIndex(
    "ReportingSetResults",
    "ReportingSetResultsByExternalId",
    Key.of(measurementConsumerId, reportResultId, externalReportingSetResultId),
    listOf("ReportResultId"),
  ) != null
}

fun AsyncDatabaseClient.TransactionContext.insertReportingSetResult(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  externalReportingSetResultId: Long,
  dimension: ReportingSetResult.Dimension,
  impressionQualificationFilterId: Long?,
  metricFrequencySpecFingerprint: Long,
  groupingDimensionFingerprint: Long,
  filterFingerprint: Long?,
  populationSize: Int,
) {
  bufferInsertMutation("ReportingSetResults") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ReportingSetResultId").to(reportingSetResultId)
    set("ExternalReportingSetResultId").to(externalReportingSetResultId)
    set("ExternalReportingSetId").to(dimension.externalReportingSetId)
    set("VennDiagramRegionType").to(dimension.vennDiagramRegionType)
    set("ImpressionQualificationFilterId")
      .to(
        impressionQualificationFilterId
          ?: ReportingSetResults.CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID
      )
    set("MetricFrequencySpec").to(dimension.metricFrequencySpec)
    set("MetricFrequencySpecFingerprint").to(metricFrequencySpecFingerprint)
    set("GroupingDimensionFingerprint").to(groupingDimensionFingerprint)
    set("EventFilters").toProtoMessageArray(dimension.eventFiltersList, EventFilter.getDescriptor())
    set("FilterFingerprint").to(filterFingerprint)
    set("PopulationSize").to(populationSize.toLong())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Primary key of ReportingSetResults table. */
private data class ReportingSetResultPKey(
  val measurementConsumerId: Long,
  val reportResultId: Long,
  val reportingSetResultId: Long,
)

private data class MutableReportingSetResultResult(
  val pKey: ReportingSetResultPKey,
  val builder: ReportingSetResult.Builder,
) {
  fun build() =
    ReportingSetResultResult(
      pKey.measurementConsumerId,
      pKey.reportResultId,
      pKey.reportingSetResultId,
      builder.build(),
    )
}

fun AsyncDatabaseClient.ReadContext.readNoisyReportingSetResults(
  impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  groupingDimensions: GroupingDimensions,
  measurementConsumerId: Long,
  reportResultId: Long,
): Flow<ReportingSetResultResult> {
  val sql =
    """
    SELECT
      MeasurementConsumerId,
      ReportResultId,
      ReportingSetResultId,
      CmmsMeasurementConsumerId,
      ExternalReportResultId,
      ExternalReportingSetResultId,
      ExternalReportingSetId,
      VennDiagramRegionType,
      ImpressionQualificationFilterId,
      MetricFrequencySpec,
      MetricFrequencySpecFingerprint,
      GroupingDimensionFingerprint,
      EventFilters,
      FilterFingerprint,
      PopulationSize,
      ReportingWindowResultId,
      ReportingWindowStartDate,
      ReportingWindowEndDate,
      CumulativeResults,
      NonCumulativeResults,
    FROM
      MeasurementConsumers
      JOIN ReportResults USING (MeasurementConsumerId)
      JOIN ReportingSetResults USING (MeasurementConsumerId, ReportResultId)
      JOIN ReportingWindowResults USING (MeasurementConsumerId, ReportResultId, ReportingSetResultId)
      JOIN NoisyReportResultValues USING (MeasurementConsumerId, ReportResultId, ReportingSetResultId, ReportingWindowResultId)
    WHERE
      MeasurementConsumerId = @measurementConsumerId
      AND ReportResultId = @reportResultId
    ORDER BY
      MeasurementConsumerId, ReportResultId, ReportingSetResultId, ReportingWindowResultId
    """
      .trimIndent()
  val query =
    statement(sql) {
      bind("measurementConsumerId").to(measurementConsumerId)
      bind("reportResultId").to(reportResultId)
    }

  return flow {
    var current: MutableReportingSetResultResult? = null
    // One row per ReportingWindowResult.
    executeQuery(query, Options.tag("action=readNoisyReportingSetResults")).collect { row ->
      val pKey =
        ReportingSetResultPKey(
          measurementConsumerId,
          reportResultId,
          row.getLong("ReportingSetResultId"),
        )

      val previous = current
      if (previous == null || previous.pKey != pKey) {
        // Emit previous.
        if (previous != null) {
          emit(previous.build())
        }

        val builder =
          ReportingSetResult.newBuilder().apply {
            cmmsMeasurementConsumerId = row.getString("CmmsMeasurementConsumerId")
            externalReportResultId = row.getLong("ExternalReportResultId")
            externalReportingSetResultId = row.getLong("ExternalReportingSetResultId")
            populationSize = row.getLong("PopulationSize").toInt()
            metricFrequencySpecFingerprint = row.getLong("MetricFrequencySpecFingerprint")
            groupingDimensionFingerprint = row.getLong("GroupingDimensionFingerprint")
            filterFingerprint = row.getLong("FilterFingerprint")
            dimension =
              ReportingSetResultKt.dimension {
                externalReportingSetId = row.getString("ExternalReportingSetId")
                vennDiagramRegionType =
                  row.getProtoEnum(
                    "VennDiagramRegionType",
                    ReportingSetResult.Dimension.VennDiagramRegionType::forNumber,
                  )
                val impressionQualificationFilterId = row.getLong("ImpressionQualificationFilterId")
                if (
                  impressionQualificationFilterId ==
                    ReportingSetResults.CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID
                ) {
                  custom = true
                } else {
                  val impressionQualificationFilter =
                    checkNotNull(
                      impressionQualificationFilterMapping.getImpressionQualificationFilterById(
                        impressionQualificationFilterId
                      )
                    ) {
                      "ImpressionQualificationFilter with internal ID $impressionQualificationFilterId not found"
                    }
                  externalImpressionQualificationFilterId =
                    impressionQualificationFilter.externalImpressionQualificationFilterId
                }
                metricFrequencySpec =
                  row.getProtoMessage(
                    "MetricFrequencySpec",
                    MetricFrequencySpec.getDefaultInstance(),
                  )
                grouping =
                  groupingDimensions.groupingByFingerprint.getValue(
                    row.getLong("GroupingDimensionFingerprint")
                  )
                eventFilters +=
                  row.getProtoMessageList("EventFilters", EventFilter.getDefaultInstance())
              }
          }
        current = MutableReportingSetResultResult(pKey, builder)
      }

      current!!
        .builder
        .addReportingWindowResults(
          ReportingSetResultKt.reportingWindowEntry {
            key =
              ReportingSetResultKt.reportingWindow {
                if (!row.isNull("ReportingWindowStartDate")) {
                  nonCumulativeStart = row.getDate("ReportingWindowStartDate").toProtoDate()
                }
                end = row.getDate("ReportingWindowEndDate").toProtoDate()
              }
            value =
              ReportingSetResultKt.reportingWindowResult {
                noisyReportResultValues = noisyReportResultValues {
                  if (!row.isNull("CumulativeResults")) {
                    cumulativeResults =
                      row.getProtoMessage(
                        "CumulativeResults",
                        ReportingSetResult.ReportingWindowResult.NoisyReportResultValues
                          .NoisyMetricSet
                          .getDefaultInstance(),
                      )
                  }
                  if (!row.isNull("NonCumulativeResults")) {
                    nonCumulativeResults =
                      row.getProtoMessage(
                        "NonCumulativeResults",
                        ReportingSetResult.ReportingWindowResult.NoisyReportResultValues
                          .NoisyMetricSet
                          .getDefaultInstance(),
                      )
                  }
                }
              }
          }
        )
    }

    // Emit final result.
    val final = current
    if (final != null) {
      emit(final.build())
    }
  }
}
