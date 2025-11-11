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
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.wfanet.measurement.common.toProtoDateTime
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.reporting.service.internal.GroupingDimensions
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.ReportResultNotFoundException

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

suspend fun AsyncDatabaseClient.ReadContext.readNoisyReportResult(
  impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  groupingDimensions: GroupingDimensions,
  cmmsMeasurementConsumerId: String,
  externalReportResultId: Long,
): ReportResultResult {
  val sql =
    """
    SELECT
      MeasurementConsumerId,
      CmmsMeasurementConsumerId,
      ReportResultId,
      ExternalReportResultId,
      ReportIntervalStartTime,
      ReportIntervalStartTimeZoneOffset,
      ReportIntervalStartTimeZoneId,
      ReportingSetResultId,
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
      CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
      AND ExternalReportResultId = @externalReportResultId
    ORDER BY
      MeasurementConsumerId, ReportResultId, ReportingSetResultId, ReportingWindowResultId
    """
      .trimIndent()
  val query =
    statement(sql) {
      bind("cmmsMeasurementConsumerId").to(cmmsMeasurementConsumerId)
      bind("externalReportResultId").to(externalReportResultId)
    }

  // One row per ReportingWindowResult.
  val rows = executeQuery(query, Options.tag("action=readNoisyReportResult"))
  val reportResultResult: ReportResultResult? =
    ReportResultResultBuilder.buildOrNull {
      rows.collect { row: Struct ->
        if (measurementConsumerId == null) {
          measurementConsumerId = row.getLong("MeasurementConsumerId")
          reportResultId = row.getLong("ReportResultId")
          updateReportResult {
            this.cmmsMeasurementConsumerId = row.getString("CmmsMeasurementConsumerId")
            this.externalReportResultId = row.getLong("ExternalReportResultId")

            val reportStartTimestamp = row.getTimestamp("ReportIntervalStartTime").toInstant()
            val zoneId =
              if (row.isNull("ReportIntervalStartTimeZoneId")) {
                ZoneOffset.ofTotalSeconds(row.getLong("ReportIntervalStartTimeZoneOffset").toInt())
              } else {
                ZoneId.of(row.getString("ReportIntervalStartTimeZoneId"))
              }
            reportStart = ZonedDateTime.ofInstant(reportStartTimestamp, zoneId)
          }
        }

        val reportingSetResultPKey =
          ReportingSetResultPKey(
            measurementConsumerId!!,
            reportResultId!!,
            row.getLong("ReportingSetResultId"),
          )

        updateReportResult {
          updateReportingSetResult(reportingSetResultPKey) {
            if (externalReportingSetResultId == null) {
              externalReportingSetResultId = row.getLong("ExternalReportingSetResultId")
              dimension =
                ReportingSetResultKt.dimension {
                  externalReportingSetId = row.getString("ExternalReportingSetId")
                  vennDiagramRegionType =
                    row.getProtoEnum(
                      "VennDiagramRegionType",
                      ReportingSetResult.Dimension.VennDiagramRegionType::forNumber,
                    )
                  val impressionQualificationFilterId =
                    row.getLong("ImpressionQualificationFilterId")
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
              populationSize = row.getLong("PopulationSize").toInt()
              metricFrequencySpecFingerprint = row.getLong("MetricFrequencySpecFingerprint")
              groupingDimensionFingerprint = row.getLong("GroupingDimensionFingerprint")
              filterFingerprint = row.getLong("FilterFingerprint")
            }

            val reportingWindow =
              ReportingSetResultKt.reportingWindow {
                if (!row.isNull("ReportingWindowStartDate")) {
                  nonCumulativeStart = row.getDate("ReportingWindowStartDate").toProtoDate()
                }
                end = row.getDate("ReportingWindowEndDate").toProtoDate()
              }
            putReportingWindowResult(
              reportingWindow,
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
              },
            )
          }
        }
      }
    }

  return reportResultResult
    ?: throw ReportResultNotFoundException(cmmsMeasurementConsumerId, externalReportResultId)
}

data class ReportResultResult(
  val measurementConsumerId: Long,
  val reportResultId: Long,
  val reportResult: ReportResult,
)

/** Primary key of ReportingSetResults table. */
private data class ReportingSetResultPKey(
  val measurementConsumerId: Long,
  val reportResultId: Long,
  val reportingSetResultId: Long,
)

private class ReportResultResultBuilder {
  @DslMarker @Target(AnnotationTarget.CLASS, AnnotationTarget.TYPE) annotation class Dsl

  var measurementConsumerId: Long? = null
  var reportResultId: Long? = null
  private val reportResultBuilder = ReportResultBuilder()

  fun updateReportResult(fill: (@Dsl ReportResultBuilder).() -> Unit) {
    reportResultBuilder.fill()
  }

  fun buildOrNull(): ReportResultResult? {
    val measurementConsumerId = measurementConsumerId ?: return null

    return ReportResultResult(
      measurementConsumerId,
      checkNotNull(reportResultId),
      reportResultBuilder.build(),
    )
  }

  class ReportResultBuilder {
    var cmmsMeasurementConsumerId: String? = null
    var externalReportResultId: Long? = null
    var reportStart: ZonedDateTime? = null
    private val reportingSetResults =
      mutableMapOf<ReportingSetResultPKey, ReportingSetResultBuilder>()

    fun updateReportingSetResult(
      key: ReportingSetResultPKey,
      fill: (@Dsl ReportingSetResultBuilder).() -> Unit,
    ) {
      reportingSetResults.getOrPut(key) { ReportingSetResultBuilder(this) }.fill()
    }

    fun build(): ReportResult {
      val source = this
      return reportResult {
        cmmsMeasurementConsumerId = checkNotNull(source.cmmsMeasurementConsumerId)
        externalReportResultId = checkNotNull(source.externalReportResultId)
        reportStart = checkNotNull(source.reportStart).toProtoDateTime()
        //        for (builder in source.reportingSetResults.values) {
        //          reportingSetResults += builder.build()
        //        }
      }
    }
  }

  class ReportingSetResultBuilder(private val parent: ReportResultBuilder) {
    var externalReportingSetResultId: Long? = null
    var dimension: ReportingSetResult.Dimension? = null
    var populationSize: Int? = null
    var metricFrequencySpecFingerprint: Long? = null
    var groupingDimensionFingerprint: Long? = null
    var filterFingerprint: Long? = null

    private val reportingWindowResults =
      mutableMapOf<ReportingSetResult.ReportingWindow, ReportingSetResult.ReportingWindowResult>()

    fun putReportingWindowResult(
      key: ReportingSetResult.ReportingWindow,
      value: ReportingSetResult.ReportingWindowResult,
    ) {
      reportingWindowResults[key] = value
    }

    fun build(): ReportingSetResult {
      val source = this
      return reportingSetResult {
        cmmsMeasurementConsumerId = checkNotNull(source.parent.cmmsMeasurementConsumerId)
        externalReportResultId = checkNotNull(source.parent.externalReportResultId)
        externalReportingSetResultId = checkNotNull(source.externalReportingSetResultId)

        dimension = checkNotNull(source.dimension)
        metricFrequencySpecFingerprint = checkNotNull(source.metricFrequencySpecFingerprint)
        groupingDimensionFingerprint = checkNotNull(source.groupingDimensionFingerprint)
        filterFingerprint = checkNotNull(source.filterFingerprint)
        populationSize = checkNotNull(source.populationSize)
        for (entry in source.reportingWindowResults) {
          reportingWindowResults +=
            ReportingSetResultKt.reportingWindowEntry {
              key = entry.key
              value = entry.value
            }
        }
      }
    }
  }

  companion object {
    suspend fun buildOrNull(
      fill: suspend (@Dsl ReportResultResultBuilder).() -> Unit
    ): ReportResultResult? {
      return ReportResultResultBuilder().apply { fill() }.buildOrNull()
    }
  }
}
