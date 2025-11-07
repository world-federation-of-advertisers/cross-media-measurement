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
import com.google.type.DateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.wfanet.measurement.common.toProtoDateTime
import org.wfanet.measurement.common.toZonedDateTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues
import org.wfanet.measurement.internal.reporting.v2.ReportResultKt.ReportingSetResultKt.reportingWindow
import org.wfanet.measurement.internal.reporting.v2.ReportResultKt.reportingSetResultKey
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.reporting.service.internal.GroupingDimensions
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.ReportResultNotFoundException

suspend fun AsyncDatabaseClient.ReadContext.reportResultExists(
  measurementConsumerId: Long,
  reportResultId: Long,
): Boolean {
  return readRow(
    "ReportResults",
    Key.of(measurementConsumerId, reportResultId),
    listOf("ReportResultId"),
  ) != null
}

suspend fun AsyncDatabaseClient.ReadContext.reportResultExistsWithExternalId(
  measurementConsumerId: Long,
  externalReportResultId: Long,
): Boolean {
  return readRowUsingIndex(
    "ReportResults",
    "ReportResultsByExternalReportResultId",
    Key.of(measurementConsumerId, externalReportResultId),
    listOf("ExternalReportResultId"),
  ) != null
}

/** Buffers an insert mutation to the ReportResults table. */
fun AsyncDatabaseClient.TransactionContext.insertReportResult(
  measurementConsumerId: Long,
  reportResultId: Long,
  externalReportResultId: Long,
  reportStart: DateTime,
) {
  val reportStartTime: ZonedDateTime = reportStart.toZonedDateTime()
  val reportStartZoneId = reportStartTime.zone

  bufferInsertMutation("ReportResults") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ExternalReportResultId").to(externalReportResultId)
    set("ReportIntervalStartTime").to(reportStartTime.toInstant().toGcloudTimestamp())
    if (reportStartZoneId is ZoneOffset) {
      set("ReportIntervalStartTimeZoneOffset").to(reportStartZoneId.totalSeconds.toLong())
    } else {
      set("ReportIntervalStartTimeZoneId").to(reportStartZoneId.id)
    }
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
    ReportResultResult.buildOrNull {
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

        val reportingSetResultKey = reportingSetResultKey {
          externalReportingSetId = row.getString("ExternalReportingSetId")
          vennDiagramRegionType =
            row.getProtoEnum("VennDiagramRegionType", ReportResult.VennDiagramRegionType::forNumber)
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
            row.getProtoMessage("MetricFrequencySpec", MetricFrequencySpec.getDefaultInstance())
          groupings +=
            groupingDimensions.groupingByFingerprint.getValue(
              row.getLong("GroupingDimensionFingerprint")
            )
          eventFilters += row.getProtoMessageList("EventFilters", EventFilter.getDefaultInstance())
        }
        updateReportResult {
          updateReportingSetResult(reportingSetResultKey) {
            populationSize = row.getLong("PopulationSize").toInt()
            metricFrequencySpecFingerprint = row.getLong("MetricFrequencySpecFingerprint")
            groupingDimensionFingerprint = row.getLong("GroupingDimensionFingerprint")
            filterFingerprint = row.getLong("FilterFingerprint")

            val reportingWindow = reportingWindow {
              if (!row.isNull("ReportingWindowStartDate")) {
                nonCumulativeStart = row.getDate("ReportingWindowStartDate").toProtoDate()
              }
              end = row.getDate("ReportingWindowEndDate").toProtoDate()
            }
            putReportingWindowResult(
              reportingWindow,
              ReportResultKt.ReportingSetResultKt.reportingWindowResult {
                noisyReportResultValues = noisyReportResultValues {
                  if (!row.isNull("CumulativeResults")) {
                    cumulativeResults =
                      row.getProtoMessage(
                        "CumulativeResults",
                        ReportResult.ReportingSetResult.ReportingWindowResult
                          .NoisyReportResultValues
                          .NoisyMetricSet
                          .getDefaultInstance(),
                      )
                  }
                  if (!row.isNull("NonCumulativeResults")) {
                    nonCumulativeResults =
                      row.getProtoMessage(
                        "NonCumulativeResults",
                        ReportResult.ReportingSetResult.ReportingWindowResult
                          .NoisyReportResultValues
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
) {
  @DslMarker @Target(AnnotationTarget.CLASS, AnnotationTarget.TYPE) annotation class Dsl

  class Builder {
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
  }

  class ReportResultBuilder {
    var cmmsMeasurementConsumerId: String? = null
    var externalReportResultId: Long? = null
    var reportStart: ZonedDateTime? = null
    private val reportingSetResults =
      mutableMapOf<ReportResult.ReportingSetResultKey, ReportingSetResultBuilder>()

    fun updateReportingSetResult(
      key: ReportResult.ReportingSetResultKey,
      fill: (@Dsl ReportingSetResultBuilder).() -> Unit,
    ) {
      reportingSetResults.getOrPut(key, ::ReportingSetResultBuilder).fill()
    }

    fun build(): ReportResult {
      val source = this
      return reportResult {
        cmmsMeasurementConsumerId = checkNotNull(source.cmmsMeasurementConsumerId)
        externalReportResultId = checkNotNull(source.externalReportResultId)
        reportStart = checkNotNull(source.reportStart).toProtoDateTime()
        for (entry in source.reportingSetResults) {
          reportingSetResults +=
            ReportResultKt.reportingSetResultEntry {
              key = entry.key
              value = entry.value.build()
            }
        }
      }
    }
  }

  class ReportingSetResultBuilder {
    var populationSize: Int? = null
    var metricFrequencySpecFingerprint: Long? = null
    var groupingDimensionFingerprint: Long? = null
    var filterFingerprint: Long? = null

    private val reportingWindowResults =
      mutableMapOf<
        ReportResult.ReportingSetResult.ReportingWindow,
        ReportResult.ReportingSetResult.ReportingWindowResult,
      >()

    fun putReportingWindowResult(
      key: ReportResult.ReportingSetResult.ReportingWindow,
      value: ReportResult.ReportingSetResult.ReportingWindowResult,
    ) {
      reportingWindowResults[key] = value
    }

    fun build(): ReportResult.ReportingSetResult {
      val source = this
      return ReportResultKt.reportingSetResult {
        populationSize = checkNotNull(source.populationSize)
        metricFrequencySpecFingerprint = checkNotNull(source.metricFrequencySpecFingerprint)
        groupingDimensionFingerprint = checkNotNull(source.groupingDimensionFingerprint)
        filterFingerprint = checkNotNull(source.filterFingerprint)
        for (entry in source.reportingWindowResults) {
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowEntry {
              key = entry.key
              value = entry.value
            }
        }
      }
    }
  }

  companion object {
    suspend fun buildOrNull(fill: suspend (@Dsl Builder).() -> Unit): ReportResultResult? {
      return Builder().apply { fill() }.buildOrNull()
    }
  }
}
