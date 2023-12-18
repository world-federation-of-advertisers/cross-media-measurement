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

package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import com.google.protobuf.Timestamp
import com.google.type.Interval
import com.google.type.interval
import io.r2dbc.postgresql.codec.Interval as PostgresInterval
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec

class MetricReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val metricId: InternalId,
    val createMetricRequestId: String,
    val metric: Metric
  )

  data class ReportingMetricKey(
    val reportingSetId: InternalId,
    val metricCalculationSpecId: InternalId,
    val timeInterval: Interval,
  )

  data class ReportingMetric(
    val reportingMetricKey: ReportingMetricKey,
    val metricId: InternalId,
    val createMetricRequestId: String,
    val externalMetricId: String,
    val metricSpec: MetricSpec,
    val metricDetails: Metric.Details
  )

  private data class MetricInfo(
    val measurementConsumerId: InternalId,
    val cmmsMeasurementConsumerId: String,
    val createMetricRequestId: String?,
    val externalReportingSetId: String,
    val metricId: InternalId,
    val externalMetricId: String,
    val createTime: Timestamp,
    val timeInterval: Interval,
    val metricSpec: MetricSpec,
    val weightedMeasurementInfoMap: MutableMap<MetricMeasurementKey, WeightedMeasurementInfo>,
    val details: Metric.Details,
  )

  private data class MetricMeasurementKey(
    val measurementConsumerId: InternalId,
    val metricId: InternalId,
    val measurementId: InternalId,
  )

  private data class WeightedMeasurementInfo(
    val weight: Int,
    val binaryRepresentation: Int,
    val measurementInfo: MeasurementInfo,
  )

  private data class MeasurementInfo(
    val cmmsMeasurementId: String?,
    val cmmsCreateMeasurementRequestId: String,
    val timeInterval: Interval,
    // Key is primitiveReportingSetBasisId.
    val primitiveReportingSetBasisInfoMap: MutableMap<InternalId, PrimitiveReportingSetBasisInfo>,
    val state: Measurement.State,
    val details: Measurement.Details,
  )

  private data class PrimitiveReportingSetBasisInfo(
    val externalReportingSetId: String,
    val filterSet: MutableSet<String>,
  )

  private val baseSqlSelect: String =
    """
    SELECT
      CmmsMeasurementConsumerId,
      Metrics.MeasurementConsumerId,
      Metrics.CreateMetricRequestId,
      ReportingSets.ExternalReportingSetId AS MetricsExternalReportingSetId,
      Metrics.MetricId,
      Metrics.ExternalMetricId,
      Metrics.TimeIntervalStart AS MetricsTimeIntervalStart,
      Metrics.TimeIntervalEndExclusive AS MetricsTimeIntervalEndExclusive,
      Metrics.MetricType,
      Metrics.DifferentialPrivacyEpsilon,
      Metrics.DifferentialPrivacyDelta,
      Metrics.FrequencyDifferentialPrivacyEpsilon,
      Metrics.FrequencyDifferentialPrivacyDelta,
      Metrics.MaximumFrequency,
      Metrics.MaximumFrequencyPerUser,
      Metrics.MaximumWatchDurationPerUser,
      Metrics.VidSamplingIntervalStart,
      Metrics.VidSamplingIntervalWidth,
      Metrics.CreateTime,
      Metrics.MetricDetails,
      MetricMeasurements.Coefficient,
      MetricMeasurements.BinaryRepresentation,
      Measurements.MeasurementId,
      Measurements.CmmsCreateMeasurementRequestId,
      Measurements.CmmsMeasurementId,
      Measurements.TimeIntervalStart AS MeasurementsTimeIntervalStart,
      Measurements.TimeIntervalEndExclusive AS MeasurementsTimeIntervalEndExclusive,
      Measurements.State,
      Measurements.MeasurementDetails,
      PrimitiveReportingSetBases.PrimitiveReportingSetBasisId,
      PrimitiveReportingSets.ExternalReportingSetId AS PrimitiveExternalReportingSetId,
      PrimitiveReportingSetBasisFilters.Filter AS PrimitiveReportingSetBasisFilter
    """
      .trimIndent()

  private val baseSqlJoins: String =
    """
    JOIN ReportingSets USING(MeasurementConsumerId, ReportingSetId)
    JOIN MetricMeasurements USING(MeasurementConsumerId, MetricId)
    JOIN Measurements USING(MeasurementConsumerId, MeasurementId)
    JOIN MeasurementPrimitiveReportingSetBases USING(MeasurementConsumerId, MeasurementId)
    JOIN PrimitiveReportingSetBases USING(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    JOIN ReportingSets AS PrimitiveReportingSets
      ON PrimitiveReportingSetBases.MeasurementConsumerId = PrimitiveReportingSets.MeasurementConsumerId
      AND PrimitiveReportingSetBases.PrimitiveReportingSetId = PrimitiveReportingSets.ReportingSetId
    LEFT JOIN PrimitiveReportingSetBasisFilters
      ON PrimitiveReportingSetBases.MeasurementConsumerId = PrimitiveReportingSetBasisFilters.MeasurementConsumerId
      AND PrimitiveReportingSetBases.PrimitiveReportingSetBasisId = PrimitiveReportingSetBasisFilters.PrimitiveReportingSetBasisId
    """
      .trimIndent()

  fun readMetricsByRequestId(
    measurementConsumerId: InternalId,
    createMetricRequestIds: Collection<String>
  ): Flow<Result> {
    if (createMetricRequestIds.isEmpty()) {
      return emptyFlow()
    }

    val sql =
      StringBuilder(
        """
          $baseSqlSelect
          FROM
            MeasurementConsumers
            JOIN Metrics USING(MeasurementConsumerId)
            $baseSqlJoins
          WHERE Metrics.MeasurementConsumerId = $1
            AND CreateMetricRequestId IN
        """
          .trimIndent()
      )

    var i = 2
    val bindingMap = mutableMapOf<String, String>()
    val inList =
      createMetricRequestIds.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerId)
        createMetricRequestIds.forEach { bind(bindingMap.getValue(it), it) }
      }

    return flow {
      val metricInfoMap = buildResultMap(statement)

      for (entry in metricInfoMap) {
        val metricInfo = entry.value

        val metric = metricInfo.buildMetric()

        val createMetricRequestId = metricInfo.createMetricRequestId ?: ""
        emit(
          Result(
            measurementConsumerId = metricInfo.measurementConsumerId,
            metricId = metricInfo.metricId,
            createMetricRequestId = createMetricRequestId,
            metric = metric
          )
        )
      }
    }
  }

  fun readReportingMetricsByReportingMetricKey(
    measurementConsumerId: InternalId,
    reportingMetricKeys: Collection<ReportingMetricKey>,
  ): Flow<ReportingMetric> {
    if (reportingMetricKeys.isEmpty()) {
      return emptyFlow()
    }

    val sqlSelect: String =
      """
      SELECT
        CmmsMeasurementConsumerId,
        Metrics.MeasurementConsumerId,
        Metrics.CreateMetricRequestId,
        ReportingSets.ReportingSetId as MetricsReportingSetId,
        MetricCalculationSpecReportingMetrics.MetricCalculationSpecId,
        Metrics.MetricId,
        Metrics.ExternalMetricId,
        Metrics.TimeIntervalStart AS MetricsTimeIntervalStart,
        Metrics.TimeIntervalEndExclusive AS MetricsTimeIntervalEndExclusive,
        Metrics.MetricType,
        Metrics.DifferentialPrivacyEpsilon,
        Metrics.DifferentialPrivacyDelta,
        Metrics.FrequencyDifferentialPrivacyEpsilon,
        Metrics.FrequencyDifferentialPrivacyDelta,
        Metrics.MaximumFrequency,
        Metrics.MaximumFrequencyPerUser,
        Metrics.MaximumWatchDurationPerUser,
        Metrics.VidSamplingIntervalStart,
        Metrics.VidSamplingIntervalWidth,
        Metrics.MetricDetails
      """
        .trimIndent()

    val sqlJoins: String =
      """
      JOIN Metrics USING(MeasurementConsumerId)
      JOIN ReportingSets USING(MeasurementConsumerId, ReportingSetId)
      LEFT JOIN MetricCalculationSpecReportingMetrics ON
        Metrics.MeasurementConsumerId = MetricCalculationSpecReportingMetrics.MeasurementConsumerId
        AND Metrics.MetricId = MetricCalculationSpecReportingMetrics.MetricId
      """
        .trimIndent()

    val sql =
      StringBuilder(
        """
          $sqlSelect
          FROM
            MeasurementConsumers
            $sqlJoins
          WHERE Metrics.MeasurementConsumerId = $1
        """
          .trimIndent()
      )

    // The index in `sql` ends at $1.
    var offset = 2
    val reportingSetIdBindingMap = mutableMapOf<InternalId, String>()
    val metricCalculationSpecIdBindingMap = mutableMapOf<InternalId, String>()
    val timeStampBindingMap = mutableMapOf<Timestamp, String>()
    val sqlWhereConditions =
      reportingMetricKeys.joinToString(separator = " OR ", prefix = "\nAND (", postfix = ")") {
        timedReportingMetricKey ->
        val reportingSetIdIndex =
          reportingSetIdBindingMap.getOrPut(timedReportingMetricKey.reportingSetId) {
            "$${offset++}"
          }
        val metricCalculationSpecIdIndex =
          metricCalculationSpecIdBindingMap.getOrPut(
            timedReportingMetricKey.metricCalculationSpecId
          ) {
            "$${offset++}"
          }
        val timeIntervalStartIndex =
          timeStampBindingMap.getOrPut(timedReportingMetricKey.timeInterval.startTime) {
            "$${offset++}"
          }
        val timeIntervalEndExclusiveIndex =
          timeStampBindingMap.getOrPut(timedReportingMetricKey.timeInterval.endTime) {
            "$${offset++}"
          }
        "(Metrics.ReportingSetId = $reportingSetIdIndex AND " +
          "MetricCalculationSpecReportingMetrics.MetricCalculationSpecId = $metricCalculationSpecIdIndex AND " +
          "Metrics.TimeIntervalStart = $timeIntervalStartIndex AND " +
          "Metrics.TimeIntervalEndExclusive = $timeIntervalEndExclusiveIndex)"
      }

    sql.append(sqlWhereConditions)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerId)
        reportingMetricKeys.forEach { timedReportingMetricKey ->
          val reportingSetIdIndex =
            reportingSetIdBindingMap.getValue(timedReportingMetricKey.reportingSetId)
          val metricCalculationSpecIdIndex =
            metricCalculationSpecIdBindingMap.getValue(
              timedReportingMetricKey.metricCalculationSpecId
            )
          val timeIntervalStartIndex =
            timeStampBindingMap.getValue(timedReportingMetricKey.timeInterval.startTime)
          val timeIntervalEndExclusiveIndex =
            timeStampBindingMap.getValue(timedReportingMetricKey.timeInterval.endTime)
          bind(reportingSetIdIndex, timedReportingMetricKey.reportingSetId)
          bind(metricCalculationSpecIdIndex, timedReportingMetricKey.metricCalculationSpecId)
          bind(
            timeIntervalStartIndex,
            timedReportingMetricKey.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC)
          )
          bind(
            timeIntervalEndExclusiveIndex,
            timedReportingMetricKey.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC)
          )
        }
      }

    return flow {
      val reportingMetricMap: Map<String, ReportingMetric> = buildReportingMetricMap(statement)
      for (entry in reportingMetricMap) {
        emit(entry.value)
      }
    }
  }

  /**
   * Returns a map that maintains the order of the query result for timed reporting metric entry.
   */
  private suspend fun buildReportingMetricMap(
    statement: BoundStatement
  ): Map<String, ReportingMetric> {
    // Key is externalMetricId.
    val reportingMetricMap: MutableMap<String, ReportingMetric> = linkedMapOf()

    val translate: (row: ResultRow) -> Unit = { row: ResultRow ->
      val createMetricRequestId: String =
        requireNotNull(row["CreateMetricRequestId"]) {
          "Metric that is associated with a MetricCalculationSpec must have createMetricRequestId"
        }
      val reportingSetId: InternalId = row["MetricsReportingSetId"]
      val metricCalculationSpecId: InternalId = row["MetricCalculationSpecId"]
      val metricId: InternalId = row["MetricId"]
      val externalMetricId: String = row["ExternalMetricId"]
      val metricTimeIntervalStart: Instant = row["MetricsTimeIntervalStart"]
      val metricTimeIntervalEnd: Instant = row["MetricsTimeIntervalEndExclusive"]
      val metricType: MetricSpec.TypeCase = MetricSpec.TypeCase.forNumber(row["MetricType"])
      val differentialPrivacyEpsilon: Double = row["DifferentialPrivacyEpsilon"]
      val differentialPrivacyDelta: Double = row["DifferentialPrivacyDelta"]
      val frequencyDifferentialPrivacyEpsilon: Double? = row["FrequencyDifferentialPrivacyEpsilon"]
      val frequencyDifferentialPrivacyDelta: Double? = row["FrequencyDifferentialPrivacyDelta"]
      val maximumFrequency: Int? = row["MaximumFrequency"]
      val maximumFrequencyPerUser: Int? = row["MaximumFrequencyPerUser"]
      val maximumWatchDurationPerUser: PostgresInterval? = row["MaximumWatchDurationPerUser"]
      val vidSamplingStart: Float = row["VidSamplingIntervalStart"]
      val vidSamplingWidth: Float = row["VidSamplingIntervalWidth"]
      val metricDetails: Metric.Details =
        row.getProtoMessage("MetricDetails", Metric.Details.parser())

      reportingMetricMap.computeIfAbsent(externalMetricId) {
        val metricTimeInterval = interval {
          startTime = metricTimeIntervalStart.toProtoTime()
          endTime = metricTimeIntervalEnd.toProtoTime()
        }

        val vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = vidSamplingStart
            width = vidSamplingWidth
          }

        val metricSpec = metricSpec {
          when (metricType) {
            MetricSpec.TypeCase.REACH ->
              reach =
                MetricSpecKt.reachParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                }
            MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
              if (
                frequencyDifferentialPrivacyDelta == null ||
                  frequencyDifferentialPrivacyEpsilon == null ||
                  maximumFrequency == null
              ) {
                throw IllegalStateException()
              }

              reachAndFrequency =
                MetricSpecKt.reachAndFrequencyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = frequencyDifferentialPrivacyEpsilon
                      delta = frequencyDifferentialPrivacyDelta
                    }
                  this.maximumFrequency = maximumFrequency
                }
            }
            MetricSpec.TypeCase.IMPRESSION_COUNT -> {
              if (maximumFrequencyPerUser == null) {
                throw IllegalStateException()
              }

              impressionCount =
                MetricSpecKt.impressionCountParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                  this.maximumFrequencyPerUser = maximumFrequencyPerUser
                }
            }
            MetricSpec.TypeCase.WATCH_DURATION -> {
              watchDuration =
                MetricSpecKt.watchDurationParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                  this.maximumWatchDurationPerUser =
                    checkNotNull(maximumWatchDurationPerUser).duration.toProtoDuration()
                }
            }
            MetricSpec.TypeCase.TYPE_NOT_SET -> throw IllegalStateException()
          }
          this.vidSamplingInterval = vidSamplingInterval
        }

        ReportingMetric(
          reportingMetricKey =
            ReportingMetricKey(
              reportingSetId = reportingSetId,
              metricCalculationSpecId = metricCalculationSpecId,
              timeInterval = metricTimeInterval,
            ),
          createMetricRequestId = createMetricRequestId,
          metricId = metricId,
          externalMetricId = externalMetricId,
          metricSpec = metricSpec,
          metricDetails = metricDetails,
        )
      }
    }

    readContext.executeQuery(statement).consume(translate).collect {}

    return reportingMetricMap
  }

  fun batchGetMetrics(
    request: BatchGetMetricsRequest,
  ): Flow<Result> {
    val sql =
      StringBuilder(
        """
          $baseSqlSelect
          FROM MeasurementConsumers
            JOIN Metrics USING(MeasurementConsumerId)
          $baseSqlJoins
          WHERE CmmsMeasurementConsumerId = $1
            AND ExternalMetricId IN
        """
          .trimIndent()
      )

    var i = 2
    val bindingMap = mutableMapOf<String, String>()
    val inList =
      request.externalMetricIdsList.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", request.cmmsMeasurementConsumerId)
        request.externalMetricIdsList.forEach { bind(bindingMap.getValue(it), it) }
      }

    return flow {
      val metricInfoMap = buildResultMap(statement)

      for (metricId in request.externalMetricIdsList) {
        val metricInfo = metricInfoMap[metricId] ?: continue

        val metric = metricInfo.buildMetric()

        val createMetricRequestId = metricInfo.createMetricRequestId ?: ""
        emit(
          Result(
            measurementConsumerId = metricInfo.measurementConsumerId,
            metricId = metricInfo.metricId,
            createMetricRequestId = createMetricRequestId,
            metric = metric
          )
        )
      }
    }
  }

  fun readMetrics(
    request: StreamMetricsRequest,
  ): Flow<Result> {
    val sql =
      """
        $baseSqlSelect
        FROM (
          SELECT *
          FROM MeasurementConsumers
            JOIN Metrics USING (MeasurementConsumerId)
          WHERE CmmsMeasurementConsumerId = $1
            AND ExternalMetricId > $2
          ORDER BY ExternalMetricId ASC
          LIMIT $3
        ) AS Metrics
        $baseSqlJoins
        ORDER BY ExternalMetricId ASC
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalMetricIdAfter)
        if (request.limit > 0) {
          bind("$3", request.limit)
        } else {
          bind("$3", 50)
        }
      }

    return flow {
      val metricInfoMap = buildResultMap(statement)

      for (entry in metricInfoMap) {
        val metricInfo = entry.value

        val metric = metricInfo.buildMetric()

        val createMetricRequestId = metricInfo.createMetricRequestId ?: ""
        emit(
          Result(
            measurementConsumerId = metricInfo.measurementConsumerId,
            metricId = metricInfo.metricId,
            createMetricRequestId = createMetricRequestId,
            metric = metric
          )
        )
      }
    }
  }

  private fun MetricInfo.buildMetric(): Metric {
    val metricInfo = this
    return metric {
      cmmsMeasurementConsumerId = metricInfo.cmmsMeasurementConsumerId
      externalMetricId = metricInfo.externalMetricId
      externalReportingSetId = metricInfo.externalReportingSetId
      createTime = metricInfo.createTime
      timeInterval = metricInfo.timeInterval
      metricSpec = metricInfo.metricSpec
      metricInfo.weightedMeasurementInfoMap.values.forEach {
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = it.weight
            binaryRepresentation = it.binaryRepresentation
            measurement = measurement {
              cmmsMeasurementConsumerId = metricInfo.cmmsMeasurementConsumerId
              if (it.measurementInfo.cmmsMeasurementId != null) {
                cmmsMeasurementId = it.measurementInfo.cmmsMeasurementId
              }
              cmmsCreateMeasurementRequestId = it.measurementInfo.cmmsCreateMeasurementRequestId
              timeInterval = it.measurementInfo.timeInterval
              it.measurementInfo.primitiveReportingSetBasisInfoMap.values.forEach {
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    externalReportingSetId = it.externalReportingSetId
                    filters += it.filterSet
                  }
              }
              state = it.measurementInfo.state
              if (it.measurementInfo.details != Measurement.Details.getDefaultInstance()) {
                details = it.measurementInfo.details
              }
            }
          }
      }
      if (metricInfo.details != Metric.Details.getDefaultInstance()) {
        details = metricInfo.details
      }
    }
  }

  /** Returns a map that maintains the order of the query result. */
  private suspend fun buildResultMap(statement: BoundStatement): Map<String, MetricInfo> {
    // Key is externalMetricId.
    val metricInfoMap: MutableMap<String, MetricInfo> = linkedMapOf()

    val translate: (row: ResultRow) -> Unit = { row: ResultRow ->
      val measurementConsumerId: InternalId = row["MeasurementConsumerId"]
      val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
      val createMetricRequestId: String? = row["CreateMetricRequestId"]
      val externalReportingSetId: String = row["MetricsExternalReportingSetId"]
      val metricId: InternalId = row["MetricId"]
      val externalMetricId: String = row["ExternalMetricId"]
      val metricTimeIntervalStart: Instant = row["MetricsTimeIntervalStart"]
      val metricTimeIntervalEnd: Instant = row["MetricsTimeIntervalEndExclusive"]
      val metricType: MetricSpec.TypeCase = MetricSpec.TypeCase.forNumber(row["MetricType"])
      val differentialPrivacyEpsilon: Double = row["DifferentialPrivacyEpsilon"]
      val differentialPrivacyDelta: Double = row["DifferentialPrivacyDelta"]
      val frequencyDifferentialPrivacyEpsilon: Double? = row["FrequencyDifferentialPrivacyEpsilon"]
      val frequencyDifferentialPrivacyDelta: Double? = row["FrequencyDifferentialPrivacyDelta"]
      val maximumFrequency: Int? = row["MaximumFrequency"]
      val maximumFrequencyPerUser: Int? = row["MaximumFrequencyPerUser"]
      val maximumWatchDurationPerUser: PostgresInterval? = row["MaximumWatchDurationPerUser"]
      val vidSamplingStart: Float = row["VidSamplingIntervalStart"]
      val vidSamplingWidth: Float = row["VidSamplingIntervalWidth"]
      val createTime: Instant = row["CreateTime"]
      val metricDetails: Metric.Details =
        row.getProtoMessage("MetricDetails", Metric.Details.parser())
      val weight: Int = row["Coefficient"]
      val binaryRepresentation: Int = row["BinaryRepresentation"]
      val measurementId: InternalId = row["MeasurementId"]
      val cmmsCreateMeasurementRequestId: UUID = row["CmmsCreateMeasurementRequestId"]
      val cmmsMeasurementId: String? = row["CmmsMeasurementId"]
      val measurementTimeIntervalStart: Instant = row["MeasurementsTimeIntervalStart"]
      val measurementTimeIntervalEnd: Instant = row["MeasurementsTimeIntervalEndExclusive"]
      val measurementState: Measurement.State = Measurement.State.forNumber(row["State"])
      val measurementDetails: Measurement.Details =
        row.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
      val primitiveReportingSetBasisId: InternalId = row["PrimitiveReportingSetBasisId"]
      val primitiveExternalReportingSetId: String = row["PrimitiveExternalReportingSetId"]
      val primitiveReportingSetBasisFilter: String? = row["PrimitiveReportingSetBasisFilter"]

      val metricInfo =
        metricInfoMap.computeIfAbsent(externalMetricId) {
          val metricTimeInterval = interval {
            startTime = metricTimeIntervalStart.toProtoTime()
            endTime = metricTimeIntervalEnd.toProtoTime()
          }

          val vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = vidSamplingStart
              width = vidSamplingWidth
            }

          val metricSpec = metricSpec {
            when (metricType) {
              MetricSpec.TypeCase.REACH ->
                reach =
                  MetricSpecKt.reachParams {
                    privacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = differentialPrivacyEpsilon
                        delta = differentialPrivacyDelta
                      }
                  }
              MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
                if (
                  frequencyDifferentialPrivacyDelta == null ||
                    frequencyDifferentialPrivacyEpsilon == null ||
                    maximumFrequency == null
                ) {
                  throw IllegalStateException()
                }

                reachAndFrequency =
                  MetricSpecKt.reachAndFrequencyParams {
                    reachPrivacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = differentialPrivacyEpsilon
                        delta = differentialPrivacyDelta
                      }
                    frequencyPrivacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = frequencyDifferentialPrivacyEpsilon
                        delta = frequencyDifferentialPrivacyDelta
                      }
                    this.maximumFrequency = maximumFrequency
                  }
              }
              MetricSpec.TypeCase.IMPRESSION_COUNT -> {
                if (maximumFrequencyPerUser == null) {
                  throw IllegalStateException()
                }

                impressionCount =
                  MetricSpecKt.impressionCountParams {
                    privacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = differentialPrivacyEpsilon
                        delta = differentialPrivacyDelta
                      }
                    this.maximumFrequencyPerUser = maximumFrequencyPerUser
                  }
              }
              MetricSpec.TypeCase.WATCH_DURATION -> {
                watchDuration =
                  MetricSpecKt.watchDurationParams {
                    privacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = differentialPrivacyEpsilon
                        delta = differentialPrivacyDelta
                      }
                    this.maximumWatchDurationPerUser =
                      checkNotNull(maximumWatchDurationPerUser).duration.toProtoDuration()
                  }
              }
              MetricSpec.TypeCase.TYPE_NOT_SET -> throw IllegalStateException()
            }
            this.vidSamplingInterval = vidSamplingInterval
          }

          MetricInfo(
            measurementConsumerId = measurementConsumerId,
            cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
            createMetricRequestId = createMetricRequestId,
            externalReportingSetId = externalReportingSetId,
            metricId = metricId,
            externalMetricId = externalMetricId,
            createTime = createTime.toProtoTime(),
            timeInterval = metricTimeInterval,
            metricSpec = metricSpec,
            details = metricDetails,
            weightedMeasurementInfoMap = mutableMapOf()
          )
        }

      val weightedMeasurementInfo =
        metricInfo.weightedMeasurementInfoMap.computeIfAbsent(
          MetricMeasurementKey(
            measurementConsumerId = measurementConsumerId,
            measurementId = measurementId,
            metricId = metricId,
          )
        ) {
          val timeInterval = interval {
            startTime = measurementTimeIntervalStart.toProtoTime()
            endTime = measurementTimeIntervalEnd.toProtoTime()
          }

          val measurementInfo =
            MeasurementInfo(
              cmmsMeasurementId = cmmsMeasurementId,
              cmmsCreateMeasurementRequestId = cmmsCreateMeasurementRequestId.toString(),
              timeInterval = timeInterval,
              state = measurementState,
              details = measurementDetails,
              primitiveReportingSetBasisInfoMap = mutableMapOf(),
            )

          WeightedMeasurementInfo(
            weight = weight,
            binaryRepresentation = binaryRepresentation,
            measurementInfo = measurementInfo,
          )
        }

      val primitiveReportingSetBasisInfo =
        weightedMeasurementInfo.measurementInfo.primitiveReportingSetBasisInfoMap.computeIfAbsent(
          primitiveReportingSetBasisId
        ) {
          PrimitiveReportingSetBasisInfo(
            externalReportingSetId = primitiveExternalReportingSetId,
            filterSet = mutableSetOf()
          )
        }

      if (primitiveReportingSetBasisFilter != null) {
        primitiveReportingSetBasisInfo.filterSet.add(primitiveReportingSetBasisFilter)
      }
    }

    readContext.executeQuery(statement).consume(translate).collect {}

    return metricInfoMap
  }
}
