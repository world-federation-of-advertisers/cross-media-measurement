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
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.TimeInterval
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.timeInterval

class MetricReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val metricId: InternalId,
    val createMetricRequestId: String,
    val metric: Metric
  )

  private data class MetricInfo(
    val measurementConsumerId: InternalId,
    val cmmsMeasurementConsumerId: String,
    val createMetricRequestId: String?,
    val externalReportingSetId: ExternalId,
    val metricId: InternalId,
    val externalMetricId: ExternalId,
    val createTime: Timestamp,
    val timeInterval: TimeInterval,
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
    val measurementInfo: MeasurementInfo,
  )

  private data class MeasurementInfo(
    val cmmsMeasurementId: String?,
    val cmmsCreateMeasurementRequestId: String,
    val timeInterval: TimeInterval,
    // Key is primitiveReportingSetBasisId.
    val primitiveReportingSetBasisInfoMap: MutableMap<InternalId, PrimitiveReportingSetBasisInfo>,
    val state: Measurement.State,
    val details: Measurement.Details,
  )

  private data class PrimitiveReportingSetBasisInfo(
    val externalReportingSetId: ExternalId,
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
      Metrics.MaximumFrequencyPerUser,
      Metrics.MaximumWatchDurationPerUser,
      Metrics.VidSamplingIntervalStart,
      Metrics.VidSamplingIntervalWidth,
      Metrics.CreateTime,
      Metrics.MetricDetails,
      MetricMeasurements.Coefficient,
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
        baseSqlSelect +
          """
        FROM MeasurementConsumers
          JOIN Metrics USING(MeasurementConsumerId)
        """ +
          baseSqlJoins +
          """
        WHERE Metrics.MeasurementConsumerId = $1
          AND CreateMetricRequestId IN
        """
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

    return createResultFlow(statement)
  }

  fun batchGetMetrics(
    request: BatchGetMetricsRequest,
  ): Flow<Result> {
    val sql =
      StringBuilder(
        baseSqlSelect +
          """
        FROM MeasurementConsumers
          JOIN Metrics USING(MeasurementConsumerId)
        """ +
          baseSqlJoins +
          """
        WHERE CmmsMeasurementConsumerId = $1
          AND ExternalMetricId IN
        """
      )

    var i = 2
    val bindingMap = mutableMapOf<Long, String>()
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

    return createResultFlow(statement)
  }

  fun readMetrics(
    request: StreamMetricsRequest,
  ): Flow<Result> {
    val statement =
      boundStatement(
        baseSqlSelect +
          """
        FROM (
          SELECT *
          FROM MeasurementConsumers
            JOIN Metrics USING (MeasurementConsumerId)
          WHERE CmmsMeasurementConsumerId = $1
            AND ExternalMetricId > $2
          ORDER BY ExternalMetricId ASC
          LIMIT $3
        ) AS Metrics
      """ +
          baseSqlJoins +
          """
          ORDER BY ExternalMetricId ASC
          """
      ) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalMetricIdAfter)
        if (request.limit > 0) {
          bind("$3", request.limit)
        } else {
          bind("$3", 50)
        }
      }

    return createResultFlow(statement)
  }

  private fun createResultFlow(statement: BoundStatement): Flow<Result> {
    return flow {
      val metricInfoMap = buildResultMap(statement)

      for (entry in metricInfoMap) {
        val metricId = entry.key
        val metricInfo = entry.value

        val metric = metric {
          cmmsMeasurementConsumerId = metricInfo.cmmsMeasurementConsumerId
          externalMetricId = metricInfo.externalMetricId.value
          externalReportingSetId = metricInfo.externalReportingSetId.value
          createTime = metricInfo.createTime
          timeInterval = metricInfo.timeInterval
          metricSpec = metricInfo.metricSpec
          metricInfo.weightedMeasurementInfoMap.values.forEach {
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = it.weight
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
                        externalReportingSetId = it.externalReportingSetId.value
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

        val createMetricRequestId = metricInfo.createMetricRequestId ?: ""
        emit(
          Result(
            measurementConsumerId = metricInfo.measurementConsumerId,
            metricId = metricId,
            createMetricRequestId = createMetricRequestId,
            metric = metric
          )
        )
      }
    }
  }

  /** Returns a map that maintains the order of the query result. */
  private suspend fun buildResultMap(statement: BoundStatement): Map<InternalId, MetricInfo> {
    // Key is metricId.
    val metricInfoMap: MutableMap<InternalId, MetricInfo> = linkedMapOf()

    val translate: (row: ResultRow) -> Unit = { row: ResultRow ->
      val measurementConsumerId: InternalId = row["MeasurementConsumerId"]
      val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
      val createMetricRequestId: String? = row["CreateMetricRequestId"]
      val externalReportingSetId: ExternalId = row["MetricsExternalReportingSetId"]
      val metricId: InternalId = row["MetricId"]
      val externalMetricId: ExternalId = row["ExternalMetricId"]
      val metricTimeIntervalStart: Instant = row["MetricsTimeIntervalStart"]
      val metricTimeIntervalEnd: Instant = row["MetricsTimeIntervalEndExclusive"]
      val metricType: MetricSpec.TypeCase = MetricSpec.TypeCase.forNumber(row["MetricType"])
      val differentialPrivacyEpsilon: Double = row["DifferentialPrivacyEpsilon"]
      val differentialPrivacyDelta: Double = row["DifferentialPrivacyDelta"]
      val frequencyDifferentialPrivacyEpsilon: Double? = row["FrequencyDifferentialPrivacyEpsilon"]
      val frequencyDifferentialPrivacyDelta: Double? = row["FrequencyDifferentialPrivacyDelta"]
      val maximumFrequencyPerUser: Int? = row["MaximumFrequencyPerUser"]
      val maximumWatchDurationPerUser: Int? = row["MaximumWatchDurationPerUser"]
      val vidSamplingStart: Float = row["VidSamplingIntervalStart"]
      val vidSamplingWidth: Float = row["VidSamplingIntervalWidth"]
      val createTime: Instant = row["CreateTime"]
      val metricDetails: Metric.Details =
        row.getProtoMessage("MetricDetails", Metric.Details.parser())
      val weight: Int = row["Coefficient"]
      val measurementId: InternalId = row["MeasurementId"]
      val cmmsCreateMeasurementRequestId: UUID = row["CmmsCreateMeasurementRequestId"]
      val cmmsMeasurementId: String? = row["CmmsMeasurementId"]
      val measurementTimeIntervalStart: Instant = row["MeasurementsTimeIntervalStart"]
      val measurementTimeIntervalEnd: Instant = row["MeasurementsTimeIntervalEndExclusive"]
      val measurementState: Measurement.State = Measurement.State.forNumber(row["State"])
      val measurementDetails: Measurement.Details =
        row.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
      val primitiveReportingSetBasisId: InternalId = row["PrimitiveReportingSetBasisId"]
      val primitiveExternalReportingSetId: ExternalId = row["PrimitiveExternalReportingSetId"]
      val primitiveReportingSetBasisFilter: String? = row["PrimitiveReportingSetBasisFilter"]

      val metricInfo =
        metricInfoMap.computeIfAbsent(metricId) {
          val metricTimeInterval = timeInterval {
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
              MetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
                if (
                  frequencyDifferentialPrivacyDelta == null ||
                    frequencyDifferentialPrivacyEpsilon == null ||
                    maximumFrequencyPerUser == null
                ) {
                  throw IllegalStateException()
                }

                frequencyHistogram =
                  MetricSpecKt.frequencyHistogramParams {
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
                    this.maximumFrequencyPerUser = maximumFrequencyPerUser
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
                if (maximumWatchDurationPerUser == null) {
                  throw IllegalStateException()
                }

                watchDuration =
                  MetricSpecKt.watchDurationParams {
                    privacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = differentialPrivacyEpsilon
                        delta = differentialPrivacyDelta
                      }
                    this.maximumWatchDurationPerUser = maximumWatchDurationPerUser
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
          val timeInterval = timeInterval {
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
