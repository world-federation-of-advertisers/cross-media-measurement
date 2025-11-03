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
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpec.VidSamplingInterval
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
    val metric: Metric,
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
    val metricDetails: Metric.Details,
    val createTime: Instant,
    val state: Metric.State,
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
    val state: Metric.State,
    val cmmsModelLineName: String?,
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
      FilteredMetrics.MeasurementConsumerId,
      FilteredMetrics.CreateMetricRequestId,
      ReportingSets.ExternalReportingSetId AS MetricsExternalReportingSetId,
      FilteredMetrics.MetricId,
      FilteredMetrics.ExternalMetricId,
      FilteredMetrics.TimeIntervalStart AS MetricsTimeIntervalStart,
      FilteredMetrics.TimeIntervalEndExclusive AS MetricsTimeIntervalEndExclusive,
      FilteredMetrics.MetricType,
      FilteredMetrics.DifferentialPrivacyEpsilon,
      FilteredMetrics.DifferentialPrivacyDelta,
      FilteredMetrics.FrequencyDifferentialPrivacyEpsilon,
      FilteredMetrics.FrequencyDifferentialPrivacyDelta,
      FilteredMetrics.MaximumFrequency,
      FilteredMetrics.MaximumFrequencyPerUser,
      FilteredMetrics.MaximumWatchDurationPerUser,
      FilteredMetrics.VidSamplingIntervalStart,
      FilteredMetrics.VidSamplingIntervalWidth,
      FilteredMetrics.SingleDataProviderDifferentialPrivacyEpsilon,
      FilteredMetrics.SingleDataProviderDifferentialPrivacyDelta,
      FilteredMetrics.SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
      FilteredMetrics.SingleDataProviderFrequencyDifferentialPrivacyDelta,
      FilteredMetrics.SingleDataProviderVidSamplingIntervalStart,
      FilteredMetrics.SingleDataProviderVidSamplingIntervalWidth,
      FilteredMetrics.CreateTime,
      FilteredMetrics.MetricDetails,
      FilteredMetrics.State as MetricsState,
      MetricMeasurements.Coefficient,
      MetricMeasurements.BinaryRepresentation,
      Measurements.MeasurementId,
      Measurements.CmmsCreateMeasurementRequestId,
      Measurements.CmmsMeasurementId,
      Measurements.TimeIntervalStart AS MeasurementsTimeIntervalStart,
      Measurements.TimeIntervalEndExclusive AS MeasurementsTimeIntervalEndExclusive,
      Measurements.State as MeasurementsState,
      Measurements.MeasurementDetails,
      PrimitiveReportingSetBases.PrimitiveReportingSetBasisId,
      PrimitiveReportingSets.ExternalReportingSetId AS PrimitiveExternalReportingSetId,
      PrimitiveReportingSetBasisFilters.Filter AS PrimitiveReportingSetBasisFilter,
      CmmsModelLineName
    """
      .trimIndent()

  private val baseSqlJoins: String =
    """
    JOIN ReportingSets USING (MeasurementConsumerId, ReportingSetId)
    JOIN MetricMeasurements USING (MeasurementConsumerId, MetricId)
    JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
    JOIN MeasurementPrimitiveReportingSetBases USING (MeasurementConsumerId, MeasurementId)
    JOIN PrimitiveReportingSetBases USING (MeasurementConsumerId, PrimitiveReportingSetBasisId)
    JOIN ReportingSets AS PrimitiveReportingSets
      ON FilteredMetrics.MeasurementConsumerId = PrimitiveReportingSets.MeasurementConsumerId
      AND PrimitiveReportingSetId = PrimitiveReportingSets.ReportingSetId
    LEFT JOIN PrimitiveReportingSetBasisFilters
      ON FilteredMetrics.MeasurementConsumerId = PrimitiveReportingSetBasisFilters.MeasurementConsumerId
      AND PrimitiveReportingSetBases.PrimitiveReportingSetBasisId = PrimitiveReportingSetBasisFilters.PrimitiveReportingSetBasisId
    """
      .trimIndent()

  fun readMetricsByRequestId(
    measurementConsumerId: InternalId,
    createMetricRequestIds: Collection<String>,
  ): Flow<Result> {
    if (createMetricRequestIds.isEmpty()) {
      return emptyFlow()
    }

    // There is an index using CreateMetricRequestId, but only for Metrics. MetricMeasurements only
    // has an index using MetricId.
    val sql =
      StringBuilder(
        """
          WITH CreateMetricRequestIds AS MATERIALIZED (
            SELECT
              CreateMetricRequestId
            FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
            AS c(CreateMetricRequestId)
          ), FilteredMetrics AS (
            SELECT
              CmmsMeasurementConsumerId,
              MeasurementConsumerId,
              CreateMetricRequestId,
              MetricId,
              ExternalMetricId,
              ReportingSetId,
              TimeIntervalStart,
              TimeIntervalEndExclusive,
              MetricType,
              DifferentialPrivacyEpsilon,
              DifferentialPrivacyDelta,
              FrequencyDifferentialPrivacyEpsilon,
              FrequencyDifferentialPrivacyDelta,
              MaximumFrequency,
              MaximumFrequencyPerUser,
              MaximumWatchDurationPerUser,
              VidSamplingIntervalStart,
              VidSamplingIntervalWidth,
              SingleDataProviderDifferentialPrivacyEpsilon,
              SingleDataProviderDifferentialPrivacyDelta,
              SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
              SingleDataProviderFrequencyDifferentialPrivacyDelta,
              SingleDataProviderVidSamplingIntervalStart,
              SingleDataProviderVidSamplingIntervalWidth,
              CreateTime,
              MetricDetails,
              State,
              CmmsModelLineName
            FROM MeasurementConsumers
            JOIN Metrics USING (MeasurementConsumerId)
            JOIN CreateMetricRequestIds USING (CreateMetricRequestId)
            WHERE MeasurementConsumers.MeasurementConsumerId = $1
          )
          $baseSqlSelect
          FROM FilteredMetrics
          $baseSqlJoins
        """
          .trimIndent()
      )

    val statement =
      valuesListBoundStatement(valuesStartIndex = 1, paramCount = 1, sql.toString()) {
        bind("$1", measurementConsumerId)
        createMetricRequestIds.forEach { addValuesBinding { bindValuesParam(0, it) } }
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
            metric = metric,
          )
        )
      }
    }
  }

  /** Read all Metrics that are associated with any Measurement with an ID in the given IDs list. */
  fun readMetricsByCmmsMeasurementId(
    measurementConsumerId: InternalId,
    cmmsMeasurementIds: Collection<String>,
  ): Flow<Result> {
    if (cmmsMeasurementIds.isEmpty()) {
      return emptyFlow()
    }

    // There is an index using CmmsMeasurementId, but only for Measurements. The other tables only
    // have an index using MeasurementId.
    val sql =
      StringBuilder(
        """
          WITH CmmsMeasurementIds AS MATERIALIZED (
            SELECT
              CmmsMeasurementId
            FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
            AS c(CmmsMeasurementId)
          ), MeasurementIds AS (
            SELECT
              MeasurementConsumerId,
              MeasurementId
            FROM Measurements
            JOIN CmmsMeasurementIds USING (CmmsMeasurementId)
            WHERE MeasurementConsumerId = $1
          ), FilteredMetrics AS (
            SELECT
              CmmsMeasurementConsumerId,
              Metrics.MeasurementConsumerId,
              Metrics.CreateMetricRequestId,
              Metrics.MetricId,
              Metrics.ExternalMetricId,
              Metrics.ReportingSetId,
              Metrics.TimeIntervalStart,
              Metrics.TimeIntervalEndExclusive,
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
              Metrics.SingleDataProviderDifferentialPrivacyEpsilon,
              Metrics.SingleDataProviderDifferentialPrivacyDelta,
              Metrics.SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
              Metrics.SingleDataProviderFrequencyDifferentialPrivacyDelta,
              Metrics.SingleDataProviderVidSamplingIntervalStart,
              Metrics.SingleDataProviderVidSamplingIntervalWidth,
              Metrics.CreateTime,
              Metrics.MetricDetails,
              Metrics.State,
              Metrics.CmmsModelLineName
            FROM MeasurementConsumers
            JOIN Metrics USING (MeasurementConsumerId)
            JOIN MetricMeasurements USING (MeasurementConsumerId, MetricId)
            JOIN MeasurementIds USING (MeasurementConsumerId, MeasurementId)
          )
          $baseSqlSelect
          FROM FilteredMetrics
          $baseSqlJoins
        """
          .trimIndent()
      )

    val statement =
      valuesListBoundStatement(valuesStartIndex = 1, paramCount = 1, sql.toString()) {
        bind("$1", measurementConsumerId)
        cmmsMeasurementIds.forEach { addValuesBinding { bindValuesParam(0, it) } }
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
            metric = metric,
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
        MetricCalculationSpecReportingMetrics.MetricCalculationSpecId,
        Metrics.CreateMetricRequestId,
        Metrics.ReportingSetId,
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
        Metrics.SingleDataProviderDifferentialPrivacyEpsilon,
        Metrics.SingleDataProviderDifferentialPrivacyDelta,
        Metrics.SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
        Metrics.SingleDataProviderFrequencyDifferentialPrivacyDelta,
        Metrics.SingleDataProviderVidSamplingIntervalStart,
        Metrics.SingleDataProviderVidSamplingIntervalWidth,
        Metrics.MetricDetails,
        Metrics.State,
        Metrics.CreateTime
      """
        .trimIndent()

    val sql =
      StringBuilder(
        """
          WITH ReportingMetricKeys AS MATERIALIZED (
            SELECT
              ReportingSetId,
              TimeIntervalStart,
              TimeIntervalEndExclusive,
              MetricCalculationSpecId
            FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
            AS c(ReportingSetId, TimeIntervalStart, TimeIntervalEndExclusive, MetricCalculationSpecId)
          )
          $sqlSelect
          FROM MeasurementConsumers
            JOIN Metrics USING (MeasurementConsumerId)
            JOIN MetricCalculationSpecReportingMetrics USING(MeasurementConsumerId, MetricId)
            JOIN ReportingMetricKeys
              ON Metrics.ReportingSetId = ReportingMetricKeys.ReportingSetId
              AND Metrics.TimeIntervalStart = ReportingMetricKeys.TimeIntervalStart
              AND Metrics.TimeIntervalEndExclusive = ReportingMetricKeys.TimeIntervalEndExclusive
              AND MetricCalculationSpecReportingMetrics.MetricCalculationSpecId = ReportingMetricKeys.MetricCalculationSpecId
          WHERE MeasurementConsumerId = $1
        """
          .trimIndent()
      )
    val statement =
      valuesListBoundStatement(valuesStartIndex = 1, paramCount = 4, sql.toString()) {
        bind("$1", measurementConsumerId)
        reportingMetricKeys.forEach {
          addValuesBinding {
            bindValuesParam(0, it.reportingSetId)
            bindValuesParam(1, it.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC))
            bindValuesParam(2, it.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC))
            bindValuesParam(3, it.metricCalculationSpecId)
          }
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
      val reportingSetId: InternalId = row["ReportingSetId"]
      val metricCalculationSpecId: InternalId = row["MetricCalculationSpecId"]
      val metricId: InternalId = row["MetricId"]
      val externalMetricId: String = row["ExternalMetricId"]
      val state: Metric.State = row.getProtoEnum("State", Metric.State::forNumber)
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
      val singleDataProviderDifferentialPrivacyEpsilon: Double? =
        row["SingleDataProviderDifferentialPrivacyEpsilon"]
      val singleDataProviderDifferentialPrivacyDelta: Double? =
        row["SingleDataProviderDifferentialPrivacyDelta"]
      val singleDataProviderFrequencyDifferentialPrivacyEpsilon: Double? =
        row["SingleDataProviderFrequencyDifferentialPrivacyEpsilon"]
      val singleDataProviderFrequencyDifferentialPrivacyDelta: Double? =
        row["SingleDataProviderFrequencyDifferentialPrivacyDelta"]
      val singleDataProviderVidSamplingStart: Float? =
        row["SingleDataProviderVidSamplingIntervalStart"]
      val singleDataProviderVidSamplingWidth: Float? =
        row["SingleDataProviderVidSamplingIntervalWidth"]
      val metricDetails: Metric.Details =
        row.getProtoMessage("MetricDetails", Metric.Details.parser())
      val createTime: Instant = row["CreateTime"]

      reportingMetricMap.computeIfAbsent(externalMetricId) {
        val metricTimeInterval = interval {
          startTime = metricTimeIntervalStart.toProtoTime()
          endTime = metricTimeIntervalEnd.toProtoTime()
        }

        val metricSpec =
          buildMetricSpec(
            metricType = metricType,
            differentialPrivacyEpsilon = differentialPrivacyEpsilon,
            differentialPrivacyDelta = differentialPrivacyDelta,
            frequencyDifferentialPrivacyEpsilon = frequencyDifferentialPrivacyEpsilon,
            frequencyDifferentialPrivacyDelta = frequencyDifferentialPrivacyDelta,
            maximumFrequency = maximumFrequency,
            maximumFrequencyPerUser = maximumFrequencyPerUser,
            maximumWatchDurationPerUser = maximumWatchDurationPerUser,
            vidSamplingStart = vidSamplingStart,
            vidSamplingWidth = vidSamplingWidth,
            singleDataProviderDifferentialPrivacyEpsilon =
              singleDataProviderDifferentialPrivacyEpsilon,
            singleDataProviderDifferentialPrivacyDelta = singleDataProviderDifferentialPrivacyDelta,
            singleDataProviderFrequencyDifferentialPrivacyEpsilon =
              singleDataProviderFrequencyDifferentialPrivacyEpsilon,
            singleDataProviderFrequencyDifferentialPrivacyDelta =
              singleDataProviderFrequencyDifferentialPrivacyDelta,
            singleDataProviderVidSamplingStart = singleDataProviderVidSamplingStart,
            singleDataProviderVidSamplingWidth = singleDataProviderVidSamplingWidth,
          )

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
          createTime = createTime,
          state = state,
        )
      }
    }

    readContext.executeQuery(statement).consume(translate).collect {}

    return reportingMetricMap
  }

  fun batchGetMetrics(request: BatchGetMetricsRequest): Flow<Result> {
    // There is an index using ExternalMetricId, but only for Metrics. MetricMeasurements only
    // has an index using MetricId.
    val sql =
      StringBuilder(
        """
          WITH MeasurementConsumerForMetrics AS (
            SELECT
              MeasurementConsumerId,
              CmmsMeasurementConsumerId
            FROM MeasurementConsumers
            WHERE CmmsMeasurementConsumerId = $1
          ), ExternalMetricIds AS MATERIALIZED (
            SELECT
              ExternalMetricId
            FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
            AS c(ExternalMetricId)
          ), FilteredMetrics AS (
            SELECT
              CmmsMeasurementConsumerId,
              MeasurementConsumerId,
              CreateMetricRequestId,
              MetricId,
              ExternalMetricId,
              ReportingSetId,
              TimeIntervalStart,
              TimeIntervalEndExclusive,
              MetricType,
              DifferentialPrivacyEpsilon,
              DifferentialPrivacyDelta,
              FrequencyDifferentialPrivacyEpsilon,
              FrequencyDifferentialPrivacyDelta,
              MaximumFrequency,
              MaximumFrequencyPerUser,
              MaximumWatchDurationPerUser,
              VidSamplingIntervalStart,
              VidSamplingIntervalWidth,
              SingleDataProviderDifferentialPrivacyEpsilon,
              SingleDataProviderDifferentialPrivacyDelta,
              SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
              SingleDataProviderFrequencyDifferentialPrivacyDelta,
              SingleDataProviderVidSamplingIntervalStart,
              SingleDataProviderVidSamplingIntervalWidth,
              CreateTime,
              MetricDetails,
              State,
              CmmsModelLineName
            FROM MeasurementConsumerForMetrics
            JOIN Metrics USING (MeasurementConsumerId)
            JOIN ExternalMetricIds USING (ExternalMetricId)
          )
          $baseSqlSelect
          FROM FilteredMetrics
          $baseSqlJoins
        """
          .trimIndent()
      )

    val statement =
      valuesListBoundStatement(valuesStartIndex = 1, paramCount = 1, sql.toString()) {
        bind("$1", request.cmmsMeasurementConsumerId)
        request.externalMetricIdsList.forEach { addValuesBinding { bindValuesParam(0, it) } }
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
            metric = metric,
          )
        )
      }
    }
  }

  fun readMetrics(request: StreamMetricsRequest): Flow<Result> {
    val sql =
      """
        $baseSqlSelect
        FROM (
          SELECT
            CmmsMeasurementConsumerId,
            MeasurementConsumerId,
            CreateMetricRequestId,
            MetricId,
            ExternalMetricId,
            ReportingSetId,
            TimeIntervalStart,
            TimeIntervalEndExclusive,
            MetricType,
            DifferentialPrivacyEpsilon,
            DifferentialPrivacyDelta,
            FrequencyDifferentialPrivacyEpsilon,
            FrequencyDifferentialPrivacyDelta,
            MaximumFrequency,
            MaximumFrequencyPerUser,
            MaximumWatchDurationPerUser,
            VidSamplingIntervalStart,
            VidSamplingIntervalWidth,
            SingleDataProviderDifferentialPrivacyEpsilon,
            SingleDataProviderDifferentialPrivacyDelta,
            SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
            SingleDataProviderFrequencyDifferentialPrivacyDelta,
            SingleDataProviderVidSamplingIntervalStart,
            SingleDataProviderVidSamplingIntervalWidth,
            CreateTime,
            MetricDetails,
            State,
            CmmsModelLineName
          FROM MeasurementConsumers
            JOIN Metrics USING (MeasurementConsumerId)
          WHERE CmmsMeasurementConsumerId = $1
            AND ExternalMetricId > $2
          ORDER BY ExternalMetricId ASC
          LIMIT $3
        ) AS FilteredMetrics
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
            metric = metric,
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
      if (metricInfo.cmmsModelLineName != null) {
        cmmsModelLine = metricInfo.cmmsModelLineName
      }
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
      state = metricInfo.state
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
      val singleDataProviderDifferentialPrivacyEpsilon: Double? =
        row["SingleDataProviderDifferentialPrivacyEpsilon"]
      val singleDataProviderDifferentialPrivacyDelta: Double? =
        row["SingleDataProviderDifferentialPrivacyDelta"]
      val singleDataProviderFrequencyDifferentialPrivacyEpsilon: Double? =
        row["SingleDataProviderFrequencyDifferentialPrivacyEpsilon"]
      val singleDataProviderFrequencyDifferentialPrivacyDelta: Double? =
        row["SingleDataProviderFrequencyDifferentialPrivacyDelta"]
      val singleDataProviderVidSamplingStart: Float? =
        row["SingleDataProviderVidSamplingIntervalStart"]
      val singleDataProviderVidSamplingWidth: Float? =
        row["SingleDataProviderVidSamplingIntervalWidth"]
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
      val measurementState: Measurement.State =
        row.getProtoEnum("MeasurementsState", Measurement.State::forNumber)
      val measurementDetails: Measurement.Details =
        row.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
      val primitiveReportingSetBasisId: InternalId = row["PrimitiveReportingSetBasisId"]
      val primitiveExternalReportingSetId: String = row["PrimitiveExternalReportingSetId"]
      val primitiveReportingSetBasisFilter: String? = row["PrimitiveReportingSetBasisFilter"]
      val metricState: Metric.State = row.getProtoEnum("MetricsState", Metric.State::forNumber)
      val cmmsModelLineName: String? = row["CmmsModelLineName"]

      val metricInfo =
        metricInfoMap.computeIfAbsent(externalMetricId) {
          val metricTimeInterval = interval {
            startTime = metricTimeIntervalStart.toProtoTime()
            endTime = metricTimeIntervalEnd.toProtoTime()
          }

          val metricSpec =
            buildMetricSpec(
              metricType = metricType,
              differentialPrivacyEpsilon = differentialPrivacyEpsilon,
              differentialPrivacyDelta = differentialPrivacyDelta,
              frequencyDifferentialPrivacyEpsilon = frequencyDifferentialPrivacyEpsilon,
              frequencyDifferentialPrivacyDelta = frequencyDifferentialPrivacyDelta,
              maximumFrequency = maximumFrequency,
              maximumFrequencyPerUser = maximumFrequencyPerUser,
              maximumWatchDurationPerUser = maximumWatchDurationPerUser,
              vidSamplingStart = vidSamplingStart,
              vidSamplingWidth = vidSamplingWidth,
              singleDataProviderDifferentialPrivacyEpsilon =
                singleDataProviderDifferentialPrivacyEpsilon,
              singleDataProviderDifferentialPrivacyDelta =
                singleDataProviderDifferentialPrivacyDelta,
              singleDataProviderFrequencyDifferentialPrivacyEpsilon =
                singleDataProviderFrequencyDifferentialPrivacyEpsilon,
              singleDataProviderFrequencyDifferentialPrivacyDelta =
                singleDataProviderFrequencyDifferentialPrivacyDelta,
              singleDataProviderVidSamplingStart = singleDataProviderVidSamplingStart,
              singleDataProviderVidSamplingWidth = singleDataProviderVidSamplingWidth,
            )

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
            weightedMeasurementInfoMap = mutableMapOf(),
            state = metricState,
            cmmsModelLineName = cmmsModelLineName,
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
            filterSet = mutableSetOf(),
          )
        }

      if (primitiveReportingSetBasisFilter != null) {
        primitiveReportingSetBasisInfo.filterSet.add(primitiveReportingSetBasisFilter)
      }
    }

    readContext.executeQuery(statement).consume(translate).collect {}

    return metricInfoMap
  }

  /** Builds a [MetricSpec] givens all the necessary parameters. */
  private fun buildMetricSpec(
    metricType: MetricSpec.TypeCase,
    differentialPrivacyEpsilon: Double,
    differentialPrivacyDelta: Double,
    frequencyDifferentialPrivacyEpsilon: Double?,
    frequencyDifferentialPrivacyDelta: Double?,
    maximumFrequency: Int?,
    maximumFrequencyPerUser: Int?,
    maximumWatchDurationPerUser: PostgresInterval?,
    vidSamplingStart: Float,
    vidSamplingWidth: Float,
    singleDataProviderDifferentialPrivacyEpsilon: Double?,
    singleDataProviderDifferentialPrivacyDelta: Double?,
    singleDataProviderFrequencyDifferentialPrivacyEpsilon: Double?,
    singleDataProviderFrequencyDifferentialPrivacyDelta: Double?,
    singleDataProviderVidSamplingStart: Float?,
    singleDataProviderVidSamplingWidth: Float?,
  ): MetricSpec {
    val vidSamplingInterval =
      MetricSpecKt.vidSamplingInterval {
        start = vidSamplingStart
        width = vidSamplingWidth
      }

    val singleDataProviderVidSamplingInterval: VidSamplingInterval? =
      if (
        singleDataProviderVidSamplingStart != null && singleDataProviderVidSamplingWidth != null
      ) {
        MetricSpecKt.vidSamplingInterval {
          start = singleDataProviderVidSamplingStart
          width = singleDataProviderVidSamplingWidth
        }
      } else {
        null
      }

    return metricSpec {
      when (metricType) {
        MetricSpec.TypeCase.REACH ->
          reach =
            MetricSpecKt.reachParams {
              multipleDataProviderParams =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                  this.vidSamplingInterval = vidSamplingInterval
                }
              if (
                singleDataProviderVidSamplingInterval != null &&
                  singleDataProviderDifferentialPrivacyEpsilon != null &&
                  singleDataProviderDifferentialPrivacyDelta != null
              ) {
                singleDataProviderParams =
                  MetricSpecKt.samplingAndPrivacyParams {
                    privacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = singleDataProviderDifferentialPrivacyEpsilon
                        delta = singleDataProviderDifferentialPrivacyDelta
                      }
                    this.vidSamplingInterval = singleDataProviderVidSamplingInterval
                  }
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
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
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
                  this.vidSamplingInterval = vidSamplingInterval
                }
              if (
                singleDataProviderVidSamplingInterval != null &&
                  singleDataProviderDifferentialPrivacyEpsilon != null &&
                  singleDataProviderDifferentialPrivacyDelta != null &&
                  singleDataProviderFrequencyDifferentialPrivacyEpsilon != null &&
                  singleDataProviderFrequencyDifferentialPrivacyDelta != null
              ) {
                singleDataProviderParams =
                  MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                    reachPrivacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = singleDataProviderDifferentialPrivacyEpsilon
                        delta = singleDataProviderDifferentialPrivacyDelta
                      }
                    frequencyPrivacyParams =
                      MetricSpecKt.differentialPrivacyParams {
                        epsilon = singleDataProviderFrequencyDifferentialPrivacyEpsilon
                        delta = singleDataProviderFrequencyDifferentialPrivacyDelta
                      }
                    this.vidSamplingInterval = singleDataProviderVidSamplingInterval
                  }
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
              params =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                  this.vidSamplingInterval = vidSamplingInterval
                }
              this.maximumFrequencyPerUser = maximumFrequencyPerUser
            }
        }
        MetricSpec.TypeCase.WATCH_DURATION -> {
          watchDuration =
            MetricSpecKt.watchDurationParams {
              params =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = differentialPrivacyEpsilon
                      delta = differentialPrivacyDelta
                    }
                  this.vidSamplingInterval = vidSamplingInterval
                }
              this.maximumWatchDurationPerUser =
                checkNotNull(maximumWatchDurationPerUser).duration.toProtoDuration()
            }
        }
        MetricSpec.TypeCase.POPULATION_COUNT -> {
          populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
        }
        MetricSpec.TypeCase.TYPE_NOT_SET -> throw IllegalStateException()
      }
    }
  }
}
