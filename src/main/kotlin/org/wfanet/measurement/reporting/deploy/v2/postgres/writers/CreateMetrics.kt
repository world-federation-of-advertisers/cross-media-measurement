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

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import io.r2dbc.postgresql.codec.Interval as PostgresInterval
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.MetricNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts Metrics into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [MetricNotFoundException] Metric not found
 * * [MetricAlreadyExistsException] Metric already exists
 */
class CreateMetrics(private val requests: List<CreateMetricRequest>) :
  PostgresWriter<List<Metric>>() {
  private data class WeightedMeasurementsAndStatementComponents(
    val weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    val measurementsCurIndex: Int,
    val measurementsRowsSqlList: List<String>,
    val measurementsBinders: List<BoundStatement.Binder.() -> Unit>,
    val metricMeasurementsCurIndex: Int,
    val metricMeasurementsRowsSqlList: List<String>,
    val metricMeasurementsBinders: List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasesCurIndex: Int,
    val primitiveReportingSetBasesRowsSqlList: List<String>,
    val primitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersCurIndex: Int,
    val primitiveReportingSetBasisFiltersRowsSqlList: List<String>,
    val primitiveReportingSetBasisFiltersBinders: List<BoundStatement.Binder.() -> Unit>,
    val measurementPrimitiveReportingSetBasesCurIndex: Int,
    val measurementPrimitiveReportingSetBasesRowsSqlList: List<String>,
    val measurementPrimitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
  )

  private data class PrimitiveReportingSetBasesStatementComponents(
    val primitiveReportingSetBasesCurIndex: Int,
    val primitiveReportingSetBasesRowsSqlList: List<String>,
    val primitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersCurIndex: Int,
    val primitiveReportingSetBasisFiltersRowsSqlList: List<String>,
    val primitiveReportingSetBasisFiltersBinders: List<BoundStatement.Binder.() -> Unit>,
    val measurementPrimitiveReportingSetBasesCurIndex: Int,
    val measurementPrimitiveReportingSetBasesRowsSqlList: List<String>,
    val measurementPrimitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
  )

  override suspend fun TransactionScope.runTransaction(): List<Metric> {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(requests[0].metric.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    // Request IDs take precedence
    val createMetricRequestIds: List<String> =
      requests.mapNotNull { it.requestId.ifBlank { null } }.distinct()

    val existingMetricsMap =
      MetricReader(transactionContext)
        .readMetricsByRequestId(measurementConsumerId, createMetricRequestIds)
        .toList()
        .associateBy({ it.createMetricRequestId }, { it.metric })

    val externalIdsSet: Set<String> =
      requests
        .mapNotNull {
          if (existingMetricsMap.containsKey(it.requestId)) {
            null
          } else {
            it.externalMetricId
          }
        }
        .toSet()

    if (externalIdsSet.isNotEmpty()) {
      val batchGetMetricsRequest: BatchGetMetricsRequest = batchGetMetricsRequest {
        cmmsMeasurementConsumerId = requests[0].metric.cmmsMeasurementConsumerId
        externalMetricIds += externalIdsSet
      }
      // If there is any metrics found, it means there are metrics already existing with different
      // request IDs or without request IDs.
      if (
        MetricReader(transactionContext)
          .batchGetMetrics(batchGetMetricsRequest)
          .toList()
          .isNotEmpty()
      ) {
        throw MetricAlreadyExistsException()
      }
    }

    val externalReportingSetIds = buildSet {
      for (request in requests) {
        if (!existingMetricsMap.containsKey(request.requestId)) {
          add(request.metric.externalReportingSetId)
          for (weightedMeasurement in request.metric.weightedMeasurementsList) {
            for (bases in weightedMeasurement.measurement.primitiveReportingSetBasesList) {
              add(bases.externalReportingSetId)
            }
          }
        }
      }
    }

    val reportingSetMap: Map<String, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    if (reportingSetMap.size < externalReportingSetIds.size) {
      throw ReportingSetNotFoundException()
    }

    val metrics = mutableListOf<Metric>()
    val metricsRowsSqlList = mutableListOf<String>()
    val metricsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    var metricsCurIndex = 1
    val metricsOffset = 20

    val metricCalculationSpecReportingMetricsBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()
    val metricCalculationSpecReportingMetricsRowsSqlList = mutableListOf<String>()
    var metricCalculationSpecReportingMetricsCurIndex = 2
    val metricCalculationSpecReportingMetricsOffset = 2

    val measurementsRowsSqlList = mutableListOf<String>()
    var measurementsCurIndex = 1
    val measurementsOffset = 9
    val measurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val metricMeasurementsRowsSqlList = mutableListOf<String>()
    var metricMeasurementsCurIndex = 1
    val metricMeasurementsOffset = 5
    val metricMeasurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val primitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasesCurIndex = 1
    val primitiveReportingSetBasesOffset = 3
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val primitiveReportingSetBasisFiltersRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasisFiltersCurIndex = 1
    val primitiveReportingSetBasisFiltersOffset = 4
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val measurementPrimitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    var measurementPrimitiveReportingSetBasesCurIndex = 1
    val measurementPrimitiveReportingSetBasesOffset = 3
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()

    requests.forEach {
      val existingMetric: Metric? = existingMetricsMap[it.requestId]
      if (existingMetric != null) {
        metrics.add(existingMetric)
      } else {
        val metricId = idGenerator.generateInternalId()
        val externalMetricId: String = it.externalMetricId
        val reportingSetId: InternalId? = reportingSetMap[it.metric.externalReportingSetId]
        val createTime = Instant.now().atOffset(ZoneOffset.UTC)
        val vidSamplingIntervalStart =
          if (it.metric.metricSpec.typeCase == MetricSpec.TypeCase.POPULATION_COUNT) 0
          else it.metric.metricSpec.vidSamplingInterval.start
        val vidSamplingIntervalWidth =
          if (it.metric.metricSpec.typeCase == MetricSpec.TypeCase.POPULATION_COUNT) 0
          else it.metric.metricSpec.vidSamplingInterval.width

        metricsRowsSqlList.add(
          generateParameterizedInsertValues(metricsCurIndex, metricsCurIndex + metricsOffset)
        )

        val tempMetricsCurIndex = metricsCurIndex
        metricsBinders.add {
          bind("$${tempMetricsCurIndex}", measurementConsumerId)
          bind("$${tempMetricsCurIndex + 1}", metricId)
          if (it.requestId.isNotEmpty()) {
            bind("$${tempMetricsCurIndex + 2}", it.requestId)
          } else {
            bind<String?>("$${tempMetricsCurIndex + 2}", null)
          }
          bind("$${tempMetricsCurIndex + 3}", reportingSetId)
          bind("$${tempMetricsCurIndex + 4}", externalMetricId)
          bind(
            "$${tempMetricsCurIndex + 5}",
            it.metric.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC),
          )
          bind(
            "$${tempMetricsCurIndex + 6}",
            it.metric.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC),
          )
          bind("$${tempMetricsCurIndex + 7}", it.metric.metricSpec.typeCase.number)
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
          when (it.metric.metricSpec.typeCase) {
            MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
              val reachAndFrequency = it.metric.metricSpec.reachAndFrequency
              bind("$${tempMetricsCurIndex + 8}", reachAndFrequency.reachPrivacyParams.epsilon)
              bind("$${tempMetricsCurIndex + 9}", reachAndFrequency.reachPrivacyParams.delta)
              bind("$${tempMetricsCurIndex + 10}", reachAndFrequency.frequencyPrivacyParams.epsilon)
              bind("$${tempMetricsCurIndex + 11}", reachAndFrequency.reachPrivacyParams.delta)
              bind<Long?>("$${tempMetricsCurIndex + 12}", null)
              bind<PostgresInterval?>("$${tempMetricsCurIndex + 13}", null)
              bind("$${tempMetricsCurIndex + 19}", reachAndFrequency.maximumFrequency)
            }
            MetricSpec.TypeCase.REACH -> {
              val reach = it.metric.metricSpec.reach
              bind("$${tempMetricsCurIndex + 8}", reach.privacyParams.epsilon)
              bind("$${tempMetricsCurIndex + 9}", reach.privacyParams.delta)
              bind<Double?>("$${tempMetricsCurIndex + 10}", null)
              bind<Double?>("$${tempMetricsCurIndex + 11}", null)
              bind<Long?>("$${tempMetricsCurIndex + 12}", null)
              bind<PostgresInterval?>("$${tempMetricsCurIndex + 13}", null)
              bind<Long?>("$${tempMetricsCurIndex + 19}", null)
            }
            MetricSpec.TypeCase.IMPRESSION_COUNT -> {
              val impressionCount = it.metric.metricSpec.impressionCount
              bind("$${tempMetricsCurIndex + 8}", impressionCount.privacyParams.epsilon)
              bind("$${tempMetricsCurIndex + 9}", impressionCount.privacyParams.delta)
              bind<Double?>("$${tempMetricsCurIndex + 10}", null)
              bind<Double?>("$${tempMetricsCurIndex + 11}", null)
              bind("$${tempMetricsCurIndex + 12}", impressionCount.maximumFrequencyPerUser)
              bind<PostgresInterval?>("$${tempMetricsCurIndex + 13}", null)
              bind<Long?>("$${tempMetricsCurIndex + 19}", null)
            }
            MetricSpec.TypeCase.WATCH_DURATION -> {
              val watchDuration = it.metric.metricSpec.watchDuration
              bind("$${tempMetricsCurIndex + 8}", watchDuration.privacyParams.epsilon)
              bind("$${tempMetricsCurIndex + 9}", watchDuration.privacyParams.delta)
              bind<Double?>("$${tempMetricsCurIndex + 10}", null)
              bind<Double?>("$${tempMetricsCurIndex + 11}", null)
              bind<Long?>("$${tempMetricsCurIndex + 12}", null)
              bind(
                "$${tempMetricsCurIndex + 13}",
                PostgresInterval.of(watchDuration.maximumWatchDurationPerUser.toDuration()),
              )
              bind<Long?>("$${tempMetricsCurIndex + 19}", null)
            }
            MetricSpec.TypeCase.POPULATION_COUNT -> {
              bind("$${tempMetricsCurIndex + 8}", 0)
              bind("$${tempMetricsCurIndex + 9}", 0)
              bind<Double?>("$${tempMetricsCurIndex + 10}", null)
              bind<Double?>("$${tempMetricsCurIndex + 11}", null)
              bind<Long?>("$${tempMetricsCurIndex + 12}", null)
              bind<PostgresInterval?>("$${tempMetricsCurIndex + 13}", null)
              bind<Long?>("$${tempMetricsCurIndex + 19}", null)
            }
            MetricSpec.TypeCase.TYPE_NOT_SET -> {}
          }
          bind("$${tempMetricsCurIndex + 14}", vidSamplingIntervalStart)
          bind("$${tempMetricsCurIndex + 15}", vidSamplingIntervalWidth)
          bind("$${tempMetricsCurIndex + 16}", createTime)
          bind("$${tempMetricsCurIndex + 17}", it.metric.details)
          bind("$${tempMetricsCurIndex + 18}", it.metric.details.toJson())
        }
        metricsCurIndex += metricsOffset

        if (it.requestId.isNotEmpty()) {
          val createMetricRequestUuid: UUID? =
            try {
              UUID.fromString(it.requestId)
            } catch (_: IllegalArgumentException) {
              // Non-Report Metrics do not have to use a UUID.
              null
            }

          if (createMetricRequestUuid != null) {
            metricCalculationSpecReportingMetricsRowsSqlList.add(
              generateParameterizedInsertValues(
                metricCalculationSpecReportingMetricsCurIndex,
                metricCalculationSpecReportingMetricsCurIndex +
                  metricCalculationSpecReportingMetricsOffset,
              )
            )

            val tempMetricCalculationSpecReportingMetricsCurIndex =
              metricCalculationSpecReportingMetricsCurIndex
            metricCalculationSpecReportingMetricsBinders.add {
              bind("$${tempMetricCalculationSpecReportingMetricsCurIndex}", metricId)
              bind(
                "$${tempMetricCalculationSpecReportingMetricsCurIndex + 1}",
                createMetricRequestUuid,
              )
            }

            metricCalculationSpecReportingMetricsCurIndex +=
              metricCalculationSpecReportingMetricsOffset
          }
        }

        val weightedMeasurementsAndStatementComponents =
          createWeightedMeasurementsStatementComponents(
            measurementConsumerId = measurementConsumerId,
            metricId = metricId,
            weightedMeasurements = it.metric.weightedMeasurementsList,
            reportingSetMap = reportingSetMap,
            measurementsStartingIndex = measurementsCurIndex,
            measurementsOffset = measurementsOffset,
            metricMeasurementsStartingIndex = metricMeasurementsCurIndex,
            metricMeasurementsOffset = metricMeasurementsOffset,
            primitiveReportingSetBasesStartingIndex = primitiveReportingSetBasesCurIndex,
            primitiveReportingSetBasesOffset = primitiveReportingSetBasesOffset,
            primitiveReportingSetBasisFiltersStartingIndex =
              primitiveReportingSetBasisFiltersCurIndex,
            primitiveReportingSetBasisFiltersOffset = primitiveReportingSetBasisFiltersOffset,
            measurementPrimitiveReportingSetBasesStartingIndex =
              measurementPrimitiveReportingSetBasesCurIndex,
            measurementPrimitiveReportingSetBasesOffset =
              measurementPrimitiveReportingSetBasesOffset,
          )

        metrics.add(
          it.metric.copy {
            this.externalMetricId = externalMetricId
            weightedMeasurements.clear()
            weightedMeasurements.addAll(
              weightedMeasurementsAndStatementComponents.weightedMeasurements
            )
            this.createTime = createTime.toInstant().toProtoTime()
          }
        )

        measurementsCurIndex = weightedMeasurementsAndStatementComponents.measurementsCurIndex
        measurementsRowsSqlList.addAll(
          weightedMeasurementsAndStatementComponents.measurementsRowsSqlList
        )
        measurementsBinders.addAll(weightedMeasurementsAndStatementComponents.measurementsBinders)

        metricMeasurementsCurIndex =
          weightedMeasurementsAndStatementComponents.metricMeasurementsCurIndex
        metricMeasurementsRowsSqlList.addAll(
          weightedMeasurementsAndStatementComponents.metricMeasurementsRowsSqlList
        )
        metricMeasurementsBinders.addAll(
          weightedMeasurementsAndStatementComponents.metricMeasurementsBinders
        )

        primitiveReportingSetBasesCurIndex =
          weightedMeasurementsAndStatementComponents.primitiveReportingSetBasesCurIndex
        primitiveReportingSetBasesRowsSqlList.addAll(
          weightedMeasurementsAndStatementComponents.primitiveReportingSetBasesRowsSqlList
        )
        primitiveReportingSetBasesBinders.addAll(
          weightedMeasurementsAndStatementComponents.primitiveReportingSetBasesBinders
        )

        primitiveReportingSetBasisFiltersCurIndex =
          weightedMeasurementsAndStatementComponents.primitiveReportingSetBasisFiltersCurIndex
        primitiveReportingSetBasisFiltersRowsSqlList.addAll(
          weightedMeasurementsAndStatementComponents.primitiveReportingSetBasisFiltersRowsSqlList
        )
        primitiveReportingSetBasisFiltersBinders.addAll(
          weightedMeasurementsAndStatementComponents.primitiveReportingSetBasisFiltersBinders
        )

        measurementPrimitiveReportingSetBasesCurIndex =
          weightedMeasurementsAndStatementComponents.measurementPrimitiveReportingSetBasesCurIndex
        measurementPrimitiveReportingSetBasesRowsSqlList.addAll(
          weightedMeasurementsAndStatementComponents
            .measurementPrimitiveReportingSetBasesRowsSqlList
        )
        measurementPrimitiveReportingSetBasesBinders.addAll(
          weightedMeasurementsAndStatementComponents.measurementPrimitiveReportingSetBasesBinders
        )
      }
    }

    val statement =
      boundStatement(
        """
        INSERT INTO Metrics
          (
            MeasurementConsumerId,
            MetricId,
            CreateMetricRequestId,
            ReportingSetId,
            ExternalMetricId,
            TimeIntervalStart,
            TimeIntervalEndExclusive,
            MetricType,
            DifferentialPrivacyEpsilon,
            DifferentialPrivacyDelta,
            FrequencyDifferentialPrivacyEpsilon,
            FrequencyDifferentialPrivacyDelta,
            MaximumFrequencyPerUser,
            MaximumWatchDurationPerUser,
            VidSamplingIntervalStart,
            VidSamplingIntervalWidth,
            CreateTime,
            MetricDetails,
            MetricDetailsJson,
            MaximumFrequency
          )
        VALUES
        ${metricsRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in metricsBinders) {
          binder()
        }
      }

    val metricCalculationSpecReportingMetricsStatement =
      boundStatement(
        """
        UPDATE MetricCalculationSpecReportingMetrics AS m SET MetricId = c.MetricId
        FROM (VALUES ${metricCalculationSpecReportingMetricsRowsSqlList.joinToString(",")})
        AS c(MetricId, CreateMetricRequestId)
        WHERE MeasurementConsumerId = $1 AND m.CreateMetricRequestId = c.CreateMetricRequestId
        """
      ) {
        bind("$1", measurementConsumerId)
        for (binder in metricCalculationSpecReportingMetricsBinders) {
          binder()
        }
      }

    val measurementsStatement =
      boundStatement(
        """
        INSERT INTO Measurements
          (
            MeasurementConsumerId,
            MeasurementId,
            CmmsCreateMeasurementRequestId,
            CmmsMeasurementId,
            TimeIntervalStart,
            TimeIntervalEndExclusive,
            State,
            MeasurementDetails,
            MeasurementDetailsJson
          )
        VALUES
        ${measurementsRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in measurementsBinders) {
          binder()
        }
      }

    val metricMeasurementsStatement =
      boundStatement(
        """
        INSERT INTO MetricMeasurements
          (
            MeasurementConsumerId,
            MetricId,
            MeasurementId,
            Coefficient,
            BinaryRepresentation
          )
        VALUES
        ${metricMeasurementsRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in metricMeasurementsBinders) {
          binder()
        }
      }

    val primitiveReportingSetBasesStatement =
      boundStatement(
        """
        INSERT INTO PrimitiveReportingSetBases
          (
            MeasurementConsumerId,
            PrimitiveReportingSetBasisId,
            PrimitiveReportingSetId
          )
        VALUES
        ${primitiveReportingSetBasesRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in primitiveReportingSetBasesBinders) {
          binder()
        }
      }

    val primitiveReportingSetBasisFiltersStatement =
      boundStatement(
        """
        INSERT INTO PrimitiveReportingSetBasisFilters
          (
            MeasurementConsumerId,
            PrimitiveReportingSetBasisId,
            PrimitiveReportingSetBasisFilterId,
            Filter
          )
        VALUES
        ${primitiveReportingSetBasisFiltersRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in primitiveReportingSetBasisFiltersBinders) {
          binder()
        }
      }

    val measurementPrimitiveReportingSetBasesStatement =
      boundStatement(
        """
        INSERT INTO MeasurementPrimitiveReportingSetBases
          (
            MeasurementConsumerId,
            MeasurementId,
            PrimitiveReportingSetBasisId
          )
        VALUES
        ${measurementPrimitiveReportingSetBasesRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in measurementPrimitiveReportingSetBasesBinders) {
          binder()
        }
      }

    if (existingMetricsMap.size < requests.size) {
      transactionContext.run {
        executeStatement(statement)
        if (metricCalculationSpecReportingMetricsBinders.size > 0) {
          executeStatement(metricCalculationSpecReportingMetricsStatement)
        }
        executeStatement(measurementsStatement)
        executeStatement(metricMeasurementsStatement)
        executeStatement(primitiveReportingSetBasesStatement)
        if (primitiveReportingSetBasisFiltersBinders.size > 0) {
          executeStatement(primitiveReportingSetBasisFiltersStatement)
        }
        executeStatement(measurementPrimitiveReportingSetBasesStatement)
      }
    }

    return metrics
  }

  private fun TransactionScope.createWeightedMeasurementsStatementComponents(
    measurementConsumerId: InternalId,
    metricId: InternalId,
    weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    reportingSetMap: Map<String, InternalId>,
    measurementsStartingIndex: Int,
    measurementsOffset: Int,
    metricMeasurementsStartingIndex: Int,
    metricMeasurementsOffset: Int,
    primitiveReportingSetBasesStartingIndex: Int,
    primitiveReportingSetBasesOffset: Int,
    primitiveReportingSetBasisFiltersStartingIndex: Int,
    primitiveReportingSetBasisFiltersOffset: Int,
    measurementPrimitiveReportingSetBasesStartingIndex: Int,
    measurementPrimitiveReportingSetBasesOffset: Int,
  ): WeightedMeasurementsAndStatementComponents {
    val updatedWeightedMeasurements = mutableListOf<Metric.WeightedMeasurement>()

    val measurementsRowsSqlList = mutableListOf<String>()
    var measurementsCurIndex = measurementsStartingIndex
    val measurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val metricMeasurementsRowsSqlList = mutableListOf<String>()
    var metricMeasurementsCurIndex = metricMeasurementsStartingIndex
    val metricMeasurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val primitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasesCurIndex = primitiveReportingSetBasesStartingIndex
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val primitiveReportingSetBasisFiltersRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersStartingIndex
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val measurementPrimitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    var measurementPrimitiveReportingSetBasesCurIndex =
      measurementPrimitiveReportingSetBasesStartingIndex
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()

    weightedMeasurements.forEach {
      val measurementId = idGenerator.generateInternalId()
      val uuid = UUID.randomUUID()
      updatedWeightedMeasurements.add(
        it.copy {
          measurement = measurement.copy { cmmsCreateMeasurementRequestId = uuid.toString() }
        }
      )
      val tempMeasurementsCurIndex = measurementsCurIndex
      measurementsBinders.add {
        bind("$${tempMeasurementsCurIndex}", measurementConsumerId)
        bind("$${tempMeasurementsCurIndex + 1}", measurementId)
        bind("$${tempMeasurementsCurIndex + 2}", uuid)
        bind<String?>("$${tempMeasurementsCurIndex + 3}", null)
        bind(
          "$${tempMeasurementsCurIndex + 4}",
          it.measurement.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC),
        )
        bind(
          "$${tempMeasurementsCurIndex + 5}",
          it.measurement.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC),
        )
        bind("$${tempMeasurementsCurIndex + 6}", Measurement.State.STATE_UNSPECIFIED)
        bind("$${tempMeasurementsCurIndex + 7}", Measurement.Details.getDefaultInstance())
        bind("$${tempMeasurementsCurIndex + 8}", Measurement.Details.getDefaultInstance().toJson())
      }
      measurementsRowsSqlList.add(
        generateParameterizedInsertValues(
          measurementsCurIndex,
          measurementsCurIndex + measurementsOffset,
        )
      )
      measurementsCurIndex += measurementsOffset

      val tempMetricsMeasurementsCurIndex = metricMeasurementsCurIndex
      metricMeasurementsBinders.add {
        bind("$${tempMetricsMeasurementsCurIndex}", measurementConsumerId)
        bind("$${tempMetricsMeasurementsCurIndex + 1}", metricId)
        bind("$${tempMetricsMeasurementsCurIndex + 2}", measurementId)
        bind("$${tempMetricsMeasurementsCurIndex + 3}", it.weight)
        bind("$${tempMetricsMeasurementsCurIndex + 4}", it.binaryRepresentation)
      }
      metricMeasurementsRowsSqlList.add(
        generateParameterizedInsertValues(
          metricMeasurementsCurIndex,
          metricMeasurementsCurIndex + metricMeasurementsOffset,
        )
      )
      metricMeasurementsCurIndex += metricMeasurementsOffset

      val primitiveReportingSetBasesStatementComponents =
        createPrimitiveReportingSetBasesStatementComponents(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          primitiveReportingSetBases = it.measurement.primitiveReportingSetBasesList,
          reportingSetMap = reportingSetMap,
          primitiveReportingSetBasesStartingIndex = primitiveReportingSetBasesCurIndex,
          primitiveReportingSetBasesOffset = primitiveReportingSetBasesOffset,
          primitiveReportingSetBasisFiltersStartingIndex =
            primitiveReportingSetBasisFiltersCurIndex,
          primitiveReportingSetBasisFiltersOffset = primitiveReportingSetBasisFiltersOffset,
          measurementPrimitiveReportingSetBasesStartingIndex =
            measurementPrimitiveReportingSetBasesCurIndex,
          measurementPrimitiveReportingSetBasesOffset = measurementPrimitiveReportingSetBasesOffset,
        )

      primitiveReportingSetBasesCurIndex =
        primitiveReportingSetBasesStatementComponents.primitiveReportingSetBasesCurIndex
      primitiveReportingSetBasesRowsSqlList.addAll(
        primitiveReportingSetBasesStatementComponents.primitiveReportingSetBasesRowsSqlList
      )
      primitiveReportingSetBasesBinders.addAll(
        primitiveReportingSetBasesStatementComponents.primitiveReportingSetBasesBinders
      )

      primitiveReportingSetBasisFiltersCurIndex =
        primitiveReportingSetBasesStatementComponents.primitiveReportingSetBasisFiltersCurIndex
      primitiveReportingSetBasisFiltersRowsSqlList.addAll(
        primitiveReportingSetBasesStatementComponents.primitiveReportingSetBasisFiltersRowsSqlList
      )
      primitiveReportingSetBasisFiltersBinders.addAll(
        primitiveReportingSetBasesStatementComponents.primitiveReportingSetBasisFiltersBinders
      )

      measurementPrimitiveReportingSetBasesCurIndex =
        primitiveReportingSetBasesStatementComponents.measurementPrimitiveReportingSetBasesCurIndex
      measurementPrimitiveReportingSetBasesRowsSqlList.addAll(
        primitiveReportingSetBasesStatementComponents
          .measurementPrimitiveReportingSetBasesRowsSqlList
      )
      measurementPrimitiveReportingSetBasesBinders.addAll(
        primitiveReportingSetBasesStatementComponents.measurementPrimitiveReportingSetBasesBinders
      )
    }

    return WeightedMeasurementsAndStatementComponents(
      weightedMeasurements = updatedWeightedMeasurements,
      measurementsCurIndex = measurementsCurIndex,
      measurementsRowsSqlList = measurementsRowsSqlList,
      measurementsBinders = measurementsBinders,
      metricMeasurementsCurIndex = metricMeasurementsCurIndex,
      metricMeasurementsRowsSqlList = metricMeasurementsRowsSqlList,
      metricMeasurementsBinders = metricMeasurementsBinders,
      primitiveReportingSetBasesCurIndex = primitiveReportingSetBasesCurIndex,
      primitiveReportingSetBasesRowsSqlList = primitiveReportingSetBasesRowsSqlList,
      primitiveReportingSetBasesBinders = primitiveReportingSetBasesBinders,
      primitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersCurIndex,
      primitiveReportingSetBasisFiltersRowsSqlList = primitiveReportingSetBasisFiltersRowsSqlList,
      primitiveReportingSetBasisFiltersBinders = primitiveReportingSetBasisFiltersBinders,
      measurementPrimitiveReportingSetBasesCurIndex = measurementPrimitiveReportingSetBasesCurIndex,
      measurementPrimitiveReportingSetBasesRowsSqlList =
        measurementPrimitiveReportingSetBasesRowsSqlList,
      measurementPrimitiveReportingSetBasesBinders = measurementPrimitiveReportingSetBasesBinders,
    )
  }

  private fun TransactionScope.createPrimitiveReportingSetBasesStatementComponents(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    primitiveReportingSetBases: Collection<ReportingSet.PrimitiveReportingSetBasis>,
    reportingSetMap: Map<String, InternalId>,
    primitiveReportingSetBasesStartingIndex: Int,
    primitiveReportingSetBasesOffset: Int,
    primitiveReportingSetBasisFiltersStartingIndex: Int,
    primitiveReportingSetBasisFiltersOffset: Int,
    measurementPrimitiveReportingSetBasesStartingIndex: Int,
    measurementPrimitiveReportingSetBasesOffset: Int,
  ): PrimitiveReportingSetBasesStatementComponents {
    val primitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasesCurIndex = primitiveReportingSetBasesStartingIndex
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val primitiveReportingSetBasisFiltersRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersStartingIndex
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    val measurementPrimitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    var measurementPrimitiveReportingSetBasesCurIndex =
      measurementPrimitiveReportingSetBasesStartingIndex
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()

    primitiveReportingSetBases.forEach {
      val primitiveReportingSetBasisId = idGenerator.generateInternalId()
      val tempPrimitiveReportingSetBasesCurIndex = primitiveReportingSetBasesCurIndex
      primitiveReportingSetBasesBinders.add {
        bind("$${tempPrimitiveReportingSetBasesCurIndex}", measurementConsumerId)
        bind("$${tempPrimitiveReportingSetBasesCurIndex + 1}", primitiveReportingSetBasisId)
        bind(
          "$${tempPrimitiveReportingSetBasesCurIndex + 2}",
          reportingSetMap[it.externalReportingSetId],
        )
      }
      primitiveReportingSetBasesRowsSqlList.add(
        generateParameterizedInsertValues(
          primitiveReportingSetBasesCurIndex,
          primitiveReportingSetBasesCurIndex + primitiveReportingSetBasesOffset,
        )
      )
      primitiveReportingSetBasesCurIndex += primitiveReportingSetBasesOffset

      it.filtersList.forEach { filter ->
        val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()
        val tempPrimitiveReportingSetBasisFilterCurIndex = primitiveReportingSetBasisFiltersCurIndex
        primitiveReportingSetBasisFiltersBinders.add {
          bind("$${tempPrimitiveReportingSetBasisFilterCurIndex}", measurementConsumerId)
          bind("$${tempPrimitiveReportingSetBasisFilterCurIndex + 1}", primitiveReportingSetBasisId)
          bind(
            "$${tempPrimitiveReportingSetBasisFilterCurIndex + 2}",
            primitiveReportingSetBasisFilterId,
          )
          bind("$${tempPrimitiveReportingSetBasisFilterCurIndex + 3}", filter)
        }
        primitiveReportingSetBasisFiltersRowsSqlList.add(
          generateParameterizedInsertValues(
            primitiveReportingSetBasisFiltersCurIndex,
            primitiveReportingSetBasisFiltersCurIndex + primitiveReportingSetBasisFiltersOffset,
          )
        )
        primitiveReportingSetBasisFiltersCurIndex += primitiveReportingSetBasisFiltersOffset
      }

      val tempMeasurementPrimitiveReportingSetBasesCurIndex =
        measurementPrimitiveReportingSetBasesCurIndex
      measurementPrimitiveReportingSetBasesBinders.add {
        bind("$${tempMeasurementPrimitiveReportingSetBasesCurIndex}", measurementConsumerId)
        bind("$${tempMeasurementPrimitiveReportingSetBasesCurIndex + 1}", measurementId)
        bind(
          "$${tempMeasurementPrimitiveReportingSetBasesCurIndex + 2}",
          primitiveReportingSetBasisId,
        )
      }
      measurementPrimitiveReportingSetBasesRowsSqlList.add(
        generateParameterizedInsertValues(
          measurementPrimitiveReportingSetBasesCurIndex,
          measurementPrimitiveReportingSetBasesCurIndex +
            measurementPrimitiveReportingSetBasesOffset,
        )
      )
      measurementPrimitiveReportingSetBasesCurIndex += measurementPrimitiveReportingSetBasesOffset
    }

    return PrimitiveReportingSetBasesStatementComponents(
      primitiveReportingSetBasesCurIndex = primitiveReportingSetBasesCurIndex,
      primitiveReportingSetBasesRowsSqlList = primitiveReportingSetBasesRowsSqlList,
      primitiveReportingSetBasesBinders = primitiveReportingSetBasesBinders,
      primitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersCurIndex,
      primitiveReportingSetBasisFiltersRowsSqlList = primitiveReportingSetBasisFiltersRowsSqlList,
      primitiveReportingSetBasisFiltersBinders = primitiveReportingSetBasisFiltersBinders,
      measurementPrimitiveReportingSetBasesCurIndex = measurementPrimitiveReportingSetBasesCurIndex,
      measurementPrimitiveReportingSetBasesRowsSqlList =
        measurementPrimitiveReportingSetBasesRowsSqlList,
      measurementPrimitiveReportingSetBasesBinders = measurementPrimitiveReportingSetBasesBinders,
    )
  }
}
