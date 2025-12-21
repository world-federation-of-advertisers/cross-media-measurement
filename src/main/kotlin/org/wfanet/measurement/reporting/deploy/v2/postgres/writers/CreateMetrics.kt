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
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
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
  private data class MetricCalculationSpecReportingMetricsValues(
    val metricId: InternalId,
    val createMetricRequestId: UUID,
  )

  private data class WeightedMeasurementsAndInsertData(
    val weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    val measurementsValuesList: List<MeasurementsValues>,
    val metricMeasurementsValuesList: List<MetricMeasurementsValues>,
    val primitiveReportingSetBasesValuesList: List<PrimitiveReportingSetBasesValues>,
    val primitiveReportingSetBasisFiltersValuesList: List<PrimitiveReportingSetBasisFiltersValues>,
    val measurementPrimitiveReportingSetBasesValuesList:
      List<MeasurementPrimitiveReportingSetBasesValues>,
  )

  private data class MeasurementsValues(
    val measurementId: InternalId,
    val cmmsCreateMeasurementRequestId: UUID,
    val timeIntervalStart: OffsetDateTime,
    val timeIntervalEndExclusive: OffsetDateTime,
    val details: Measurement.Details,
  )

  private data class MetricMeasurementsValues(
    val metricId: InternalId,
    val measurementId: InternalId,
    val coefficient: Int,
    val binaryRepresentation: Int,
  )

  private data class PrimitiveReportingSetBasesInsertData(
    val primitiveReportingSetBasesValuesList: List<PrimitiveReportingSetBasesValues>,
    val primitiveReportingSetBasisFiltersValuesList: List<PrimitiveReportingSetBasisFiltersValues>,
    val measurementPrimitiveReportingSetBasesValuesList:
      List<MeasurementPrimitiveReportingSetBasesValues>,
  )

  private data class PrimitiveReportingSetBasesValues(
    val primitiveReportingSetBasisId: InternalId,
    val primitiveReportingSetId: InternalId,
  )

  private data class PrimitiveReportingSetBasisFiltersValues(
    val primitiveReportingSetBasisId: InternalId,
    val primitiveReportingSetBasisFilterId: InternalId,
    val filter: String,
  )

  private data class MeasurementPrimitiveReportingSetBasesValues(
    val measurementId: InternalId,
    val primitiveReportingSetBasisId: InternalId,
  )

  override suspend fun TransactionScope.runTransaction(): List<Metric> {
    val cmmsMeasurementConsumerId = requests[0].metric.cmmsMeasurementConsumerId
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext).getByCmmsId(cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException(cmmsMeasurementConsumerId))
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
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
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

    for (externalReportingSetId in externalReportingSetIds) {
      if (!reportingSetMap.containsKey(externalReportingSetId)) {
        throw ReportingSetNotFoundException(cmmsMeasurementConsumerId, externalReportingSetId)
      }
    }

    val metrics = mutableListOf<Metric>()

    val metricCalculationSpecReportingMetricsValuesList =
      mutableListOf<MetricCalculationSpecReportingMetricsValues>()
    val measurementsValuesList = mutableListOf<MeasurementsValues>()
    val metricMeasurementsValuesList = mutableListOf<MetricMeasurementsValues>()
    val primitiveReportingSetBasesValuesList = mutableListOf<PrimitiveReportingSetBasesValues>()
    val primitiveReportingSetBasisFiltersValuesList =
      mutableListOf<PrimitiveReportingSetBasisFiltersValues>()
    val measurementPrimitiveReportingSetBasesValuesList =
      mutableListOf<MeasurementPrimitiveReportingSetBasesValues>()

    val statement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 28,
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
          MaximumFrequency,
          VidSamplingIntervalStart,
          VidSamplingIntervalWidth,
          CreateTime,
          MetricDetails,
          MetricDetailsJson,
          State,
          SingleDataProviderDifferentialPrivacyEpsilon,
          SingleDataProviderDifferentialPrivacyDelta,
          SingleDataProviderFrequencyDifferentialPrivacyEpsilon,
          SingleDataProviderFrequencyDifferentialPrivacyDelta,
          SingleDataProviderVidSamplingIntervalStart,
          SingleDataProviderVidSamplingIntervalWidth,
          CmmsModelLineName
        )
        VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
         """,
      ) {
        val createTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)
        requests.forEach {
          val existingMetric: Metric? = existingMetricsMap[it.requestId]
          if (existingMetric != null) {
            metrics.add(existingMetric)
          } else {
            val metricId = idGenerator.generateInternalId()
            val externalMetricId: String = it.externalMetricId
            val reportingSetId: InternalId? = reportingSetMap[it.metric.externalReportingSetId]

            addValuesBinding {
              bindValuesParam(0, measurementConsumerId)
              bindValuesParam(1, metricId)
              if (it.requestId.isNotEmpty()) {
                bindValuesParam(2, it.requestId)
              } else {
                bindValuesParam<String>(2, null)
              }
              bindValuesParam(3, reportingSetId)
              bindValuesParam(4, externalMetricId)
              bindValuesParam(
                5,
                it.metric.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC),
              )
              bindValuesParam(
                6,
                it.metric.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC),
              )
              bindValuesParam(7, it.metric.metricSpec.typeCase.number)
              if (it.metric.cmmsModelLine.isNotEmpty()) {
                bindValuesParam(27, it.metric.cmmsModelLine)
              } else {
                bindValuesParam<String>(27, null)
              }
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (it.metric.metricSpec.typeCase) {
                MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
                  val reachAndFrequency = it.metric.metricSpec.reachAndFrequency
                  bindValuesParam(
                    8,
                    reachAndFrequency.multipleDataProviderParams.reachPrivacyParams.epsilon,
                  )
                  bindValuesParam(
                    9,
                    reachAndFrequency.multipleDataProviderParams.reachPrivacyParams.delta,
                  )
                  bindValuesParam(
                    10,
                    reachAndFrequency.multipleDataProviderParams.frequencyPrivacyParams.epsilon,
                  )
                  bindValuesParam(
                    11,
                    reachAndFrequency.multipleDataProviderParams.frequencyPrivacyParams.delta,
                  )
                  bindValuesParam<Long>(12, null)
                  bindValuesParam<PostgresInterval>(13, null)
                  bindValuesParam(14, reachAndFrequency.maximumFrequency)
                  bindValuesParam(
                    15,
                    reachAndFrequency.multipleDataProviderParams.vidSamplingInterval.start,
                  )
                  bindValuesParam(
                    16,
                    reachAndFrequency.multipleDataProviderParams.vidSamplingInterval.width,
                  )
                  if (reachAndFrequency.hasSingleDataProviderParams()) {
                    bindValuesParam(
                      21,
                      reachAndFrequency.singleDataProviderParams.reachPrivacyParams.epsilon,
                    )
                    bindValuesParam(
                      22,
                      reachAndFrequency.singleDataProviderParams.reachPrivacyParams.delta,
                    )
                    bindValuesParam(
                      23,
                      reachAndFrequency.singleDataProviderParams.frequencyPrivacyParams.epsilon,
                    )
                    bindValuesParam(
                      24,
                      reachAndFrequency.singleDataProviderParams.frequencyPrivacyParams.delta,
                    )
                    bindValuesParam(
                      25,
                      reachAndFrequency.singleDataProviderParams.vidSamplingInterval.start,
                    )
                    bindValuesParam(
                      26,
                      reachAndFrequency.singleDataProviderParams.vidSamplingInterval.width,
                    )
                  } else {
                    bindValuesParam<Double>(21, null)
                    bindValuesParam<Double>(22, null)
                    bindValuesParam<Double>(23, null)
                    bindValuesParam<Double>(24, null)
                    bindValuesParam<Float>(25, null)
                    bindValuesParam<Float>(26, null)
                  }
                }
                MetricSpec.TypeCase.REACH -> {
                  val reach = it.metric.metricSpec.reach
                  bindValuesParam(8, reach.multipleDataProviderParams.privacyParams.epsilon)
                  bindValuesParam(9, reach.multipleDataProviderParams.privacyParams.delta)
                  bindValuesParam<Double>(10, null)
                  bindValuesParam<Double>(11, null)
                  bindValuesParam<Long>(12, null)
                  bindValuesParam<PostgresInterval>(13, null)
                  bindValuesParam<Long>(14, null)
                  bindValuesParam(15, reach.multipleDataProviderParams.vidSamplingInterval.start)
                  bindValuesParam(16, reach.multipleDataProviderParams.vidSamplingInterval.width)
                  bindValuesParam<Double>(23, null)
                  bindValuesParam<Double>(24, null)
                  if (reach.hasSingleDataProviderParams()) {
                    bindValuesParam(21, reach.singleDataProviderParams.privacyParams.epsilon)
                    bindValuesParam(22, reach.singleDataProviderParams.privacyParams.delta)
                    bindValuesParam(25, reach.singleDataProviderParams.vidSamplingInterval.start)
                    bindValuesParam(26, reach.singleDataProviderParams.vidSamplingInterval.width)
                  } else {
                    bindValuesParam<Double>(21, null)
                    bindValuesParam<Double>(22, null)
                    bindValuesParam<Float>(25, null)
                    bindValuesParam<Float>(26, null)
                  }
                }
                MetricSpec.TypeCase.IMPRESSION_COUNT -> {
                  val impressionCount = it.metric.metricSpec.impressionCount
                  bindValuesParam(8, impressionCount.params.privacyParams.epsilon)
                  bindValuesParam(9, impressionCount.params.privacyParams.delta)
                  bindValuesParam<Double>(10, null)
                  bindValuesParam<Double>(11, null)
                  bindValuesParam(12, impressionCount.maximumFrequencyPerUser)
                  bindValuesParam<PostgresInterval>(13, null)
                  bindValuesParam<Long>(14, null)
                  bindValuesParam(15, impressionCount.params.vidSamplingInterval.start)
                  bindValuesParam(16, impressionCount.params.vidSamplingInterval.width)
                  bindValuesParam<Double>(21, null)
                  bindValuesParam<Double>(22, null)
                  bindValuesParam<Double>(23, null)
                  bindValuesParam<Double>(24, null)
                  bindValuesParam<Float>(25, null)
                  bindValuesParam<Float>(26, null)
                }
                MetricSpec.TypeCase.WATCH_DURATION -> {
                  val watchDuration = it.metric.metricSpec.watchDuration
                  bindValuesParam(8, watchDuration.params.privacyParams.epsilon)
                  bindValuesParam(9, watchDuration.params.privacyParams.delta)
                  bindValuesParam<Double>(10, null)
                  bindValuesParam<Double>(11, null)
                  bindValuesParam<Long>(12, null)
                  bindValuesParam(
                    13,
                    PostgresInterval.of(watchDuration.maximumWatchDurationPerUser.toDuration()),
                  )
                  bindValuesParam<Long>(14, null)
                  bindValuesParam(15, watchDuration.params.vidSamplingInterval.start)
                  bindValuesParam(16, watchDuration.params.vidSamplingInterval.width)
                  bindValuesParam<Double>(21, null)
                  bindValuesParam<Double>(22, null)
                  bindValuesParam<Double>(23, null)
                  bindValuesParam<Double>(24, null)
                  bindValuesParam<Float>(25, null)
                  bindValuesParam<Float>(26, null)
                }
                MetricSpec.TypeCase.POPULATION_COUNT -> {
                  bindValuesParam(8, 0)
                  bindValuesParam(9, 0)
                  bindValuesParam<Double>(10, null)
                  bindValuesParam<Double>(11, null)
                  bindValuesParam<Long>(12, null)
                  bindValuesParam<PostgresInterval>(13, null)
                  bindValuesParam<Long>(14, null)
                  bindValuesParam(15, 0)
                  bindValuesParam(16, 0)
                  bindValuesParam<Double>(21, null)
                  bindValuesParam<Double>(22, null)
                  bindValuesParam<Double>(23, null)
                  bindValuesParam<Double>(24, null)
                  bindValuesParam<Float>(25, null)
                  bindValuesParam<Float>(26, null)
                }
                MetricSpec.TypeCase.TYPE_NOT_SET -> {}
              }
              bindValuesParam(17, createTime)
              bindValuesParam(18, it.metric.details)
              bindValuesParam(19, it.metric.details.toJson())
              bindValuesParam(20, Metric.State.RUNNING)
            }

            if (it.requestId.isNotEmpty()) {
              val createMetricRequestUuid: UUID? =
                try {
                  UUID.fromString(it.requestId)
                } catch (_: IllegalArgumentException) {
                  // Non-Report Metrics do not have to use a UUID.
                  null
                }

              if (createMetricRequestUuid != null) {
                metricCalculationSpecReportingMetricsValuesList.add(
                  MetricCalculationSpecReportingMetricsValues(
                    metricId = metricId,
                    createMetricRequestId = createMetricRequestUuid,
                  )
                )
              }
            }

            val weightedMeasurementsAndInsertData =
              createWeightedMeasurementsInsertData(
                metricId = metricId,
                it.metric.weightedMeasurementsList,
                reportingSetMap,
              )

            metrics.add(
              it.metric.copy {
                this.externalMetricId = externalMetricId
                weightedMeasurements.clear()
                weightedMeasurements.addAll(weightedMeasurementsAndInsertData.weightedMeasurements)
                this.createTime = createTime.toInstant().toProtoTime()
                state = Metric.State.RUNNING
              }
            )

            measurementsValuesList.addAll(weightedMeasurementsAndInsertData.measurementsValuesList)
            metricMeasurementsValuesList.addAll(
              weightedMeasurementsAndInsertData.metricMeasurementsValuesList
            )
            primitiveReportingSetBasesValuesList.addAll(
              weightedMeasurementsAndInsertData.primitiveReportingSetBasesValuesList
            )
            primitiveReportingSetBasisFiltersValuesList.addAll(
              weightedMeasurementsAndInsertData.primitiveReportingSetBasisFiltersValuesList
            )
            measurementPrimitiveReportingSetBasesValuesList.addAll(
              weightedMeasurementsAndInsertData.measurementPrimitiveReportingSetBasesValuesList
            )
          }
        }
      }

    val metricCalculationSpecReportingMetricsStatement =
      valuesListBoundStatement(
        valuesStartIndex = 1,
        paramCount = 2,
        """
        UPDATE MetricCalculationSpecReportingMetrics AS m SET MetricId = c.MetricId
        FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
        AS c(MetricId, CreateMetricRequestId)
        WHERE MeasurementConsumerId = $1 AND m.CreateMetricRequestId = c.CreateMetricRequestId
        """,
      ) {
        bind("$1", measurementConsumerId)
        metricCalculationSpecReportingMetricsValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, it.metricId)
            bindValuesParam(1, it.createMetricRequestId)
          }
        }
      }

    val measurementsStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 9,
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
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        measurementsValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.measurementId)
            bindValuesParam(2, it.cmmsCreateMeasurementRequestId)
            bindValuesParam<String>(3, null)
            bindValuesParam(4, it.timeIntervalStart)
            bindValuesParam(5, it.timeIntervalEndExclusive)
            bindValuesParam(6, Measurement.State.STATE_UNSPECIFIED)
            bindValuesParam(7, it.details)
            bindValuesParam(8, it.details.toJson())
          }
        }
      }

    val metricMeasurementsStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 5,
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
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        metricMeasurementsValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.metricId)
            bindValuesParam(2, it.measurementId)
            bindValuesParam(3, it.coefficient)
            bindValuesParam(4, it.binaryRepresentation)
          }
        }
      }

    val primitiveReportingSetBasesStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 3,
        """
        INSERT INTO PrimitiveReportingSetBases
          (
            MeasurementConsumerId,
            PrimitiveReportingSetBasisId,
            PrimitiveReportingSetId
          )
        VALUES
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        primitiveReportingSetBasesValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.primitiveReportingSetBasisId)
            bindValuesParam(2, it.primitiveReportingSetId)
          }
        }
      }

    val primitiveReportingSetBasisFiltersStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 4,
        """
        INSERT INTO PrimitiveReportingSetBasisFilters
          (
            MeasurementConsumerId,
            PrimitiveReportingSetBasisId,
            PrimitiveReportingSetBasisFilterId,
            Filter
          )
        VALUES
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        primitiveReportingSetBasisFiltersValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.primitiveReportingSetBasisId)
            bindValuesParam(2, it.primitiveReportingSetBasisFilterId)
            bindValuesParam(3, it.filter)
          }
        }
      }

    val measurementPrimitiveReportingSetBasesStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 3,
        """
        INSERT INTO MeasurementPrimitiveReportingSetBases
          (
            MeasurementConsumerId,
            MeasurementId,
            PrimitiveReportingSetBasisId
          )
        VALUES
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        measurementPrimitiveReportingSetBasesValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.measurementId)
            bindValuesParam(2, it.primitiveReportingSetBasisId)
          }
        }
      }

    if (existingMetricsMap.size < requests.size) {
      transactionContext.run {
        executeStatement(statement)
        if (metricCalculationSpecReportingMetricsValuesList.isNotEmpty()) {
          executeStatement(metricCalculationSpecReportingMetricsStatement)
        }
        executeStatement(measurementsStatement)
        executeStatement(metricMeasurementsStatement)
        executeStatement(primitiveReportingSetBasesStatement)
        if (primitiveReportingSetBasisFiltersValuesList.isNotEmpty()) {
          executeStatement(primitiveReportingSetBasisFiltersStatement)
        }
        executeStatement(measurementPrimitiveReportingSetBasesStatement)
      }
    }

    return metrics
  }

  private fun TransactionScope.createWeightedMeasurementsInsertData(
    metricId: InternalId,
    weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    reportingSetMap: Map<String, InternalId>,
  ): WeightedMeasurementsAndInsertData {
    val updatedWeightedMeasurements = mutableListOf<Metric.WeightedMeasurement>()
    val measurementsValuesList = mutableListOf<MeasurementsValues>()
    val metricMeasurementsValuesList = mutableListOf<MetricMeasurementsValues>()
    val primitiveReportingSetBasesValuesList = mutableListOf<PrimitiveReportingSetBasesValues>()
    val primitiveReportingSetBasisFiltersValuesList =
      mutableListOf<PrimitiveReportingSetBasisFiltersValues>()
    val measurementPrimitiveReportingSetBasesValuesList =
      mutableListOf<MeasurementPrimitiveReportingSetBasesValues>()

    weightedMeasurements.forEach {
      val measurementId = idGenerator.generateInternalId()
      val uuid = UUID.randomUUID()
      updatedWeightedMeasurements.add(
        it.copy {
          measurement = measurement.copy { cmmsCreateMeasurementRequestId = uuid.toString() }
        }
      )
      measurementsValuesList.add(
        MeasurementsValues(
          measurementId = measurementId,
          cmmsCreateMeasurementRequestId = uuid,
          timeIntervalStart =
            it.measurement.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC),
          timeIntervalEndExclusive =
            it.measurement.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC),
          details = it.measurement.details,
        )
      )

      metricMeasurementsValuesList.add(
        MetricMeasurementsValues(
          metricId = metricId,
          measurementId = measurementId,
          coefficient = it.weight,
          binaryRepresentation = it.binaryRepresentation,
        )
      )

      val primitiveReportingSetBasesInsertData =
        createPrimitiveReportingSetBasesInsertData(
          measurementId = measurementId,
          it.measurement.primitiveReportingSetBasesList,
          reportingSetMap,
        )

      primitiveReportingSetBasesValuesList.addAll(
        primitiveReportingSetBasesInsertData.primitiveReportingSetBasesValuesList
      )
      primitiveReportingSetBasisFiltersValuesList.addAll(
        primitiveReportingSetBasesInsertData.primitiveReportingSetBasisFiltersValuesList
      )
      measurementPrimitiveReportingSetBasesValuesList.addAll(
        primitiveReportingSetBasesInsertData.measurementPrimitiveReportingSetBasesValuesList
      )
    }

    return WeightedMeasurementsAndInsertData(
      weightedMeasurements = updatedWeightedMeasurements,
      measurementsValuesList = measurementsValuesList,
      metricMeasurementsValuesList = metricMeasurementsValuesList,
      primitiveReportingSetBasesValuesList = primitiveReportingSetBasesValuesList,
      primitiveReportingSetBasisFiltersValuesList = primitiveReportingSetBasisFiltersValuesList,
      measurementPrimitiveReportingSetBasesValuesList =
        measurementPrimitiveReportingSetBasesValuesList,
    )
  }

  private fun TransactionScope.createPrimitiveReportingSetBasesInsertData(
    measurementId: InternalId,
    primitiveReportingSetBases: Collection<ReportingSet.PrimitiveReportingSetBasis>,
    reportingSetMap: Map<String, InternalId>,
  ): PrimitiveReportingSetBasesInsertData {
    val primitiveReportingSetBasesValuesList = mutableListOf<PrimitiveReportingSetBasesValues>()
    val primitiveReportingSetBasisFiltersValuesList =
      mutableListOf<PrimitiveReportingSetBasisFiltersValues>()
    val measurementPrimitiveReportingSetBasesValuesList =
      mutableListOf<MeasurementPrimitiveReportingSetBasesValues>()

    primitiveReportingSetBases.forEach {
      val primitiveReportingSetBasisId = idGenerator.generateInternalId()
      primitiveReportingSetBasesValuesList.add(
        PrimitiveReportingSetBasesValues(
          primitiveReportingSetBasisId = primitiveReportingSetBasisId,
          primitiveReportingSetId = reportingSetMap.getValue(it.externalReportingSetId),
        )
      )

      it.filtersList.forEach { filter ->
        val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()
        primitiveReportingSetBasisFiltersValuesList.add(
          PrimitiveReportingSetBasisFiltersValues(
            primitiveReportingSetBasisId = primitiveReportingSetBasisId,
            primitiveReportingSetBasisFilterId = primitiveReportingSetBasisFilterId,
            filter = filter,
          )
        )
      }

      measurementPrimitiveReportingSetBasesValuesList.add(
        MeasurementPrimitiveReportingSetBasesValues(
          measurementId = measurementId,
          primitiveReportingSetBasisId = primitiveReportingSetBasisId,
        )
      )
    }

    return PrimitiveReportingSetBasesInsertData(
      primitiveReportingSetBasesValuesList = primitiveReportingSetBasesValuesList,
      primitiveReportingSetBasisFiltersValuesList = primitiveReportingSetBasisFiltersValuesList,
      measurementPrimitiveReportingSetBasesValuesList =
        measurementPrimitiveReportingSetBasesValuesList,
    )
  }
}
