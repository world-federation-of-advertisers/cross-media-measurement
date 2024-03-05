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
  private data class WeightedMeasurementsAndBinders(
    val weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    val measurementsBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
    val metricMeasurementsBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasesBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
    val measurementPrimitiveReportingSetBasesBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
  )

  private data class PrimitiveReportingSetBasesBinders(
    val primitiveReportingSetBasesBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
    val measurementPrimitiveReportingSetBasesBinders: List<ValuesListBoundStatement.Binder.() -> Unit>,
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

    val metricCalculationSpecReportingMetricsBinders =
      mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val measurementsBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val metricMeasurementsBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasesBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()

    val statement =
      valuesListBoundStatement(valuesStartIndex = 0, paramCount = 20,
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
          MetricDetailsJson
        )
        VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
         """
      ) {
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

            addValuesBinding {
              bindValuesParam(0, measurementConsumerId)
              bindValuesParam(1, metricId)
              if (it.requestId.isNotEmpty()) {
                bindValuesParam(2, it.requestId)
              } else {
                bindValuesParam<String?>(2, null)
              }
              bindValuesParam(3, reportingSetId)
              bindValuesParam(4, externalMetricId)
              bindValuesParam(5, it.metric.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC))
              bindValuesParam(6, it.metric.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC))
              bindValuesParam(7, it.metric.metricSpec.typeCase.number)
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (it.metric.metricSpec.typeCase) {
                MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
                  val reachAndFrequency = it.metric.metricSpec.reachAndFrequency
                  bindValuesParam(8, reachAndFrequency.reachPrivacyParams.epsilon)
                  bindValuesParam(9, reachAndFrequency.reachPrivacyParams.delta)
                  bindValuesParam(10, reachAndFrequency.frequencyPrivacyParams.epsilon)
                  bindValuesParam(11, reachAndFrequency.reachPrivacyParams.delta)
                  bindValuesParam<Long?>(12, null)
                  bindValuesParam<PostgresInterval?>(13, null)
                  bindValuesParam(14, reachAndFrequency.maximumFrequency)
                }
                MetricSpec.TypeCase.REACH -> {
                  val reach = it.metric.metricSpec.reach
                  bindValuesParam(8, reach.privacyParams.epsilon)
                  bindValuesParam(9, reach.privacyParams.delta)
                  bindValuesParam<Double?>(10, null)
                  bindValuesParam<Double?>(11, null)
                  bindValuesParam<Long?>(12, null)
                  bindValuesParam<PostgresInterval?>(13, null)
                  bindValuesParam<Long?>(14, null)
                }
                MetricSpec.TypeCase.IMPRESSION_COUNT -> {
                  val impressionCount = it.metric.metricSpec.impressionCount
                  bindValuesParam(8, impressionCount.privacyParams.epsilon)
                  bindValuesParam(9, impressionCount.privacyParams.delta)
                  bindValuesParam<Double?>(10, null)
                  bindValuesParam<Double?>(11, null)
                  bindValuesParam(12, impressionCount.maximumFrequencyPerUser)
                  bindValuesParam<PostgresInterval?>(13, null)
                  bindValuesParam<Long?>(14, null)
                }
                MetricSpec.TypeCase.WATCH_DURATION -> {
                  val watchDuration = it.metric.metricSpec.watchDuration
                  bindValuesParam(8, watchDuration.privacyParams.epsilon)
                  bindValuesParam(9, watchDuration.privacyParams.delta)
                  bindValuesParam<Double?>(10, null)
                  bindValuesParam<Double?>(11, null)
                  bindValuesParam<Long?>(12, null)
                  bindValuesParam(
                    13,
                    PostgresInterval.of(watchDuration.maximumWatchDurationPerUser.toDuration()),
                  )
                  bindValuesParam<Long?>(14, null)
                }
                MetricSpec.TypeCase.POPULATION_COUNT -> {
                  bindValuesParam(8, 0)
                  bindValuesParam(9, 0)
                  bindValuesParam<Double?>(10, null)
                  bindValuesParam<Double?>(11, null)
                  bindValuesParam<Long?>(12, null)
                  bindValuesParam<PostgresInterval?>(13, null)
                  bindValuesParam<Long?>(14, null)
                }
                MetricSpec.TypeCase.TYPE_NOT_SET -> {}
              }
              bindValuesParam(15, vidSamplingIntervalStart)
              bindValuesParam(16, vidSamplingIntervalWidth)
              bindValuesParam(17, createTime)
              bindValuesParam(18, it.metric.details)
              bindValuesParam(19, it.metric.details.toJson())
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
                metricCalculationSpecReportingMetricsBinders.add {
                  bindValuesParam(0, metricId)
                  bindValuesParam(1, createMetricRequestUuid)
                }
              }
            }

            val weightedMeasurementsAndBindings =
              createWeightedMeasurementsBindings(
                measurementConsumerId = measurementConsumerId,
                metricId = metricId,
                it.metric.weightedMeasurementsList,
                reportingSetMap,
              )

            metrics.add(
              it.metric.copy {
                this.externalMetricId = externalMetricId
                weightedMeasurements.clear()
                weightedMeasurements.addAll(weightedMeasurementsAndBindings.weightedMeasurements)
                this.createTime = createTime.toInstant().toProtoTime()
              }
            )

            measurementsBinders.addAll(weightedMeasurementsAndBindings.measurementsBinders)
            metricMeasurementsBinders.addAll(
              weightedMeasurementsAndBindings.metricMeasurementsBinders
            )
            primitiveReportingSetBasesBinders.addAll(
              weightedMeasurementsAndBindings.primitiveReportingSetBasesBinders
            )
            primitiveReportingSetBasisFiltersBinders.addAll(
              weightedMeasurementsAndBindings.primitiveReportingSetBasisFiltersBinders
            )
            measurementPrimitiveReportingSetBasesBinders.addAll(
              weightedMeasurementsAndBindings.measurementPrimitiveReportingSetBasesBinders
            )
          }
        }
      }

    val metricCalculationSpecReportingMetricsStatement =
      valuesListBoundStatement(valuesStartIndex = 1, paramCount = 2,
        """
        UPDATE MetricCalculationSpecReportingMetrics AS m SET MetricId = c.MetricId
        FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
        AS c(MetricId, CreateMetricRequestId)
        WHERE MeasurementConsumerId = $1 AND m.CreateMetricRequestId = c.CreateMetricRequestId
        """
      ) {
        bind("$1", measurementConsumerId)
        metricCalculationSpecReportingMetricsBinders.forEach { addValuesBinding(it) }
      }

    val measurementsStatement =
      valuesListBoundStatement(valuesStartIndex = 0, paramCount = 9,
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
        """
      ) {
        measurementsBinders.forEach { addValuesBinding(it) }
      }

    val metricMeasurementsStatement =
      valuesListBoundStatement(valuesStartIndex = 0, paramCount = 5,
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
        """
      ) {
        metricMeasurementsBinders.forEach { addValuesBinding(it) }
      }

    val primitiveReportingSetBasesStatement =
      valuesListBoundStatement(valuesStartIndex = 0, 3,
        """
        INSERT INTO PrimitiveReportingSetBases
          (
            MeasurementConsumerId,
            PrimitiveReportingSetBasisId,
            PrimitiveReportingSetId
          )
        VALUES
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """
      ) {
        primitiveReportingSetBasesBinders.forEach { addValuesBinding(it) }
      }

    val primitiveReportingSetBasisFiltersStatement =
      valuesListBoundStatement(valuesStartIndex = 0, paramCount = 4,
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
        """
      ) {
        primitiveReportingSetBasisFiltersBinders.forEach { addValuesBinding(it) }
      }

    val measurementPrimitiveReportingSetBasesStatement =
      valuesListBoundStatement(valuesStartIndex = 0, paramCount = 3,
        """
        INSERT INTO MeasurementPrimitiveReportingSetBases
          (
            MeasurementConsumerId,
            MeasurementId,
            PrimitiveReportingSetBasisId
          )
        VALUES
        ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """
      ) {
        measurementPrimitiveReportingSetBasesBinders.forEach { addValuesBinding(it) }
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

  private fun TransactionScope.createWeightedMeasurementsBindings(
    measurementConsumerId: InternalId,
    metricId: InternalId,
    weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    reportingSetMap: Map<String, InternalId>,
  ): WeightedMeasurementsAndBinders {
    val updatedWeightedMeasurements = mutableListOf<Metric.WeightedMeasurement>()
    val measurementsBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val metricMeasurementsBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasesBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()

    weightedMeasurements.forEach {
      val measurementId = idGenerator.generateInternalId()
      val uuid = UUID.randomUUID()
      updatedWeightedMeasurements.add(
        it.copy {
          measurement = measurement.copy { cmmsCreateMeasurementRequestId = uuid.toString() }
        }
      )
      measurementsBinders.add {
        bindValuesParam(0, measurementConsumerId)
        bindValuesParam(1, measurementId)
        bindValuesParam(2, uuid)
        bindValuesParam<String?>(3, null)
        bindValuesParam(4, it.measurement.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC))
        bindValuesParam(5, it.measurement.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC))
        bindValuesParam(6, Measurement.State.STATE_UNSPECIFIED)
        bindValuesParam(7, Measurement.Details.getDefaultInstance())
        bindValuesParam(8, Measurement.Details.getDefaultInstance().toJson())
      }

      metricMeasurementsBinders.add {
        bindValuesParam(0, measurementConsumerId)
        bindValuesParam(1, metricId)
        bindValuesParam(2, measurementId)
        bindValuesParam(3, it.weight)
        bindValuesParam(4, it.binaryRepresentation)
      }

      val primitiveReportingSetBasesBindings =
        createPrimitiveReportingSetBasesBindings(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          it.measurement.primitiveReportingSetBasesList,
          reportingSetMap,
        )

      primitiveReportingSetBasesBinders.addAll(
        primitiveReportingSetBasesBindings.primitiveReportingSetBasesBinders
      )
      primitiveReportingSetBasisFiltersBinders.addAll(
        primitiveReportingSetBasesBindings.primitiveReportingSetBasisFiltersBinders
      )
      measurementPrimitiveReportingSetBasesBinders.addAll(
        primitiveReportingSetBasesBindings.measurementPrimitiveReportingSetBasesBinders
      )
    }

    return WeightedMeasurementsAndBinders(
      weightedMeasurements = updatedWeightedMeasurements,
      measurementsBinders = measurementsBinders,
      metricMeasurementsBinders = metricMeasurementsBinders,
      primitiveReportingSetBasesBinders = primitiveReportingSetBasesBinders,
      primitiveReportingSetBasisFiltersBinders = primitiveReportingSetBasisFiltersBinders,
      measurementPrimitiveReportingSetBasesBinders = measurementPrimitiveReportingSetBasesBinders,
    )
  }

  private fun TransactionScope.createPrimitiveReportingSetBasesBindings(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    primitiveReportingSetBases: Collection<ReportingSet.PrimitiveReportingSetBasis>,
    reportingSetMap: Map<String, InternalId>,
  ): PrimitiveReportingSetBasesBinders {
    val primitiveReportingSetBasesBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<ValuesListBoundStatement.Binder.() -> Unit>()

    primitiveReportingSetBases.forEach {
      val primitiveReportingSetBasisId = idGenerator.generateInternalId()
      primitiveReportingSetBasesBinders.add {
        bindValuesParam(0, measurementConsumerId)
        bindValuesParam(1, primitiveReportingSetBasisId)
        bindValuesParam(2, reportingSetMap[it.externalReportingSetId])
      }

      it.filtersList.forEach { filter ->
        val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()
        primitiveReportingSetBasisFiltersBinders.add {
          bindValuesParam(0, measurementConsumerId)
          bindValuesParam(1, primitiveReportingSetBasisId)
          bindValuesParam(2, primitiveReportingSetBasisFilterId)
          bindValuesParam(3, filter)
        }
      }

      measurementPrimitiveReportingSetBasesBinders.add {
        bindValuesParam(0, measurementConsumerId)
        bindValuesParam(1, measurementId)
        bindValuesParam(2, primitiveReportingSetBasisId)
      }
    }

    return PrimitiveReportingSetBasesBinders(
      primitiveReportingSetBasesBinders = primitiveReportingSetBasesBinders,
      primitiveReportingSetBasisFiltersBinders = primitiveReportingSetBasisFiltersBinders,
      measurementPrimitiveReportingSetBasesBinders = measurementPrimitiveReportingSetBasesBinders,
    )
  }
}
