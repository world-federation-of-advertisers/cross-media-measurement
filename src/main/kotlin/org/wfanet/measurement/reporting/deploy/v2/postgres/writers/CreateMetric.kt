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

import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Metric into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class CreateMetrics(private val requests: List<CreateMetricRequest>) :
  PostgresWriter<List<Metric>>() {
  private data class WeightedMeasurementsAndBinders(
    val weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    val measurementsBinders: List<BoundStatement.Binder.() -> Unit>,
    val metricMeasurementsBinders: List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersBinders: List<BoundStatement.Binder.() -> Unit>,
    val measurementPrimitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
  )

  private data class PrimitiveReportingSetBasesBinders(
    val primitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersBinders: List<BoundStatement.Binder.() -> Unit>,
    val measurementPrimitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
  )

  override suspend fun TransactionScope.runTransaction(): List<Metric> {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(requests[0].metric.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val createMetricRequestIds: List<String> = requests.map { it.requestId }.distinct()

    val existingMetricsMap: Map<String, Metric> =
      MetricReader(transactionContext)
        .readMetricsByRequestId(measurementConsumerId, createMetricRequestIds)
        .toList()
        .associateBy({ it.createMetricRequestId }, { it.metric })

    val externalReportingSetIds = mutableSetOf<ExternalId>()
    requests.forEach {
      if (!existingMetricsMap.containsKey(it.requestId)) {
        externalReportingSetIds.add(ExternalId(it.metric.externalReportingSetId))
        it.metric.weightedMeasurementsList.forEach { weightedMeasurement ->
          weightedMeasurement.measurement.primitiveReportingSetBasesList.forEach { bases ->
            externalReportingSetIds.add(ExternalId(bases.externalReportingSetId))
          }
        }
      }
    }

    val reportingSetMap: Map<ExternalId, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    if (reportingSetMap.size < externalReportingSetIds.size) {
      throw ReportingSetNotFoundException()
    }

    val metrics = mutableListOf<Metric>()

    val measurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val metricMeasurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()

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
          MetricDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
      """
      ) {
        requests.forEach {
          val existingMetric: Metric? = existingMetricsMap[it.requestId]
          if (existingMetric != null) {
            metrics.add(existingMetric)
          } else {
            val metricId = idGenerator.generateInternalId()
            val externalMetricId = idGenerator.generateExternalId()
            val reportingSetId: InternalId? =
              reportingSetMap[ExternalId(it.metric.externalReportingSetId)]
            val createTime = Instant.now().atOffset(ZoneOffset.UTC)

            addBinding {
              bind("$1", measurementConsumerId)
              bind("$2", metricId)
              if (it.requestId.isNotBlank()) {
                bind("$3", it.requestId)
              } else {
                bind<String?>("$3", null)
              }
              bind("$4", reportingSetId)
              bind("$5", externalMetricId)
              bind("$6", it.metric.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC))
              bind("$7", it.metric.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC))
              bind("$8", it.metric.metricSpec.typeCase.number)
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (it.metric.metricSpec.typeCase) {
                MetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
                  val frequencyHistogram = it.metric.metricSpec.frequencyHistogram
                  bind("$9", frequencyHistogram.reachPrivacyParams.epsilon)
                  bind("$10", frequencyHistogram.reachPrivacyParams.delta)
                  bind("$11", frequencyHistogram.frequencyPrivacyParams.epsilon)
                  bind("$12", frequencyHistogram.reachPrivacyParams.delta)
                  bind("$13", frequencyHistogram.maximumFrequencyPerUser)
                  bind<Long?>("$14", null)
                }
                MetricSpec.TypeCase.REACH -> {
                  val reach = it.metric.metricSpec.reach
                  bind("$9", reach.privacyParams.epsilon)
                  bind("$10", reach.privacyParams.delta)
                  bind<Double?>("$11", null)
                  bind<Double?>("$12", null)
                  bind<Long?>("$13", null)
                  bind<Long?>("$14", null)
                }
                MetricSpec.TypeCase.IMPRESSION_COUNT -> {
                  val impressionCount = it.metric.metricSpec.impressionCount
                  bind("$9", impressionCount.privacyParams.epsilon)
                  bind("$10", impressionCount.privacyParams.delta)
                  bind<Double?>("$11", null)
                  bind<Double?>("$12", null)
                  bind("$13", impressionCount.maximumFrequencyPerUser)
                  bind<Long?>("$14", null)
                }
                MetricSpec.TypeCase.WATCH_DURATION -> {
                  val watchDuration = it.metric.metricSpec.watchDuration
                  bind("$9", watchDuration.privacyParams.epsilon)
                  bind("$10", watchDuration.privacyParams.delta)
                  bind<Double?>("$11", null)
                  bind<Double?>("$12", null)
                  bind<Long?>("$13", null)
                  bind("$14", watchDuration.maximumWatchDurationPerUser)
                }
                MetricSpec.TypeCase.TYPE_NOT_SET -> {}
              }
              bind("$15", it.metric.metricSpec.vidSamplingInterval.start)
              bind("$16", it.metric.metricSpec.vidSamplingInterval.width)
              bind("$17", createTime)
              bind("$18", it.metric.details)
              bind("$19", it.metric.details.toJson())
            }

            val weightedMeasurementsAndBindings =
              createWeightedMeasurementsBindings(
                measurementConsumerId = measurementConsumerId,
                metricId = metricId,
                it.metric.weightedMeasurementsList,
                reportingSetMap
              )

            metrics.add(
              it.metric.copy {
                this.externalMetricId = externalMetricId.value
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      """
      ) {
        measurementsBinders.forEach { addBinding(it) }
      }

    val metricMeasurementsStatement =
      boundStatement(
        """
      INSERT INTO MetricMeasurements
        (
          MeasurementConsumerId,
          MetricId,
          MeasurementId,
          Coefficient
        )
        VALUES ($1, $2, $3, $4)
      """
      ) {
        metricMeasurementsBinders.forEach { addBinding(it) }
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
        VALUES ($1, $2, $3)
      """
      ) {
        primitiveReportingSetBasesBinders.forEach { addBinding(it) }
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
        VALUES ($1, $2, $3, $4)
      """
      ) {
        primitiveReportingSetBasisFiltersBinders.forEach { addBinding(it) }
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
        VALUES ($1, $2, $3)
      """
      ) {
        measurementPrimitiveReportingSetBasesBinders.forEach { addBinding(it) }
      }

    transactionContext.run {
      executeStatement(statement)
      executeStatement(measurementsStatement)
      executeStatement(metricMeasurementsStatement)
      executeStatement(primitiveReportingSetBasesStatement)
      if (primitiveReportingSetBasisFiltersBinders.size > 0) {
        executeStatement(primitiveReportingSetBasisFiltersStatement)
      }
      executeStatement(measurementPrimitiveReportingSetBasesStatement)
    }

    return metrics
  }

  private fun TransactionScope.createWeightedMeasurementsBindings(
    measurementConsumerId: InternalId,
    metricId: InternalId,
    weightedMeasurements: Collection<Metric.WeightedMeasurement>,
    reportingSetMap: Map<ExternalId, InternalId>,
  ): WeightedMeasurementsAndBinders {
    val updatedWeightedMeasurements = mutableListOf<Metric.WeightedMeasurement>()
    val measurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val metricMeasurementsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
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
      measurementsBinders.add {
        bind("$1", measurementConsumerId)
        bind("$2", measurementId)
        bind("$3", uuid)
        bind<String?>("$4", null)
        bind("$5", it.measurement.timeInterval.startTime.toInstant().atOffset(ZoneOffset.UTC))
        bind("$6", it.measurement.timeInterval.endTime.toInstant().atOffset(ZoneOffset.UTC))
        bind("$7", Measurement.State.STATE_UNSPECIFIED)
        bind("$8", Measurement.Details.getDefaultInstance())
        bind("$9", Measurement.Details.getDefaultInstance().toJson())
      }

      metricMeasurementsBinders.add {
        bind("$1", measurementConsumerId)
        bind("$2", metricId)
        bind("$3", measurementId)
        bind("$4", it.weight)
      }

      val primitiveReportingSetBasesBindings =
        createPrimitiveReportingSetBasesBindings(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          it.measurement.primitiveReportingSetBasesList,
          reportingSetMap
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
      measurementPrimitiveReportingSetBasesBinders = measurementPrimitiveReportingSetBasesBinders
    )
  }

  private fun TransactionScope.createPrimitiveReportingSetBasesBindings(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    primitiveReportingSetBases: Collection<ReportingSet.PrimitiveReportingSetBasis>,
    reportingSetMap: Map<ExternalId, InternalId>,
  ): PrimitiveReportingSetBasesBinders {
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val measurementPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()

    primitiveReportingSetBases.forEach {
      val primitiveReportingSetBasisId = idGenerator.generateInternalId()
      primitiveReportingSetBasesBinders.add {
        bind("$1", measurementConsumerId)
        bind("$2", primitiveReportingSetBasisId)
        bind("$3", reportingSetMap[ExternalId(it.externalReportingSetId)])
      }

      it.filtersList.forEach { filter ->
        val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()
        primitiveReportingSetBasisFiltersBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", primitiveReportingSetBasisId)
          bind("$3", primitiveReportingSetBasisFilterId)
          bind("$4", filter)
        }
      }

      measurementPrimitiveReportingSetBasesBinders.add {
        bind("$1", measurementConsumerId)
        bind("$2", measurementId)
        bind("$3", primitiveReportingSetBasisId)
      }
    }

    return PrimitiveReportingSetBasesBinders(
      primitiveReportingSetBasesBinders = primitiveReportingSetBasesBinders,
      primitiveReportingSetBasisFiltersBinders = primitiveReportingSetBasisFiltersBinders,
      measurementPrimitiveReportingSetBasesBinders = measurementPrimitiveReportingSetBasesBinders
    )
  }
}
