// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.deploy.postgres.writers

import com.google.protobuf.Duration
import com.google.protobuf.duration
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import java.time.Instant
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.Metric
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.ReportKt
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.TimeInterval
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.measurement
import org.wfanet.measurement.internal.reporting.timeInterval
import org.wfanet.measurement.reporting.deploy.postgres.readers.MeasurementReader
import org.wfanet.measurement.reporting.deploy.postgres.readers.MeasurementResultsReader
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementStateInvalidException

private const val NANOS_PER_SECOND = 1_000_000_000

/**
 * Updates the result for a Measurement and for the corresponding Report too if the report result
 * has been computed.
 *
 * Throws the following on [execute]:
 * * [MeasurementNotFoundException] Measurement not found.
 * * [MeasurementStateInvalidException] Measurement does not have PENDING state.
 */
class SetMeasurementResult(private val request: SetMeasurementResultRequest) :
  PostgresWriter<Measurement>() {
  data class MeasurementResult(val result: Measurement.Result, val coefficient: Int)

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val measurementResult =
      MeasurementReader()
        .readMeasurementByReferenceIds(
          transactionContext,
          measurementConsumerReferenceId = request.measurementConsumerReferenceId,
          measurementReferenceId = request.measurementReferenceId
        )
        ?: throw MeasurementNotFoundException()

    if (measurementResult.measurement.state != Measurement.State.PENDING) {
      throw MeasurementStateInvalidException()
    }

    val updateMeasurementStatement =
      boundStatement(
        """
      UPDATE Measurements
      SET State = $1, Result = $2
      WHERE MeasurementConsumerReferenceId = $3 AND MeasurementReferenceId = $4
      """
      ) {
        bind("$1", Measurement.State.SUCCEEDED_VALUE)
        bind("$2", request.result)
        bind("$3", request.measurementConsumerReferenceId)
        bind("$4", request.measurementReferenceId)
      }

    transactionContext.run {
      val numRowsUpdated = executeStatement(updateMeasurementStatement).numRowsUpdated
      if (numRowsUpdated == 0L) {
        return@run
      }

      val measurementResultsMap = mutableMapOf<String, MeasurementResultsReader.Result>()
      val reportsSet = mutableSetOf<Long>()
      val incompleteReportsSet = mutableSetOf<Long>()
      MeasurementResultsReader()
        .listMeasurementsForReportsByMeasurementReferenceId(
          transactionContext,
          request.measurementConsumerReferenceId,
          request.measurementReferenceId
        )
        .collect { result ->
          if (result.state == Measurement.State.SUCCEEDED) {
            reportsSet.add(result.reportId.value)
            measurementResultsMap[result.measurementReferenceId] = result
          } else {
            incompleteReportsSet.add(result.reportId.value)
          }
        }

      reportsSet.forEach {
        if (incompleteReportsSet.contains(it)) {
          return@forEach
        }

        val reportResult =
          ReportReader()
            .getReportById(transactionContext, request.measurementConsumerReferenceId, it)

        val updatedDetails =
          reportResult.report.details.copy {
            this.result = constructResult(reportResult.report, measurementResultsMap)
          }

        val updateReportStatement =
          boundStatement(
            """
              UPDATE Reports
              SET ReportDetails = $1, State = $2
              WHERE MeasurementConsumerReferenceId = $3
              AND ReportId = $4
              """
          ) {
            bind("$1", updatedDetails)
            bind("$2", Report.State.SUCCEEDED.number)
            bind("$3", request.measurementConsumerReferenceId)
            bind("$4", it)
          }
        executeStatement(updateReportStatement)
      }
    }

    return measurement {
      measurementConsumerReferenceId = request.measurementConsumerReferenceId
      measurementReferenceId = request.measurementReferenceId
      state = Measurement.State.SUCCEEDED
      result = request.result
    }
  }

  private fun constructResult(
    report: Report,
    measurementResultsMap: Map<String, MeasurementResultsReader.Result>
  ): Report.Details.Result {
    return ReportKt.DetailsKt.result {
      val rowHeaders = getRowHeaders(report)
      val scalarTableColumnsList = ArrayList<Report.Details.Result.Column>()
      // Each metric contains results for several columns
      for (metric in report.metricsList) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (val metricType = metric.details.metricTypeCase) {
          // REACH, IMPRESSION_COUNT, and WATCH_DURATION are aggregated in one table.
          Metric.Details.MetricTypeCase.REACH,
          Metric.Details.MetricTypeCase.IMPRESSION_COUNT,
          Metric.Details.MetricTypeCase.WATCH_DURATION -> {
            // One namedSetOperation is one column in the report
            for (namedSetOperation in metric.namedSetOperationsList) {
              scalarTableColumnsList +=
                namedSetOperation.toResultColumn(metricType, measurementResultsMap)
            }
          }
          Metric.Details.MetricTypeCase.FREQUENCY_HISTOGRAM -> {
            histogramTables += metric.toHistogramTable(rowHeaders, measurementResultsMap)
          }
          Metric.Details.MetricTypeCase.METRICTYPE_NOT_SET -> error("Metric Type should be set.")
        }
      }
      if (scalarTableColumnsList.size > 0) {
        scalarTable =
          ReportKt.DetailsKt.ResultKt.scalarTable {
            this.rowHeaders += rowHeaders
            columns += scalarTableColumnsList
          }
      }
    }
  }

  /** Generate row headers of [Report.Details.Result] from a [Report]. */
  private fun getRowHeaders(report: Report): List<String> {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (report.timeCase) {
      Report.TimeCase.TIME_INTERVALS -> {
        report.timeIntervals.timeIntervalsList
          .sortedWith { a, b ->
            val start = Timestamps.compare(a.startTime, b.startTime)
            if (start != 0) {
              start
            } else {
              Timestamps.compare(a.endTime, b.endTime)
            }
          }
          .map(TimeInterval::toRowHeader)
      }
      Report.TimeCase.PERIODIC_TIME_INTERVAL -> {

        report.periodicTimeInterval.toTimeIntervals().map(TimeInterval::toRowHeader)
      }
      Report.TimeCase.TIME_NOT_SET -> {
        error("Time should be set.")
      }
    }
  }

  private fun PeriodicTimeInterval.toTimeIntervals(): List<TimeInterval> {
    val source = this
    var startTime = checkNotNull(source.startTime)
    return (0 until source.intervalCount).map {
      timeInterval {
        this.startTime = startTime
        this.endTime = Timestamps.add(startTime, source.increment)
        startTime = this.endTime
      }
    }
  }

  /** Convert an [Metric.NamedSetOperation] to a [Report.Details.Result.Column] of a [Report] */
  private fun Metric.NamedSetOperation.toResultColumn(
    metricType: Metric.Details.MetricTypeCase,
    measurementResultsMap: Map<String, MeasurementResultsReader.Result>,
    maximumFrequency: Int = 0
  ): Report.Details.Result.Column {
    val source = this
    return ReportKt.DetailsKt.ResultKt.column {
      columnHeader = buildColumnHeader(metricType.name, source.displayName)
      for (measurementCalculation in
        source.measurementCalculationsList.sortedWith { a, b ->
          val start = Timestamps.compare(a.timeInterval.startTime, b.timeInterval.startTime)
          if (start != 0) {
            start
          } else {
            Timestamps.compare(a.timeInterval.endTime, b.timeInterval.endTime)
          }
        }) {
        setOperations.addAll(
          measurementCalculation.toSetOperationResults(
            metricType,
            measurementResultsMap,
            maximumFrequency
          )
        )
      }
    }
  }

  /** Build a column header given the metric and set operation name. */
  private fun buildColumnHeader(metricTypeName: String, setOperationName: String): String {
    return "${metricTypeName}_$setOperationName"
  }

  /** Calculate the equation in [MeasurementCalculation] to get the result. */
  private fun MeasurementCalculation.toSetOperationResults(
    metricType: Metric.Details.MetricTypeCase,
    measurementResultsMap: Map<String, MeasurementResultsReader.Result>,
    maximumFrequency: Int = 0
  ): List<Double> {
    val source = this
    val measurementResultsList =
      source.weightedMeasurementsList.map {
        MeasurementResult(
          measurementResultsMap[it.measurementReferenceId]?.result
            ?: throw MeasurementNotFoundException(),
          it.coefficient
        )
      }
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (metricType) {
      Metric.Details.MetricTypeCase.REACH -> listOf(calculateReachResults(measurementResultsList))
      Metric.Details.MetricTypeCase.IMPRESSION_COUNT ->
        listOf(calculateImpressionResults(measurementResultsList))
      Metric.Details.MetricTypeCase.WATCH_DURATION ->
        listOf(calculateWatchDurationResults(measurementResultsList))
      Metric.Details.MetricTypeCase.FREQUENCY_HISTOGRAM -> {
        calculateFrequencyHistogramResults(measurementResultsList, maximumFrequency)
      }
      Metric.Details.MetricTypeCase.METRICTYPE_NOT_SET -> {
        error("Metric Type should be set.")
      }
    }
  }

  /** Calculate the reach result by summing up weighted [Measurement]s. */
  private fun calculateReachResults(measurementResultsList: List<MeasurementResult>): Double {
    return measurementResultsList
      .sumOf { (result, coefficient) ->
        if (!result.hasReach()) {
          error("Reach measurement is missing.")
        }
        result.reach.value * coefficient
      }
      .toDouble()
  }

  /** Calculate the impression result by summing up weighted [Measurement]s. */
  private fun calculateImpressionResults(
    measurementCoefficientPairsList: List<MeasurementResult>
  ): Double {
    return measurementCoefficientPairsList
      .sumOf { (result, coefficient) ->
        if (!result.hasImpression()) {
          error("Impression measurement is missing.")
        }
        result.impression.value * coefficient
      }
      .toDouble()
  }

  /** Calculate the watch duration result by summing up weighted [Measurement]s. */
  private fun calculateWatchDurationResults(
    measurementCoefficientPairsList: List<MeasurementResult>
  ): Double {
    val watchDuration =
      measurementCoefficientPairsList
        .map { (result, coefficient) ->
          if (!result.hasWatchDuration()) {
            error("Watch duration measurement is missing.")
          }
          result.watchDuration.value * coefficient
        }
        .reduce { sum, element -> sum + element }

    return watchDuration.seconds + (watchDuration.nanos.toDouble() / NANOS_PER_SECOND)
  }

  private operator fun Duration.times(coefficient: Int): Duration {
    val source = this
    return duration {
      val weightedTotalNanos: Long =
        (TimeUnit.SECONDS.toNanos(source.seconds) + source.nanos) * coefficient
      seconds = TimeUnit.NANOSECONDS.toSeconds(weightedTotalNanos)
      nanos = (weightedTotalNanos % NANOS_PER_SECOND).toInt()
    }
  }

  private operator fun Duration.plus(other: Duration): Duration {
    val source = this
    return Durations.add(source, other)
  }

  /** Calculate the frequency histogram result by summing up weighted [Measurement]s. */
  private fun calculateFrequencyHistogramResults(
    measurementCoefficientPairsList: List<MeasurementResult>,
    maximumFrequency: Int
  ): List<Double> {
    val aggregatedFrequencyHistogramMap =
      measurementCoefficientPairsList
        .map { (result, coefficient) ->
          if (!result.hasFrequency() || !result.hasReach()) {
            error("Reach-Frequency measurement is missing.")
          }
          val reach = result.reach.value
          result.frequency.relativeFrequencyDistributionMap.mapValues {
            it.value * coefficient * reach
          }
        }
        .fold(mutableMapOf<Long, Double>().withDefault { 0.0 }) {
          aggregatedFrequencyHistogramMap: MutableMap<Long, Double>,
          weightedFrequencyHistogramMap ->
          for ((frequency, count) in weightedFrequencyHistogramMap) {
            aggregatedFrequencyHistogramMap[frequency] =
              aggregatedFrequencyHistogramMap.getValue(frequency) + count
          }
          aggregatedFrequencyHistogramMap
        }
    val resultsList = aggregatedFrequencyHistogramMap.entries.sortedBy { it.key }.map { it.value }
    return if (resultsList.size < maximumFrequency) {
      val paddedResultsList = resultsList.toMutableList()
      paddedResultsList.addAll(DoubleArray(maximumFrequency - resultsList.size) { 0.0 }.toList())
      paddedResultsList
    } else {
      resultsList
    }
  }

  /** Convert a [Metric] to a [Report.Details.Result.HistogramTable] of a [Report] */
  private fun Metric.toHistogramTable(
    rowHeaders: List<String>,
    measurementResultsMap: Map<String, MeasurementResultsReader.Result>
  ): Report.Details.Result.HistogramTable {
    val source = this
    val maximumFrequency = source.details.frequencyHistogram.maximumFrequencyPerUser
    return ReportKt.DetailsKt.ResultKt.histogramTable {
      for (rowHeader in rowHeaders) {
        for (frequency in 1..maximumFrequency) {
          rows +=
            ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
              this.rowHeader = rowHeader
              this.frequency = frequency
            }
        }
      }
      for (namedSetOperation in source.namedSetOperationsList) {
        columns +=
          namedSetOperation.toResultColumn(
            source.details.metricTypeCase,
            measurementResultsMap,
            maximumFrequency
          )
      }
    }
  }
}

/** Convert a [TimeInterval] to a row header in String. */
private fun TimeInterval.toRowHeader(): String {
  val source = this
  val startTimeInstant =
    Instant.ofEpochSecond(source.startTime.seconds, source.startTime.nanos.toLong())
  val endTimeInstant = Instant.ofEpochSecond(source.endTime.seconds, source.endTime.nanos.toLong())
  return "$startTimeInstant-$endTimeInstant"
}
