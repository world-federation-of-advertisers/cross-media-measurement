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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.duration
import com.google.protobuf.util.Durations.add as durationsAdd
import com.google.protobuf.util.Timestamps.add as timestampsAdd
import io.grpc.Status
import java.time.Instant
import kotlin.math.min
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2.alpha.ListReportsPageToken
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.Measurement.Result as InternalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.impression as internalImpression
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.reach as internalReach
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.watchDuration as internalWatchDuration
import org.wfanet.measurement.internal.reporting.MeasurementKt.failure as internalFailure
import org.wfanet.measurement.internal.reporting.MeasurementKt.result as internalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.Metric.Details as InternalMetricDetails
import org.wfanet.measurement.internal.reporting.Metric.FrequencyHistogramParams as InternalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.Metric.ImpressionCountParams as InternalImpressionCountParams
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation as InternalMeasurementCalculation
import org.wfanet.measurement.internal.reporting.Metric.NamedSetOperation as InternalNamedSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation as InternalSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation.Operand as InternalOperand
import org.wfanet.measurement.internal.reporting.Metric.WatchDurationParams as InternalWatchDurationParams
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.Report.Details.Result as InternalReportResult
import org.wfanet.measurement.internal.reporting.Report.Details.Result.HistogramTable
import org.wfanet.measurement.internal.reporting.Report.Details.Result.HistogramTable.Column as HistogramTableColumn
import org.wfanet.measurement.internal.reporting.Report.Details.Result.ScalarTable.Column as ScalarTableColumn
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt.ResultKt.HistogramTableKt.column as histogramTableColumn
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt.ResultKt.HistogramTableKt.row as histogramTableRow
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt.ResultKt.ScalarTableKt.column as scalarTableColumn
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt.ResultKt.histogramTable
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt.result as internalReportResult
import org.wfanet.measurement.internal.reporting.ReportKt.details as internalReportDetails
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.getMeasurementRequest as getInternalMeasurementRequest
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.streamReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.Metric.FrequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.Metric.ImpressionCountParams
import org.wfanet.measurement.reporting.v1alpha.Metric.NamedSetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.WatchDurationParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.impressionCountParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.watchDurationParams
import org.wfanet.measurement.reporting.v1alpha.PeriodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.TimeIntervals
import org.wfanet.measurement.reporting.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.timeInterval
import org.wfanet.measurement.reporting.v1alpha.timeIntervals

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ReportsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificateStub: CertificatesCoroutineStub,
  private val privateKey: PrivateKeyHandle,
  private val apiAuthenticationKey: String,
) : ReportsCoroutineImplBase() {

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val principal = principalFromCurrentContext
    val listReportsPageToken = request.toListReportsPageToken()

    // Based on AIP-132#Errors
    when (val resourceKey = principal.resourceKey) {
      is MeasurementConsumerKey -> {
        if (request.parent != resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ReportingSets belonging to other MeasurementConsumers."
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to list ReportingSets."
        }
      }
    }

    val results: List<InternalReport> =
      internalReportsStub.streamReports(listReportsPageToken.toStreamReportsRequest()).toList()

    if (results.isEmpty()) {
      return ListReportsResponse.getDefaultInstance()
    }

    return listReportsResponse {
      reports +=
        results
          .subList(0, min(results.size, listReportsPageToken.pageSize))
          .map { it.updateReport() }
          .map(InternalReport::toReport)

      if (results.size > listReportsPageToken.pageSize) {
        val pageToken =
          listReportsPageToken.copy {
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                results[results.lastIndex - 1].measurementConsumerReferenceId
              externalReportId = results[results.lastIndex - 1].externalReportId
            }
          }
        nextPageToken = pageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Update an [InternalReport] and its [InternalMeasurement]s. */
  private fun InternalReport.updateReport(): InternalReport {
    val source = this

    // Report with SUCCEEDED state is already updated.
    if (source.state == InternalReport.State.SUCCEEDED) {
      return source
    } else if (
      source.state == InternalReport.State.STATE_UNSPECIFIED ||
        source.state == InternalReport.State.UNRECOGNIZED
    ) {
      error("The report cannot be updated if its state is either unspecified or unrecognized.")
    }

    // Update measurements
    var allMeasurementSucceeded = true
    var anyMeasurementFailed = false
    for ((measurementReferenceID, internalMeasurement) in source.measurementsMap) {
      val measurementState =
        updateMeasurement(
          measurementReferenceID,
          source.measurementConsumerReferenceId,
          internalMeasurement,
        )

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (measurementState) {
        InternalMeasurement.State.PENDING -> allMeasurementSucceeded = false
        InternalMeasurement.State.SUCCEEDED -> {} // No action needed
        InternalMeasurement.State.FAILED -> anyMeasurementFailed = true
        InternalMeasurement.State.STATE_UNSPECIFIED ->
          error("The measurement state should've been set.")
        InternalMeasurement.State.UNRECOGNIZED -> error("Unrecognized measurement state.")
      }
    }

    // Update report
    if (allMeasurementSucceeded) {
      return source.copy {
        state = InternalReport.State.SUCCEEDED
        details = internalReportDetails {
          eventGroupFilters.putAll(source.details.eventGroupFiltersMap)
          result = constructResult(source)
        }
      }
    }
    if (anyMeasurementFailed) {
      return source.copy { state = InternalReport.State.FAILED }
    }
    return source
  }

  /**
   * Given the measurement reference ID, update [InternalMeasurement] with the CMM [Measurement].
   */
  private fun updateMeasurement(
    measurementReferenceId: String,
    measurementConsumerReferenceId: String,
    internalMeasurement: InternalMeasurement,
  ): InternalMeasurement.State {
    // Measurement with SUCCEEDED state is already updated
    if (internalMeasurement.state == InternalMeasurement.State.SUCCEEDED)
      return InternalMeasurement.State.SUCCEEDED

    val measurement =
      runBlocking(Dispatchers.IO) {
        measurementsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementReferenceId })
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (measurement.state) {
      Measurement.State.SUCCEEDED -> {
        runBlocking(Dispatchers.IO) {
          // Convert a Measurement to an InternalMeasurement and store it into the database with
          // SUCCEEDED state
          internalMeasurementsStub.setMeasurementResult(
            getSetMeasurementResult(
              measurementConsumerReferenceId,
              measurementReferenceId,
              measurement
            )
          )
        }
        return InternalMeasurement.State.SUCCEEDED
      }
      Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
      Measurement.State.COMPUTING -> {
        return InternalMeasurement.State.PENDING
      }
      Measurement.State.FAILED,
      Measurement.State.CANCELLED -> {
        runBlocking(Dispatchers.IO) {
          internalMeasurementsStub.setMeasurementFailure(
            setMeasurementFailureRequest {
              this.measurementConsumerReferenceId = measurementConsumerReferenceId
              this.measurementReferenceId = measurementReferenceId
              failure = measurement.failure.toFailure()
            }
          )
        }
        return InternalMeasurement.State.FAILED
      }
      Measurement.State.STATE_UNSPECIFIED -> error("The measurement state should've been set.")
      Measurement.State.UNRECOGNIZED -> error("Unrecognized measurement state.")
    }
  }

  private fun getSetMeasurementResult(
    measurementConsumerReferenceId: String,
    measurementReferenceId: String,
    measurement: Measurement,
  ): SetMeasurementResultRequest {
    return setMeasurementResultRequest {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      this.measurementReferenceId = measurementReferenceId
      result =
        aggregateResults(
          measurement.resultsList.map { it.toMeasurementResult() }.map(Measurement.Result::toResult)
        )
    }
  }

  /** Decrypt a [Measurement.ResultPair] to [Measurement.Result] */
  private fun Measurement.ResultPair.toMeasurementResult(): Measurement.Result {
    val source = this
    val certificate =
      runBlocking(Dispatchers.IO) {
        certificateStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getCertificate(getCertificateRequest { name = source.certificate })
      }

    val signedResult = decryptResult(source.encryptedResult, privateKey)

    val result = Measurement.Result.parseFrom(signedResult.data)

    if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
      error("Signature of the result is invalid.")
    }
    return result
  }

  /** Construct an [InternalReportResult] when all measurements are ready */
  private fun constructResult(report: InternalReport): InternalReportResult {
    return internalReportResult {
      val rowHeaders = getRowHeaders(report)

      this.scalarTable.rowHeadersList.addAll(rowHeaders)

      // Each metric contains results for several columns
      for (metric in report.metricsList) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (val metricType = metric.details.metricTypeCase) {
          // REACH, IMPRESSION_COUNT, and WATCH_DURATION are aggregated in one table.
          InternalMetricDetails.MetricTypeCase.REACH,
          InternalMetricDetails.MetricTypeCase.IMPRESSION_COUNT,
          InternalMetricDetails.MetricTypeCase.WATCH_DURATION -> {
            // One namedSetOperation is one column in the report
            for (namedSetOperation in metric.namedSetOperationsList) {
              this.scalarTable.columnList +=
                namedSetOperation.toScalarTableColumn(
                  report.measurementConsumerReferenceId,
                  metricType
                )
            }
          }
          InternalMetricDetails.MetricTypeCase.FREQUENCY_HISTOGRAM -> {
            this.histogramTables +=
              metric.toHistogramTable(
                rowHeaders,
                report.measurementConsumerReferenceId,
              )
          }
          InternalMetricDetails.MetricTypeCase.METRICTYPE_NOT_SET ->
            error("The metric type in the internal report should've be set.")
        }
      }
    }
  }

  /** Convert an [InternalMetric] to a [HistogramTable] of an [InternalReport] */
  private fun InternalMetric.toHistogramTable(
    rowHeaders: List<String>,
    measurementConsumerReferenceId: String,
  ): HistogramTable {
    val source = this
    val maximumFrequency = source.details.frequencyHistogram.maximumFrequencyPerUser
    return histogramTable {
      for (rowHeader in rowHeaders) {
        for (frequency in 1..maximumFrequency) {
          rows += histogramTableRow {
            this.rowHeader = rowHeader
            this.frequency = frequency
          }
        }
      }
      for (namedSetOperation in source.namedSetOperationsList) {
        column +=
          namedSetOperation.toHistogramTableColumn(
            measurementConsumerReferenceId,
            source.details.metricTypeCase,
          )
      }
    }
  }

  // TODO(Use one Column message instead of two in the internal report proto)
  /** Convert an [InternalNamedSetOperation] to a [HistogramTableColumn] of an [InternalReport] */
  private fun InternalNamedSetOperation.toHistogramTableColumn(
    measurementConsumerReferenceId: String,
    metricType: InternalMetricDetails.MetricTypeCase,
  ): HistogramTableColumn {
    val source = this
    return histogramTableColumn {
      columnHeader = source.displayName

      for (measurementCalculation in source.measurementCalculationList) {
        setOperations.addAll(
          measurementCalculation.toSetOperationResults(measurementConsumerReferenceId, metricType)
        )
      }
    }
  }

  /** Convert an [InternalNamedSetOperation] to a [ScalarTableColumn] of an [InternalReport] */
  private fun InternalNamedSetOperation.toScalarTableColumn(
    measurementConsumerReferenceId: String,
    metricType: InternalMetricDetails.MetricTypeCase,
  ): ScalarTableColumn {
    val source = this
    return scalarTableColumn {
      columnHeader = source.displayName

      for (measurementCalculation in source.measurementCalculationList) {
        setOperations.addAll(
          measurementCalculation.toSetOperationResults(measurementConsumerReferenceId, metricType)
        )
      }
    }
  }

  /** Calculate the equation in [InternalMeasurementCalculation] to get the result. */
  private fun InternalMeasurementCalculation.toSetOperationResults(
    measurementConsumerReferenceId: String,
    metricType: InternalMetricDetails.MetricTypeCase,
  ): List<Int> {
    val source = this

    val measurementCoefficientPairsList =
      source.weightedMeasurementsList.map {
        val measurement =
          runBlocking(Dispatchers.IO) {
            internalMeasurementsStub.getMeasurement(
              getInternalMeasurementRequest {
                this.measurementConsumerReferenceId = measurementConsumerReferenceId
                this.measurementReferenceId = it.measurementReferenceId
              }
            )
          }

        Pair(measurement, it.coefficient)
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (metricType) {
      InternalMetricDetails.MetricTypeCase.REACH ->
        listOf(calculateReachResults(measurementCoefficientPairsList))
      InternalMetricDetails.MetricTypeCase.IMPRESSION_COUNT ->
        listOf(calculateImpressionResults(measurementCoefficientPairsList))
      InternalMetricDetails.MetricTypeCase.WATCH_DURATION ->
        listOf(calculateWatchDurationResults(measurementCoefficientPairsList))
      InternalMetricDetails.MetricTypeCase.FREQUENCY_HISTOGRAM ->
        calculateFrequencyHistogramResults(measurementCoefficientPairsList)
      InternalMetricDetails.MetricTypeCase.METRICTYPE_NOT_SET -> {
        error("Metric Type should've been set.")
      }
    }
  }
}

private fun calculateReachResults(
  measurementCoefficientPairsList: List<Pair<InternalMeasurement, Int>>
): Int {
  return measurementCoefficientPairsList
    .sumOf { (measurement, coefficient) ->
      if (!measurement.result.hasReach()) {
        error("Reach measurement is missing.")
      }
      measurement.result.reach.value * coefficient
    }
    .toInt()
}

private fun calculateImpressionResults(
  measurementCoefficientPairsList: List<Pair<InternalMeasurement, Int>>
): Int {
  return measurementCoefficientPairsList
    .sumOf { (measurement, coefficient) ->
      if (!measurement.result.hasImpression()) {
        error("Impression measurement is missing.")
      }
      measurement.result.impression.value * coefficient
    }
    .toInt()
}

private fun calculateWatchDurationResults(
  measurementCoefficientPairsList: List<Pair<InternalMeasurement, Int>>
): Int {
  return measurementCoefficientPairsList
    .map { (measurement, coefficient) ->
      if (!measurement.result.hasWatchDuration()) {
        error("Watch duration measurement is missing.")
      }
      duration {
        seconds = measurement.result.watchDuration.value.seconds * coefficient
        nanos = measurement.result.watchDuration.value.nanos * coefficient
        while (nanos >= 1_000_000_000) {
          seconds += 1
          nanos -= 1_000_000_000
        }
      }
    }
    .reduce { sum, element -> durationsAdd(sum, element) }
    .seconds // TODO(Only keep the seconds. Include nanos for better accuracy?)
    .toInt()
}

private fun calculateFrequencyHistogramResults(
  measurementCoefficientPairsList: List<Pair<InternalMeasurement, Int>>
): List<Int> {
  val aggregatedFrequency =
    measurementCoefficientPairsList
      .map { (measurement, coefficient) ->
        if (!measurement.result.hasFrequency()) {
          error("Frequency measurement is missing.")
        }
        measurement.result.frequency.relativeFrequencyDistributionMap.mapValues {
          it.value * coefficient
        }
      }
      .reduce { sum, element ->
        val tempFrequency = sum.toMutableMap()
        for ((key, value) in element) {
          tempFrequency[key] = tempFrequency.getOrDefault(key, 0.0) + value
        }
        tempFrequency
      }

  // Normalize the frequency distribution
  val norm = aggregatedFrequency.values.sum()
  return aggregatedFrequency.values.map { (it / norm).toInt() }
}

/** Convert an [InternalPeriodicTimeInterval] to a list of [InternalTimeInterval]s. */
private fun InternalPeriodicTimeInterval.toInternalTimeIntervals(): List<InternalTimeInterval> {
  val source = this
  var startTime = checkNotNull(source.startTime)
  return (0 until source.intervalCount).map {
    internalTimeInterval {
      this.startTime = startTime
      this.endTime = timestampsAdd(startTime, source.increment)
      startTime = this.endTime
    }
  }
}

/** Generate row headers of [InternalReportResult] from an [InternalReport]. */
private fun getRowHeaders(report: InternalReport): List<String> {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (report.timeCase) {
    InternalReport.TimeCase.TIME_INTERVALS -> {
      report.timeIntervals.timeIntervalsList.map(InternalTimeInterval::toRowHeader)
    }
    InternalReport.TimeCase.PERIODIC_TIME_INTERVAL -> {
      report.periodicTimeInterval.toInternalTimeIntervals().map(InternalTimeInterval::toRowHeader)
    }
    InternalReport.TimeCase.TIME_NOT_SET -> {
      error("Time in the internal report should've been set.")
    }
  }
}

/** Convert an [InternalTimeInterval] to a row header in String. */
private fun InternalTimeInterval.toRowHeader(): String {
  val source = this
  val startTimeInstant =
    Instant.ofEpochSecond(source.startTime.seconds, source.startTime.nanos.toLong())
  val endTimeInstant = Instant.ofEpochSecond(source.endTime.seconds, source.endTime.nanos.toLong())
  return "$startTimeInstant-$endTimeInstant"
}

/** Convert a CMM [Measurement.Failure] to an [InternalMeasurement.Failure]. */
private fun Measurement.Failure.toFailure(): InternalMeasurement.Failure {
  val source = this

  return internalFailure {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    reason =
      when (source.reason) {
        Measurement.Failure.Reason.REASON_UNSPECIFIED ->
          InternalMeasurement.Failure.Reason.REASON_UNSPECIFIED
        Measurement.Failure.Reason.CERTIFICATE_REVOKED ->
          InternalMeasurement.Failure.Reason.CERTIFICATE_REVOKED
        Measurement.Failure.Reason.REQUISITION_REFUSED ->
          InternalMeasurement.Failure.Reason.REQUISITION_REFUSED
        Measurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED ->
          InternalMeasurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED
        Measurement.Failure.Reason.UNRECOGNIZED -> InternalMeasurement.Failure.Reason.UNRECOGNIZED
      }
    message = source.message
  }
}

/** Aggregate a list of [InternalMeasurementResult] to a [InternalMeasurementResult] */
private fun aggregateResults(
  internalResultsList: List<InternalMeasurementResult>
): InternalMeasurementResult {
  return internalResultsList.reduce { sum, element ->
    internalMeasurementResult {
      if (element.hasReach()) {
        this.reach = internalReach { value = sum.reach.value + element.reach.value }
      }
      if (element.hasFrequency()) {
        val tempFrequency = sum.frequency.relativeFrequencyDistributionMap.toMutableMap()
        for ((key, value) in element.frequency.relativeFrequencyDistributionMap) {
          tempFrequency[key] = tempFrequency.getOrDefault(key, 0.0) + value
        }
        this.frequency.relativeFrequencyDistributionMap.putAll(tempFrequency)
      }
      if (element.hasImpression()) {
        this.impression = internalImpression {
          value = sum.impression.value + element.impression.value
        }
      }
      if (element.hasWatchDuration()) {
        this.watchDuration = internalWatchDuration {
          value = durationsAdd(sum.watchDuration.value, element.watchDuration.value)
        }
      }
    }
  }
}

/** Converts a CMM [Measurement.Result] to an [InternalMeasurementResult]. */
private fun Measurement.Result.toResult(): InternalMeasurementResult {
  val source = this

  return internalMeasurementResult {
    if (source.hasReach()) {
      this.reach = internalReach { value = source.reach.value }
    }
    if (source.hasFrequency()) {
      this.frequency.relativeFrequencyDistributionMap.putAll(
        source.frequency.relativeFrequencyDistributionMap
      )
    }
    if (source.hasImpression()) {
      this.impression = internalImpression { value = source.impression.value }
    }
    if (source.hasWatchDuration()) {
      this.watchDuration = internalWatchDuration { value = source.watchDuration.value }
    }
  }
}

/** Converts an internal [InternalReport] to a public [Report]. */
private fun InternalReport.toReport(): Report {
  val source = this
  return report {
    name =
      ReportKey(
          measurementConsumerId = source.measurementConsumerReferenceId,
          reportId = externalIdToApiId(source.externalReportId)
        )
        .toName()

    reportIdempotencyKey = source.reportIdempotencyKey

    measurementConsumer = source.measurementConsumerReferenceId

    for (eventGroupFilter in source.details.eventGroupFiltersMap) {
      eventGroupUniverse.eventGroupEntriesList += eventGroupEntry {
        key = eventGroupFilter.key
        value = eventGroupFilter.value
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.timeCase) {
      InternalReport.TimeCase.TIME_INTERVALS ->
        this.timeIntervals = source.timeIntervals.toTimeIntervals()
      InternalReport.TimeCase.PERIODIC_TIME_INTERVAL ->
        this.periodicTimeInterval = source.periodicTimeInterval.toPeriodicTimeInterval()
      InternalReport.TimeCase.TIME_NOT_SET ->
        error("The time in the internal report should've be set.")
    }

    for (metric in source.metricsList) {
      this.metrics += metric.toMetric()
    }

    this.state = source.state.toState()
  }
}

private fun InternalReport.State.toState(): Report.State {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source) {
    InternalReport.State.RUNNING -> Report.State.RUNNING
    InternalReport.State.SUCCEEDED -> Report.State.SUCCEEDED
    InternalReport.State.FAILED -> Report.State.FAILED
    InternalReport.State.STATE_UNSPECIFIED -> error("Report state should've be set.")
    InternalReport.State.UNRECOGNIZED -> error("Unrecognized report state.")
  }
}

/** Converts an internal [InternalMetric] to a public [Metric]. */
private fun InternalMetric.toMetric(): Metric {
  val source = this

  return metric {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.details.metricTypeCase) {
      InternalMetric.Details.MetricTypeCase.REACH -> this.reach = reachParams {}
      InternalMetric.Details.MetricTypeCase.FREQUENCY_HISTOGRAM ->
        this.frequencyHistogram = source.details.frequencyHistogram.toFrequencyHistogram()
      InternalMetric.Details.MetricTypeCase.IMPRESSION_COUNT ->
        source.details.impressionCount.toImpressionCount()
      InternalMetric.Details.MetricTypeCase.WATCH_DURATION ->
        source.details.watchDuration.toWatchDuration()
      InternalMetric.Details.MetricTypeCase.METRICTYPE_NOT_SET ->
        error("The metric type in the internal report should've be set.")
    }

    cumulative = source.details.cumulative

    for (internalSetOperation in source.namedSetOperationsList) {
      this.setOperations += internalSetOperation.toNamedSetOperation()
    }
  }
}

/** Converts an internal [InternalNamedSetOperation] to a public [NamedSetOperation]. */
private fun InternalNamedSetOperation.toNamedSetOperation(): NamedSetOperation {
  val source = this

  return namedSetOperation {
    displayName = source.displayName
    setOperation = source.setOperation.toSetOperation()
  }
}

/** Converts an internal [InternalSetOperation] to a public [SetOperation]. */
private fun InternalSetOperation.toSetOperation(): SetOperation {
  val source = this

  return setOperation {
    this.type = source.type.toType()
    this.lhs = source.lhs.toOperand(true)
    this.rhs = source.lhs.toOperand(false)
  }
}

/** Converts an internal [InternalOperand] to a public [SetOperation.Operand]. */
private fun InternalOperand.toOperand(isLhs: Boolean): SetOperation.Operand {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.operandCase) {
    InternalOperand.OperandCase.OPERATION ->
      operand { operation = source.operation.toSetOperation() }
    InternalOperand.OperandCase.REPORTINGSETID ->
      operand {
        reportingSet =
          ReportKey(
              source.reportingSetId.measurementConsumerReferenceId,
              externalIdToApiId(source.reportingSetId.externalReportingSetId)
            )
            .toName()
      }
    InternalOperand.OperandCase.OPERAND_NOT_SET ->
      if (isLhs) {
        error("Operand on the left hand side should've be set.")
      } else {
        operand {}
      }
  }
}

/** Converts an internal [InternalSetOperation.Type] to a public [SetOperation.Type]. */
private fun InternalSetOperation.Type.toType(): SetOperation.Type {
  val source = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source) {
    InternalSetOperation.Type.UNION -> SetOperation.Type.UNION
    InternalSetOperation.Type.INTERSECTION -> SetOperation.Type.INTERSECTION
    InternalSetOperation.Type.DIFFERENCE -> SetOperation.Type.DIFFERENCE
    org.wfanet.measurement.internal.reporting.Metric.SetOperation.Type.TYPE_UNSPECIFIED ->
      error("Set operator type should've be set.")
    org.wfanet.measurement.internal.reporting.Metric.SetOperation.Type.UNRECOGNIZED ->
      error("Unrecognized Set operator type.")
  }
}

/** Converts an internal [InternalWatchDurationParams] to a public [WatchDurationParams]. */
private fun InternalWatchDurationParams.toWatchDuration(): WatchDurationParams {
  val source = this
  return watchDurationParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
    maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
  }
}

/** Converts an internal [InternalImpressionCountParams] to a public [ImpressionCountParams]. */
private fun InternalImpressionCountParams.toImpressionCount(): ImpressionCountParams {
  val source = this
  return impressionCountParams { maximumFrequencyPerUser = source.maximumFrequencyPerUser }
}

/**
 * Converts an internal [InternalFrequencyHistogramParams] to a public [FrequencyHistogramParams].
 */
private fun InternalFrequencyHistogramParams.toFrequencyHistogram(): FrequencyHistogramParams {
  val source = this
  return frequencyHistogramParams { maximumFrequencyPerUser = source.maximumFrequencyPerUser }
}

/** Converts an internal [InternalPeriodicTimeInterval] to a public [PeriodicTimeInterval]. */
private fun InternalPeriodicTimeInterval.toPeriodicTimeInterval(): PeriodicTimeInterval {
  val source = this
  return periodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}

/** Converts an internal [InternalTimeIntervals] to a public [TimeIntervals]. */
private fun InternalTimeIntervals.toTimeIntervals(): TimeIntervals {
  val source = this
  return timeIntervals {
    for (internalTimeInternal in source.timeIntervalsList) {
      this.timeIntervals += timeInterval {
        startTime = internalTimeInternal.startTime
        endTime = internalTimeInternal.endTime
      }
    }
  }
}

/** Converts an internal [ListReportsPageToken] to an internal [StreamReportsRequest]. */
private fun ListReportsPageToken.toStreamReportsRequest(): StreamReportsRequest {
  val source = this
  return streamReportsRequest {
    // get 1 more than the actual page size for deciding whether or not to set page token
    limit = pageSize + 1
    filter = filter {
      measurementConsumerReferenceId = source.measurementConsumerReferenceId
      externalReportIdAfter = source.lastReport.externalReportId
    }
  }
}

/** Converts a public [ListReportsRequest] to an internal [ListReportsPageToken]. */
private fun ListReportsRequest.toListReportsPageToken(): ListReportsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }
  val measurementConsumerReferenceId = parentKey.measurementConsumerId

  return if (pageToken.isNotBlank()) {
    ListReportsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
      grpcRequire(this.measurementConsumerReferenceId == measurementConsumerReferenceId) {
        "Arguments must be kept the same when using a page token"
      }

      if (
        source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE
      ) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
    }
  }
}
