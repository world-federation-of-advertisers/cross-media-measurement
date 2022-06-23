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

import io.grpc.Status
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
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
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
import org.wfanet.measurement.internal.reporting.MeasurementKt.result as internalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.Metric.FrequencyHistogramParams as InternalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.Metric.ImpressionCountParams as InternalImpressionCountParams
import org.wfanet.measurement.internal.reporting.Metric.NamedSetOperation as InternalNamedSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation as InternalSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation.Operand as InternalOperand
import org.wfanet.measurement.internal.reporting.Metric.WatchDurationParams as InternalWatchDurationParams
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.streamReportsRequest
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
          .map {
            it.updateReport(
              internalReportsStub,
              internalMeasurementsStub,
              measurementsStub,
              certificateStub,
              privateKey,
              apiAuthenticationKey
            )
          }
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
}

/** Update an internal report with its state and its measurements' states. */
private fun InternalReport.updateReport(
  internalReportsStub: ReportsCoroutineStub,
  internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  measurementsStub: MeasurementsCoroutineStub,
  certificateStub: CertificatesCoroutineStub,
  privateKey: PrivateKeyHandle,
  apiAuthenticationKey: String,
): InternalReport {
  val source = this

  if (source.state == InternalReport.State.SUCCEEDED) {
    return source
  } else if (
    source.state == InternalReport.State.STATE_UNSPECIFIED ||
      source.state == InternalReport.State.UNRECOGNIZED
  ) {
    error("The report cannot be updated if its state is either unspecified or unrecognized.")
  }

  // Update measurement state
  val allMeasurementSucceeded = true
  for ((measurementReferenceID, internalMeasurement) in source.measurementsMap) {
    if (internalMeasurement.state == InternalMeasurement.State.SUCCEEDED) continue

    val measurement =
      runBlocking(Dispatchers.IO) {
        measurementsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementReferenceID })
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (measurement.state) {
      Measurement.State.SUCCEEDED -> {
        runBlocking {
          internalMeasurementsStub.setMeasurementResult(
            setMeasurementResultRequest {
              measurementConsumerReferenceId = source.measurementConsumerReferenceId
              measurementReferenceId = measurementReferenceID
              result =
                aggregateResults(
                  measurement.resultsList
                    .map {
                      getMeasurementResult(it, certificateStub, apiAuthenticationKey, privateKey)
                    }
                    .map(Measurement.Result::toResult)
                )
            }
          )
        }
      }
      Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
      Measurement.State.COMPUTING -> {
        error("")
      }
      Measurement.State.FAILED,
      Measurement.State.CANCELLED -> {
        error("")
      }
      Measurement.State.STATE_UNSPECIFIED -> error("")
      Measurement.State.UNRECOGNIZED -> error("")
    }
  }
  return this
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
          // TODO(Find a correct way)
          value = sum.watchDuration.value + element.watchDuration.value
        }
      }
    }
  }
}

/** Converts an CMM [Measurement.Result] to an internal [InternalMeasurementResult]. */
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

/** Decrypt a [Measurement.ResultPair] to [Measurement.Result] */
private fun getMeasurementResult(
  resultPair: Measurement.ResultPair,
  certificateStub: CertificatesCoroutineStub,
  apiAuthenticationKey: String,
  privateKey: PrivateKeyHandle
): Measurement.Result {
  val certificate = runBlocking {
    certificateStub
      .withAuthenticationKey(apiAuthenticationKey)
      .getCertificate(getCertificateRequest { name = resultPair.certificate })
  }

  val signedResult = decryptResult(resultPair.encryptedResult, privateKey)

  val result = Measurement.Result.parseFrom(signedResult.data)

  if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
    error("Signature of the result is invalid.")
  }
  return result
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

    // TODO(Assign the idempotency key from internal report when the fix is ready.)
    reportIdempotencyKey = externalIdToApiId(source.externalReportId)

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
