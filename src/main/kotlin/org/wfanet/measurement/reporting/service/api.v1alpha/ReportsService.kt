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

import com.google.protobuf.ByteString
import com.google.protobuf.Duration
import com.google.protobuf.duration
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.security.PrivateKey
import java.security.SecureRandom
import java.time.Instant
import kotlin.math.min
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2.alpha.ListReportsPageToken
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value as dataProviderEntryValue
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration as measurementSpecDuration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression as measurementSpecImpression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency as measurementSpecReachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value as eventGroupEntryValue
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter as requisitionSpecEventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.TimeInterval as MeasurementTimeInterval
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval as measurementTimeInterval
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.CreateReportRequest.MeasurementKey as InternalMeasurementKey
import org.wfanet.measurement.internal.reporting.CreateReportRequestKt.measurementKey as internalMeasurementKey
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.Measurement.Result as InternalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.frequency as internalFrequency
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.impression as internalImpression
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.reach as internalReach
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.watchDuration as internalWatchDuration
import org.wfanet.measurement.internal.reporting.MeasurementKt.failure as internalFailure
import org.wfanet.measurement.internal.reporting.MeasurementKt.result as internalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.Metric.Details as InternalMetricDetails
import org.wfanet.measurement.internal.reporting.Metric.Details.MetricTypeCase as InternalMetricTypeCase
import org.wfanet.measurement.internal.reporting.Metric.FrequencyHistogramParams as InternalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.Metric.ImpressionCountParams as InternalImpressionCountParams
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation.WeightedMeasurement as InternalWeightedMeasurement
import org.wfanet.measurement.internal.reporting.Metric.NamedSetOperation as InternalNamedSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation as InternalSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation.Operand as InternalOperand
import org.wfanet.measurement.internal.reporting.Metric.WatchDurationParams as InternalWatchDurationParams
import org.wfanet.measurement.internal.reporting.MetricKt.MeasurementCalculationKt.weightedMeasurement as internalWeightedMeasurement
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.operand as internalOperand
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.reportingSetKey
import org.wfanet.measurement.internal.reporting.MetricKt.details as internalMetricDetails
import org.wfanet.measurement.internal.reporting.MetricKt.frequencyHistogramParams as internalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.MetricKt.impressionCountParams as internalImpressionCountParams
import org.wfanet.measurement.internal.reporting.MetricKt.measurementCalculation as internalMeasurementCalculation
import org.wfanet.measurement.internal.reporting.MetricKt.namedSetOperation as internalNamedSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.reachParams as internalReachParams
import org.wfanet.measurement.internal.reporting.MetricKt.setOperation as internalSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.watchDurationParams as internalWatchDurationParams
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.ReportKt.details as internalReportDetails
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest as SetInternalMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequest as StreamInternalReportsRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.getMeasurementRequest as getInternalMeasurementRequest
import org.wfanet.measurement.internal.reporting.getReportByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.getReportRequest as getInternalReportRequest
import org.wfanet.measurement.internal.reporting.getReportingSetRequest
import org.wfanet.measurement.internal.reporting.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.metric as internalMetric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.report as internalReport
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest as setInternalMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest as setInternalMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.streamReportsRequest as streamInternalReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval
import org.wfanet.measurement.internal.reporting.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.reporting.v1alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.Metric.FrequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.Metric.ImpressionCountParams
import org.wfanet.measurement.reporting.v1alpha.Metric.MetricTypeCase
import org.wfanet.measurement.reporting.v1alpha.Metric.NamedSetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation.Operand
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
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry as eventGroupUniverseEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
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

private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / 300.0f
private const val NUMBER_REACH_ONLY_BUCKETS = 16
private val REACH_ONLY_VID_SAMPLING_START_LIST =
  (0 until NUMBER_REACH_ONLY_BUCKETS).map { it * REACH_ONLY_VID_SAMPLING_WIDTH }
private const val REACH_ONLY_REACH_EPSILON = 0.0041
private const val REACH_ONLY_FREQUENCY_EPSILON = 0.0001
private const val REACH_ONLY_MAXIMUM_FREQUENCY_PER_USER = 1

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / 300.0f
private const val NUMBER_REACH_FREQUENCY_BUCKETS = 19
private val REACH_FREQUENCY_VID_SAMPLING_START_LIST =
  (0 until NUMBER_REACH_FREQUENCY_BUCKETS).map {
    REACH_ONLY_VID_SAMPLING_START_LIST.last() +
      REACH_ONLY_VID_SAMPLING_WIDTH +
      it * REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / 300.0f
private const val NUMBER_IMPRESSION_BUCKETS = 1
private val IMPRESSION_VID_SAMPLING_START_LIST =
  (0 until NUMBER_IMPRESSION_BUCKETS).map {
    REACH_FREQUENCY_VID_SAMPLING_START_LIST.last() +
      REACH_FREQUENCY_VID_SAMPLING_WIDTH +
      it * IMPRESSION_VID_SAMPLING_WIDTH
  }
private const val IMPRESSION_EPSILON = 0.0011

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / 300.0f
private const val NUMBER_WATCH_DURATION_BUCKETS = 1
private val WATCH_DURATION_VID_SAMPLING_START_LIST =
  (0 until NUMBER_WATCH_DURATION_BUCKETS).map {
    IMPRESSION_VID_SAMPLING_START_LIST.last() +
      IMPRESSION_VID_SAMPLING_WIDTH +
      it * WATCH_DURATION_VID_SAMPLING_WIDTH
  }
private const val WATCH_DURATION_EPSILON = 0.001

private const val DIFFERENTIAL_PRIVACY_DETLA = 1e-12

data class ServiceStubs(
  val internalReportsStub: InternalReportsCoroutineStub,
  val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  val dataProvidersStub: DataProvidersCoroutineStub,
  val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  val measurementsStub: MeasurementsCoroutineStub,
  val certificateStub: CertificatesCoroutineStub,
)

data class Credential(
  val measurementConsumerReferenceId: String,
  val measurementConsumerResourceName: String,
  val reportIdempotencyKey: String,
  val encryptionPrivateKeyHandle: PrivateKeyHandle,
  val signingPrivateKey: PrivateKey,
  val apiAuthenticationKey: String,
  val secureRandom: SecureRandom,
)

/** TODO(@renjiez) Have a function to get public/private keys */
class ReportsService(
  private val serviceStubs: ServiceStubs,
  private val encryptionPrivateKeyHandle: PrivateKeyHandle,
  private val signingPrivateKey: PrivateKey,
  private val apiAuthenticationKey: String,
) : ReportsCoroutineImplBase() {
  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

  override suspend fun createReport(request: CreateReportRequest): Report {
    val principal = principalFromCurrentContext
    val resourceKey = principal.resourceKey

    when (resourceKey) {
      is MeasurementConsumerKey -> {
        if (request.parent != resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Report for another MeasurementConsumer."
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to create a Report." }
      }
    }

    grpcRequire(request.hasReport()) { "Report is not specified." }

    grpcRequire(request.report.reportIdempotencyKey.isNotEmpty()) {
      "ReportIdempotencyKey is not specified."
    }

    try {
      serviceStubs.internalReportsStub.getReportByIdempotencyKey(
        getReportByIdempotencyKeyRequest {
          measurementConsumerReferenceId = resourceKey.measurementConsumerId
          reportIdempotencyKey = request.report.reportIdempotencyKey
        }
      )
      failGrpc(Status.ALREADY_EXISTS) {
        "Report with reportIdempotencyKey=${request.report.reportIdempotencyKey} already exists."
      }
    } catch (_: RuntimeException) {} // No existing reports have the same reportIdempotencyKey

    val credential =
      Credential(
        resourceKey.measurementConsumerId,
        request.parent,
        request.report.reportIdempotencyKey,
        encryptionPrivateKeyHandle,
        signingPrivateKey,
        apiAuthenticationKey,
        secureRandom
      )

    return serviceStubs.internalReportsStub
      .createReport(request.toInternal(serviceStubs, credential))
      .toReport()
  }

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val principal = principalFromCurrentContext
    val listReportsPageToken = request.toListReportsPageToken()

    // Based on AIP-132#Errors
    when (val resourceKey = principal.resourceKey) {
      is MeasurementConsumerKey -> {
        if (request.parent != resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Reports belonging to other MeasurementConsumers."
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list Reports." }
      }
    }

    val results: List<InternalReport> =
      serviceStubs.internalReportsStub
        .streamReports(listReportsPageToken.toStreamReportsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListReportsResponse.getDefaultInstance()
    }

    val nextPageToken: ListReportsPageToken? =
      if (results.size > listReportsPageToken.pageSize) {
        listReportsPageToken.copy {
          lastReport = previousPageEnd {
            measurementConsumerReferenceId =
              results[results.lastIndex - 1].measurementConsumerReferenceId
            externalReportId = results[results.lastIndex - 1].externalReportId
          }
        }
      } else null

    return listReportsResponse {
      reports +=
        results
          .subList(0, min(results.size, listReportsPageToken.pageSize))
          .map { syncReports(it) }
          .map(InternalReport::toReport)

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Syncs the [InternalReport] and all [InternalMeasurement]s used by it. */
  private suspend fun syncReports(internalReport: InternalReport): InternalReport {
    // Report with SUCCEEDED or FAILED state is already synced.
    if (
      internalReport.state == InternalReport.State.SUCCEEDED ||
        internalReport.state == InternalReport.State.FAILED
    ) {
      return internalReport
    } else if (
      internalReport.state == InternalReport.State.STATE_UNSPECIFIED ||
        internalReport.state == InternalReport.State.UNRECOGNIZED
    ) {
      error(
        "The measurements cannot be synced because the report state was not set correctly as it " +
          "should've been."
      )
    }

    // Syncs measurements
    syncMeasurements(internalReport.measurementsMap, internalReport.measurementConsumerReferenceId)

    return serviceStubs.internalReportsStub.getReport(
      getInternalReportRequest {
        measurementConsumerReferenceId = internalReport.measurementConsumerReferenceId
        externalReportId = internalReport.externalReportId
      }
    )
  }

  /** Syncs [InternalMeasurement]s. */
  private suspend fun syncMeasurements(
    measurementsMap: Map<String, InternalMeasurement>,
    measurementConsumerReferenceId: String,
  ) = coroutineScope {
    for ((measurementReferenceId, internalMeasurement) in measurementsMap) {
      // Measurement with SUCCEEDED state is already synced
      if (internalMeasurement.state == InternalMeasurement.State.SUCCEEDED) continue

      launch {
        syncMeasurement(
          measurementReferenceId,
          measurementConsumerReferenceId,
        )
      }
    }
  }

  /** Syncs [InternalMeasurement] with the CMM [Measurement] given the measurement reference ID. */
  private suspend fun syncMeasurement(
    measurementReferenceId: String,
    measurementConsumerReferenceId: String,
  ) {
    val measurement =
      serviceStubs.measurementsStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getMeasurement(getMeasurementRequest { name = measurementReferenceId })

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (measurement.state) {
      Measurement.State.SUCCEEDED -> {
        // Converts a Measurement to an InternalMeasurement and store it into the database with
        // SUCCEEDED state
        val setInternalMeasurementResultRequest =
          getSetInternalMeasurementResultRequest(
            measurementConsumerReferenceId,
            measurementReferenceId,
            measurement.resultsList,
          )
        serviceStubs.internalMeasurementsStub.setMeasurementResult(
          setInternalMeasurementResultRequest
        )
      }
      Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
      Measurement.State.COMPUTING -> {} // No action needed
      Measurement.State.FAILED,
      Measurement.State.CANCELLED -> {
        serviceStubs.internalMeasurementsStub.setMeasurementFailure(
          setInternalMeasurementFailureRequest {
            this.measurementConsumerReferenceId = measurementConsumerReferenceId
            this.measurementReferenceId = measurementReferenceId
            failure = measurement.failure.toInternal()
          }
        )
      }
      Measurement.State.STATE_UNSPECIFIED -> error("The measurement state should've been set.")
      Measurement.State.UNRECOGNIZED -> error("Unrecognized measurement state.")
    }
  }

  /** Gets a [SetInternalMeasurementResultRequest]. */
  private suspend fun getSetInternalMeasurementResultRequest(
    measurementConsumerReferenceId: String,
    measurementReferenceId: String,
    resultsList: List<Measurement.ResultPair>,
  ): SetInternalMeasurementResultRequest {

    return setInternalMeasurementResultRequest {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      this.measurementReferenceId = measurementReferenceId
      result =
        aggregateResults(
          resultsList.map { it.toMeasurementResult() }.map(Measurement.Result::toInternal)
        )
    }
  }

  /** Decrypts a [Measurement.ResultPair] to [Measurement.Result] */
  private suspend fun Measurement.ResultPair.toMeasurementResult(): Measurement.Result {
    val source = this
    val certificate =
      serviceStubs.certificateStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = source.certificate })

    val signedResult = decryptResult(source.encryptedResult, encryptionPrivateKeyHandle)

    val result = Measurement.Result.parseFrom(signedResult.data)

    if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
      error("Signature of the result is invalid.")
    }
    return result
  }
}

/** Converts a public [CreateReportRequest] to an [InternalCreateReportRequest]. */
private suspend fun CreateReportRequest.toInternal(
  serviceStubs: ServiceStubs,
  credential: Credential,
): InternalCreateReportRequest {
  val source = this

  grpcRequire(source.report.measurementConsumer == source.parent) {
    "Cannot create a Report for another MeasurementConsumer."
  }
  grpcRequire(source.report.hasEventGroupUniverse()) { "EventGroupUniverse is not specified." }
  grpcRequire(source.report.metricsList.isNotEmpty()) { "Metrics in Report cannot be empty." }

  checkSetOperationDisplayNamesUniqueness(source.report.metricsList)

  val eventGroupFilters =
    source.report.eventGroupUniverse.eventGroupEntriesList.associate { it.key to it.value }
  val setOperationCompiler = SetOperationCompiler()

  val internalReport = internalReport {
    measurementConsumerReferenceId = credential.measurementConsumerReferenceId

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val internalTimeIntervalsList: List<InternalTimeInterval> =
      when (source.report.timeCase) {
        Report.TimeCase.TIME_INTERVALS -> {
          this.timeIntervals = source.report.timeIntervals.toInternal()
          this.timeIntervals.timeIntervalsList.map { it }
        }
        Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
          this.periodicTimeInterval = source.report.periodicTimeInterval.toInternal()
          this.periodicTimeInterval.toInternalTimeIntervalsList()
        }
        Report.TimeCase.TIME_NOT_SET -> error("The time in Report is not specified.")
      }

    coroutineScope {
      for (metric in source.report.metricsList) {
        launch {
          this@internalReport.metrics +=
            metric.toInternal(
              serviceStubs,
              credential,
              setOperationCompiler,
              internalTimeIntervalsList,
              eventGroupFilters,
            )
        }
      }
    }
    details = internalReportDetails { this.eventGroupFilters.putAll(eventGroupFilters) }
    reportIdempotencyKey = source.report.reportIdempotencyKey
  }

  return internalCreateReportRequest {
    report = internalReport
    measurements +=
      internalReport.metricsList
        .map { it.toInternalMeasurementKey(credential.measurementConsumerReferenceId) }
        .flatten()
  }
}

/** Check if the display names of the set operations within the same metric are unique. */
private fun checkSetOperationDisplayNamesUniqueness(metricsList: List<Metric>) {
  val metricToSetOperationDisplayNamesList =
    mutableMapOf<MetricTypeCase, MutableSet<String>>().withDefault { mutableSetOf() }

  for (metric in metricsList) {
    for (setOperation in metric.setOperationsList) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (metric.metricTypeCase) {
        MetricTypeCase.REACH -> {
          grpcRequire(
            !metricToSetOperationDisplayNamesList
              .getValue(MetricTypeCase.REACH)
              .contains(setOperation.displayName)
          ) { "The display names of the set operations within the same metric should be unique." }
          metricToSetOperationDisplayNamesList.getValue(MetricTypeCase.REACH) +=
            setOperation.displayName
        }
        MetricTypeCase.FREQUENCY_HISTOGRAM -> {
          grpcRequire(
            !metricToSetOperationDisplayNamesList
              .getValue(MetricTypeCase.FREQUENCY_HISTOGRAM)
              .contains(setOperation.displayName)
          ) { "The display names of the set operations within the same metric should be unique." }
          metricToSetOperationDisplayNamesList.getValue(MetricTypeCase.FREQUENCY_HISTOGRAM) +=
            setOperation.displayName
        }
        MetricTypeCase.IMPRESSION_COUNT -> {
          grpcRequire(
            !metricToSetOperationDisplayNamesList
              .getValue(MetricTypeCase.IMPRESSION_COUNT)
              .contains(setOperation.displayName)
          ) { "The display names of the set operations within the same metric should be unique." }
          metricToSetOperationDisplayNamesList.getValue(MetricTypeCase.IMPRESSION_COUNT) +=
            setOperation.displayName
        }
        MetricTypeCase.WATCH_DURATION -> {
          grpcRequire(
            !metricToSetOperationDisplayNamesList
              .getValue(MetricTypeCase.WATCH_DURATION)
              .contains(setOperation.displayName)
          ) { "The display names of the set operations within the same metric should be unique." }
          metricToSetOperationDisplayNamesList.getValue(MetricTypeCase.WATCH_DURATION) +=
            setOperation.displayName
        }
        MetricTypeCase.METRICTYPE_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) {
            "The metric type in the internal report should've be set."
          }
      }
    }
  }
}

/** Converts a [InternalMetric] to a list of [InternalMeasurementKey]s. */
private fun InternalMetric.toInternalMeasurementKey(
  measurementConsumerReferenceId: String
): List<InternalMeasurementKey> {
  return this.toMeasurementReferenceIds().map { measurementReferenceId ->
    internalMeasurementKey {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      this.measurementReferenceId = measurementReferenceId
    }
  }
}

/** Converts an [InternalMetric] to a list of measurement reference IDs. */
private fun InternalMetric.toMeasurementReferenceIds(): List<String> {
  return this.namedSetOperationsList
    .map { namedSetOperation -> namedSetOperation.toMeasurementReferenceIds() }
    .flatten()
}

/** Converts an [InternalNamedSetOperation] to a list of measurement reference IDs. */
private fun InternalNamedSetOperation.toMeasurementReferenceIds(): List<String> {
  return this.measurementCalculationsList
    .map { measurementCalculation ->
      measurementCalculation.weightedMeasurementsList.map { it.measurementReferenceId }
    }
    .flatten()
}

/** Converts a public [Metric] to an [InternalMetric]. */
private suspend fun Metric.toInternal(
  serviceStubs: ServiceStubs,
  credential: Credential,
  setOperationCompiler: SetOperationCompiler,
  internalTimeIntervalsList: List<InternalTimeInterval>,
  eventGroupFilters: Map<String, String>,
): InternalMetric {
  val source = this

  return internalMetric {
    details = internalMetricDetails {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (source.metricTypeCase) {
        MetricTypeCase.REACH -> reach = internalReachParams {}
        MetricTypeCase.FREQUENCY_HISTOGRAM ->
          frequencyHistogram = source.frequencyHistogram.toInternal()
        MetricTypeCase.IMPRESSION_COUNT -> impressionCount = source.impressionCount.toInternal()
        MetricTypeCase.WATCH_DURATION -> watchDuration = source.watchDuration.toInternal()
        MetricTypeCase.METRICTYPE_NOT_SET ->
          error("The metric type in the internal report should've be set.")
      }

      cumulative = source.cumulative
    }

    coroutineScope {
      source.setOperationsList.map { setOperation ->
        launch {
          val internalNamedSetOperation =
            setOperation.toInternal(
              serviceStubs,
              credential,
              setOperationCompiler,
              internalTimeIntervalsList,
              eventGroupFilters,
              this@internalMetric.details,
            )
          namedSetOperations += internalNamedSetOperation
        }
      }
    }
  }
}

/** Converts an [InternalTimeInterval] to a [MeasurementTimeInterval] for measurement request. */
private fun InternalTimeInterval.toMeasurementTimeInterval(): MeasurementTimeInterval {
  val source = this
  return measurementTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Converts a public [NamedSetOperation] to an [InternalNamedSetOperation]. */
private suspend fun NamedSetOperation.toInternal(
  serviceStubs: ServiceStubs,
  credential: Credential,
  setOperationCompiler: SetOperationCompiler,
  internalTimeIntervalsList: List<InternalTimeInterval>,
  eventGroupFilters: Map<String, String>,
  internalMetricDetails: InternalMetricDetails,
): InternalNamedSetOperation {
  val source = this

  val internalReportTimeIntervalsList =
    if (internalMetricDetails.cumulative) {
      val startTime = internalTimeIntervalsList.first().startTime
      internalTimeIntervalsList.map { internalTimeInterval ->
        internalTimeInterval {
          this.startTime = startTime
          endTime = internalTimeInterval.endTime
        }
      }
    } else internalTimeIntervalsList

  return internalNamedSetOperation {
    displayName = source.displayName
    setOperation = source.setOperation.toInternal(serviceStubs, credential, eventGroupFilters)

    val weightedMeasurementsList = setOperationCompiler.compileSetOperation(source)

    this.measurementCalculations +=
      getMeasurementCalculationList(
        serviceStubs,
        credential,
        weightedMeasurementsList,
        internalReportTimeIntervalsList,
        eventGroupFilters,
        internalMetricDetails,
        displayName,
      )
  }
}

/**
 * Gets a list of [MeasurementCalculation]s from a list of public [WeightedMeasurement]s and a list
 * of [InternalTimeInterval]s.
 */
private suspend fun getMeasurementCalculationList(
  serviceStubs: ServiceStubs,
  credential: Credential,
  weightedMeasurementsList: List<WeightedMeasurement>,
  internalReportTimeIntervalsList: List<InternalTimeInterval>,
  eventGroupFilters: Map<String, String>,
  internalMetricDetails: InternalMetricDetails,
  setOperationDisplayName: String,
): List<MeasurementCalculation> {
  return internalReportTimeIntervalsList.map { timeInterval ->
    internalMeasurementCalculation {
      this.timeInterval = timeInterval
      weightedMeasurements +=
        weightedMeasurementsList.mapIndexed { index, weightedMeasurement ->
          weightedMeasurement.toInternalWeightedMeasurement(
            serviceStubs,
            credential,
            timeInterval,
            eventGroupFilters,
            internalMetricDetails,
            setOperationDisplayName,
            index
          )
        }
    }
  }
}

/** Converts a [WeightedMeasurement] to an [InternalWeightedMeasurement] */
private suspend fun WeightedMeasurement.toInternalWeightedMeasurement(
  serviceStubs: ServiceStubs,
  credential: Credential,
  timeInterval: org.wfanet.measurement.internal.reporting.TimeInterval,
  eventGroupFilters: Map<String, String>,
  internalMetricDetails: org.wfanet.measurement.internal.reporting.Metric.Details,
  setOperationDisplayName: String,
  index: Int
): InternalWeightedMeasurement {
  val measurementReferenceId =
    getMeasurementReferenceId(
      credential.reportIdempotencyKey,
      timeInterval,
      internalMetricDetails,
      setOperationDisplayName,
      index,
    )
  return this.toInternal(
    serviceStubs,
    credential,
    timeInterval,
    eventGroupFilters,
    internalMetricDetails,
    measurementReferenceId,
  )
}

/** Gets a unique reference ID for a [Measurement]. */
private fun getMeasurementReferenceId(
  reportIdempotencyKey: String,
  internalTimeInterval: InternalTimeInterval,
  internalMetricDetails: InternalMetricDetails,
  setOperationDisplayName: String,
  index: Int,
): String {
  val rowHeader = internalTimeInterval.toRowHeader()

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  val metricType =
    when (internalMetricDetails.metricTypeCase) {
      InternalMetricTypeCase.REACH -> "Reach"
      InternalMetricTypeCase.FREQUENCY_HISTOGRAM -> "FrequencyHistogram"
      InternalMetricTypeCase.IMPRESSION_COUNT -> "ImpressionCount"
      InternalMetricTypeCase.WATCH_DURATION -> "WatchDuration"
      InternalMetricTypeCase.METRICTYPE_NOT_SET ->
        error("Unset metric type should've already raised error.")
    }

  return "$reportIdempotencyKey-$rowHeader-$metricType-$setOperationDisplayName-measurement-$index"
}

/** Converts a public [WeightedMeasurement] to an [InternalWeightedMeasurement]. */
private suspend fun WeightedMeasurement.toInternal(
  serviceStubs: ServiceStubs,
  credential: Credential,
  internalTimeInterval: InternalTimeInterval,
  eventGroupFilters: Map<String, String>,
  internalMetricDetails: InternalMetricDetails,
  measurementReferenceId: String,
): InternalWeightedMeasurement {
  val source = this

  try {
    serviceStubs.internalMeasurementsStub.getMeasurement(
      getInternalMeasurementRequest {
        measurementConsumerReferenceId = credential.measurementConsumerReferenceId
        this.measurementReferenceId = measurementReferenceId
      }
    )
  } catch (_: RuntimeException) {
    val createMeasurementRequest: CreateMeasurementRequest =
      getCreateMeasurementRequest(
        serviceStubs,
        credential,
        internalTimeInterval,
        eventGroupFilters,
        internalMetricDetails,
        measurementReferenceId,
        source.reportingSets
      )

    serviceStubs.measurementsStub
      .withAuthenticationKey(credential.apiAuthenticationKey)
      .createMeasurement(createMeasurementRequest)

    serviceStubs.internalMeasurementsStub.createMeasurement(
      internalMeasurement {
        measurementConsumerReferenceId = credential.measurementConsumerReferenceId
        this.measurementReferenceId = measurementReferenceId
        state = InternalMeasurement.State.PENDING
      }
    )
  }

  return internalWeightedMeasurement {
    this.measurementReferenceId = measurementReferenceId
    coefficient = source.coefficient
  }
}

/** Gets a [CreateMeasurementRequest]. */
private suspend fun getCreateMeasurementRequest(
  serviceStubs: ServiceStubs,
  credential: Credential,
  internalTimeInterval: org.wfanet.measurement.internal.reporting.TimeInterval,
  eventGroupFilters: Map<String, String>,
  internalMetricDetails: org.wfanet.measurement.internal.reporting.Metric.Details,
  measurementReferenceId: String,
  reportingSetNames: List<String>,
): CreateMeasurementRequest {
  val measurementConsumer =
    serviceStubs.measurementConsumersStub
      .withAuthenticationKey(credential.apiAuthenticationKey)
      .getMeasurementConsumer(
        getMeasurementConsumerRequest { name = credential.measurementConsumerResourceName }
      )
  val measurementConsumerCertificate = readCertificate(measurementConsumer.certificateDer)
  val measurementConsumerSigningKey =
    SigningKeyHandle(measurementConsumerCertificate, credential.signingPrivateKey)
  val measurementEncryptionPublicKey = measurementConsumer.publicKey.data

  val measurement = measurement {
    this.measurementConsumerCertificate = measurementConsumer.certificate

    dataProviders +=
      getDataProviderEntries(
        serviceStubs,
        credential,
        reportingSetNames,
        internalTimeInterval.toMeasurementTimeInterval(),
        eventGroupFilters,
        measurementEncryptionPublicKey,
        measurementConsumerSigningKey,
      )

    val unsignedMeasurementSpec: MeasurementSpec =
      getUnsignedMeasurementSpec(
        measurementEncryptionPublicKey,
        dataProviders.map { it.value.nonceHash },
        internalMetricDetails
      )

    this.measurementSpec =
      signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)

    this.measurementReferenceId = measurementReferenceId
  }

  return createMeasurementRequest { this.measurement = measurement }
}

/** Gets a list of [DataProviderEntry]s. */
private suspend fun getDataProviderEntries(
  serviceStubs: ServiceStubs,
  credential: Credential,
  reportingSetNames: List<String>,
  timeInterval: MeasurementTimeInterval,
  eventGroupFilters: Map<String, String>,
  measurementEncryptionPublicKey: ByteString,
  measurementConsumerSigningKey: SigningKeyHandle,
): List<DataProviderEntry> {

  val dataProviderNameToInternalEventGroupEntriesList =
    aggregateInternalEventGroupEntryByDataProviderName(
      serviceStubs.internalReportingSetsStub,
      reportingSetNames,
      timeInterval,
      eventGroupFilters
    )

  return dataProviderNameToInternalEventGroupEntriesList.map {
    (dataProviderName, eventGroupEntriesList) ->
    dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        eventGroups += eventGroupEntriesList
        this.measurementPublicKey = measurementEncryptionPublicKey
        nonce = credential.secureRandom.nextLong()
      }

      val dataProvider =
        serviceStubs.dataProvidersStub
          .withAuthenticationKey(credential.apiAuthenticationKey)
          .getDataProvider(getDataProviderRequest { name = dataProviderName })

      key = dataProviderName
      value = dataProviderEntryValue {
        dataProviderCertificate = dataProvider.certificate
        dataProviderPublicKey = dataProvider.publicKey
        encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
            EncryptionPublicKey.parseFrom(dataProvider.publicKey.data)
          )
        nonceHash = hashSha256(requisitionSpec.nonce)
      }
    }
  }
}

/** Gets a map of data provider resource name to a list of [EventGroupEntry]s. */
private suspend fun aggregateInternalEventGroupEntryByDataProviderName(
  internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  reportingSetNames: List<String>,
  timeInterval: MeasurementTimeInterval,
  eventGroupFilters: Map<String, String>,
): Map<String, List<EventGroupEntry>> {
  val dataProviderNameToInternalEventGroupEntriesList =
    mutableMapOf<String, MutableList<EventGroupEntry>>().withDefault { mutableListOf() }

  coroutineScope {
    for (reportingSetName in reportingSetNames) {
      val reportingSetKey =
        grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
          "Invalid reporting set name ${reportingSetName}."
        }

      val internalReportingSet = async {
        internalReportingSetsStub.getReportingSet(
          getReportingSetRequest {
            measurementConsumerReferenceId = reportingSetKey.measurementConsumerId
            externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
          }
        )
      }

      for (eventGroupKey in internalReportingSet.await().eventGroupKeysList) {
        val dataProviderName = DataProviderKey(eventGroupKey.dataProviderReferenceId).toName()
        val eventGroupName =
          EventGroupKey(
              eventGroupKey.measurementConsumerReferenceId,
              eventGroupKey.dataProviderReferenceId,
              eventGroupKey.eventGroupReferenceId
            )
            .toName()

        dataProviderNameToInternalEventGroupEntriesList
          .getValue(dataProviderName)
          .add(
            eventGroupEntry {
              key = eventGroupName
              value = eventGroupEntryValue {
                collectionInterval = timeInterval

                val filter =
                  combineEventGroupFilters(
                    internalReportingSet.await().filter,
                    eventGroupFilters[eventGroupName]
                  )
                if (filter != null) {
                  this.filter = requisitionSpecEventFilter { expression = filter }
                }
              }
            }
          )
      }
    }
  }

  return dataProviderNameToInternalEventGroupEntriesList.mapValues { it.value.toList() }.toMap()
}

/** Combines two event group filters. */
private fun combineEventGroupFilters(filter1: String?, filter2: String?): String? {
  if (filter1 == null) return filter2

  return if (filter2 == null) filter1
  else {
    "($filter1) AND ($filter2)"
  }
}

/** Gets the unsigned [MeasurementSpec]. */
private fun getUnsignedMeasurementSpec(
  measurementEncryptionPublicKey: ByteString,
  nonceHashes: List<ByteString>,
  internalMetricDetails: InternalMetricDetails
): MeasurementSpec {
  return measurementSpec {
    measurementPublicKey = measurementEncryptionPublicKey
    this.nonceHashes += nonceHashes

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (internalMetricDetails.metricTypeCase) {
      InternalMetricTypeCase.REACH -> {
        reachAndFrequency = getMeasurementSpecReachOnly()
        vidSamplingInterval = getReachOnlyVidSamplingInterval()
      }
      InternalMetricTypeCase.FREQUENCY_HISTOGRAM -> {
        reachAndFrequency =
          getMeasurementSpecReachAndFrequency(
            internalMetricDetails.frequencyHistogram.maximumFrequencyPerUser
          )
        vidSamplingInterval = getReachAndFrequencyVidSamplingInterval()
      }
      InternalMetricTypeCase.IMPRESSION_COUNT -> {
        impression =
          getMeasurementSpecImpression(
            internalMetricDetails.impressionCount.maximumFrequencyPerUser
          )
        vidSamplingInterval = getImpressionVidSamplingInterval()
      }
      InternalMetricTypeCase.WATCH_DURATION -> {
        duration =
          getMeasurementSpecDuration(
            internalMetricDetails.watchDuration.maximumWatchDurationPerUser,
            internalMetricDetails.watchDuration.maximumFrequencyPerUser
          )
        vidSamplingInterval = getDurationVidSamplingInterval()
      }
      InternalMetricTypeCase.METRICTYPE_NOT_SET ->
        error("Unset metric type should've already raised error.")
    }
  }
}

/** Gets a [VidSamplingInterval] for reach-only. */
private fun getReachOnlyVidSamplingInterval(): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    start = REACH_ONLY_VID_SAMPLING_START_LIST.random()
    width = REACH_ONLY_VID_SAMPLING_WIDTH
  }
}

/** Gets a [VidSamplingInterval] for reach-frequency. */
private fun getReachAndFrequencyVidSamplingInterval(): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    start = REACH_FREQUENCY_VID_SAMPLING_START_LIST.random()
    width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
}

/** Gets a [VidSamplingInterval] for impression count. */
private fun getImpressionVidSamplingInterval(): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    start = IMPRESSION_VID_SAMPLING_START_LIST.random()
    width = IMPRESSION_VID_SAMPLING_WIDTH
  }
}

/** Gets a [VidSamplingInterval] for watch duration. */
private fun getDurationVidSamplingInterval(): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    start = WATCH_DURATION_VID_SAMPLING_START_LIST.random()
    width = WATCH_DURATION_VID_SAMPLING_WIDTH
  }
}

/** Gets a [MeasurementSpec.ReachAndFrequency] for reach-only. */
private fun getMeasurementSpecReachOnly(): MeasurementSpec.ReachAndFrequency {
  return measurementSpecReachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_ONLY_REACH_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DETLA
    }
    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_ONLY_FREQUENCY_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DETLA
    }
    maximumFrequencyPerUser = REACH_ONLY_MAXIMUM_FREQUENCY_PER_USER
  }
}

/** Gets a [MeasurementSpec.ReachAndFrequency] for reach-frequency. */
private fun getMeasurementSpecReachAndFrequency(
  maximumFrequencyPerUser: Int
): MeasurementSpec.ReachAndFrequency {
  return measurementSpecReachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_FREQUENCY_REACH_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DETLA
    }
    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DETLA
    }
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
}

/** Gets a [MeasurementSpec.ReachAndFrequency] for impression count. */
private fun getMeasurementSpecImpression(maximumFrequencyPerUser: Int): MeasurementSpec.Impression {
  return measurementSpecImpression {
    privacyParams = differentialPrivacyParams {
      epsilon = IMPRESSION_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DETLA
    }
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
}

/** Gets a [MeasurementSpec.ReachAndFrequency] for watch duration. */
private fun getMeasurementSpecDuration(
  maximumWatchDurationPerUser: Int,
  maximumFrequencyPerUser: Int
): MeasurementSpec.Duration {
  return measurementSpecDuration {
    privacyParams = differentialPrivacyParams {
      epsilon = WATCH_DURATION_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DETLA
    }
    this.maximumWatchDurationPerUser = maximumWatchDurationPerUser
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
}

/** Converts a public [SetOperation] to an [InternalSetOperation]. */
private suspend fun SetOperation.toInternal(
  serviceStubs: ServiceStubs,
  credential: Credential,
  eventGroupFilters: Map<String, String>,
): InternalSetOperation {
  val source = this

  return internalSetOperation {
    this.type = source.type.toInternal()
    this.lhs = source.lhs.toInternal(serviceStubs, credential, eventGroupFilters)
    this.rhs = source.rhs.toInternal(serviceStubs, credential, eventGroupFilters)
  }
}

/** Converts an [Operand] to an [InternalOperand]. */
private suspend fun Operand.toInternal(
  serviceStubs: ServiceStubs,
  credential: Credential,
  eventGroupFilters: Map<String, String>,
): InternalOperand {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.operandCase) {
    Operand.OperandCase.OPERATION ->
      internalOperand {
        operation = source.operation.toInternal(serviceStubs, credential, eventGroupFilters)
      }
    Operand.OperandCase.REPORTING_SET -> {
      val reportingSetId =
        checkReportingSet(
          source.reportingSet,
          credential.measurementConsumerReferenceId,
          serviceStubs.internalReportingSetsStub,
          eventGroupFilters
        )

      internalOperand {
        this.reportingSetId = reportingSetKey {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          externalReportingSetId = apiIdToExternalId(reportingSetId)
        }
      }
    }
    Operand.OperandCase.OPERAND_NOT_SET -> internalOperand {}
  }
}

/**
 * Check if the event groups in the public [ReportingSet] are covered by the event group universe.
 */
private suspend fun checkReportingSet(
  reportingSet: String,
  measurementConsumerReferenceId: String,
  internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  eventGroupFilters: Map<String, String>,
): String {
  val reportingSetKey =
    grpcRequireNotNull(ReportingSetKey.fromName(reportingSet)) {
      "Invalid reporting set name ${reportingSet}."
    }

  grpcRequire(reportingSetKey.measurementConsumerId == measurementConsumerReferenceId) {
    "No access to the reporting set [${reportingSet}]."
  }

  val internalReportingSet = coroutineScope {
    async {
      internalReportingSetsStub.getReportingSet(
        getReportingSetRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
        }
      )
    }
  }

  for (eventGroupKey in internalReportingSet.await().eventGroupKeysList) {
    val eventGroupName =
      EventGroupKey(
          eventGroupKey.measurementConsumerReferenceId,
          eventGroupKey.dataProviderReferenceId,
          eventGroupKey.eventGroupReferenceId
        )
        .toName()

    grpcRequire(eventGroupFilters.containsKey(eventGroupName)) {
      "The event group [$eventGroupName] in the reporting Set [${reportingSetKey.toName()}] is " +
        "not included in the event group universe."
    }
  }

  return reportingSetKey.reportingSetId
}

/** Converts a public [SetOperation.Type] to an [InternalSetOperation.Type]. */
private fun SetOperation.Type.toInternal(): InternalSetOperation.Type {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source) {
    SetOperation.Type.UNION -> InternalSetOperation.Type.UNION
    SetOperation.Type.INTERSECTION -> InternalSetOperation.Type.INTERSECTION
    SetOperation.Type.DIFFERENCE -> InternalSetOperation.Type.DIFFERENCE
    SetOperation.Type.TYPE_UNSPECIFIED -> error("Set operator type is not specified.")
    SetOperation.Type.UNRECOGNIZED -> error("Unrecognized Set operator type.")
  }
}

/** Converts a [WatchDurationParams] to an [InternalWatchDurationParams]. */
private fun WatchDurationParams.toInternal(): InternalWatchDurationParams {
  val source = this
  return internalWatchDurationParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
    maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
  }
}

/** Converts a [ImpressionCountParams] to an [InternalImpressionCountParams]. */
private fun ImpressionCountParams.toInternal(): InternalImpressionCountParams {
  val source = this
  return internalImpressionCountParams { maximumFrequencyPerUser = source.maximumFrequencyPerUser }
}

/** Converts a [FrequencyHistogramParams] to an [InternalFrequencyHistogramParams]. */
private fun FrequencyHistogramParams.toInternal(): InternalFrequencyHistogramParams {
  val source = this
  return internalFrequencyHistogramParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
  }
}

/** Converts a public [PeriodicTimeInterval] to an [InternalPeriodicTimeInterval]. */
private fun PeriodicTimeInterval.toInternal(): InternalPeriodicTimeInterval {
  val source = this
  return internalPeriodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}

/** Converts a public [TimeIntervals] to an [InternalTimeIntervals]. */
private fun TimeIntervals.toInternal(): InternalTimeIntervals {
  val source = this
  return internalTimeIntervals {
    for (timeInternal in source.timeIntervalsList) {
      this.timeIntervals += internalTimeInterval {
        startTime = timeInternal.startTime
        endTime = timeInternal.endTime
      }
    }
  }
}

/** Convert an [InternalPeriodicTimeInterval] to a list of [InternalTimeInterval]s. */
private fun InternalPeriodicTimeInterval.toInternalTimeIntervalsList(): List<InternalTimeInterval> {
  val source = this
  var startTime = checkNotNull(source.startTime)
  return (0 until source.intervalCount).map {
    internalTimeInterval {
      this.startTime = startTime
      this.endTime = Timestamps.add(startTime, source.increment)
      startTime = this.endTime
    }
  }
}

/** Generate row headers of [InternalReport] from an [InternalReport]. */
private fun getRowHeaders(report: InternalReport): List<String> {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (report.timeCase) {
    InternalReport.TimeCase.TIME_INTERVALS -> {
      report.timeIntervals.timeIntervalsList.map(InternalTimeInterval::toRowHeader)
    }
    InternalReport.TimeCase.PERIODIC_TIME_INTERVAL -> {
      report.periodicTimeInterval
        .toInternalTimeIntervalsList()
        .map(InternalTimeInterval::toRowHeader)
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

private operator fun Duration.plus(other: Duration): Duration {
  return Durations.add(this, other)
}

/** Converts a CMM [Measurement.Failure] to an [InternalMeasurement.Failure]. */
private fun Measurement.Failure.toInternal(): InternalMeasurement.Failure {
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
  if (internalResultsList.isEmpty()) {
    error("No measurement result.")
  }

  var reachValue = 0L
  var impressionValue = 0L
  val frequencyDistribution = mutableMapOf<Long, Double>()
  var watchDurationValue = duration {
    seconds = 0
    nanos = 0
  }

  // Aggregation
  for (result in internalResultsList) {
    if (result.hasFrequency()) {
      if (!result.hasReach()) {
        error("Missing reach measurement in the Reach-Frequency measurement.")
      }
      for ((frequency, percentage) in result.frequency.relativeFrequencyDistributionMap) {
        val previousTotalReachCount =
          frequencyDistribution.getOrDefault(frequency, 0.0) * reachValue
        val currentReachCount = percentage * result.reach.value
        frequencyDistribution[frequency] =
          (previousTotalReachCount + currentReachCount) / (reachValue + result.reach.value)
      }
    }
    if (result.hasReach()) {
      reachValue += result.reach.value
    }
    if (result.hasImpression()) {
      impressionValue += result.impression.value
    }
    if (result.hasWatchDuration()) {
      watchDurationValue += result.watchDuration.value
    }
  }

  return internalMeasurementResult {
    if (internalResultsList.first().hasReach()) {
      this.reach = internalReach { value = reachValue }
    }
    if (internalResultsList.first().hasFrequency()) {
      this.frequency = internalFrequency {
        relativeFrequencyDistribution.putAll(frequencyDistribution)
      }
    }
    if (internalResultsList.first().hasImpression()) {
      this.impression = internalImpression { value = impressionValue }
    }
    if (internalResultsList.first().hasWatchDuration()) {
      this.watchDuration = internalWatchDuration { value = watchDurationValue }
    }
  }
}

/** Converts a CMM [Measurement.Result] to an [InternalMeasurementResult]. */
private fun Measurement.Result.toInternal(): InternalMeasurementResult {
  val source = this

  return internalMeasurementResult {
    if (source.hasReach()) {
      this.reach = internalReach { value = source.reach.value }
    }
    if (source.hasFrequency()) {
      this.frequency = internalFrequency {
        relativeFrequencyDistribution.putAll(source.frequency.relativeFrequencyDistributionMap)
      }
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
  val measurementConsumerResourceName =
    MeasurementConsumerKey(source.measurementConsumerReferenceId).toName()
  val reportResourceName =
    ReportKey(
        measurementConsumerId = source.measurementConsumerReferenceId,
        reportId = externalIdToApiId(source.externalReportId)
      )
      .toName()
  val eventGroupEntries =
    source.details.eventGroupFiltersMap.toList().map { (eventGroupResourceName, filterPredicate) ->
      eventGroupUniverseEntry {
        key = eventGroupResourceName
        value = filterPredicate
      }
    }

  return report {
    name = reportResourceName
    reportIdempotencyKey = source.reportIdempotencyKey
    measurementConsumer = measurementConsumerResourceName
    eventGroupUniverse = eventGroupUniverse { this.eventGroupEntries += eventGroupEntries }

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

    // TODO(@riemanli) Add conversion of the result.
  }
}

/** Converts an [InternalReport.State] to a public [Report.State]. */
private fun InternalReport.State.toState(): Report.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
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
      InternalMetricTypeCase.REACH -> reach = reachParams {}
      InternalMetricTypeCase.FREQUENCY_HISTOGRAM ->
        frequencyHistogram = source.details.frequencyHistogram.toFrequencyHistogram()
      InternalMetricTypeCase.IMPRESSION_COUNT ->
        impressionCount = source.details.impressionCount.toImpressionCount()
      InternalMetricTypeCase.WATCH_DURATION ->
        watchDuration = source.details.watchDuration.toWatchDuration()
      InternalMetricTypeCase.METRICTYPE_NOT_SET ->
        error("The metric type in the internal report should've be set.")
    }

    cumulative = source.details.cumulative

    for (internalSetOperation in source.namedSetOperationsList) {
      setOperations += internalSetOperation.toNamedSetOperation()
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
    this.lhs = source.lhs.toOperand()
    this.rhs = source.rhs.toOperand()
  }
}

/** Converts an internal [InternalOperand] to a public [Operand]. */
private fun InternalOperand.toOperand(): Operand {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.operandCase) {
    InternalOperand.OperandCase.OPERATION ->
      operand { operation = source.operation.toSetOperation() }
    InternalOperand.OperandCase.REPORTINGSETID ->
      operand {
        reportingSet =
          ReportingSetKey(
              source.reportingSetId.measurementConsumerReferenceId,
              externalIdToApiId(source.reportingSetId.externalReportingSetId)
            )
            .toName()
      }
    InternalOperand.OperandCase.OPERAND_NOT_SET -> operand {}
  }
}

/** Converts an internal [InternalSetOperation.Type] to a public [SetOperation.Type]. */
private fun InternalSetOperation.Type.toType(): SetOperation.Type {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
    InternalSetOperation.Type.UNION -> SetOperation.Type.UNION
    InternalSetOperation.Type.INTERSECTION -> SetOperation.Type.INTERSECTION
    InternalSetOperation.Type.DIFFERENCE -> SetOperation.Type.DIFFERENCE
    InternalSetOperation.Type.TYPE_UNSPECIFIED -> error("Set operator type should've be set.")
    InternalSetOperation.Type.UNRECOGNIZED -> error("Unrecognized Set operator type.")
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

/** Converts an internal [ListReportsPageToken] to an internal [StreamInternalReportsRequest]. */
private fun ListReportsPageToken.toStreamReportsRequest(): StreamInternalReportsRequest {
  val source = this
  return streamInternalReportsRequest {
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

  val isValidPageSize =
    source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE

  return if (pageToken.isNotBlank()) {
    ListReportsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
      grpcRequire(this.measurementConsumerReferenceId == measurementConsumerReferenceId) {
        "Arguments must be kept the same when using a page token"
      }

      if (isValidPageSize) {
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
