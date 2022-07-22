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
import io.grpc.StatusException
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
import org.wfanet.measurement.api.v2alpha.MeasurementKey
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
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
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
import org.wfanet.measurement.reporting.v1alpha.GetReportRequest
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
import org.wfanet.measurement.reporting.v1alpha.Report.Result
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry as eventGroupUniverseEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.HistogramTableKt.row
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.column
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.histogramTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.scalarTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportKt.result
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

private const val NUMBER_VID_BUCKETS = 300
private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
private const val NUMBER_REACH_ONLY_BUCKETS = 16
private val REACH_ONLY_VID_SAMPLING_START_LIST =
  (0 until NUMBER_REACH_ONLY_BUCKETS).map { it * REACH_ONLY_VID_SAMPLING_WIDTH }
private const val REACH_ONLY_REACH_EPSILON = 0.0041
private const val REACH_ONLY_FREQUENCY_EPSILON = 0.0001
private const val REACH_ONLY_MAXIMUM_FREQUENCY_PER_USER = 1

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val NUMBER_REACH_FREQUENCY_BUCKETS = 19
private val REACH_FREQUENCY_VID_SAMPLING_START_LIST =
  (0 until NUMBER_REACH_FREQUENCY_BUCKETS).map {
    REACH_ONLY_VID_SAMPLING_START_LIST.last() +
      REACH_ONLY_VID_SAMPLING_WIDTH +
      it * REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val NUMBER_IMPRESSION_BUCKETS = 1
private val IMPRESSION_VID_SAMPLING_START_LIST =
  (0 until NUMBER_IMPRESSION_BUCKETS).map {
    REACH_FREQUENCY_VID_SAMPLING_START_LIST.last() +
      REACH_FREQUENCY_VID_SAMPLING_WIDTH +
      it * IMPRESSION_VID_SAMPLING_WIDTH
  }
private const val IMPRESSION_EPSILON = 0.0011

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val NUMBER_WATCH_DURATION_BUCKETS = 1
private val WATCH_DURATION_VID_SAMPLING_START_LIST =
  (0 until NUMBER_WATCH_DURATION_BUCKETS).map {
    IMPRESSION_VID_SAMPLING_START_LIST.last() +
      IMPRESSION_VID_SAMPLING_WIDTH +
      it * WATCH_DURATION_VID_SAMPLING_WIDTH
  }
private const val WATCH_DURATION_EPSILON = 0.001

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

private val REACH_ONLY_MEASUREMENT_SPEC = measurementSpecReachAndFrequency {
  reachPrivacyParams = differentialPrivacyParams {
    epsilon = REACH_ONLY_REACH_EPSILON
    delta = DIFFERENTIAL_PRIVACY_DELTA
  }
  frequencyPrivacyParams = differentialPrivacyParams {
    epsilon = REACH_ONLY_FREQUENCY_EPSILON
    delta = DIFFERENTIAL_PRIVACY_DELTA
  }
  maximumFrequencyPerUser = REACH_ONLY_MAXIMUM_FREQUENCY_PER_USER
}

class ReportsService(
  private val internalReportsStub: InternalReportsCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificateStub: CertificatesCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val signingPrivateKey: PrivateKey,
  private val apiAuthenticationKey: String,
  private val secureRandom: SecureRandom,
) : ReportsCoroutineImplBase() {
  private val setOperationCompiler = SetOperationCompiler()

  override suspend fun createReport(request: CreateReportRequest): Report {
    val principal = principalFromCurrentContext
    val resourceKey = principal.resourceKey

    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }

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

    // TODO(@riemanli) Put the check here as the reportIdempotencyKey will be moved to the request
    //  level in the future.
    grpcRequire(request.report.reportIdempotencyKey.isNotEmpty()) {
      "ReportIdempotencyKey is not specified."
    }

    return internalReportsStub
      .createReport(
        buildInternalCreateReportRequest(
          request,
          resourceKey.measurementConsumerId,
          request.report.reportIdempotencyKey,
        )
      )
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
      internalReportsStub.streamReports(listReportsPageToken.toStreamReportsRequest()).toList()

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
          .map { syncReport(it) }
          .map(InternalReport::toReport)

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val reportKey =
      grpcRequireNotNull(ReportKey.fromName(request.name)) {
        "Report name is either unspecified or invalid"
      }

    val principal = principalFromCurrentContext

    when (val resourceKey = principal.resourceKey) {
      is MeasurementConsumerKey -> {
        if (reportKey.measurementConsumerId != resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get Report belonging to other MeasurementConsumers."
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to get Report." }
      }
    }

    val internalReport =
      internalReportsStub.getReport(
        getInternalReportRequest {
          measurementConsumerReferenceId = reportKey.measurementConsumerId
          externalReportId = apiIdToExternalId(reportKey.reportId)
        }
      )

    val syncedInternalReport = syncReport(internalReport)

    return syncedInternalReport.toReport()
  }

  /** Syncs the [InternalReport] and all [InternalMeasurement]s used by it. */
  private suspend fun syncReport(internalReport: InternalReport): InternalReport {
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

    return internalReportsStub.getReport(
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
    val measurementResourceName =
      MeasurementKey(measurementConsumerReferenceId, measurementReferenceId).toName()
    val measurement =
      measurementsStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getMeasurement(getMeasurementRequest { name = measurementResourceName })

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (measurement.state) {
      Measurement.State.SUCCEEDED -> {
        // Converts a Measurement to an InternalMeasurement and store it into the database with
        // SUCCEEDED state
        val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
        val encryptionPrivateKeyHandle =
          encryptionKeyPairStore.getPrivateKeyHandle(measurementSpec.measurementPublicKey)
            ?: failGrpc(Status.PERMISSION_DENIED) { "Encryption private key not found" }

        val setInternalMeasurementResultRequest =
          buildSetInternalMeasurementResultRequest(
            measurementConsumerReferenceId,
            measurementReferenceId,
            measurement.resultsList,
            encryptionPrivateKeyHandle
          )
        internalMeasurementsStub.setMeasurementResult(setInternalMeasurementResultRequest)
      }
      Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
      Measurement.State.COMPUTING -> {} // No action needed
      Measurement.State.FAILED,
      Measurement.State.CANCELLED -> {
        internalMeasurementsStub.setMeasurementFailure(
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

  /** Builds a [SetInternalMeasurementResultRequest]. */
  private suspend fun buildSetInternalMeasurementResultRequest(
    measurementConsumerReferenceId: String,
    measurementReferenceId: String,
    resultsList: List<Measurement.ResultPair>,
    privateKeyHandle: PrivateKeyHandle
  ): SetInternalMeasurementResultRequest {

    return setInternalMeasurementResultRequest {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      this.measurementReferenceId = measurementReferenceId
      result =
        aggregateResults(
          resultsList
            .map { decryptMeasurementResultPair(it, privateKeyHandle) }
            .map(Measurement.Result::toInternal)
        )
    }
  }

  /** Decrypts a [Measurement.ResultPair] to [Measurement.Result] */
  private suspend fun decryptMeasurementResultPair(
    measurementResultPair: Measurement.ResultPair,
    encryptionPrivateKeyHandle: PrivateKeyHandle
  ): Measurement.Result {
    val certificate =
      certificateStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = measurementResultPair.certificate })

    val signedResult =
      decryptResult(measurementResultPair.encryptedResult, encryptionPrivateKeyHandle)

    val result = Measurement.Result.parseFrom(signedResult.data)

    if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
      error("Signature of the result is invalid.")
    }
    return result
  }

  /** Builds an [InternalCreateReportRequest] from a public [CreateReportRequest]. */
  private suspend fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String,
  ): InternalCreateReportRequest {
    val internalReport: InternalReport =
      try {
        internalReportsStub.getReportByIdempotencyKey(
          getReportByIdempotencyKeyRequest {
            this.measurementConsumerReferenceId = measurementConsumerReferenceId
            this.reportIdempotencyKey = reportIdempotencyKey
          }
        )
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.NOT_FOUND) {
          throw Exception(
            "Unable to retrieve a report from the reporting database using the provided " +
              "reportIdempotencyKey [${reportIdempotencyKey}]."
          )
        }

        grpcRequire(request.report.measurementConsumer == request.parent) {
          "Cannot create a Report for another MeasurementConsumer."
        }
        grpcRequire(request.report.hasEventGroupUniverse()) {
          "EventGroupUniverse is not specified."
        }
        grpcRequire(request.report.metricsList.isNotEmpty()) {
          "Metrics in Report cannot be empty."
        }
        checkSetOperationNamesUniqueness(request.report.metricsList)

        val eventGroupFilters =
          request.report.eventGroupUniverse.eventGroupEntriesList.associate { it.key to it.value }

        internalReport {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId

          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
          val internalTimeIntervalsList: List<InternalTimeInterval> =
            when (request.report.timeCase) {
              Report.TimeCase.TIME_INTERVALS -> {
                this.timeIntervals = request.report.timeIntervals.toInternal()
                this.timeIntervals.timeIntervalsList.map { it }
              }
              Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
                this.periodicTimeInterval = request.report.periodicTimeInterval.toInternal()
                this.periodicTimeInterval.toInternalTimeIntervalsList()
              }
              Report.TimeCase.TIME_NOT_SET ->
                failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
            }

          coroutineScope {
            for (metric in request.report.metricsList) {
              launch {
                this@internalReport.metrics +=
                  buildInternalMetric(
                    metric,
                    measurementConsumerReferenceId,
                    reportIdempotencyKey,
                    internalTimeIntervalsList,
                    eventGroupFilters,
                  )
              }
            }
          }
          details = internalReportDetails { this.eventGroupFilters.putAll(eventGroupFilters) }
          this.reportIdempotencyKey = reportIdempotencyKey
        }
      }

    return internalCreateReportRequest {
      report = internalReport
      measurements +=
        internalReport.metricsList.flatMap { internalMetric ->
          buildInternalMeasurementKeys(internalMetric, measurementConsumerReferenceId)
        }
    }
  }

  /** Builds an [InternalMetric] from a public [Metric]. */
  private suspend fun buildInternalMetric(
    metric: Metric,
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String,
    internalTimeIntervalsList: List<InternalTimeInterval>,
    eventGroupFilters: Map<String, String>,
  ): InternalMetric {
    return internalMetric {
      details = internalMetricDetails {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metric.metricTypeCase) {
          MetricTypeCase.REACH -> reach = internalReachParams {}
          MetricTypeCase.FREQUENCY_HISTOGRAM ->
            frequencyHistogram = metric.frequencyHistogram.toInternal()
          MetricTypeCase.IMPRESSION_COUNT -> impressionCount = metric.impressionCount.toInternal()
          MetricTypeCase.WATCH_DURATION -> watchDuration = metric.watchDuration.toInternal()
          MetricTypeCase.METRICTYPE_NOT_SET ->
            failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Report is not specified." }
        }

        cumulative = metric.cumulative
      }

      val firstStartTime = internalTimeIntervalsList.first().startTime
      val internalCumulativeTimeIntervalsList =
        internalTimeIntervalsList.map { internalTimeInterval ->
          internalTimeInterval {
            this.startTime = firstStartTime
            endTime = internalTimeInterval.endTime
          }
        }

      coroutineScope {
        metric.setOperationsList.map { setOperation ->
          launch {
            val timeIntervals =
              if (metric.cumulative) internalCumulativeTimeIntervalsList
              else internalTimeIntervalsList
            val internalNamedSetOperation =
              buildInternalNamedSetOperation(
                setOperation,
                measurementConsumerReferenceId,
                reportIdempotencyKey,
                timeIntervals,
                eventGroupFilters,
                this@internalMetric.details,
              )
            namedSetOperations += internalNamedSetOperation
          }
        }
      }
    }
  }

  /** Builds an [InternalNamedSetOperation] from a public [NamedSetOperation]. */
  private suspend fun buildInternalNamedSetOperation(
    namedSetOperation: NamedSetOperation,
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String,
    internalTimeIntervalsList: List<InternalTimeInterval>,
    eventGroupFilters: Map<String, String>,
    internalMetricDetails: InternalMetricDetails,
  ): InternalNamedSetOperation {
    return internalNamedSetOperation {
      displayName = namedSetOperation.uniqueName
      setOperation =
        buildInternalSetOperation(
          namedSetOperation.setOperation,
          measurementConsumerReferenceId,
          eventGroupFilters
        )

      val weightedMeasurementsList = setOperationCompiler.compileSetOperation(namedSetOperation)

      this.measurementCalculations +=
        buildMeasurementCalculationList(
          weightedMeasurementsList,
          measurementConsumerReferenceId,
          reportIdempotencyKey,
          internalTimeIntervalsList,
          eventGroupFilters,
          internalMetricDetails,
          namedSetOperation.uniqueName,
        )
    }
  }

  /** Builds an [InternalSetOperation] from a public [SetOperation]. */
  private suspend fun buildInternalSetOperation(
    setOperation: SetOperation,
    measurementConsumerReferenceId: String,
    eventGroupFilters: Map<String, String>,
  ): InternalSetOperation {
    return internalSetOperation {
      this.type = setOperation.type.toInternal()
      this.lhs =
        buildInternalOperand(setOperation.lhs, measurementConsumerReferenceId, eventGroupFilters)
      this.rhs =
        buildInternalOperand(setOperation.rhs, measurementConsumerReferenceId, eventGroupFilters)
    }
  }

  /** Builds an [InternalOperand] from an [Operand]. */
  private suspend fun buildInternalOperand(
    operand: Operand,
    measurementConsumerReferenceId: String,
    eventGroupFilters: Map<String, String>,
  ): InternalOperand {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (operand.operandCase) {
      Operand.OperandCase.OPERATION ->
        internalOperand {
          operation =
            buildInternalSetOperation(
              operand.operation,
              measurementConsumerReferenceId,
              eventGroupFilters
            )
        }
      Operand.OperandCase.REPORTING_SET -> {
        val reportingSetId =
          checkReportingSet(
            operand.reportingSet,
            measurementConsumerReferenceId,
            internalReportingSetsStub,
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
   * Builds a list of [MeasurementCalculation]s from a list of public [WeightedMeasurement]s and a
   * list of [InternalTimeInterval]s.
   */
  private suspend fun buildMeasurementCalculationList(
    weightedMeasurementsList: List<WeightedMeasurement>,
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String,
    internalReportTimeIntervalsList: List<InternalTimeInterval>,
    eventGroupFilters: Map<String, String>,
    internalMetricDetails: InternalMetricDetails,
    setOperationUniqueName: String,
  ): List<MeasurementCalculation> {
    return internalReportTimeIntervalsList.map { timeInterval ->
      internalMeasurementCalculation {
        this.timeInterval = timeInterval
        weightedMeasurements +=
          weightedMeasurementsList.mapIndexed { index, weightedMeasurement ->
            val measurementReferenceId =
              buildMeasurementReferenceId(
                reportIdempotencyKey,
                timeInterval,
                internalMetricDetails,
                setOperationUniqueName,
                index,
              )

            buildInternalWeightedMeasurement(
              weightedMeasurement,
              measurementConsumerReferenceId,
              timeInterval,
              eventGroupFilters,
              internalMetricDetails,
              measurementReferenceId
            )
          }
      }
    }
  }

  /** Builds an [InternalWeightedMeasurement] from a public [WeightedMeasurement]. */
  private suspend fun buildInternalWeightedMeasurement(
    weightedMeasurement: WeightedMeasurement,
    measurementConsumerReferenceId: String,
    internalTimeInterval: InternalTimeInterval,
    eventGroupFilters: Map<String, String>,
    internalMetricDetails: InternalMetricDetails,
    measurementReferenceId: String,
  ): InternalWeightedMeasurement {
    try {
      internalMeasurementsStub.getMeasurement(
        getInternalMeasurementRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.measurementReferenceId = measurementReferenceId
        }
      )
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        throw Exception(
          "Unable to retrieve the measurement [$measurementReferenceId] from the reporting database."
        )
      }

      val createMeasurementRequest: CreateMeasurementRequest =
        buildCreateMeasurementRequest(
          measurementConsumerReferenceId,
          internalTimeInterval,
          eventGroupFilters,
          internalMetricDetails,
          measurementReferenceId,
          weightedMeasurement.reportingSets
        )

      measurementsStub
        .withAuthenticationKey(apiAuthenticationKey)
        .createMeasurement(createMeasurementRequest)

      internalMeasurementsStub.createMeasurement(
        internalMeasurement {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.measurementReferenceId = measurementReferenceId
          state = InternalMeasurement.State.PENDING
        }
      )
    }

    return internalWeightedMeasurement {
      this.measurementReferenceId = measurementReferenceId
      coefficient = weightedMeasurement.coefficient
    }
  }

  /** Builds a [CreateMeasurementRequest]. */
  private suspend fun buildCreateMeasurementRequest(
    measurementConsumerReferenceId: String,
    internalTimeInterval: InternalTimeInterval,
    eventGroupFilters: Map<String, String>,
    internalMetricDetails: InternalMetricDetails,
    measurementReferenceId: String,
    reportingSetNames: List<String>,
  ): CreateMeasurementRequest {
    val measurementConsumer =
      measurementConsumersStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getMeasurementConsumer(
          getMeasurementConsumerRequest {
            name = MeasurementConsumerKey(measurementConsumerReferenceId).toName()
          }
        )
    val measurementConsumerCertificate = readCertificate(measurementConsumer.certificateDer)
    val measurementConsumerSigningKey =
      SigningKeyHandle(measurementConsumerCertificate, signingPrivateKey)
    val measurementEncryptionPublicKey = measurementConsumer.publicKey.data

    val measurementResourceName =
      MeasurementKey(measurementConsumerReferenceId, measurementReferenceId).toName()

    val dataProviderNameToInternalEventGroupEntriesList =
      aggregateInternalEventGroupEntryByDataProviderName(
        reportingSetNames,
        internalTimeInterval.toMeasurementTimeInterval(),
        eventGroupFilters
      )

    val measurement = measurement {
      name = measurementResourceName
      this.measurementConsumerCertificate = measurementConsumer.certificate

      dataProviders +=
        buildDataProviderEntries(
          dataProviderNameToInternalEventGroupEntriesList,
          measurementEncryptionPublicKey,
          measurementConsumerSigningKey,
        )

      val unsignedMeasurementSpec: MeasurementSpec =
        buildUnsignedMeasurementSpec(
          measurementEncryptionPublicKey,
          dataProviders.map { it.value.nonceHash },
          internalMetricDetails,
          secureRandom,
        )

      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)

      this.measurementReferenceId = measurementReferenceId
    }

    return createMeasurementRequest { this.measurement = measurement }
  }

  /** Builds a map of data provider resource name to a list of [EventGroupEntry]s. */
  private suspend fun aggregateInternalEventGroupEntryByDataProviderName(
    reportingSetNames: List<String>,
    timeInterval: MeasurementTimeInterval,
    eventGroupFilters: Map<String, String>,
  ): Map<String, List<EventGroupEntry>> {
    val internalReportingSetsList = mutableListOf<InternalReportingSet>()

    coroutineScope {
      for (reportingSetName in reportingSetNames) {
        val reportingSetKey =
          grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
            "Invalid reporting set name $reportingSetName."
          }

        launch {
          internalReportingSetsList +=
            internalReportingSetsStub.getReportingSet(
              getReportingSetRequest {
                measurementConsumerReferenceId = reportingSetKey.measurementConsumerId
                externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
              }
            )
        }
      }
    }

    val dataProviderNameToInternalEventGroupEntriesList =
      mutableMapOf<String, MutableList<EventGroupEntry>>()

    for (internalReportingSet in internalReportingSetsList) {
      for (eventGroupKey in internalReportingSet.eventGroupKeysList) {
        val dataProviderName = DataProviderKey(eventGroupKey.dataProviderReferenceId).toName()
        val eventGroupName =
          EventGroupKey(
              eventGroupKey.measurementConsumerReferenceId,
              eventGroupKey.dataProviderReferenceId,
              eventGroupKey.eventGroupReferenceId
            )
            .toName()

        dataProviderNameToInternalEventGroupEntriesList.getOrPut(
          dataProviderName,
          ::mutableListOf
        ) +=
          eventGroupEntry {
            key = eventGroupName
            value = eventGroupEntryValue {
              collectionInterval = timeInterval

              val filter =
                combineEventGroupFilters(
                  internalReportingSet.filter,
                  eventGroupFilters[eventGroupName]
                )
              if (filter != null) {
                this.filter = requisitionSpecEventFilter { expression = filter }
              }
            }
          }
      }
    }

    return dataProviderNameToInternalEventGroupEntriesList.mapValues { it.value.toList() }.toMap()
  }

  /** Builds a list of [DataProviderEntry]s from lists of [EventGroupEntry]s. */
  private suspend fun buildDataProviderEntries(
    dataProviderNameToInternalEventGroupEntriesList: Map<String, List<EventGroupEntry>>,
    measurementEncryptionPublicKey: ByteString,
    measurementConsumerSigningKey: SigningKeyHandle,
  ): List<DataProviderEntry> {
    return dataProviderNameToInternalEventGroupEntriesList.map {
      (dataProviderName, eventGroupEntriesList) ->
      dataProviderEntry {
        val requisitionSpec = requisitionSpec {
          eventGroups += eventGroupEntriesList
          this.measurementPublicKey = measurementEncryptionPublicKey
          nonce = secureRandom.nextLong()
        }

        val dataProvider =
          dataProvidersStub
            .withAuthenticationKey(apiAuthenticationKey)
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
}

/** Check if the names of the set operations within the same metric type are unique. */
private fun checkSetOperationNamesUniqueness(metricsList: List<Metric>) {
  val seenNames = mutableMapOf<MetricTypeCase, MutableSet<String>>().withDefault { mutableSetOf() }

  for (metric in metricsList) {
    for (setOperation in metric.setOperationsList) {
      grpcRequire(!seenNames.getValue(metric.metricTypeCase).contains(setOperation.uniqueName)) {
        "The names of the set operations within the same metric type should be unique."
      }
      seenNames.getOrPut(metric.metricTypeCase, ::mutableSetOf) += setOperation.uniqueName
    }
  }
}

/** Builds a list of [InternalMeasurementKey]s from an [InternalMetric]. */
private fun buildInternalMeasurementKeys(
  internalMetric: InternalMetric,
  measurementConsumerReferenceId: String
): List<InternalMeasurementKey> {
  return internalMetric.namedSetOperationsList
    .flatMap { namedSetOperation ->
      namedSetOperation.measurementCalculationsList.flatMap { measurementCalculation ->
        measurementCalculation.weightedMeasurementsList.map { it.measurementReferenceId }
      }
    }
    .map { measurementReferenceId ->
      internalMeasurementKey {
        this.measurementConsumerReferenceId = measurementConsumerReferenceId
        this.measurementReferenceId = measurementReferenceId
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

/** Builds a unique reference ID for a [Measurement]. */
private fun buildMeasurementReferenceId(
  reportIdempotencyKey: String,
  internalTimeInterval: InternalTimeInterval,
  internalMetricDetails: InternalMetricDetails,
  setOperationUniqueName: String,
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

  return "$reportIdempotencyKey-$rowHeader-$metricType-$setOperationUniqueName-measurement-$index"
}

/** Combines two event group filters. */
private fun combineEventGroupFilters(filter1: String?, filter2: String?): String? {
  if (filter1 == null) return filter2

  return if (filter2 == null) filter1
  else {
    "($filter1) AND ($filter2)"
  }
}

/** Builds the unsigned [MeasurementSpec]. */
private fun buildUnsignedMeasurementSpec(
  measurementEncryptionPublicKey: ByteString,
  nonceHashes: List<ByteString>,
  internalMetricDetails: InternalMetricDetails,
  secureRandom: SecureRandom,
): MeasurementSpec {
  return measurementSpec {
    measurementPublicKey = measurementEncryptionPublicKey
    this.nonceHashes += nonceHashes

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (internalMetricDetails.metricTypeCase) {
      InternalMetricTypeCase.REACH -> {
        reachAndFrequency = REACH_ONLY_MEASUREMENT_SPEC
        vidSamplingInterval = buildReachOnlyVidSamplingInterval(secureRandom)
      }
      InternalMetricTypeCase.FREQUENCY_HISTOGRAM -> {
        reachAndFrequency =
          buildReachAndFrequencyMeasurementSpec(
            internalMetricDetails.frequencyHistogram.maximumFrequencyPerUser
          )
        vidSamplingInterval = buildReachAndFrequencyVidSamplingInterval(secureRandom)
      }
      InternalMetricTypeCase.IMPRESSION_COUNT -> {
        impression =
          buildImpressionMeasurementSpec(
            internalMetricDetails.impressionCount.maximumFrequencyPerUser
          )
        vidSamplingInterval = buildImpressionVidSamplingInterval(secureRandom)
      }
      InternalMetricTypeCase.WATCH_DURATION -> {
        duration =
          buildDurationMeasurementSpec(
            internalMetricDetails.watchDuration.maximumWatchDurationPerUser,
            internalMetricDetails.watchDuration.maximumFrequencyPerUser
          )
        vidSamplingInterval = buildDurationVidSamplingInterval(secureRandom)
      }
      InternalMetricTypeCase.METRICTYPE_NOT_SET ->
        error("Unset metric type should've already raised error.")
    }
  }
}

/** Builds a [VidSamplingInterval] for reach-only. */
private fun buildReachOnlyVidSamplingInterval(secureRandom: SecureRandom): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    val index = secureRandom.nextInt(NUMBER_REACH_ONLY_BUCKETS)
    start = REACH_ONLY_VID_SAMPLING_START_LIST[index]
    width = REACH_ONLY_VID_SAMPLING_WIDTH
  }
}

/** Builds a [VidSamplingInterval] for reach-frequency. */
private fun buildReachAndFrequencyVidSamplingInterval(
  secureRandom: SecureRandom
): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    val index = secureRandom.nextInt(NUMBER_REACH_FREQUENCY_BUCKETS)
    start = REACH_FREQUENCY_VID_SAMPLING_START_LIST[index]
    width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
}

/** Builds a [VidSamplingInterval] for impression count. */
private fun buildImpressionVidSamplingInterval(secureRandom: SecureRandom): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    val index = secureRandom.nextInt(NUMBER_IMPRESSION_BUCKETS)
    start = IMPRESSION_VID_SAMPLING_START_LIST[index]
    width = IMPRESSION_VID_SAMPLING_WIDTH
  }
}

/** Builds a [VidSamplingInterval] for watch duration. */
private fun buildDurationVidSamplingInterval(secureRandom: SecureRandom): VidSamplingInterval {
  return vidSamplingInterval {
    // Random draw the start point from the list
    val index = secureRandom.nextInt(NUMBER_WATCH_DURATION_BUCKETS)
    start = WATCH_DURATION_VID_SAMPLING_START_LIST[index]
    width = WATCH_DURATION_VID_SAMPLING_WIDTH
  }
}

/** Builds a [MeasurementSpec.ReachAndFrequency] for reach-frequency. */
private fun buildReachAndFrequencyMeasurementSpec(
  maximumFrequencyPerUser: Int
): MeasurementSpec.ReachAndFrequency {
  return measurementSpecReachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_FREQUENCY_REACH_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
}

/** Builds a [MeasurementSpec.ReachAndFrequency] for impression count. */
private fun buildImpressionMeasurementSpec(
  maximumFrequencyPerUser: Int
): MeasurementSpec.Impression {
  return measurementSpecImpression {
    privacyParams = differentialPrivacyParams {
      epsilon = IMPRESSION_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
}

/** Builds a [MeasurementSpec.ReachAndFrequency] for watch duration. */
private fun buildDurationMeasurementSpec(
  maximumWatchDurationPerUser: Int,
  maximumFrequencyPerUser: Int
): MeasurementSpec.Duration {
  return measurementSpecDuration {
    privacyParams = differentialPrivacyParams {
      epsilon = WATCH_DURATION_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    this.maximumWatchDurationPerUser = maximumWatchDurationPerUser
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
}

/**
 * Check if the event groups in the public [ReportingSet] are covered by the event group universe.
 */
private suspend fun checkReportingSet(
  reportingSetName: String,
  measurementConsumerReferenceId: String,
  internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  eventGroupFilters: Map<String, String>,
): String {
  val reportingSetKey =
    grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
      "Invalid reporting set name $reportingSetName."
    }

  grpcRequire(reportingSetKey.measurementConsumerId == measurementConsumerReferenceId) {
    "No access to the reporting set [$reportingSetName]."
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
    val internalReportingSetDisplayName = internalReportingSet.await().displayName
    grpcRequire(eventGroupFilters.containsKey(eventGroupName)) {
      "The event group [$eventGroupName] in the reporting set [$internalReportingSetDisplayName]" +
        " is not included in the event group universe."
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
  val reportResourceName =
    ReportKey(
        measurementConsumerId = source.measurementConsumerReferenceId,
        reportId = externalIdToApiId(source.externalReportId)
      )
      .toName()
  val measurementConsumerResourceName =
    MeasurementConsumerKey(source.measurementConsumerReferenceId).toName()
  val eventGroupEntries =
    source.details.eventGroupFiltersMap.map { (eventGroupResourceName, filterPredicate) ->
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
        error("The time in the internal report should've been set.")
    }

    for (metric in source.metricsList) {
      this.metrics += metric.toMetric()
    }

    this.state = source.state.toState()
    if (source.details.hasResult()) {
      this.result = source.details.result.toResult()
    }
  }
}

/** Converts an [InternalReport.State] to a public [Report.State]. */
private fun InternalReport.State.toState(): Report.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
    InternalReport.State.RUNNING -> Report.State.RUNNING
    InternalReport.State.SUCCEEDED -> Report.State.SUCCEEDED
    InternalReport.State.FAILED -> Report.State.FAILED
    InternalReport.State.STATE_UNSPECIFIED -> error("Report state should've been set.")
    InternalReport.State.UNRECOGNIZED -> error("Unrecognized report state.")
  }
}

/** Converts an [InternalReport.Details.Result] to a public [Report.Result]. */
private fun InternalReport.Details.Result.toResult(): Result {
  val source = this
  return result {
    scalarTable = scalarTable {
      rowHeaders += source.scalarTable.rowHeadersList
      for (sourceColumn in source.scalarTable.columnsList) {
        columns += column {
          columnHeader = sourceColumn.columnHeader
          setOperations += sourceColumn.setOperationsList
        }
      }
    }
    for (sourceHistogram in source.histogramTablesList) {
      histogramTables += histogramTable {
        for (sourceRow in sourceHistogram.rowsList) {
          rows += row {
            rowHeader = sourceRow.rowHeader
            frequency = sourceRow.frequency
          }
        }
        for (sourceColumn in sourceHistogram.columnsList) {
          columns += column {
            columnHeader = sourceColumn.columnHeader
            setOperations += sourceColumn.setOperationsList
          }
        }
      }
    }
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
        error("The metric type in the internal report should've been set.")
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
    uniqueName = source.displayName
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
    InternalSetOperation.Type.TYPE_UNSPECIFIED -> error("Set operator type should've been set.")
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
