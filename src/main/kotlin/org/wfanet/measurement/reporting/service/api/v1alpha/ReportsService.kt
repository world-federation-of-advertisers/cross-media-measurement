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
import java.io.File
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Instant
import kotlin.math.min
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2.alpha.ListReportsPageToken
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.TimeInterval as MeasurementTimeInterval
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval as measurementTimeInterval
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.CreateReportRequest.MeasurementKey as InternalMeasurementKey
import org.wfanet.measurement.internal.reporting.CreateReportRequestKt as InternalCreateReportRequestKt
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.Measurement.Result as InternalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.Metric.Details as InternalMetricDetails
import org.wfanet.measurement.internal.reporting.Metric.Details.MetricTypeCase as InternalMetricTypeCase
import org.wfanet.measurement.internal.reporting.Metric.FrequencyHistogramParams as InternalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.Metric.ImpressionCountParams as InternalImpressionCountParams
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation
import org.wfanet.measurement.internal.reporting.Metric.NamedSetOperation as InternalNamedSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation as InternalSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation.Operand as InternalOperand
import org.wfanet.measurement.internal.reporting.Metric.WatchDurationParams as InternalWatchDurationParams
import org.wfanet.measurement.internal.reporting.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.MetricKt.MeasurementCalculationKt
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt as InternalSetOperationKt
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest as SetInternalMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequest as StreamInternalReportsRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.batchGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.getReportByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.getReportRequest as getInternalReportRequest
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
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.HistogramTableKt.row
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.column
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.histogramTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.scalarTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportKt.result
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.TimeInterval
import org.wfanet.measurement.reporting.v1alpha.TimeIntervals
import org.wfanet.measurement.reporting.v1alpha.copy
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

private val REACH_ONLY_MEASUREMENT_SPEC =
  MeasurementSpecKt.reachAndFrequency {
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

private val timeIntervalComparator: (TimeInterval, TimeInterval) -> Int = { a, b ->
  val start = Timestamps.compare(a.startTime, b.startTime)
  if (start != 0) {
    start
  } else {
    Timestamps.compare(a.endTime, b.endTime)
  }
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
  private val secureRandom: SecureRandom,
  private val signingPrivateKeyDir: File,
  private val trustedCertificates: Map<ByteString, X509Certificate>
) : ReportsCoroutineImplBase() {
  private val setOperationCompiler = SetOperationCompiler()

  private data class ReportInfo(
    val measurementConsumerReferenceId: String,
    val reportIdempotencyKey: String,
    val eventGroupFilters: Map<String, String>,
  )

  private data class SigningConfig(
    val signingCertificateName: String,
    val signingCertificateDer: ByteString,
    val signingPrivateKey: PrivateKey,
  )

  private data class WeightedMeasurementInfo(
    val reportingMeasurementId: String,
    val weightedMeasurement: WeightedMeasurement,
    val timeInterval: TimeInterval,
    val reportTimeInterval: TimeInterval,
    var kingdomMeasurementId: String? = null,
  )

  private data class SetOperationResult(
    val weightedMeasurementInfoList: List<WeightedMeasurementInfo>,
    val internalMetricDetails: InternalMetricDetails,
  )

  override suspend fun createReport(request: CreateReportRequest): Report {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }
    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Report for another MeasurementConsumer."
          }
        }
      }
    }

    val resourceKey = principal.resourceKey
    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequire(request.hasReport()) { "Report is not specified." }

    // TODO(@riemanli) Put the check here as the reportIdempotencyKey will be moved to the request
    //  level in the future.
    grpcRequire(request.report.reportIdempotencyKey.isNotBlank()) {
      "ReportIdempotencyKey is not specified."
    }
    grpcRequire(request.report.measurementConsumer == request.parent) {
      "Cannot create a Report for another MeasurementConsumer."
    }

    grpcRequire(request.report.metricsList.isNotEmpty()) { "Metrics in Report cannot be empty." }
    request.report.metricsList.forEach {
      grpcRequire(it.setOperationsList.isNotEmpty()) { "Metric setOperationsList cannot be empty." }
      it.setOperationsList.forEach { namedSetOperation ->
        grpcRequire(namedSetOperation.uniqueName.isNotBlank()) {
          "NamedSetOperation uniqueName is unspecified."
        }
        grpcRequire(
          !namedSetOperation.setOperation.lhs.operandCase.equals(
            Operand.OperandCase.OPERAND_NOT_SET
          )
        ) {
          "NamedSetOperation SetOperation Operand is unspecified."
        }
        grpcRequire(
          !namedSetOperation.setOperation.type.equals(SetOperation.Type.TYPE_UNSPECIFIED)
        ) {
          "NamedSetOperation SetOperation Type is unspecified."
        }
      }
    }
    checkSetOperationNamesUniqueness(request.report.metricsList)

    val existingInternalReport: InternalReport? =
      getInternalReport(resourceKey.measurementConsumerId, request.report.reportIdempotencyKey)

    if (existingInternalReport != null) return existingInternalReport.toReport()

    val reportInfo: ReportInfo = buildReportInfo(request, resourceKey.measurementConsumerId)

    val metrics =
      Metrics(reportInfo, internalReportingSetsStub, setOperationCompiler, request.report)
    metrics.process()
    val namedSetOperationResults: Map<String, SetOperationResult> =
      metrics.getNamedSetOperationResults()
    val internalReportingSetMap: Map<Long, InternalReportingSet> =
      metrics.getInternalReportingSetsMap()

    val measurementConsumer =
      try {
        measurementConsumersStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest {
              name = MeasurementConsumerKey(resourceKey.measurementConsumerId).toName()
            }
          )
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the measurement consumer " +
            "[${MeasurementConsumerKey(resourceKey.measurementConsumerId).toName()}].",
          e
        )
      }

    // TODO: Factor this out to a separate class similar to EncryptionKeyPairStore.
    val signingPrivateKeyDer: ByteString =
      signingPrivateKeyDir.resolve(principal.config.signingPrivateKeyPath).readByteString()

    val signingCertificateDer: ByteString =
      getSigningCertificateDer(apiAuthenticationKey, principal.config.signingCertificateName)

    val signingConfig =
      SigningConfig(
        principal.config.signingCertificateName,
        signingCertificateDer,
        readPrivateKey(
          signingPrivateKeyDer,
          readCertificate(signingCertificateDer).publicKey.algorithm
        )
      )

    createMeasurements(
      request,
      namedSetOperationResults,
      reportInfo,
      measurementConsumer,
      apiAuthenticationKey,
      signingConfig,
      internalReportingSetMap
    )

    val internalCreateReportRequest: InternalCreateReportRequest =
      buildInternalCreateReportRequest(
        request,
        reportInfo,
        namedSetOperationResults,
      )
    try {
      return internalReportsStub.createReport(internalCreateReportRequest).toReport()
    } catch (e: StatusException) {
      throw Exception("Unable to create a report in the reporting database.", e)
    }
  }

  /** Gets a signing certificate x509Der in ByteString. */
  private suspend fun getSigningCertificateDer(
    apiAuthenticationKey: String,
    signingCertificateName: String
  ): ByteString {
    // TODO: Replace this with caching certificates or having them stored alongside the private key.
    return try {
      certificateStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = signingCertificateName })
        .x509Der
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve the signing certificate for the measurement consumer " +
          "[$signingCertificateName].",
        e
      )
    }
  }

  /** Builds a [ReportInfo] from a [CreateReportRequest]. */
  private fun buildReportInfo(
    request: CreateReportRequest,
    measurementConsumerReferenceId: String
  ): ReportInfo {
    grpcRequire(request.report.hasEventGroupUniverse()) { "EventGroupUniverse is not specified." }
    grpcRequire(request.report.eventGroupUniverse.eventGroupEntriesList.isNotEmpty()) {
      "EventGroupUniverse's eventGroupEntriesList cannot be empty."
    }

    val eventGroupFilters =
      request.report.eventGroupUniverse.eventGroupEntriesList.associate {
        grpcRequireNotNull(EventGroupKey.fromName(it.key)) {
          "EventGroupEntry key is not specified or invalid."
        }
        it.key to it.value
      }

    return ReportInfo(
      measurementConsumerReferenceId,
      request.report.reportIdempotencyKey,
      eventGroupFilters,
    )
  }

  /** Creates CMM public [Measurement]s and [InternalMeasurement]s from [SetOperationResult]s. */
  private suspend fun createMeasurements(
    request: CreateReportRequest,
    namedSetOperationResults: Map<String, SetOperationResult>,
    reportInfo: ReportInfo,
    measurementConsumer: MeasurementConsumer,
    apiAuthenticationKey: String,
    signingConfig: SigningConfig,
    internalReportingSetMap: Map<Long, InternalReportingSet>
  ) = coroutineScope {
    val deferred = mutableListOf<Deferred<Measurement>>()
    for (metric in request.report.metricsList) {
      val internalMetricDetails = buildInternalMetricDetails(metric)

      for (namedSetOperation in metric.setOperationsList) {
        val setOperationId =
          buildSetOperationId(
            reportInfo.reportIdempotencyKey,
            internalMetricDetails,
            namedSetOperation.uniqueName,
          )

        val setOperationResult: SetOperationResult =
          namedSetOperationResults[setOperationId] ?: continue

        setOperationResult.weightedMeasurementInfoList.forEach { weightedMeasurementInfo ->
          deferred.add(
            async {
              createMeasurement(
                weightedMeasurementInfo,
                reportInfo,
                setOperationResult.internalMetricDetails,
                measurementConsumer,
                apiAuthenticationKey,
                signingConfig,
                internalReportingSetMap
              )
            }
          )
        }
      }
    }

    // map of kingdom measurementReferenceId to kingdom apiId
    val measurementMap = mutableMapOf<String, String>()
    deferred.awaitAll().forEach { measurement ->
      try {
        val apiId = MeasurementKey.fromName(measurement.name)!!.measurementId
        measurementMap[measurement.measurementReferenceId] = apiId
        internalMeasurementsStub.createMeasurement(
          internalMeasurement {
            this.measurementConsumerReferenceId = reportInfo.measurementConsumerReferenceId
            this.measurementReferenceId = apiId
            state = InternalMeasurement.State.PENDING
          }
        )
      } catch (e: StatusException) {
        if (!e.status.code.equals(Status.Code.ALREADY_EXISTS)) {
          throw Exception(
            "Unable to create the measurement [${measurement.name}] " +
              "in the reporting database.",
            e
          )
        }
      }
    }

    for (metric in request.report.metricsList) {
      val internalMetricDetails = buildInternalMetricDetails(metric)

      for (namedSetOperation in metric.setOperationsList) {
        val setOperationId =
          buildSetOperationId(
            reportInfo.reportIdempotencyKey,
            internalMetricDetails,
            namedSetOperation.uniqueName,
          )

        val setOperationResult: SetOperationResult =
          namedSetOperationResults[setOperationId] ?: continue

        setOperationResult.weightedMeasurementInfoList.forEach { weightedMeasurementInfo ->
          weightedMeasurementInfo.kingdomMeasurementId =
            measurementMap[weightedMeasurementInfo.reportingMeasurementId]
        }
      }
    }
  }

  /** Creates a kingdom measurement for a [WeightedMeasurement]. */
  private suspend fun createMeasurement(
    weightedMeasurementInfo: WeightedMeasurementInfo,
    reportInfo: ReportInfo,
    internalMetricDetails: InternalMetricDetails,
    measurementConsumer: MeasurementConsumer,
    apiAuthenticationKey: String,
    signingConfig: SigningConfig,
    internalReportingSetMap: Map<Long, InternalReportingSet>
  ): Measurement {
    val eventGroupEntriesByDataProvider =
      groupEventGroupEntriesByDataProvider(
        weightedMeasurementInfo.weightedMeasurement.reportingSets,
        weightedMeasurementInfo.timeInterval.toMeasurementTimeInterval(),
        reportInfo.eventGroupFilters,
        internalReportingSetMap
      )

    val createMeasurementRequest: CreateMeasurementRequest =
      buildCreateMeasurementRequest(
        measurementConsumer,
        eventGroupEntriesByDataProvider,
        internalMetricDetails,
        weightedMeasurementInfo.reportingMeasurementId,
        apiAuthenticationKey,
        signingConfig,
      )

    try {
      return measurementsStub
        .withAuthenticationKey(apiAuthenticationKey)
        .createMeasurement(createMeasurementRequest)
    } catch (e: StatusException) {
      throw Exception(
        "Unable to create the measurement [${createMeasurementRequest.measurement.name}].",
        e
      )
    }
  }

  /** Gets an [InternalReport]. */
  private suspend fun getInternalReport(
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String,
  ): InternalReport? {
    return try {
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
            "reportIdempotencyKey [$reportIdempotencyKey].",
          e
        )
      }
      null
    }
  }

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val listReportsPageToken = request.toListReportsPageToken()

    // Based on AIP-132#Errors
    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Reports belonging to other MeasurementConsumers."
          }
        }
      }
    }
    val principalName = principal.resourceKey.toName()

    val apiAuthenticationKey: String = principal.config.apiKey

    val streamInternalReportsRequest: StreamInternalReportsRequest =
      listReportsPageToken.toStreamReportsRequest()
    val results: List<InternalReport> =
      try {
        internalReportsStub.streamReports(streamInternalReportsRequest).toList()
      } catch (e: StatusException) {
        throw Exception("Unable to list reports from the reporting database.", e)
      }

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
          .map { syncReport(it, apiAuthenticationKey, principalName) }
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

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (reportKey.measurementConsumerId != principal.resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get Report belonging to other MeasurementConsumers."
          }
        }
      }
    }
    val principalName = principal.resourceKey.toName()

    val apiAuthenticationKey: String = principal.config.apiKey

    val internalReport =
      try {
        internalReportsStub.getReport(
          getInternalReportRequest {
            measurementConsumerReferenceId = reportKey.measurementConsumerId
            externalReportId = apiIdToExternalId(reportKey.reportId)
          }
        )
      } catch (e: StatusException) {
        throw Exception("Unable to get the report from the reporting database.", e)
      }

    val syncedInternalReport = syncReport(internalReport, apiAuthenticationKey, principalName)

    return syncedInternalReport.toReport()
  }

  /** Syncs the [InternalReport] and all [InternalMeasurement]s used by it. */
  private suspend fun syncReport(
    internalReport: InternalReport,
    apiAuthenticationKey: String,
    principalName: String,
  ): InternalReport {
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
    syncMeasurements(
      internalReport.measurementsMap,
      internalReport.measurementConsumerReferenceId,
      apiAuthenticationKey,
      principalName,
    )

    return try {
      internalReportsStub.getReport(
        getInternalReportRequest {
          measurementConsumerReferenceId = internalReport.measurementConsumerReferenceId
          externalReportId = internalReport.externalReportId
        }
      )
    } catch (e: StatusException) {
      val reportName =
        ReportKey(
            internalReport.measurementConsumerReferenceId,
            externalIdToApiId(internalReport.externalReportId)
          )
          .toName()
      throw Exception("Unable to get the report [$reportName] from the reporting database.", e)
    }
  }

  /** Syncs [InternalMeasurement]s. */
  private suspend fun syncMeasurements(
    measurementsMap: Map<String, InternalMeasurement>,
    measurementConsumerReferenceId: String,
    apiAuthenticationKey: String,
    principalName: String,
  ) {
    for ((measurementReferenceId, internalMeasurement) in measurementsMap) {
      // Measurement with SUCCEEDED state is already synced
      if (internalMeasurement.state == InternalMeasurement.State.SUCCEEDED) continue

      syncMeasurement(
        measurementReferenceId,
        measurementConsumerReferenceId,
        apiAuthenticationKey,
        principalName,
      )
    }
  }

  /** Syncs [InternalMeasurement] with the CMM [Measurement] given the measurement reference ID. */
  private suspend fun syncMeasurement(
    measurementReferenceId: String,
    measurementConsumerReferenceId: String,
    apiAuthenticationKey: String,
    principalName: String,
  ) {
    val measurementResourceName =
      MeasurementKey(measurementConsumerReferenceId, measurementReferenceId).toName()
    val measurement =
      try {
        measurementsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementResourceName })
      } catch (e: StatusException) {
        throw Exception("Unable to retrieve the measurement [$measurementResourceName].", e)
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (measurement.state) {
      Measurement.State.SUCCEEDED -> {
        // Converts a Measurement to an InternalMeasurement and store it into the database with
        // SUCCEEDED state
        val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
        val encryptionPrivateKeyHandle =
          encryptionKeyPairStore.getPrivateKeyHandle(
            principalName,
            EncryptionPublicKey.parseFrom(measurementSpec.measurementPublicKey).data
          )
            ?: failGrpc(Status.PERMISSION_DENIED) { "Encryption private key not found" }

        val setInternalMeasurementResultRequest =
          buildSetInternalMeasurementResultRequest(
            measurementConsumerReferenceId,
            measurementReferenceId,
            measurement.resultsList,
            encryptionPrivateKeyHandle,
            apiAuthenticationKey,
          )

        try {
          internalMeasurementsStub.setMeasurementResult(setInternalMeasurementResultRequest)
        } catch (e: StatusException) {
          throw Exception(
            "Unable to update the measurement [$measurementResourceName] in the reporting " +
              "database.",
            e
          )
        }
      }
      Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
      Measurement.State.COMPUTING -> {} // No action needed
      Measurement.State.FAILED,
      Measurement.State.CANCELLED -> {
        val setInternalMeasurementFailureRequest = setInternalMeasurementFailureRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.measurementReferenceId = measurementReferenceId
          failure = measurement.failure.toInternal()
        }

        try {
          internalMeasurementsStub.setMeasurementFailure(setInternalMeasurementFailureRequest)
        } catch (e: StatusException) {
          throw Exception(
            "Unable to update the measurement [$measurementResourceName] in the reporting " +
              "database.",
            e
          )
        }
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
    privateKeyHandle: PrivateKeyHandle,
    apiAuthenticationKey: String,
  ): SetInternalMeasurementResultRequest {

    return setInternalMeasurementResultRequest {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      this.measurementReferenceId = measurementReferenceId
      result =
        aggregateResults(
          resultsList
            .map { decryptMeasurementResultPair(it, privateKeyHandle, apiAuthenticationKey) }
            .map(Measurement.Result::toInternal)
        )
    }
  }

  /** Decrypts a [Measurement.ResultPair] to [Measurement.Result] */
  private suspend fun decryptMeasurementResultPair(
    measurementResultPair: Measurement.ResultPair,
    encryptionPrivateKeyHandle: PrivateKeyHandle,
    apiAuthenticationKey: String
  ): Measurement.Result {
    // TODO: Cache the certificate
    val certificate =
      try {
        certificateStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getCertificate(getCertificateRequest { name = measurementResultPair.certificate })
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the certificate [${measurementResultPair.certificate}].",
          e
        )
      }

    val signedResult =
      decryptResult(measurementResultPair.encryptedResult, encryptionPrivateKeyHandle)

    val x509Certificate: X509Certificate = readCertificate(certificate.x509Der)
    val trustedIssuer: X509Certificate =
      checkNotNull(trustedCertificates[checkNotNull(x509Certificate.authorityKeyIdentifier)]) {
        "${certificate.name} not issued by trusted CA"
      }
    // TODO: Record verification failure in internal Measurement rather than having the RPC fail.
    try {
      verifyResult(signedResult, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw Exception("Certificate path for ${certificate.name} is invalid", e)
    } catch (e: SignatureException) {
      throw Exception("Measurement result signature is invalid", e)
    }
    return Measurement.Result.parseFrom(signedResult.data)
  }

  /** Builds an [InternalCreateReportRequest] from a public [CreateReportRequest]. */
  private suspend fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
    reportInfo: ReportInfo,
    namedSetOperationResults: Map<String, SetOperationResult>,
  ): InternalCreateReportRequest {
    val internalReport: InternalReport = internalReport {
      this.measurementConsumerReferenceId = reportInfo.measurementConsumerReferenceId

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (request.report.timeCase) {
        Report.TimeCase.TIME_INTERVALS -> {
          this.timeIntervals = request.report.timeIntervals.toInternal()
        }
        Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
          this.periodicTimeInterval = request.report.periodicTimeInterval.toInternal()
        }
        Report.TimeCase.TIME_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
      }

      for (metric in request.report.metricsList) {
        this@internalReport.metrics +=
          buildInternalMetric(metric, reportInfo, namedSetOperationResults)
      }

      details =
        InternalReportKt.details { this.eventGroupFilters.putAll(reportInfo.eventGroupFilters) }

      this.reportIdempotencyKey = reportInfo.reportIdempotencyKey
    }

    return internalCreateReportRequest {
      report = internalReport
      measurements +=
        internalReport.metricsList.flatMap { internalMetric ->
          buildInternalMeasurementKeys(internalMetric, reportInfo.measurementConsumerReferenceId)
        }
    }
  }

  /** Builds an [InternalMetric] from a public [Metric]. */
  private suspend fun buildInternalMetric(
    metric: Metric,
    reportInfo: ReportInfo,
    namedSetOperationResults: Map<String, SetOperationResult>,
  ): InternalMetric {
    return internalMetric {
      details = buildInternalMetricDetails(metric)

      metric.setOperationsList.map { setOperation ->
        val setOperationId =
          buildSetOperationId(
            reportInfo.reportIdempotencyKey,
            details,
            setOperation.uniqueName,
          )

        namedSetOperationResults[setOperationId]?.let { setOperationResult ->
          val internalNamedSetOperation =
            buildInternalNamedSetOperation(
              setOperation,
              reportInfo,
              setOperationResult,
            )
          namedSetOperations += internalNamedSetOperation
        }
      }
    }
  }

  /** Builds an [InternalNamedSetOperation] from a public [NamedSetOperation]. */
  private suspend fun buildInternalNamedSetOperation(
    namedSetOperation: NamedSetOperation,
    reportInfo: ReportInfo,
    setOperationResult: SetOperationResult,
  ): InternalNamedSetOperation {
    return InternalMetricKt.namedSetOperation {
      displayName = namedSetOperation.uniqueName
      setOperation =
        buildInternalSetOperation(
          namedSetOperation.setOperation,
          reportInfo.measurementConsumerReferenceId
        )

      this.measurementCalculations +=
        buildMeasurementCalculationList(
          setOperationResult,
        )
    }
  }

  /** Builds an [InternalSetOperation] from a public [SetOperation]. */
  private suspend fun buildInternalSetOperation(
    setOperation: SetOperation,
    measurementConsumerReferenceId: String,
  ): InternalSetOperation {
    return InternalMetricKt.setOperation {
      this.type = setOperation.type.toInternal()
      this.lhs = buildInternalOperand(setOperation.lhs, measurementConsumerReferenceId)
      this.rhs = buildInternalOperand(setOperation.rhs, measurementConsumerReferenceId)
    }
  }

  /** Builds an [InternalOperand] from an [Operand]. */
  private suspend fun buildInternalOperand(
    operand: Operand,
    measurementConsumerReferenceId: String,
  ): InternalOperand {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (operand.operandCase) {
      Operand.OperandCase.OPERATION ->
        InternalSetOperationKt.operand {
          operation = buildInternalSetOperation(operand.operation, measurementConsumerReferenceId)
        }
      Operand.OperandCase.REPORTING_SET -> {
        val reportingSetId =
          grpcRequireNotNull(ReportingSetKey.fromName(operand.reportingSet)) {
              "Invalid reporting set name ${operand.reportingSet}."
            }
            .reportingSetId

        InternalSetOperationKt.operand {
          this.reportingSetId =
            InternalSetOperationKt.reportingSetKey {
              this.measurementConsumerReferenceId = measurementConsumerReferenceId
              externalReportingSetId = apiIdToExternalId(reportingSetId)
            }
        }
      }
      Operand.OperandCase.OPERAND_NOT_SET -> InternalSetOperationKt.operand {}
    }
  }

  /**
   * Builds a list of [MeasurementCalculation]s from a list of [WeightedMeasurement]s and a list of
   * [InternalTimeInterval]s.
   */
  private fun buildMeasurementCalculationList(
    setOperationResult: SetOperationResult,
  ): List<MeasurementCalculation> {
    val measurementCalculations = mutableListOf<MeasurementCalculation>()
    setOperationResult.weightedMeasurementInfoList
      .groupBy { it.reportTimeInterval }
      .forEach { (reportTimeInterval, weightedMeasurementInfos) ->
        measurementCalculations.add(
          InternalMetricKt.measurementCalculation {
            this.timeInterval = reportTimeInterval.toInternal()

            weightedMeasurementInfos.forEach {
              weightedMeasurements +=
                MeasurementCalculationKt.weightedMeasurement {
                  this.measurementReferenceId = it.kingdomMeasurementId!!
                  coefficient = it.weightedMeasurement.coefficient
                }
            }
          }
        )
      }
    return measurementCalculations
  }

  /** Builds a [CreateMeasurementRequest]. */
  private suspend fun buildCreateMeasurementRequest(
    measurementConsumer: MeasurementConsumer,
    eventGroupEntriesByDataProvider: Map<DataProviderKey, List<EventGroupEntry>>,
    internalMetricDetails: InternalMetricDetails,
    measurementReferenceId: String,
    apiAuthenticationKey: String,
    signingConfig: SigningConfig,
  ): CreateMeasurementRequest {
    val measurementConsumerReferenceId =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(measurementConsumer.name)) {
          "Invalid measurement consumer name [${measurementConsumer.name}]"
        }
        .measurementConsumerId

    val measurementConsumerCertificate = readCertificate(signingConfig.signingCertificateDer)
    val measurementConsumerSigningKey =
      SigningKeyHandle(measurementConsumerCertificate, signingConfig.signingPrivateKey)
    val measurementEncryptionPublicKey = measurementConsumer.publicKey.data

    val measurementResourceName =
      MeasurementKey(measurementConsumerReferenceId, measurementReferenceId).toName()

    val measurement = measurement {
      name = measurementResourceName
      this.measurementConsumerCertificate = signingConfig.signingCertificateName

      dataProviders +=
        buildDataProviderEntries(
          eventGroupEntriesByDataProvider,
          measurementEncryptionPublicKey,
          measurementConsumerSigningKey,
          apiAuthenticationKey,
        )

      val unsignedMeasurementSpec: MeasurementSpec =
        buildUnsignedMeasurementSpec(
          measurementEncryptionPublicKey,
          dataProviders.map { it.value.nonceHash },
          internalMetricDetails,
        )

      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)

      this.measurementReferenceId = measurementReferenceId
    }

    return createMeasurementRequest { this.measurement = measurement }
  }

  /**
   * Converts internal event group entries into [EventGroupEntry] messages, grouping them by
   * DataProvider.
   */
  private fun groupEventGroupEntriesByDataProvider(
    reportingSetNames: List<String>,
    timeInterval: MeasurementTimeInterval,
    eventGroupFilters: Map<String, String>,
    internalReportingSetMap: Map<Long, InternalReportingSet>
  ): Map<DataProviderKey, List<EventGroupEntry>> {
    return reportingSetNames
      .flatMap {
        val reportingSetKey =
          grpcRequireNotNull(ReportingSetKey.fromName(it)) { "Invalid reporting set name $it" }
        val internalReportingSet =
          internalReportingSetMap.getValue(apiIdToExternalId(reportingSetKey.reportingSetId))
        internalReportingSet.eventGroupKeysList.map { internalEventGroupKey ->
          val eventGroupKey =
            EventGroupKey(
              internalEventGroupKey.measurementConsumerReferenceId,
              internalEventGroupKey.dataProviderReferenceId,
              internalEventGroupKey.eventGroupReferenceId
            )
          val eventGroupName = eventGroupKey.toName()
          val filter =
            combineEventGroupFilters(internalReportingSet.filter, eventGroupFilters[eventGroupName])

          eventGroupKey to
            RequisitionSpecKt.eventGroupEntry {
              key = eventGroupName
              value =
                RequisitionSpecKt.EventGroupEntryKt.value {
                  collectionInterval = timeInterval
                  if (filter != null) {
                    this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                  }
                }
            }
        }
      }
      .groupBy(
        { (eventGroupKey, _) -> DataProviderKey(eventGroupKey.dataProviderReferenceId) },
        { (_, eventGroupEntry) -> eventGroupEntry }
      )
  }

  /** Builds a [List] of [DataProviderEntry] messages from [eventGroupEntriesByDataProvider]. */
  private suspend fun buildDataProviderEntries(
    eventGroupEntriesByDataProvider: Map<DataProviderKey, List<EventGroupEntry>>,
    measurementEncryptionPublicKey: ByteString,
    measurementConsumerSigningKey: SigningKeyHandle,
    apiAuthenticationKey: String,
  ): List<DataProviderEntry> {
    return eventGroupEntriesByDataProvider.map { (dataProviderKey, eventGroupEntriesList) ->
      // TODO(@SanjayVas): Consider caching the public key and certificate.
      val dataProviderName: String = dataProviderKey.toName()
      val dataProvider: DataProvider =
        try {
          dataProvidersStub
            .withAuthenticationKey(apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest { name = dataProviderName })
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.NOT_FOUND ->
                Status.FAILED_PRECONDITION.withDescription("$dataProviderName not found")
              else -> Status.UNKNOWN.withDescription("Unable to retrieve $dataProviderName")
            }
            .withCause(e)
            .asRuntimeException()
        }

      val certificate: Certificate =
        try {
          certificateStub
            .withAuthenticationKey(apiAuthenticationKey)
            .getCertificate(getCertificateRequest { name = dataProvider.certificate })
        } catch (e: StatusException) {
          throw Exception("Unable to retrieve Certificate ${dataProvider.certificate}", e)
        }
      if (certificate.revocationState != Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "${certificate.name} revocation state is ${certificate.revocationState}"
          )
          .asRuntimeException()
      }

      val x509Certificate: X509Certificate = readCertificate(certificate.x509Der)
      val trustedIssuer: X509Certificate =
        trustedCertificates[checkNotNull(x509Certificate.authorityKeyIdentifier)]
          ?: throw Status.FAILED_PRECONDITION.withDescription(
              "${certificate.name} not issued by trusted CA"
            )
            .asRuntimeException()
      try {
        verifyEncryptionPublicKey(dataProvider.publicKey, x509Certificate, trustedIssuer)
      } catch (e: CertPathValidatorException) {
        throw Status.FAILED_PRECONDITION.withCause(e)
          .withDescription("Certificate path for ${certificate.name} is invalid")
          .asRuntimeException()
      } catch (e: SignatureException) {
        throw Status.FAILED_PRECONDITION.withCause(e)
          .withDescription("DataProvider public key signature is invalid")
          .asRuntimeException()
      }

      val requisitionSpec = requisitionSpec {
        eventGroups += eventGroupEntriesList
        measurementPublicKey = measurementEncryptionPublicKey
        nonce = secureRandom.nextLong()
      }
      val encryptRequisitionSpec =
        encryptRequisitionSpec(
          signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
          EncryptionPublicKey.parseFrom(dataProvider.publicKey.data)
        )

      dataProviderEntry {
        key = dataProvider.name
        value =
          DataProviderEntryKt.value {
            dataProviderCertificate = certificate.name
            dataProviderPublicKey = dataProvider.publicKey
            this.encryptedRequisitionSpec = encryptRequisitionSpec
            nonceHash = hashSha256(requisitionSpec.nonce)
          }
      }
    }
  }

  /** Builds the unsigned [MeasurementSpec]. */
  private fun buildUnsignedMeasurementSpec(
    measurementEncryptionPublicKey: ByteString,
    nonceHashes: List<ByteString>,
    internalMetricDetails: InternalMetricDetails,
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

  private class Metrics(
    private val reportInfo: ReportInfo,
    private val internalReportingSetsStub: ReportingSetsGrpcKt.ReportingSetsCoroutineStub,
    private val setOperationCompiler: SetOperationCompiler,
    private val report: Report
  ) {

    private val reportingSetExternalIds: MutableSet<Long> = mutableSetOf()
    private lateinit var namedSetOperationResults: Map<String, SetOperationResult>
    private lateinit var internalReportingSetMap: Map<Long, InternalReportingSet>

    suspend fun process() {
      if (this::namedSetOperationResults.isInitialized) {
        return
      }
      namedSetOperationResults = compileAllSetOperations()
      internalReportingSetMap = getReportingSets()
      internalReportingSetMap.values.forEach {
        it.checkReportingSetEventGroupFilters(reportInfo.eventGroupFilters)
      }
    }

    suspend fun getNamedSetOperationResults(): Map<String, SetOperationResult> {
      if (!this::namedSetOperationResults.isInitialized) {
        process()
      }
      return namedSetOperationResults
    }

    suspend fun getInternalReportingSetsMap(): Map<Long, InternalReportingSet> {
      if (!this::namedSetOperationResults.isInitialized) {
        process()
      }
      return internalReportingSetMap
    }

    /** Compiles all [SetOperation]s and outputs each result with measurement reference ID. */
    private suspend fun compileAllSetOperations(): Map<String, SetOperationResult> {
      val namedSetOperationResults = mutableMapOf<String, SetOperationResult>()

      var hasCumulativeMetric = false
      for (metric in report.metricsList) {
        if (metric.cumulative) {
          hasCumulativeMetric = true
          break
        }
      }
      val timeIntervalsList = report.timeIntervalsList(hasCumulativeMetric)
      val sortedTimeIntervalsList = timeIntervalsList.sortedWith(timeIntervalComparator)
      val cumulativeTimeIntervalsList =
        sortedTimeIntervalsList.map { timeInterval ->
          timeInterval.copy { this.startTime = sortedTimeIntervalsList.first().startTime }
        }

      for (metric in report.metricsList) {
        val metricTimeIntervalsList =
          if (metric.cumulative) cumulativeTimeIntervalsList else sortedTimeIntervalsList
        val internalMetricDetails: InternalMetricDetails = buildInternalMetricDetails(metric)

        for (namedSetOperation in metric.setOperationsList) {
          checkSetOperationReportingSetName(namedSetOperation.setOperation)

          val setOperationId =
            buildSetOperationId(
              reportInfo.reportIdempotencyKey,
              internalMetricDetails,
              namedSetOperation.uniqueName
            )

          val weightedMeasurementInfoList =
            compileSetOperation(
              namedSetOperation.setOperation,
              setOperationId,
              metricTimeIntervalsList,
              sortedTimeIntervalsList
            )
          namedSetOperationResults[setOperationId] =
            SetOperationResult(weightedMeasurementInfoList, internalMetricDetails)
        }
      }

      return namedSetOperationResults.toMap()
    }

    /** Checks if all reporting sets under a [SetOperation] have valid names. */
    private fun checkSetOperationReportingSetName(setOperation: SetOperation) {
      checkOperandReportingSetName(setOperation.lhs)
      checkOperandReportingSetName(setOperation.rhs)
    }

    /** Checks if all reporting sets under a [Operand] have valid names. */
    private fun checkOperandReportingSetName(operand: Operand) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (operand.operandCase) {
        Operand.OperandCase.OPERATION -> checkSetOperationReportingSetName(operand.operation)
        Operand.OperandCase.REPORTING_SET -> checkReportingSetName(operand.reportingSet)
        Operand.OperandCase.OPERAND_NOT_SET -> {}
      }
    }

    /** Check if the event groups in the public [ReportingSet] have valid names. */
    private fun checkReportingSetName(reportingSetName: String) {
      val reportingSetKey =
        grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
          "Invalid reporting set name $reportingSetName."
        }

      grpcRequire(
        reportingSetKey.measurementConsumerId == reportInfo.measurementConsumerReferenceId
      ) {
        "No access to the reporting set [$reportingSetName]."
      }

      reportingSetExternalIds.add(apiIdToExternalId(reportingSetKey.reportingSetId))
    }

    /**
     * Compiles a [SetOperation] and outputs each result with measurement reference ID.
     *
     * metricTimeIntervalsList and timeIntervalsList are required to be the same size.
     */
    private suspend fun compileSetOperation(
      setOperation: SetOperation,
      setOperationId: String,
      metricTimeIntervalsList: List<TimeInterval>,
      reportTimeIntervalsList: List<TimeInterval>,
    ): List<WeightedMeasurementInfo> {
      if (metricTimeIntervalsList.size != reportTimeIntervalsList.size) {
        throw IllegalArgumentException()
      }
      val sortedReportTimeIntervalsList = reportTimeIntervalsList.sortedWith(timeIntervalComparator)

      val weightedMeasurementsList = setOperationCompiler.compileSetOperation(setOperation)

      return metricTimeIntervalsList.sortedWith(timeIntervalComparator).flatMapIndexed {
        timeIntervalsIndex,
        timeInterval ->
        weightedMeasurementsList.mapIndexed { index, weightedMeasurement ->
          val measurementReferenceId =
            buildMeasurementReferenceId(
              setOperationId,
              timeInterval,
              index,
            )

          WeightedMeasurementInfo(
            measurementReferenceId,
            weightedMeasurement,
            timeInterval = timeInterval,
            reportTimeInterval = sortedReportTimeIntervalsList[timeIntervalsIndex]
          )
        }
      }
    }

    private suspend fun getReportingSets(): Map<Long, InternalReportingSet> {
      val batchGetReportingSetRequest = batchGetReportingSetRequest {
        measurementConsumerReferenceId = reportInfo.measurementConsumerReferenceId
        reportingSetExternalIds.forEach { externalReportingSetIds += it }
      }

      val internalReportingSetsList =
        internalReportingSetsStub.batchGetReportingSet(batchGetReportingSetRequest).toList()

      if (internalReportingSetsList.size < reportingSetExternalIds.size) {
        val errorMessage = StringBuilder("The following reporting set names were not found:")
        internalReportingSetsList.forEach {
          reportingSetExternalIds.remove(it.externalReportingSetId)
        }
        reportingSetExternalIds.forEach {
          errorMessage.append(
            " ${ReportingSetKey(reportInfo.measurementConsumerReferenceId, externalIdToApiId(it)).toName()}"
          )
        }
        failGrpc(Status.NOT_FOUND) { errorMessage.toString() }
      }

      return internalReportingSetsList.associateBy { it.externalReportingSetId }
    }
  }
}

/** Converts the time in [Report] to a list of [TimeInterval]. */
private fun Report.timeIntervalsList(hasCumulativeMetric: Boolean): List<TimeInterval> {
  val source = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.timeCase) {
    Report.TimeCase.TIME_INTERVALS -> {
      if (hasCumulativeMetric) {
        failGrpc(Status.INVALID_ARGUMENT) { "Cannot use TimeIntervals with a cumulative Metric." }
      }
      grpcRequire(source.timeIntervals.timeIntervalsList.isNotEmpty()) {
        "TimeIntervals timeIntervalsList is empty."
      }
      source.timeIntervals.timeIntervalsList.forEach {
        grpcRequire(it.startTime.seconds > 0 || it.startTime.nanos > 0) {
          "TimeInterval startTime is unspecified."
        }
        grpcRequire(it.endTime.seconds > 0 || it.endTime.nanos > 0) {
          "TimeInterval endTime is unspecified."
        }
        grpcRequire(
          it.endTime.seconds > it.startTime.seconds || it.endTime.nanos > it.startTime.nanos
        ) {
          "TimeInterval endTime is not later than startTime."
        }
      }
      source.timeIntervals.timeIntervalsList.map { it }
    }
    Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
      grpcRequire(
        source.periodicTimeInterval.startTime.seconds > 0 ||
          source.periodicTimeInterval.startTime.nanos > 0
      ) {
        "PeriodicTimeInterval startTime is unspecified."
      }
      grpcRequire(
        source.periodicTimeInterval.increment.seconds > 0 ||
          source.periodicTimeInterval.increment.nanos > 0
      ) {
        "PeriodicTimeInterval increment is unspecified."
      }
      grpcRequire(source.periodicTimeInterval.intervalCount > 0) {
        "PeriodicTimeInterval intervalCount is unspecified."
      }
      source.periodicTimeInterval.toTimeIntervalsList()
    }
    Report.TimeCase.TIME_NOT_SET ->
      failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
  }
}

/**
 * Check if the event groups in the internal [InternalReportingSet] are covered by the event group
 * universe.
 */
private fun InternalReportingSet.checkReportingSetEventGroupFilters(
  eventGroupFilters: Map<String, String>
) {
  for (eventGroupKey in this.eventGroupKeysList) {
    val eventGroupName =
      EventGroupKey(
          eventGroupKey.measurementConsumerReferenceId,
          eventGroupKey.dataProviderReferenceId,
          eventGroupKey.eventGroupReferenceId
        )
        .toName()
    val internalReportingSetDisplayName = this.displayName
    grpcRequire(eventGroupFilters.containsKey(eventGroupName)) {
      "The event group [$eventGroupName] in the reporting set " +
        "[$internalReportingSetDisplayName] is not included in the event group universe."
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
      InternalCreateReportRequestKt.measurementKey {
        this.measurementConsumerReferenceId = measurementConsumerReferenceId
        this.measurementReferenceId = measurementReferenceId
      }
    }
}

/** Converts an [TimeInterval] to a [MeasurementTimeInterval] for measurement request. */
private fun TimeInterval.toMeasurementTimeInterval(): MeasurementTimeInterval {
  val source = this
  return measurementTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Builds an [InternalMetricDetails] from a [Metric]. */
private fun buildInternalMetricDetails(metric: Metric): InternalMetricDetails {
  return InternalMetricKt.details {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (metric.metricTypeCase) {
      MetricTypeCase.REACH -> reach = InternalMetricKt.reachParams {}
      MetricTypeCase.FREQUENCY_HISTOGRAM ->
        frequencyHistogram = metric.frequencyHistogram.toInternal()
      MetricTypeCase.IMPRESSION_COUNT -> impressionCount = metric.impressionCount.toInternal()
      MetricTypeCase.WATCH_DURATION -> watchDuration = metric.watchDuration.toInternal()
      MetricTypeCase.METRICTYPE_NOT_SET ->
        failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Report is not specified." }
    }

    cumulative = metric.cumulative
  }
}

/** Builds a unique ID for a [SetOperation]. */
private fun buildSetOperationId(
  reportIdempotencyKey: String,
  internalMetricDetails: InternalMetricDetails,
  setOperationUniqueName: String,
): String {
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

  return "$reportIdempotencyKey-$metricType-$setOperationUniqueName"
}

/** Builds a unique reference ID for a [Measurement]. */
private fun buildMeasurementReferenceId(
  setOperationId: String,
  timeInterval: TimeInterval,
  index: Int,
): String {
  val rowHeader = buildRowHeader(timeInterval)
  return "$setOperationId-$rowHeader-measurement-$index"
}

/** Combines two event group filters. */
private fun combineEventGroupFilters(filter1: String?, filter2: String?): String? {
  if (filter1 == null) return filter2

  return if (filter2 == null) filter1
  else {
    "($filter1) AND ($filter2)"
  }
}

/** Builds a [VidSamplingInterval] for reach-only. */
private fun buildReachOnlyVidSamplingInterval(secureRandom: SecureRandom): VidSamplingInterval {
  return MeasurementSpecKt.vidSamplingInterval {
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
  return MeasurementSpecKt.vidSamplingInterval {
    // Random draw the start point from the list
    val index = secureRandom.nextInt(NUMBER_REACH_FREQUENCY_BUCKETS)
    start = REACH_FREQUENCY_VID_SAMPLING_START_LIST[index]
    width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
}

/** Builds a [VidSamplingInterval] for impression count. */
private fun buildImpressionVidSamplingInterval(secureRandom: SecureRandom): VidSamplingInterval {
  return MeasurementSpecKt.vidSamplingInterval {
    // Random draw the start point from the list
    val index = secureRandom.nextInt(NUMBER_IMPRESSION_BUCKETS)
    start = IMPRESSION_VID_SAMPLING_START_LIST[index]
    width = IMPRESSION_VID_SAMPLING_WIDTH
  }
}

/** Builds a [VidSamplingInterval] for watch duration. */
private fun buildDurationVidSamplingInterval(secureRandom: SecureRandom): VidSamplingInterval {
  return MeasurementSpecKt.vidSamplingInterval {
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
  return MeasurementSpecKt.reachAndFrequency {
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
  return MeasurementSpecKt.impression {
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
  return MeasurementSpecKt.duration {
    privacyParams = differentialPrivacyParams {
      epsilon = WATCH_DURATION_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    this.maximumWatchDurationPerUser = maximumWatchDurationPerUser
    this.maximumFrequencyPerUser = maximumFrequencyPerUser
  }
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
  return InternalMetricKt.watchDurationParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
    maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
  }
}

/** Converts a [ImpressionCountParams] to an [InternalImpressionCountParams]. */
private fun ImpressionCountParams.toInternal(): InternalImpressionCountParams {
  val source = this
  return InternalMetricKt.impressionCountParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
  }
}

/** Converts a [FrequencyHistogramParams] to an [InternalFrequencyHistogramParams]. */
private fun FrequencyHistogramParams.toInternal(): InternalFrequencyHistogramParams {
  val source = this
  return InternalMetricKt.frequencyHistogramParams {
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

/** Converts a public [TimeInterval] to an [InternalTimeInterval]. */
private fun TimeInterval.toInternal(): InternalTimeInterval {
  val source = this
  return internalTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
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

/** Convert an [PeriodicTimeInterval] to a list of [TimeInterval]s. */
private fun PeriodicTimeInterval.toTimeIntervalsList(): List<TimeInterval> {
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

/** Builds a row header in String from an [TimeInterval]. */
private fun buildRowHeader(timeInterval: TimeInterval): String {
  val startTimeInstant =
    Instant.ofEpochSecond(timeInterval.startTime.seconds, timeInterval.startTime.nanos.toLong())
  val endTimeInstant =
    Instant.ofEpochSecond(timeInterval.endTime.seconds, timeInterval.endTime.nanos.toLong())
  return "$startTimeInstant-$endTimeInstant"
}

private operator fun Duration.plus(other: Duration): Duration {
  return Durations.add(this, other)
}

/** Converts a CMM [Measurement.Failure] to an [InternalMeasurement.Failure]. */
private fun Measurement.Failure.toInternal(): InternalMeasurement.Failure {
  val source = this

  return InternalMeasurementKt.failure {
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

  return InternalMeasurementKt.result {
    if (internalResultsList.first().hasReach()) {
      this.reach = InternalMeasurementKt.ResultKt.reach { value = reachValue }
    }
    if (internalResultsList.first().hasFrequency()) {
      this.frequency =
        InternalMeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(frequencyDistribution)
        }
    }
    if (internalResultsList.first().hasImpression()) {
      this.impression = InternalMeasurementKt.ResultKt.impression { value = impressionValue }
    }
    if (internalResultsList.first().hasWatchDuration()) {
      this.watchDuration =
        InternalMeasurementKt.ResultKt.watchDuration { value = watchDurationValue }
    }
  }
}

/** Converts a CMM [Measurement.Result] to an [InternalMeasurementResult]. */
private fun Measurement.Result.toInternal(): InternalMeasurementResult {
  val source = this

  return InternalMeasurementKt.result {
    if (source.hasReach()) {
      this.reach = InternalMeasurementKt.ResultKt.reach { value = source.reach.value }
    }
    if (source.hasFrequency()) {
      this.frequency =
        InternalMeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(source.frequency.relativeFrequencyDistributionMap)
        }
    }
    if (source.hasImpression()) {
      this.impression =
        InternalMeasurementKt.ResultKt.impression { value = source.impression.value }
    }
    if (source.hasWatchDuration()) {
      this.watchDuration =
        InternalMeasurementKt.ResultKt.watchDuration { value = source.watchDuration.value }
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
      EventGroupUniverseKt.eventGroupEntry {
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
