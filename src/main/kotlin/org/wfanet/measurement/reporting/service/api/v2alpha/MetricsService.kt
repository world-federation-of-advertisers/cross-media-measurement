// Copyright 2023 The Cross-Media Measurement Authors/*
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.security.PrivateKey
import java.security.SecureRandom
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.internal.reporting.v2alpha.BatchSetCmmsMeasurementIdRequest.MeasurementIds
import org.wfanet.measurement.internal.reporting.v2alpha.BatchSetCmmsMeasurementIdRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.Metric.WeightedMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet.SetExpression as InternalSetExpression
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2alpha.batchGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2alpha.batchSetCmmsMeasurementIdRequest
import org.wfanet.measurement.internal.reporting.v2alpha.copy
import org.wfanet.measurement.internal.reporting.v2alpha.getMetricByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.v2alpha.getReportingSetRequest as getInternalReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2alpha.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportingSet.SetExpression
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.metric

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

class MetricsService(
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val internalMetricsStub: InternalMetricsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificateStub: CertificatesCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val secureRandom: SecureRandom,
  private val signingPrivateKeyDir: File,
) : MetricsCoroutineImplBase() {

  data class SigningConfig(
    val signingCertificateName: String,
    val signingCertificateDer: ByteString,
    val signingPrivateKey: PrivateKey,
  )

  override suspend fun createMetric(request: CreateMetricRequest): Metric {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Metric for another MeasurementConsumer."
          }
        }
      }
    }

    val resourceKey = principal.resourceKey
    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequire(request.hasMetric()) { "Metric is not specified." }
    grpcRequire(request.metric.reportingSet.isNotBlank()) {
      "Reporting set in metric is not specified."
    }
    grpcRequire(request.metric.hasTimeInterval()) { "Time interval in metric is not specified." }
    grpcRequire(request.metric.hasMetricSpec()) { "Metric spec in metric is not specified." }

    val initialInternalMetric: InternalMetric =
      createInitialInternalMetric(resourceKey.measurementConsumerId, request)

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

    /**
     * Measurement Supplier - createMeasurements
     * 1. For each internal measurement, a. call createMeasurement
     *     - request a corresponding kingdom measurement
     *     - update the cmmsMeasurementId in the internal measurement.
     */
    createMeasurements(
      initialInternalMetric,
      resourceKey.measurementConsumerId,
      apiAuthenticationKey,
      signingConfig,
    )

    // Convert the internal metric to public and return it.

    return metric {}
  }

  /** Creates CMM public [Measurement]s and [InternalMeasurement]s from [SetOperationResult]s. */
  private suspend fun createMeasurements(
    initialInternalMetric: InternalMetric,
    cmmsMeasurementConsumerId: String,
    apiAuthenticationKey: String,
    signingConfig: SigningConfig,
  ) = coroutineScope {
    // Get measurementConsumer and signingConfig
    val measurementConsumer: MeasurementConsumer =
      getMeasurementConsumer(cmmsMeasurementConsumerId, apiAuthenticationKey)

    val externalPrimitiveReportingSetIds: Set<Long> =
      initialInternalMetric.weightedMeasurementsList
        .flatMap { weightedMeasurements ->
          weightedMeasurements.measurement.primitiveReportingSetBasesList.map { it.externalReportingSetId }
        }
        .toSet()

    val internalPrimitiveReportingSetMap: Map<Long, InternalReportingSet> =
      buildInternalReportingSetMap(cmmsMeasurementConsumerId, externalPrimitiveReportingSetIds)

    val deferred = mutableListOf<Deferred<MeasurementIds>>()

    for (weightedMeasurement in initialInternalMetric.weightedMeasurementsList) {
      deferred.add(
        async {
          measurementIds {
            externalMeasurementId = weightedMeasurement.measurement.externalMeasurementId
            measurementReferenceId =
              createMeasurement(
                  weightedMeasurement,
                  internalPrimitiveReportingSetMap,
                  measurementConsumer,
                  apiAuthenticationKey,
                  signingConfig,
                )
                .measurementReferenceId
          }
        }
      )
    }

    // Set CMMs measurement IDs.
    try {
      internalMeasurementsStub.batchSetCmmsMeasurementId(
        batchSetCmmsMeasurementIdRequest {
          measurementConsumerReferenceId = cmmsMeasurementConsumerId
          measurementIds += deferred.awaitAll()
        }
      )
    } catch (e: StatusException) {
      throw Exception("Unable to set the CMMs measurement IDs in the reporting database.", e)
    }
  }

  private fun createMeasurement(
    internalWeightedMeasurement: WeightedMeasurement,
    internalPrimitiveReportingSetMap: Map<Long, InternalReportingSet>,
    measurementConsumer: MeasurementConsumer,
    apiAuthenticationKey: String,
    signingConfig: SigningConfig,
  ): Measurement {
    TODO("Not yet implemented")
  }

  private suspend fun createInitialInternalMetric(
    cmmsMeasurementConsumerId: String,
    request: CreateMetricRequest,
  ): InternalMetric {
    // Check if there's any existing metric using the unique request ID.
    val existingInternalMetric: InternalMetric? =
      if (request.requestId.isBlank()) null
      else getInternalMetricByIdempotencyKey(cmmsMeasurementConsumerId, request.requestId)

    if (existingInternalMetric != null) return existingInternalMetric

    val internalReportingSet: InternalReportingSet =
      getInternalReportingSet(cmmsMeasurementConsumerId, request.metric.reportingSet)

    return internalMetricsStub.createMetric(
      internalMetric {
        measurementConsumerReferenceId = cmmsMeasurementConsumerId
        metricIdempotencyKey = request.requestId
        externalReportingSetId = internalReportingSet.externalReportingSetId
        timeInterval = request.metric.timeInterval.toInternal()
        metricSpec = request.metric.metricSpec.toInternal()
        weightedMeasurements +=
          buildInitialInternalMeasurements(
            cmmsMeasurementConsumerId,
            request.metric,
            internalReportingSet
          )
      }
    )
  }

  private fun buildInitialInternalMeasurements(
    cmmsMeasurementConsumerId: String,
    metric: Metric,
    internalReportingSet: InternalReportingSet
  ): List<WeightedMeasurement> {
    return internalReportingSet.weightedSubsetUnionsList.map { weightedSubsetUnion ->
      weightedMeasurement {
        weight = weightedSubsetUnion.weight
        measurement = internalMeasurement {
          measurementConsumerReferenceId = cmmsMeasurementConsumerId
          timeInterval = metric.timeInterval.toInternal()
          this.primitiveReportingSetBases +=
            weightedSubsetUnion.primitiveReportingSetBasesList.map { primitiveReportingSetBasis ->
              primitiveReportingSetBasis.copy { filters += metric.filtersList }
            }
        }
      }
    }
  }

  private suspend fun buildInternalReportingSetMap(
    cmmsMeasurementConsumerId: String,
    externalReportingSetIds: Set<Long>,
  ): Map<Long, InternalReportingSet> {
    val batchGetReportingSetRequest = batchGetReportingSetRequest {
      measurementConsumerReferenceId = cmmsMeasurementConsumerId
      externalReportingSetIds.forEach { this.externalReportingSetIds += it }
    }

    val internalReportingSetsList =
      internalReportingSetsStub.batchGetReportingSet(batchGetReportingSetRequest).toList()

    if (internalReportingSetsList.size < externalReportingSetIds.size) {
      val missingExternalReportingSetIds = externalReportingSetIds.toMutableSet()
      val errorMessage = StringBuilder("The following reporting set names were not found:")
      internalReportingSetsList.forEach {
        missingExternalReportingSetIds.remove(it.externalReportingSetId)
      }
      missingExternalReportingSetIds.forEach {
        errorMessage.append(
          " ${ReportingSetKey(cmmsMeasurementConsumerId, externalIdToApiId(it)).toName()}"
        )
      }
      failGrpc(Status.NOT_FOUND) { errorMessage.toString() }
    }

    return internalReportingSetsList.associateBy { it.externalReportingSetId }
  }

  private suspend fun getMeasurementConsumer(
    cmmsMeasurementConsumerId: String,
    apiAuthenticationKey: String,
  ): MeasurementConsumer {
    return try {
      measurementConsumersStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getMeasurementConsumer(
          getMeasurementConsumerRequest {
            name = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
          }
        )
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve the measurement consumer " +
          "[${MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()}].",
        e
      )
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

  /** Gets an [InternalMetric]. */
  private suspend fun getInternalMetricByIdempotencyKey(
    measurementConsumerReferenceId: String,
    metricIdempotencyKey: String,
  ): InternalMetric? {
    return try {
      internalMetricsStub.getMetricByIdempotencyKey(
        getMetricByIdempotencyKeyRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.metricIdempotencyKey = metricIdempotencyKey
        }
      )
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        throw Exception(
          "Unable to retrieve a metric from the reporting database using the provided " +
            "metricIdempotencyKey [$metricIdempotencyKey].",
          e
        )
      }
      null
    }
  }

  /** Gets an [InternalMetric]. */
  private suspend fun getInternalReportingSet(
    measurementConsumerReferenceId: String,
    reportingSetName: String,
  ): InternalReportingSet {
    val reportingSetKey =
      grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
        "Invalid reporting set name $reportingSetName."
      }

    grpcRequire(reportingSetKey.measurementConsumerId == measurementConsumerReferenceId) {
      "No access to the reporting set [$reportingSetName]."
    }

    return try {
      internalReportingSetsStub.getReportingSet(
        getInternalReportingSetRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
        }
      )
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve a reporting set from the reporting database using the provided " +
          "reportingSet [$reportingSetName].",
        e
      )
    }
  }
}

private fun MetricSpec.toInternal(): InternalMetricSpec {
  val source = this
  return internalMetricSpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.typeCase) {
      MetricSpec.TypeCase.REACH -> reach = MetricSpecKt.reachParams {}
      MetricSpec.TypeCase.FREQUENCY_HISTOGRAM ->
        MetricSpecKt.frequencyHistogramParams {
          maximumFrequencyPerUser = source.frequencyHistogram.maximumFrequencyPerUser
        }
      MetricSpec.TypeCase.IMPRESSION_COUNT ->
        MetricSpecKt.impressionCountParams {
          maximumFrequencyPerUser = source.impressionCount.maximumFrequencyPerUser
        }
      MetricSpec.TypeCase.WATCH_DURATION ->
        MetricSpecKt.watchDurationParams {
          maximumWatchDurationPerUser = source.watchDuration.maximumWatchDurationPerUser
        }
      MetricSpec.TypeCase.TYPE_NOT_SET ->
        failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Metric is not specified." }
    }
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

private fun InternalSetExpression.toSetExpression(): SetExpression {
  TODO("Not yet implemented")
}

private fun InternalMetric.toMetric(): Metric {
  val source = this
  // val metricResourceName = MetricKey(
  //   measurementConsumerId = source.measurementConsumerReferenceId,
  //
  // )
  return metric {}
}
