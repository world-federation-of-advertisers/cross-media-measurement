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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.util.concurrent.UncheckedExecutionException
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Duration
import com.google.protobuf.duration
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.util.Durations
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.File
import java.lang.IllegalStateException
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.api.v2alpha.BatchCreateMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.BatchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementFailuresResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest.MeasurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementResultsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequestKt.measurementFailure
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequestKt.measurementResult
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest as InternalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.CustomDirectMethodology
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2.Metric.WeightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest as internalBatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest as internalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2.metric as internalMetric
import org.wfanet.measurement.measurementconsumer.stats.CustomDirectFrequencyMethodology
import org.wfanet.measurement.measurementconsumer.stats.CustomDirectScalarMethodology
import org.wfanet.measurement.measurementconsumer.stats.DeterministicMethodology
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyVariances
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsSketchMethodology
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsV2Methodology
import org.wfanet.measurement.measurementconsumer.stats.Methodology
import org.wfanet.measurement.measurementconsumer.stats.NoiseMechanism as StatsNoiseMechanism
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.ReachMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.Variances
import org.wfanet.measurement.measurementconsumer.stats.WatchDurationMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.WatchDurationMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.WatchDurationMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.WeightedFrequencyMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.WeightedImpressionMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.WeightedReachMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.WeightedWatchDurationMeasurementVarianceParams
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.submitBatchRequests
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.GetMetricRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.bin
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.binResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.histogramResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.impressionCountResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.populationCountResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachAndFrequencyResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.watchDurationResult
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.listMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.univariateStatistics

private const val MAX_BATCH_SIZE = 1000
private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val NANOS_PER_SECOND = 1_000_000_000
private const val BATCH_KINGDOM_MEASUREMENTS_LIMIT = 50
private const val BATCH_GET_REPORTING_SETS_LIMIT = 1000
private const val BATCH_SET_CMMS_MEASUREMENT_IDS_LIMIT = 1000
private const val BATCH_SET_MEASUREMENT_RESULTS_LIMIT = 1000
private const val BATCH_SET_MEASUREMENT_FAILURES_LIMIT = 1000

class MetricsService(
  private val metricSpecConfig: MetricSpecConfig,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMetricsStub: InternalMetricsCoroutineStub,
  private val variances: Variances,
  internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  dataProvidersStub: DataProvidersCoroutineStub,
  measurementsStub: MeasurementsCoroutineStub,
  certificatesStub: CertificatesCoroutineStub,
  measurementConsumersStub: MeasurementConsumersCoroutineStub,
  encryptionKeyPairStore: EncryptionKeyPairStore,
  secureRandom: SecureRandom,
  signingPrivateKeyDir: File,
  trustedCertificates: Map<ByteString, X509Certificate>,
  certificateCacheRefreshDuration: JavaDuration = JavaDuration.ofMinutes(5),
  certificateCacheExpirationDuration: JavaDuration = JavaDuration.ofMinutes(60),
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : MetricsCoroutineImplBase() {

  private data class DataProviderInfo(
    val dataProviderName: String,
    val publicKey: SignedMessage,
    val certificateName: String,
  )

  private val measurementSupplier =
    MeasurementSupplier(
      internalReportingSetsStub,
      internalMeasurementsStub,
      measurementsStub,
      dataProvidersStub,
      certificatesStub,
      measurementConsumersStub,
      encryptionKeyPairStore,
      secureRandom,
      signingPrivateKeyDir,
      trustedCertificates,
      certificateCacheRefreshDuration,
      certificateCacheExpirationDuration,
      coroutineContext,
    )

  private class MeasurementSupplier(
    private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
    private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
    private val measurementsStub: MeasurementsCoroutineStub,
    private val dataProvidersStub: DataProvidersCoroutineStub,
    private val certificatesStub: CertificatesCoroutineStub,
    private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
    private val encryptionKeyPairStore: EncryptionKeyPairStore,
    private val secureRandom: SecureRandom,
    private val signingPrivateKeyDir: File,
    private val trustedCertificates: Map<ByteString, X509Certificate>,
    certificateCacheRefreshDuration: JavaDuration,
    certificateCacheExpirationDuration: JavaDuration,
    private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  ) {
    private data class ResourceNameApiAuthenticationKey(
      val name: String,
      val apiAuthenticationKey: String,
    )

    private val scope = CoroutineScope(Dispatchers.IO)

    private val certificateCache: AsyncLoadingCache<ResourceNameApiAuthenticationKey, Certificate> =
      Caffeine.newBuilder()
        .refreshAfterWrite(certificateCacheRefreshDuration)
        .expireAfterWrite(certificateCacheExpirationDuration)
        .buildAsync { key: ResourceNameApiAuthenticationKey, _ ->
          scope.future {
            getCertificate(name = key.name, apiAuthenticationKey = key.apiAuthenticationKey)
          }
        }

    /**
     * Creates CMM public [Measurement]s and [InternalMeasurement]s from a list of [InternalMetric].
     */
    suspend fun createCmmsMeasurements(
      internalMetricsList: List<InternalMetric>,
      principal: MeasurementConsumerPrincipal,
    ) {
      val measurementConsumer: MeasurementConsumer = getMeasurementConsumer(principal)

      // Gets all external IDs of primitive reporting sets from the metric list.
      val externalPrimitiveReportingSetIds: Flow<String> =
        internalMetricsList
          .flatMap { internalMetric ->
            internalMetric.weightedMeasurementsList.flatMap { weightedMeasurement ->
              weightedMeasurement.measurement.primitiveReportingSetBasesList.map {
                it.externalReportingSetId
              }
            }
          }
          .distinct()
          .asFlow()

      val callBatchGetInternalReportingSetsRpc:
        suspend (List<String>) -> BatchGetReportingSetsResponse =
        { items ->
          batchGetInternalReportingSets(principal.resourceKey.measurementConsumerId, items)
        }

      val internalPrimitiveReportingSetMap: Map<String, InternalReportingSet> =
        submitBatchRequests(
            externalPrimitiveReportingSetIds,
            BATCH_GET_REPORTING_SETS_LIMIT,
            callBatchGetInternalReportingSetsRpc,
          ) { response: BatchGetReportingSetsResponse ->
            response.reportingSetsList
          }
          .toList()
          .associateBy { it.externalReportingSetId }

      val dataProviderNames = mutableSetOf<String>()
      for (internalPrimitiveReportingSet in internalPrimitiveReportingSetMap.values) {
        for (eventGroupKey in internalPrimitiveReportingSet.primitive.eventGroupKeysList) {
          dataProviderNames.add(DataProviderKey(eventGroupKey.cmmsDataProviderId).toName())
        }
      }
      val dataProviderInfoMap: Map<String, DataProviderInfo> =
        buildDataProviderInfoMap(principal.config.apiKey, dataProviderNames)

      val measurementConsumerSigningKey = getMeasurementConsumerSigningKey(principal)

      val cmmsCreateMeasurementRequests: List<CreateMeasurementRequest> =
        internalMetricsList.flatMap { internalMetric ->
          internalMetric.weightedMeasurementsList
            .filter { it.measurement.cmmsMeasurementId.isBlank() }
            .map {
              buildCreateMeasurementRequest(
                it.measurement,
                internalMetric.metricSpec,
                internalPrimitiveReportingSetMap,
                measurementConsumer,
                principal,
                dataProviderInfoMap,
                measurementConsumerSigningKey,
              )
            }
        }

      // Create CMMS measurements.
      val callBatchCreateMeasurementsRpc:
        suspend (List<CreateMeasurementRequest>) -> BatchCreateMeasurementsResponse =
        { items ->
          batchCreateCmmsMeasurements(principal, items)
        }

      val cmmsMeasurements: Flow<Measurement> =
        submitBatchRequests(
          cmmsCreateMeasurementRequests.asFlow(),
          BATCH_KINGDOM_MEASUREMENTS_LIMIT,
          callBatchCreateMeasurementsRpc,
        ) { response: BatchCreateMeasurementsResponse ->
          response.measurementsList
        }

      // Set CMMS measurement IDs.
      val callBatchSetCmmsMeasurementIdsRpc:
        suspend (List<MeasurementIds>) -> BatchSetCmmsMeasurementIdsResponse =
        { items ->
          batchSetCmmsMeasurementIds(principal.resourceKey.measurementConsumerId, items)
        }

      submitBatchRequests(
          cmmsMeasurements.map {
            measurementIds {
              cmmsCreateMeasurementRequestId = it.measurementReferenceId
              cmmsMeasurementId = MeasurementKey.fromName(it.name)!!.measurementId
            }
          },
          BATCH_SET_CMMS_MEASUREMENT_IDS_LIMIT,
          callBatchSetCmmsMeasurementIdsRpc,
        ) { response: BatchSetCmmsMeasurementIdsResponse ->
          response.measurementsList
        }
        .toList()
    }

    /** Sets a batch of CMMS [MeasurementIds] to the [InternalMeasurement] table. */
    private suspend fun batchSetCmmsMeasurementIds(
      cmmsMeasurementConsumerId: String,
      measurementIds: List<MeasurementIds>,
    ): BatchSetCmmsMeasurementIdsResponse {
      return try {
        internalMeasurementsStub.batchSetCmmsMeasurementIds(
          batchSetCmmsMeasurementIdsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.measurementIds += measurementIds
          }
        )
      } catch (e: StatusException) {
        throw Exception("Unable to set the CMMS measurement IDs for the measurements.", e)
      }
    }

    /** Batch create CMMS measurements. */
    private suspend fun batchCreateCmmsMeasurements(
      principal: MeasurementConsumerPrincipal,
      createMeasurementRequests: List<CreateMeasurementRequest>,
    ): BatchCreateMeasurementsResponse {
      try {
        return measurementsStub
          .withAuthenticationKey(principal.config.apiKey)
          .batchCreateMeasurements(
            batchCreateMeasurementsRequest {
              parent = principal.resourceKey.toName()
              requests += createMeasurementRequests
            }
          )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.INVALID_ARGUMENT ->
              Status.INVALID_ARGUMENT.withDescription("Required field unspecified or invalid.")
            Status.Code.PERMISSION_DENIED ->
              Status.PERMISSION_DENIED.withDescription(
                "Cannot create CMMS Measurements for another MeasurementConsumer."
              )
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription("Failed precondition.")
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("${principal.resourceKey.toName()} is not found.")
            else -> Status.UNKNOWN.withDescription("Unable to create CMMS Measurements.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Builds a CMMS [CreateMeasurementRequest]. */
    private fun buildCreateMeasurementRequest(
      internalMeasurement: InternalMeasurement,
      metricSpec: InternalMetricSpec,
      internalPrimitiveReportingSetMap: Map<String, InternalReportingSet>,
      measurementConsumer: MeasurementConsumer,
      principal: MeasurementConsumerPrincipal,
      dataProviderInfoMap: Map<String, DataProviderInfo>,
      measurementConsumerSigningKey: SigningKeyHandle,
    ): CreateMeasurementRequest {
      val eventGroupEntriesByDataProvider =
        groupEventGroupEntriesByDataProvider(internalMeasurement, internalPrimitiveReportingSetMap)
      val packedMeasurementEncryptionPublicKey = measurementConsumer.publicKey.message

      return createMeasurementRequest {
        parent = measurementConsumer.name
        measurement = measurement {
          measurementConsumerCertificate = principal.config.signingCertificateName

          dataProviders +=
            buildDataProviderEntries(
              eventGroupEntriesByDataProvider,
              packedMeasurementEncryptionPublicKey,
              measurementConsumerSigningKey,
              dataProviderInfoMap,
            )

          val unsignedMeasurementSpec: MeasurementSpec =
            buildUnsignedMeasurementSpec(
              packedMeasurementEncryptionPublicKey,
              dataProviders.map { it.value.nonceHash },
              metricSpec,
            )

          measurementSpec =
            signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
          // To help map reporting measurements to cmms measurements.
          measurementReferenceId = internalMeasurement.cmmsCreateMeasurementRequestId
        }
        requestId = internalMeasurement.cmmsCreateMeasurementRequestId
      }
    }

    /** Gets a [SigningKeyHandle] for a [MeasurementConsumerPrincipal]. */
    private suspend fun getMeasurementConsumerSigningKey(
      principal: MeasurementConsumerPrincipal
    ): SigningKeyHandle {
      // TODO: Factor this out to a separate class similar to EncryptionKeyPairStore.
      val signingPrivateKeyDer: ByteString =
        withContext(coroutineContext) {
          signingPrivateKeyDir.resolve(principal.config.signingPrivateKeyPath).readByteString()
        }
      val measurementConsumerCertificate: X509Certificate =
        readCertificate(getSigningCertificateDer(principal))
      val signingPrivateKey: PrivateKey =
        readPrivateKey(signingPrivateKeyDer, measurementConsumerCertificate.publicKey.algorithm)

      return SigningKeyHandle(measurementConsumerCertificate, signingPrivateKey)
    }

    /** Builds an unsigned [MeasurementSpec]. */
    private fun buildUnsignedMeasurementSpec(
      packedMeasurementEncryptionPublicKey: ProtoAny,
      nonceHashes: List<ByteString>,
      metricSpec: InternalMetricSpec,
    ): MeasurementSpec {
      return measurementSpec {
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        this.nonceHashes += nonceHashes

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metricSpec.typeCase) {
          InternalMetricSpec.TypeCase.REACH -> {
            reach = metricSpec.reach.toReach()
          }
          InternalMetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
            reachAndFrequency = metricSpec.reachAndFrequency.toReachAndFrequency()
          }
          InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
            impression = metricSpec.impressionCount.toImpression()
          }
          InternalMetricSpec.TypeCase.WATCH_DURATION -> {
            duration = metricSpec.watchDuration.toDuration()
          }
          InternalMetricSpec.TypeCase.POPULATION_COUNT -> {
            population = MeasurementSpec.Population.getDefaultInstance()
          }
          InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
            failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
              "Unset metric type should've already raised error."
            }
        }
        vidSamplingInterval = metricSpec.vidSamplingInterval.toCmmsVidSamplingInterval()
        // TODO(@jojijac0b): Add modelLine
      }
    }

    /** Builds a [Map] of [DataProvider] name to [DataProviderInfo]. */
    private suspend fun buildDataProviderInfoMap(
      apiAuthenticationKey: String,
      dataProviderNames: Collection<String>,
    ): Map<String, DataProviderInfo> {
      val dataProviderInfoMap = mutableMapOf<String, DataProviderInfo>()

      if (dataProviderNames.isEmpty()) {
        return dataProviderInfoMap
      }

      val deferredDataProviderInfoList = mutableListOf<Deferred<DataProviderInfo>>()
      coroutineScope {
        for (dataProviderName in dataProviderNames) {
          deferredDataProviderInfoList.add(
            async {
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

              val certificate =
                try {
                  certificateCache
                    .get(
                      ResourceNameApiAuthenticationKey(
                        name = dataProvider.certificate,
                        apiAuthenticationKey = apiAuthenticationKey,
                      )
                    )
                    .await()
                } catch (e: UncheckedExecutionException) {
                  if (e.cause != null) {
                    throw e.cause!!
                  } else {
                    throw Status.UNKNOWN.withDescription(
                        "Unable to retrieve Certificate ${dataProvider.certificate}."
                      )
                      .asRuntimeException()
                  }
                }

              if (
                certificate.revocationState !=
                  Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED
              ) {
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

              DataProviderInfo(dataProvider.name, dataProvider.publicKey, certificate.name)
            }
          )
        }

        for (deferredDataProviderInfo in deferredDataProviderInfoList.awaitAll()) {
          dataProviderInfoMap[deferredDataProviderInfo.dataProviderName] = deferredDataProviderInfo
        }
      }

      return dataProviderInfoMap
    }

    /**
     * Builds a [List] of [Measurement.DataProviderEntry] messages from
     * [eventGroupEntriesByDataProvider].
     */
    private fun buildDataProviderEntries(
      eventGroupEntriesByDataProvider: Map<DataProviderKey, List<EventGroupEntry>>,
      packedMeasurementEncryptionPublicKey: ProtoAny,
      measurementConsumerSigningKey: SigningKeyHandle,
      dataProviderInfoMap: Map<String, DataProviderInfo>,
    ): List<Measurement.DataProviderEntry> {
      return eventGroupEntriesByDataProvider.map { (dataProviderKey, eventGroupEntriesList) ->
        val dataProviderName: String = dataProviderKey.toName()
        val dataProviderInfo = dataProviderInfoMap.getValue(dataProviderName)

        val requisitionSpec = requisitionSpec {
          events = RequisitionSpecKt.events { eventGroups += eventGroupEntriesList }
          measurementPublicKey = packedMeasurementEncryptionPublicKey
          nonce = secureRandom.nextLong()
        }
        val encryptRequisitionSpec =
          encryptRequisitionSpec(
            signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
            dataProviderInfo.publicKey.unpack(),
          )

        dataProviderEntry {
          key = dataProviderName
          value =
            MeasurementKt.DataProviderEntryKt.value {
              dataProviderCertificate = dataProviderInfo.certificateName
              dataProviderPublicKey = dataProviderInfo.publicKey.message
              this.encryptedRequisitionSpec = encryptRequisitionSpec
              nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
            }
        }
      }
    }

    /**
     * Converts the event groups included in an [InternalMeasurement] to [EventGroupEntry]s,
     * grouping them by DataProvider.
     */
    private fun groupEventGroupEntriesByDataProvider(
      measurement: InternalMeasurement,
      internalPrimitiveReportingSetMap: Map<String, InternalReportingSet>,
    ): Map<DataProviderKey, List<EventGroupEntry>> {
      return measurement.primitiveReportingSetBasesList
        .flatMap { primitiveReportingSetBasis ->
          val internalPrimitiveReportingSet =
            internalPrimitiveReportingSetMap.getValue(
              primitiveReportingSetBasis.externalReportingSetId
            )

          internalPrimitiveReportingSet.primitive.eventGroupKeysList.map { internalEventGroupKey ->
            val cmmsEventGroupKey =
              CmmsEventGroupKey(
                internalEventGroupKey.cmmsDataProviderId,
                internalEventGroupKey.cmmsEventGroupId,
              )
            val filtersList = primitiveReportingSetBasis.filtersList.filter { !it.isNullOrBlank() }
            val filter: String? = if (filtersList.isEmpty()) null else buildConjunction(filtersList)

            cmmsEventGroupKey to
              RequisitionSpecKt.eventGroupEntry {
                key = cmmsEventGroupKey.toName()
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = measurement.timeInterval
                    if (filter != null) {
                      this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                    }
                  }
              }
          }
        }
        .groupBy(
          { (cmmsEventGroupKey, _) -> DataProviderKey(cmmsEventGroupKey.dataProviderId) },
          { (_, eventGroupEntry) -> eventGroupEntry },
        )
    }

    /** Combines event group filters. */
    private fun buildConjunction(filters: Collection<String>): String {
      return filters.joinToString(separator = " && ") { filter -> "($filter)" }
    }

    /** Gets a [MeasurementConsumer] based on a CMMS ID. */
    private suspend fun getMeasurementConsumer(
      principal: MeasurementConsumerPrincipal
    ): MeasurementConsumer {
      return try {
        measurementConsumersStub
          .withAuthenticationKey(principal.config.apiKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest { name = principal.resourceKey.toName() }
          )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("${principal.resourceKey.toName()} not found.")
            else ->
              Status.UNKNOWN.withDescription(
                "Unable to retrieve the measurement consumer [${principal.resourceKey.toName()}]."
              )
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Gets a batch of [InternalReportingSet]s. */
    private suspend fun batchGetInternalReportingSets(
      cmmsMeasurementConsumerId: String,
      externalReportingSetIds: List<String>,
    ): BatchGetReportingSetsResponse {
      return try {
        internalReportingSetsStub.batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalReportingSetIds += externalReportingSetIds
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("Reporting Set not found.")
            else ->
              Status.UNKNOWN.withDescription(
                "Unable to retrieve ReportingSets used in the requesting metric."
              )
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Gets a signing certificate x509Der in ByteString. */
    private suspend fun getSigningCertificateDer(
      principal: MeasurementConsumerPrincipal
    ): ByteString {
      val certificate =
        try {
          certificateCache
            .get(
              ResourceNameApiAuthenticationKey(
                name = principal.config.signingCertificateName,
                apiAuthenticationKey = principal.config.apiKey,
              )
            )
            .await()
        } catch (e: UncheckedExecutionException) {
          if (e.cause != null) {
            throw e.cause!!
          } else {
            throw Status.UNKNOWN.withDescription(
                "Unable to retrieve Certificate ${principal.config.signingCertificateName}."
              )
              .asRuntimeException()
          }
        }
      return certificate.x509Der
    }

    /**
     * Syncs [InternalMeasurement]s with the CMMS [Measurement]s.
     *
     * @return a boolean to indicate whether any [InternalMeasurement] was updated.
     */
    suspend fun syncInternalMeasurements(
      internalMeasurements: List<InternalMeasurement>,
      apiAuthenticationKey: String,
      principal: MeasurementConsumerPrincipal,
    ): Boolean {
      val newStateToCmmsMeasurements: Map<Measurement.State, List<Measurement>> =
        getCmmsMeasurements(internalMeasurements, principal).groupBy { measurement ->
          measurement.state
        }

      var anyUpdate = false

      for ((newState, measurementsList) in newStateToCmmsMeasurements) {
        when (newState) {
          Measurement.State.SUCCEEDED -> {
            val callBatchSetInternalMeasurementResultsRpc:
              suspend (List<Measurement>) -> BatchSetCmmsMeasurementResultsResponse =
              { items ->
                batchSetInternalMeasurementResults(items, apiAuthenticationKey, principal)
              }
            submitBatchRequests(
                measurementsList.asFlow(),
                BATCH_SET_MEASUREMENT_RESULTS_LIMIT,
                callBatchSetInternalMeasurementResultsRpc,
              ) { response: BatchSetCmmsMeasurementResultsResponse ->
                response.measurementsList
              }
              .toList()

            anyUpdate = true
          }
          Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
          Measurement.State.COMPUTING -> {} // Do nothing.
          Measurement.State.FAILED,
          Measurement.State.CANCELLED -> {
            val callBatchSetInternalMeasurementFailuresRpc:
              suspend (List<Measurement>) -> BatchSetCmmsMeasurementFailuresResponse =
              { items ->
                batchSetInternalMeasurementFailures(
                  items,
                  principal.resourceKey.measurementConsumerId,
                )
              }
            submitBatchRequests(
                measurementsList.asFlow(),
                BATCH_SET_MEASUREMENT_FAILURES_LIMIT,
                callBatchSetInternalMeasurementFailuresRpc,
              ) { response: BatchSetCmmsMeasurementFailuresResponse ->
                response.measurementsList
              }
              .toList()

            anyUpdate = true
          }
          Measurement.State.STATE_UNSPECIFIED ->
            failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
              "The CMMS measurement state should've been set."
            }
          Measurement.State.UNRECOGNIZED -> {
            failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
              "Unrecognized CMMS measurement state."
            }
          }
        }
      }

      return anyUpdate
    }

    /**
     * Sets a batch of failed [InternalMeasurement]s and stores their failure states using the given
     * failed or canceled CMMS [Measurement]s.
     */
    private suspend fun batchSetInternalMeasurementFailures(
      failedMeasurementsList: List<Measurement>,
      cmmsMeasurementConsumerId: String,
    ): BatchSetCmmsMeasurementFailuresResponse {
      val batchSetInternalMeasurementFailuresRequest = batchSetMeasurementFailuresRequest {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        measurementFailures +=
          failedMeasurementsList.map { measurement ->
            measurementFailure {
              cmmsMeasurementId = MeasurementKey.fromName(measurement.name)!!.measurementId
              failure = measurement.failure.toInternal()
            }
          }
      }

      return try {
        internalMeasurementsStub.batchSetMeasurementFailures(
          batchSetInternalMeasurementFailuresRequest
        )
      } catch (e: StatusException) {
        throw Exception("Unable to set measurement failures for Measurements.", e)
      }
    }

    /**
     * Sets a batch of succeeded [InternalMeasurement]s and stores the measurement results of the
     * given succeeded CMMS [Measurement]s.
     */
    private suspend fun batchSetInternalMeasurementResults(
      succeededMeasurementsList: List<Measurement>,
      apiAuthenticationKey: String,
      principal: MeasurementConsumerPrincipal,
    ): BatchSetCmmsMeasurementResultsResponse {
      val batchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = principal.resourceKey.measurementConsumerId
        measurementResults +=
          succeededMeasurementsList.map { measurement ->
            buildInternalMeasurementResult(
              measurement,
              apiAuthenticationKey,
              principal.resourceKey.toName(),
            )
          }
      }

      return try {
        internalMeasurementsStub.batchSetMeasurementResults(batchSetMeasurementResultsRequest)
      } catch (e: StatusException) {
        throw Exception("Unable to set measurement results for Measurements.", e)
      }
    }

    /** Retrieves [Measurement]s from the CMMS. */
    private suspend fun getCmmsMeasurements(
      internalMeasurements: List<InternalMeasurement>,
      principal: MeasurementConsumerPrincipal,
    ): List<Measurement> {
      val measurementNames: List<String> =
        internalMeasurements
          .map { internalMeasurement ->
            MeasurementKey(
                principal.resourceKey.measurementConsumerId,
                internalMeasurement.cmmsMeasurementId,
              )
              .toName()
          }
          .distinct()

      val callBatchGetMeasurementsRpc: suspend (List<String>) -> BatchGetMeasurementsResponse =
        { items ->
          batchGetCmmsMeasurements(principal, items)
        }

      return submitBatchRequests(
          measurementNames.asFlow(),
          BATCH_KINGDOM_MEASUREMENTS_LIMIT,
          callBatchGetMeasurementsRpc,
        ) { response: BatchGetMeasurementsResponse ->
          response.measurementsList
        }
        .toList()
    }

    /** Batch get CMMS measurements. */
    private suspend fun batchGetCmmsMeasurements(
      principal: MeasurementConsumerPrincipal,
      measurementNames: List<String>,
    ): BatchGetMeasurementsResponse {
      try {
        return measurementsStub
          .withAuthenticationKey(principal.config.apiKey)
          .batchGetMeasurements(
            batchGetMeasurementsRequest {
              parent = principal.resourceKey.toName()
              names += measurementNames
            }
          )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("Measurements not found.")
            Status.Code.PERMISSION_DENIED ->
              Status.PERMISSION_DENIED.withDescription(
                "Doesn't have permission to get measurements."
              )
            else -> Status.UNKNOWN.withDescription("Unable to retrieve Measurements.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Builds an [InternalMeasurement.Result]. */
    private suspend fun buildInternalMeasurementResult(
      measurement: Measurement,
      apiAuthenticationKey: String,
      principalName: String,
    ): BatchSetMeasurementResultsRequest.MeasurementResult {
      val measurementSpec: MeasurementSpec = measurement.measurementSpec.unpack()
      val encryptionPrivateKeyHandle =
        encryptionKeyPairStore.getPrivateKeyHandle(
          principalName,
          measurementSpec.measurementPublicKey.unpack<EncryptionPublicKey>().data,
        )
          ?: failGrpc(Status.FAILED_PRECONDITION) {
            "Encryption private key not found for the measurement ${measurement.name}."
          }

      val decryptedMeasurementResults: List<Measurement.Result> =
        measurement.resultsList.map {
          decryptMeasurementResultOutput(it, encryptionPrivateKeyHandle, apiAuthenticationKey)
        }

      return measurementResult {
        cmmsMeasurementId = MeasurementKey.fromName(measurement.name)!!.measurementId
        results +=
          decryptedMeasurementResults.map {
            try {
              it.toInternal(measurement.protocolConfig)
            } catch (e: NoiseMechanismUnrecognizedException) {
              failGrpc(Status.UNKNOWN) {
                listOfNotNull("Unrecognized noise mechanism.", e.message, e.cause?.message)
                  .joinToString(separator = "\n")
              }
            } catch (e: Throwable) {
              failGrpc(Status.UNKNOWN) {
                listOfNotNull("Unable to read measurement result.", e.message, e.cause?.message)
                  .joinToString(separator = "\n")
              }
            }
          }
      }
    }

    /** Decrypts a [Measurement.ResultOutput] to [Measurement.Result] */
    private suspend fun decryptMeasurementResultOutput(
      measurementResultOutput: Measurement.ResultOutput,
      encryptionPrivateKeyHandle: PrivateKeyHandle,
      apiAuthenticationKey: String,
    ): Measurement.Result {
      val certificate =
        try {
          certificateCache
            .get(
              ResourceNameApiAuthenticationKey(
                name = measurementResultOutput.certificate,
                apiAuthenticationKey = apiAuthenticationKey,
              )
            )
            .await()
        } catch (e: UncheckedExecutionException) {
          if (e.cause != null) {
            throw e.cause!!
          } else {
            throw Status.UNKNOWN.withDescription(
                "Unable to retrieve Certificate ${measurementResultOutput.certificate}."
              )
              .asRuntimeException()
          }
        }
      val signedResult =
        decryptResult(measurementResultOutput.encryptedResult, encryptionPrivateKeyHandle)

      if (certificate.revocationState != Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "${certificate.name} revocation state is ${certificate.revocationState}"
          )
          .asRuntimeException()
      }

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
      return signedResult.unpack()
    }

    /**
     * Returns the [Certificate] from the CMMS system.
     *
     * @param[name] resource name of the [Certificate]
     * @param[apiAuthenticationKey] API key to act as the [MeasurementConsumer] client
     * @throws [StatusRuntimeException] with [Status.FAILED_PRECONDITION] when retrieving the
     *   [Certificate] fails.
     */
    private suspend fun getCertificate(name: String, apiAuthenticationKey: String): Certificate {
      return try {
        certificatesStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getCertificate(getCertificateRequest { this.name = name })
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("$name not found.")
            else -> Status.UNKNOWN.withDescription("Unable to retrieve Certificate $name.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }
  }

  override suspend fun getMetric(request: GetMetricRequest): Metric {
    val metricKey =
      grpcRequireNotNull(MetricKey.fromName(request.name)) {
        "Metric name is either unspecified or invalid."
      }

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (metricKey.parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get a Metric for another MeasurementConsumer."
          }
        }
      }
    }

    val internalMetric: InternalMetric =
      getInternalMetric(metricKey.cmmsMeasurementConsumerId, metricKey.metricId)

    // Early exit when the metric is at a terminal state.
    if (internalMetric.state != Metric.State.RUNNING) {
      return internalMetric.toMetric(variances)
    }

    // Only syncs pending measurements which can only be in metrics that are still running.
    val toBeSyncedInternalMeasurements: List<InternalMeasurement> =
      internalMetric.weightedMeasurementsList
        .map { weightedMeasurement -> weightedMeasurement.measurement }
        .filter { internalMeasurement ->
          internalMeasurement.state == InternalMeasurement.State.PENDING
        }

    val anyMeasurementUpdated: Boolean =
      measurementSupplier.syncInternalMeasurements(
        toBeSyncedInternalMeasurements,
        principal.config.apiKey,
        principal,
      )

    return if (anyMeasurementUpdated) {
      getInternalMetric(metricKey.cmmsMeasurementConsumerId, metricKey.metricId).toMetric(variances)
    } else {
      internalMetric.toMetric(variances)
    }
  }

  override suspend fun batchGetMetrics(request: BatchGetMetricsRequest): BatchGetMetricsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get Metrics for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.namesList.isNotEmpty()) { "No metric name is provided." }
    grpcRequire(request.namesList.size <= MAX_BATCH_SIZE) {
      "At most $MAX_BATCH_SIZE metrics can be supported in a batch."
    }

    val metricIds: List<String> =
      request.namesList.map { metricName ->
        val metricKey =
          grpcRequireNotNull(MetricKey.fromName(metricName)) {
            "Metric name is either unspecified or invalid."
          }
        metricKey.metricId
      }

    val internalMetrics: List<InternalMetric> =
      batchGetInternalMetrics(principal.resourceKey.measurementConsumerId, metricIds)

    // Only syncs pending measurements which can only be in metrics that are still running.
    val toBeSyncedInternalMeasurements: List<InternalMeasurement> =
      internalMetrics
        .filter { internalMetric -> internalMetric.state == Metric.State.RUNNING }
        .flatMap { internalMetric -> internalMetric.weightedMeasurementsList }
        .map { weightedMeasurement -> weightedMeasurement.measurement }
        .filter { internalMeasurement ->
          internalMeasurement.state == InternalMeasurement.State.PENDING
        }

    val anyMeasurementUpdated: Boolean =
      measurementSupplier.syncInternalMeasurements(
        toBeSyncedInternalMeasurements,
        principal.config.apiKey,
        principal,
      )

    return batchGetMetricsResponse {
      metrics +=
        /**
         * TODO(@riemanli): a potential improvement can be done by only getting the metrics whose
         *   measurements are updated. Re-evaluate when a load-test is ready after deployment.
         */
        if (anyMeasurementUpdated) {
          batchGetInternalMetrics(principal.resourceKey.measurementConsumerId, metricIds).map {
            it.toMetric(variances)
          }
        } else {
          internalMetrics.map { it.toMetric(variances) }
        }
    }
  }

  override suspend fun listMetrics(request: ListMetricsRequest): ListMetricsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Metrics belonging to other MeasurementConsumers."
          }
        }
      }
    }
    val listMetricsPageToken: ListMetricsPageToken = request.toListMetricsPageToken()

    val apiAuthenticationKey: String = principal.config.apiKey

    val streamInternalMetricRequest: StreamMetricsRequest =
      listMetricsPageToken.toStreamMetricsRequest()

    val results: List<InternalMetric> =
      try {
        internalMetricsStub.streamMetrics(streamInternalMetricRequest).toList()
      } catch (e: StatusException) {
        throw Exception("Unable to list Metrics.", e)
      }

    if (results.isEmpty()) {
      return ListMetricsResponse.getDefaultInstance()
    }

    val nextPageToken: ListMetricsPageToken? =
      if (results.size > listMetricsPageToken.pageSize) {
        listMetricsPageToken.copy {
          lastMetric = previousPageEnd {
            cmmsMeasurementConsumerId = results[results.lastIndex - 1].cmmsMeasurementConsumerId
            externalMetricId = results[results.lastIndex - 1].externalMetricId
          }
        }
      } else {
        null
      }

    val subResults: List<InternalMetric> =
      results.subList(0, min(results.size, listMetricsPageToken.pageSize))

    // Only syncs pending measurements which can only be in metrics that are still running.
    val toBeSyncedInternalMeasurements: List<InternalMeasurement> =
      subResults
        .filter { internalMetric -> internalMetric.state == Metric.State.RUNNING }
        .flatMap { internalMetric -> internalMetric.weightedMeasurementsList }
        .map { weightedMeasurement -> weightedMeasurement.measurement }
        .filter { internalMeasurement ->
          internalMeasurement.state == InternalMeasurement.State.PENDING
        }

    val anyMeasurementUpdated: Boolean =
      measurementSupplier.syncInternalMeasurements(
        toBeSyncedInternalMeasurements,
        apiAuthenticationKey,
        principal,
      )

    /**
     * If any measurement got updated, pull the list of the up-to-date internal metrics. Otherwise,
     * use the original list.
     *
     * TODO(@riemanli): a potential improvement can be done by only getting the metrics whose
     *   measurements are updated. Re-evaluate when a load-test is ready after deployment.
     */
    val internalMetrics: List<InternalMetric> =
      if (anyMeasurementUpdated) {
        batchGetInternalMetrics(
          principal.resourceKey.measurementConsumerId,
          subResults.map { internalMetric -> internalMetric.externalMetricId },
        )
      } else {
        subResults
      }

    return listMetricsResponse {
      metrics += internalMetrics.map { it.toMetric(variances) }

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Gets a batch of [InternalMetric]s. */
  private suspend fun batchGetInternalMetrics(
    cmmsMeasurementConsumerId: String,
    metricIds: List<String>,
  ): List<InternalMetric> {
    val batchGetMetricsRequest = batchGetMetricsRequest {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalMetricIds += metricIds
    }

    return try {
      internalMetricsStub.batchGetMetrics(batchGetMetricsRequest).metricsList
    } catch (e: StatusException) {
      throw Exception("Unable to get Metrics.", e)
    }
  }

  /** Gets an [InternalMetric]. */
  private suspend fun getInternalMetric(
    cmmsMeasurementConsumerId: String,
    metricId: String,
  ): InternalMetric {
    return try {
      batchGetInternalMetrics(cmmsMeasurementConsumerId, listOf(metricId)).first()
    } catch (e: StatusException) {
      val metricName = MetricKey(cmmsMeasurementConsumerId, metricId).toName()
      throw Exception("Unable to get the Metric with the name = [${metricName}].", e)
    }
  }

  override suspend fun createMetric(request: CreateMetricRequest): Metric {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Metric for another MeasurementConsumer."
          }
        }
      }
    }

    val internalCreateMetricRequest: InternalCreateMetricRequest =
      buildInternalCreateMetricRequest(principal.resourceKey.measurementConsumerId, request)

    val internalMetric =
      try {
        internalMetricsStub.createMetric(internalCreateMetricRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.ALREADY_EXISTS ->
              Status.ALREADY_EXISTS.withDescription(
                "Metric with ID ${request.metricId} already exists under ${request.parent}"
              )
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("Reporting set used in the metric not found.")
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription(
                "Unable to create the metric. The measurement consumer not found."
              )
            else -> Status.UNKNOWN.withDescription("Unable to create Metric.")
          }
          .withCause(e)
          .asRuntimeException()
      }

    if (internalMetric.state == Metric.State.RUNNING) {
      measurementSupplier.createCmmsMeasurements(listOf(internalMetric), principal)
    }

    // Convert the internal metric to public and return it.
    return internalMetric.toMetric(variances)
  }

  override suspend fun batchCreateMetrics(
    request: BatchCreateMetricsRequest
  ): BatchCreateMetricsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Metric for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.requestsList.isNotEmpty()) { "Requests is empty." }
    grpcRequire(request.requestsList.size <= MAX_BATCH_SIZE) {
      "At most $MAX_BATCH_SIZE requests can be supported in a batch."
    }

    val metricIds = request.requestsList.map { it.metricId }
    grpcRequire(metricIds.size == metricIds.distinct().size) {
      "Duplicate metric IDs in the request."
    }

    val internalCreateMetricRequestsList: List<InternalCreateMetricRequest> =
      request.requestsList.map { createMetricRequest ->
        buildInternalCreateMetricRequest(parentKey.measurementConsumerId, createMetricRequest)
      }

    val internalMetrics =
      try {
        internalMetricsStub
          .batchCreateMetrics(
            internalBatchCreateMetricsRequest {
              cmmsMeasurementConsumerId = parentKey.measurementConsumerId
              requests += internalCreateMetricRequestsList
            }
          )
          .metricsList
          .filter { internalMetric -> internalMetric.state == Metric.State.RUNNING }
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("Reporting set used in metrics not found.")
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription(
                "Unable to create the metrics. The measurement consumer not found."
              )
            else -> Status.UNKNOWN.withDescription("Unable to create Metrics.")
          }
          .withCause(e)
          .asRuntimeException()
      }

    measurementSupplier.createCmmsMeasurements(internalMetrics, principal)

    // Convert the internal metric to public and return it.
    return batchCreateMetricsResponse { metrics += internalMetrics.map { it.toMetric(variances) } }
  }

  /** Builds an [InternalCreateMetricRequest]. */
  private suspend fun buildInternalCreateMetricRequest(
    cmmsMeasurementConsumerId: String,
    request: CreateMetricRequest,
  ): InternalCreateMetricRequest {
    grpcRequire(request.hasMetric()) { "Metric is not specified." }

    grpcRequire(request.metricId.matches(RESOURCE_ID_REGEX)) { "Metric ID is invalid." }
    grpcRequire(request.metric.reportingSet.isNotEmpty()) {
      "Reporting set in metric is not specified."
    }
    grpcRequire(request.metric.hasTimeInterval()) { "Time interval in metric is not specified." }
    grpcRequire(
      request.metric.timeInterval.startTime.seconds > 0 ||
        request.metric.timeInterval.startTime.nanos > 0
    ) {
      "TimeInterval startTime is unspecified."
    }
    grpcRequire(
      request.metric.timeInterval.endTime.seconds > 0 ||
        request.metric.timeInterval.endTime.nanos > 0
    ) {
      "TimeInterval endTime is unspecified."
    }
    grpcRequire(
      request.metric.timeInterval.endTime.seconds > request.metric.timeInterval.startTime.seconds ||
        request.metric.timeInterval.endTime.nanos > request.metric.timeInterval.startTime.nanos
    ) {
      "TimeInterval endTime is not later than startTime."
    }
    grpcRequire(request.metric.hasMetricSpec()) { "Metric spec in metric is not specified." }

    val internalReportingSet: InternalReportingSet =
      getInternalReportingSet(cmmsMeasurementConsumerId, request.metric.reportingSet)

    // Utilizes the property of the set expression compilation result -- If the set expression
    // contains only union operators, the compilation result has to be a single component.
    if (
      request.metric.metricSpec.hasReachAndFrequency() &&
        internalReportingSet.weightedSubsetUnionsList.size != 1
    ) {
      failGrpc(Status.INVALID_ARGUMENT) {
        "Reach-and-frequency metrics can only be computed on union-only set expressions."
      }
    }

    return internalCreateMetricRequest {
      requestId = request.requestId
      externalMetricId = request.metricId
      metric = internalMetric {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalReportingSetId = internalReportingSet.externalReportingSetId
        timeInterval = request.metric.timeInterval
        metricSpec =
          try {
            request.metric.metricSpec.withDefaults(metricSpecConfig).toInternal()
          } catch (e: MetricSpecDefaultsException) {
            failGrpc(Status.INVALID_ARGUMENT) {
              listOfNotNull("Invalid metric spec.", e.message, e.cause?.message)
                .joinToString(separator = "\n")
            }
          } catch (e: Exception) {
            failGrpc(Status.UNKNOWN) { "Failed to read the metric spec." }
          }
        weightedMeasurements +=
          buildInitialInternalMeasurements(
            cmmsMeasurementConsumerId,
            request.metric,
            internalReportingSet,
          )
        details = InternalMetricKt.details { filters += request.metric.filtersList }
      }
    }
  }

  /** Builds [InternalMeasurement]s for a [Metric] over an [InternalReportingSet]. */
  private fun buildInitialInternalMeasurements(
    cmmsMeasurementConsumerId: String,
    metric: Metric,
    internalReportingSet: InternalReportingSet,
  ): List<WeightedMeasurement> {
    return internalReportingSet.weightedSubsetUnionsList.map { weightedSubsetUnion ->
      weightedMeasurement {
        weight = weightedSubsetUnion.weight
        binaryRepresentation = weightedSubsetUnion.binaryRepresentation
        measurement = internalMeasurement {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          timeInterval = metric.timeInterval
          this.primitiveReportingSetBases +=
            weightedSubsetUnion.primitiveReportingSetBasesList.map { primitiveReportingSetBasis ->
              primitiveReportingSetBasis.copy { filters += metric.filtersList }
            }
        }
      }
    }
  }

  /** Gets an [InternalReportingSet] based on a reporting set name. */
  private suspend fun getInternalReportingSet(
    cmmsMeasurementConsumerId: String,
    reportingSetName: String,
  ): InternalReportingSet {
    val reportingSetKey =
      grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
        "Invalid reporting set name $reportingSetName."
      }

    if (reportingSetKey.cmmsMeasurementConsumerId != cmmsMeasurementConsumerId) {
      failGrpc(Status.PERMISSION_DENIED) { "No access to the reporting set [$reportingSetName]." }
    }

    return try {
      internalReportingSetsStub
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalReportingSetIds += reportingSetKey.reportingSetId
          }
        )
        .reportingSetsList
        .first()
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve ReportingSet using the provided name [$reportingSetName].",
        e,
      )
    }
  }

  companion object {
    private val RESOURCE_ID_REGEX = Regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
  }
}

/** Converts a public [ListMetricsRequest] to a [ListMetricsPageToken]. */
fun ListMetricsRequest.toListMetricsPageToken(): ListMetricsPageToken {
  val source = this

  grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0." }

  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(source.parent)) {
      "Parent is either unspecified or invalid."
    }
  val cmmsMeasurementConsumerId = parentKey.measurementConsumerId

  return if (pageToken.isNotBlank()) {
    ListMetricsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
        "Arguments must be kept the same when using a page token."
      }

      if (source.pageSize in MIN_PAGE_SIZE..MAX_PAGE_SIZE) {
        pageSize = source.pageSize
      }
    }
  } else {
    listMetricsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    }
  }
}

/** Converts an [InternalMetric] to a public [Metric]. */
private fun InternalMetric.toMetric(variances: Variances): Metric {
  val source = this
  return metric {
    name =
      MetricKey(
          cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId,
          metricId = source.externalMetricId,
        )
        .toName()
    reportingSet =
      ReportingSetKey(source.cmmsMeasurementConsumerId, source.externalReportingSetId).toName()
    timeInterval = source.timeInterval
    metricSpec = source.metricSpec.toMetricSpec()
    filters += source.details.filtersList
    state = source.state
    createTime = source.createTime
    if (state == Metric.State.SUCCEEDED) {
      result = buildMetricResult(source, variances)
    }
  }
}

/** Builds a [MetricResult] from the given [InternalMetric]. */
private fun buildMetricResult(metric: InternalMetric, variances: Variances): MetricResult {
  return metricResult {
    cmmsMeasurements +=
      metric.weightedMeasurementsList.map {
        MeasurementKey(metric.cmmsMeasurementConsumerId, it.measurement.cmmsMeasurementId).toName()
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (metric.metricSpec.typeCase) {
      InternalMetricSpec.TypeCase.REACH -> {
        reach =
          calculateReachResult(
            metric.weightedMeasurementsList,
            metric.metricSpec.vidSamplingInterval,
            metric.metricSpec.reach.privacyParams,
            variances,
          )
      }
      InternalMetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
        reachAndFrequency = reachAndFrequencyResult {
          reach =
            calculateReachResult(
              metric.weightedMeasurementsList,
              metric.metricSpec.vidSamplingInterval,
              metric.metricSpec.reachAndFrequency.reachPrivacyParams,
              variances,
            )
          frequencyHistogram =
            calculateFrequencyHistogramResults(
              metric.weightedMeasurementsList,
              metric.metricSpec,
              variances,
            )
        }
      }
      InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount =
          calculateImpressionResult(metric.weightedMeasurementsList, metric.metricSpec, variances)
      }
      InternalMetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration =
          calculateWatchDurationResult(
            metric.weightedMeasurementsList,
            metric.metricSpec,
            variances,
          )
      }
      InternalMetricSpec.TypeCase.POPULATION_COUNT -> {
        populationCount = calculatePopulationResult(metric.weightedMeasurementsList)
      }
      InternalMetricSpec.TypeCase.TYPE_NOT_SET -> {
        failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
          "Metric Type should've been set."
        }
      }
    }
  }
}

/** Aggregates a list of [InternalMeasurement.Result]s to a [InternalMeasurement.Result] */
private fun aggregateResults(
  internalMeasurementResults: List<InternalMeasurement.Result>
): InternalMeasurement.Result {
  if (internalMeasurementResults.isEmpty()) {
    failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
      "No measurement result."
    }
  }
  var reachValue = 0L
  var impressionValue = 0L
  val frequencyDistribution = mutableMapOf<Long, Double>()
  var watchDurationValue = duration {
    seconds = 0
    nanos = 0
  }
  var populationValue = 0L

  // Aggregation
  for (result in internalMeasurementResults) {
    if (result.hasFrequency()) {
      if (!result.hasReach()) {
        failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
          "Missing reach measurement in the Reach-Frequency measurement."
        }
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
    if (result.hasPopulation()) {
      populationValue += result.population.value
    }
  }

  return InternalMeasurementKt.result {
    if (internalMeasurementResults.first().hasReach()) {
      this.reach = InternalMeasurementKt.ResultKt.reach { value = reachValue }
    }
    if (internalMeasurementResults.first().hasFrequency()) {
      this.frequency =
        InternalMeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(frequencyDistribution)
        }
    }
    if (internalMeasurementResults.first().hasImpression()) {
      this.impression = InternalMeasurementKt.ResultKt.impression { value = impressionValue }
    }
    if (internalMeasurementResults.first().hasWatchDuration()) {
      this.watchDuration =
        InternalMeasurementKt.ResultKt.watchDuration { value = watchDurationValue }
    }
    if (internalMeasurementResults.first().hasPopulation()) {
      this.population = InternalMeasurementKt.ResultKt.population { value = populationValue }
    }
  }
}

/** Calculates the watch duration result from [WeightedMeasurement]s. */
private fun calculateWatchDurationResult(
  weightedMeasurements: List<WeightedMeasurement>,
  metricSpec: InternalMetricSpec,
  variances: Variances,
): MetricResult.WatchDurationResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasWatchDuration() }) {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "Watch duration measurement result is missing."
      }
    }
  }
  return watchDurationResult {
    val watchDuration: Duration =
      weightedMeasurements
        .map { weightedMeasurement ->
          aggregateResults(weightedMeasurement.measurement.details.resultsList)
            .watchDuration
            .value * weightedMeasurement.weight
        }
        .reduce { sum, element -> sum + element }
    value = watchDuration.toDoubleSecond()

    // Only compute univariate statistics for union-only operations, i.e. single source measurement.
    if (weightedMeasurements.size == 1) {
      val weightedMeasurement = weightedMeasurements.first()
      val weightedMeasurementVarianceParamsList:
        List<WeightedWatchDurationMeasurementVarianceParams?> =
        buildWeightedWatchDurationMeasurementVarianceParamsPerResult(
          weightedMeasurement,
          metricSpec,
        )

      // If any measurement result contains insufficient data for variance calculation, univariate
      // statistics won't be computed.
      if (weightedMeasurementVarianceParamsList.all { it != null }) {
        univariateStatistics = univariateStatistics {
          // Watch duration results in a measurement are independent to each other. The variance is
          // the sum of the variances of each result.
          standardDeviation =
            sqrt(
              weightedMeasurementVarianceParamsList.sumOf { weightedMeasurementVarianceParams ->
                try {
                  variances.computeMetricVariance(
                    WatchDurationMetricVarianceParams(
                      listOf(requireNotNull(weightedMeasurementVarianceParams))
                    )
                  )
                } catch (e: Throwable) {
                  failGrpc(Status.UNKNOWN) {
                    listOfNotNull(
                        "Unable to compute variance of watch duration metric.",
                        e.message,
                        e.cause?.message,
                      )
                      .joinToString(separator = "\n")
                  }
                }
              }
            )
        }
      }
    }
  }
}

/** Calculates the population result from [WeightedMeasurement]s. */
private fun calculatePopulationResult(
  weightedMeasurements: List<WeightedMeasurement>
): MetricResult.PopulationCountResult {
  // Only take the first measurement because Population measurements will only have one element.
  val populationResult =
    aggregateResults(weightedMeasurements.single().measurement.details.resultsList)
  return populationCountResult { value = populationResult.population.value }
}

/** Converts [Duration] format to [Double] second. */
private fun Duration.toDoubleSecond(): Double {
  val source = this
  return source.seconds + (source.nanos.toDouble() / NANOS_PER_SECOND)
}

/**
 * Builds a list of nullable [WeightedWatchDurationMeasurementVarianceParams].
 *
 * @throws io.grpc.StatusRuntimeException when measurement noise mechanism is unrecognized.
 */
fun buildWeightedWatchDurationMeasurementVarianceParamsPerResult(
  weightedMeasurement: WeightedMeasurement,
  metricSpec: MetricSpec,
): List<WeightedWatchDurationMeasurementVarianceParams?> {
  val watchDurationResults: List<InternalMeasurement.Result.WatchDuration> =
    weightedMeasurement.measurement.details.resultsList.map { it.watchDuration }

  if (watchDurationResults.isEmpty()) {
    failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
      "WatchDuration measurement should've had results."
    }
  }

  return watchDurationResults.map { watchDurationResult ->
    val statsNoiseMechanism: StatsNoiseMechanism =
      try {
        watchDurationResult.noiseMechanism.toStatsNoiseMechanism()
      } catch (e: NoiseMechanismUnspecifiedException) {
        return@map null
      } catch (e: NoiseMechanismUnrecognizedException) {
        failGrpc(Status.UNKNOWN) {
          listOfNotNull(
              "Unrecognized noise mechanism should've been caught earlier.",
              e.message,
              e.cause?.message,
            )
            .joinToString(separator = "\n")
        }
      }

    val methodology: Methodology =
      try {
        buildStatsMethodology(watchDurationResult)
      } catch (e: MeasurementVarianceNotComputableException) {
        return@map null
      }

    WeightedWatchDurationMeasurementVarianceParams(
      binaryRepresentation = weightedMeasurement.binaryRepresentation,
      weight = weightedMeasurement.weight,
      measurementVarianceParams =
        WatchDurationMeasurementVarianceParams(
          duration = max(0.0, watchDurationResult.value.toDoubleSecond()),
          measurementParams =
            WatchDurationMeasurementParams(
              vidSamplingInterval = metricSpec.vidSamplingInterval.toStatsVidSamplingInterval(),
              dpParams = metricSpec.watchDuration.privacyParams.toNoiserDpParams(),
              maximumDurationPerUser =
                metricSpec.watchDuration.maximumWatchDurationPerUser.toDoubleSecond(),
              noiseMechanism = statsNoiseMechanism,
            ),
        ),
      methodology = methodology,
    )
  }
}

/** Builds a [Methodology] from an [InternalMeasurement.Result.WatchDuration]. */
fun buildStatsMethodology(
  watchDurationResult: InternalMeasurement.Result.WatchDuration
): Methodology {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (watchDurationResult.methodologyCase) {
    InternalMeasurement.Result.WatchDuration.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (watchDurationResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          CustomDirectScalarMethodology(watchDurationResult.customDirectMethodology.variance.scalar)
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Custom direct methodology for frequency is not supported for watch duration."
          }
        }
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          throw MeasurementVarianceNotComputableException(
            "Watch duration computed from a custom methodology doesn't have variance."
          )
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Variance in CustomDirectMethodology should've been set."
          }
        }
      }
    }
    InternalMeasurement.Result.WatchDuration.MethodologyCase.DETERMINISTIC_SUM -> {
      DeterministicMethodology
    }
    InternalMeasurement.Result.WatchDuration.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Watch duration methodology is not set.")
    }
  }
}

/** Calculates the impression result from [WeightedMeasurement]s. */
private fun calculateImpressionResult(
  weightedMeasurements: List<WeightedMeasurement>,
  metricSpec: InternalMetricSpec,
  variances: Variances,
): MetricResult.ImpressionCountResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasImpression() }) {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "Impression measurement result is missing."
      }
    }
  }

  return impressionCountResult {
    value =
      weightedMeasurements.sumOf { weightedMeasurement ->
        aggregateResults(weightedMeasurement.measurement.details.resultsList).impression.value *
          weightedMeasurement.weight
      }

    // Only compute univariate statistics for union-only operations, i.e. single source measurement.
    if (weightedMeasurements.size == 1) {
      val weightedMeasurement = weightedMeasurements.first()
      val weightedMeasurementVarianceParamsList:
        List<WeightedImpressionMeasurementVarianceParams?> =
        buildWeightedImpressionMeasurementVarianceParamsPerResult(weightedMeasurement, metricSpec)

      // If any measurement result contains insufficient data for variance calculation, univariate
      // statistics won't be computed.
      if (weightedMeasurementVarianceParamsList.all { it != null }) {
        univariateStatistics = univariateStatistics {
          // Impression results in a measurement are independent to each other. The variance is the
          // sum of the variances of each result.
          standardDeviation =
            sqrt(
              weightedMeasurementVarianceParamsList.sumOf { weightedMeasurementVarianceParams ->
                try {
                  variances.computeMetricVariance(
                    ImpressionMetricVarianceParams(
                      listOf(requireNotNull(weightedMeasurementVarianceParams))
                    )
                  )
                } catch (e: Throwable) {
                  failGrpc(Status.UNKNOWN) {
                    listOfNotNull(
                        "Unable to compute variance of impression metric.",
                        e.message,
                        e.cause?.message,
                      )
                      .joinToString(separator = "\n")
                  }
                }
              }
            )
        }
      }
    }
  }
}

/**
 * Builds a list of nullable [WeightedImpressionMeasurementVarianceParams].
 *
 * @throws io.grpc.StatusRuntimeException when measurement noise mechanism is unrecognized.
 */
fun buildWeightedImpressionMeasurementVarianceParamsPerResult(
  weightedMeasurement: WeightedMeasurement,
  metricSpec: MetricSpec,
): List<WeightedImpressionMeasurementVarianceParams?> {
  val impressionResults: List<InternalMeasurement.Result.Impression> =
    weightedMeasurement.measurement.details.resultsList.map { it.impression }

  if (impressionResults.isEmpty()) {
    failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
      "Impression measurement should've had results."
    }
  }

  return impressionResults.map { impressionResult ->
    val statsNoiseMechanism: StatsNoiseMechanism =
      try {
        impressionResult.noiseMechanism.toStatsNoiseMechanism()
      } catch (e: NoiseMechanismUnspecifiedException) {
        return@map null
      } catch (e: NoiseMechanismUnrecognizedException) {
        failGrpc(Status.UNKNOWN) {
          listOfNotNull(
              "Unrecognized noise mechanism should've been caught earlier.",
              e.message,
              e.cause?.message,
            )
            .joinToString(separator = "\n")
        }
      }

    val methodology: Methodology =
      try {
        buildStatsMethodology(impressionResult)
      } catch (e: MeasurementVarianceNotComputableException) {
        return@map null
      }

    WeightedImpressionMeasurementVarianceParams(
      binaryRepresentation = weightedMeasurement.binaryRepresentation,
      weight = weightedMeasurement.weight,
      measurementVarianceParams =
        ImpressionMeasurementVarianceParams(
          impression = max(0L, impressionResult.value),
          measurementParams =
            ImpressionMeasurementParams(
              vidSamplingInterval = metricSpec.vidSamplingInterval.toStatsVidSamplingInterval(),
              dpParams = metricSpec.impressionCount.privacyParams.toNoiserDpParams(),
              maximumFrequencyPerUser = metricSpec.impressionCount.maximumFrequencyPerUser,
              noiseMechanism = statsNoiseMechanism,
            ),
        ),
      methodology = methodology,
    )
  }
}

/** Builds a [Methodology] from an [InternalMeasurement.Result.Impression]. */
fun buildStatsMethodology(impressionResult: InternalMeasurement.Result.Impression): Methodology {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (impressionResult.methodologyCase) {
    InternalMeasurement.Result.Impression.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (impressionResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          CustomDirectScalarMethodology(impressionResult.customDirectMethodology.variance.scalar)
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Custom direct methodology for frequency is not supported for impression."
          }
        }
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          throw MeasurementVarianceNotComputableException(
            "Impression computed from a custom methodology doesn't have variance."
          )
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Variance case in CustomDirectMethodology should've been set."
          }
        }
      }
    }
    InternalMeasurement.Result.Impression.MethodologyCase.DETERMINISTIC_COUNT -> {
      DeterministicMethodology
    }
    InternalMeasurement.Result.Impression.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Impression methodology is not set.")
    }
  }
}

/** Calculates the frequency histogram result from [WeightedMeasurement]s. */
private fun calculateFrequencyHistogramResults(
  weightedMeasurements: List<WeightedMeasurement>,
  metricSpec: InternalMetricSpec,
  variances: Variances,
): MetricResult.HistogramResult {
  val aggregatedFrequencyHistogramMap: MutableMap<Long, Double> =
    weightedMeasurements
      .map { weightedMeasurement ->
        if (
          weightedMeasurement.measurement.details.resultsList.any {
            !it.hasReach() || !it.hasFrequency()
          }
        ) {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Reach-Frequency measurement is missing."
          }
        }
        val result = aggregateResults(weightedMeasurement.measurement.details.resultsList)
        val reach = result.reach.value
        result.frequency.relativeFrequencyDistributionMap.mapValues { (_, rate) ->
          rate * weightedMeasurement.weight * reach
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

  // Fill the buckets that don't have any count with zeros.
  for (frequency in (1L..metricSpec.reachAndFrequency.maximumFrequency)) {
    if (!aggregatedFrequencyHistogramMap.containsKey(frequency)) {
      aggregatedFrequencyHistogramMap[frequency] = 0.0
    }
  }

  val weightedMeasurementVarianceParamsList: List<WeightedFrequencyMeasurementVarianceParams> =
    weightedMeasurements.mapNotNull { weightedMeasurement ->
      buildWeightedFrequencyMeasurementVarianceParams(weightedMeasurement, metricSpec, variances)
    }

  val frequencyVariances: FrequencyVariances? =
    if (weightedMeasurementVarianceParamsList.size == weightedMeasurements.size) {
      try {
        variances.computeMetricVariance(
          FrequencyMetricVarianceParams(weightedMeasurementVarianceParamsList)
        )
      } catch (e: Throwable) {
        failGrpc(Status.UNKNOWN) {
          listOfNotNull(
              "Unable to compute variance of reach-frequency metric.",
              e.message,
              e.cause?.message,
            )
            .joinToString(separator = "\n")
        }
      }
    } else {
      null
    }

  return histogramResult {
    bins +=
      aggregatedFrequencyHistogramMap.map { (frequency, count) ->
        bin {
          label = frequency.toString()
          binResult = binResult { value = count }
          if (frequencyVariances != null) {
            resultUnivariateStatistics = univariateStatistics {
              standardDeviation =
                sqrt(frequencyVariances.countVariances.getValue(frequency.toInt()))
            }
            relativeUnivariateStatistics = univariateStatistics {
              standardDeviation =
                sqrt(frequencyVariances.relativeVariances.getValue(frequency.toInt()))
            }
            kPlusUnivariateStatistics = univariateStatistics {
              standardDeviation =
                sqrt(frequencyVariances.kPlusCountVariances.getValue(frequency.toInt()))
            }
            relativeKPlusUnivariateStatistics = univariateStatistics {
              standardDeviation =
                sqrt(frequencyVariances.kPlusRelativeVariances.getValue(frequency.toInt()))
            }
          }
        }
      }
  }
}

/**
 * Builds a [WeightedFrequencyMeasurementVarianceParams].
 *
 * @return null when measurement noise mechanism is not specified or measurement methodology is not
 *   set.
 * @throws io.grpc.StatusRuntimeException when measurement noise mechanism is unrecognized.
 */
fun buildWeightedFrequencyMeasurementVarianceParams(
  weightedMeasurement: WeightedMeasurement,
  metricSpec: MetricSpec,
  variances: Variances,
): WeightedFrequencyMeasurementVarianceParams? {
  // Get reach measurement variance params
  val weightedReachMeasurementVarianceParams: WeightedReachMeasurementVarianceParams =
    buildWeightedReachMeasurementVarianceParams(
      weightedMeasurement,
      metricSpec.vidSamplingInterval,
      metricSpec.reachAndFrequency.reachPrivacyParams,
    ) ?: return null

  val reachMeasurementVariance: Double =
    variances.computeMeasurementVariance(
      weightedReachMeasurementVarianceParams.methodology,
      ReachMeasurementVarianceParams(
        weightedReachMeasurementVarianceParams.measurementVarianceParams.reach,
        weightedReachMeasurementVarianceParams.measurementVarianceParams.measurementParams,
      ),
    )

  val frequencyResult: InternalMeasurement.Result.Frequency =
    if (weightedMeasurement.measurement.details.resultsList.size == 1) {
      weightedMeasurement.measurement.details.resultsList.first().frequency
    } else if (weightedMeasurement.measurement.details.resultsList.size > 1) {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "No supported methodology generates more than one frequency result."
      }
    } else {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "Frequency measurement should've had frequency results."
      }
    }

  val frequencyStatsNoiseMechanism: StatsNoiseMechanism =
    try {
      frequencyResult.noiseMechanism.toStatsNoiseMechanism()
    } catch (e: NoiseMechanismUnspecifiedException) {
      return null
    } catch (e: NoiseMechanismUnrecognizedException) {
      failGrpc(Status.UNKNOWN) {
        listOfNotNull(
            "Unrecognized noise mechanism should've been caught earlier.",
            e.message,
            e.cause?.message,
          )
          .joinToString(separator = "\n")
      }
    }

  val frequencyMethodology: Methodology =
    try {
      buildStatsMethodology(frequencyResult)
    } catch (e: MeasurementVarianceNotComputableException) {
      return null
    }

  return WeightedFrequencyMeasurementVarianceParams(
    binaryRepresentation = weightedMeasurement.binaryRepresentation,
    weight = weightedMeasurement.weight,
    measurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach = weightedReachMeasurementVarianceParams.measurementVarianceParams.reach,
        reachMeasurementVariance = reachMeasurementVariance,
        relativeFrequencyDistribution =
          frequencyResult.relativeFrequencyDistributionMap.mapKeys { it.key.toInt() },
        measurementParams =
          FrequencyMeasurementParams(
            vidSamplingInterval = metricSpec.vidSamplingInterval.toStatsVidSamplingInterval(),
            dpParams = metricSpec.reachAndFrequency.frequencyPrivacyParams.toNoiserDpParams(),
            noiseMechanism = frequencyStatsNoiseMechanism,
            maximumFrequency = metricSpec.reachAndFrequency.maximumFrequency,
          ),
      ),
    methodology = frequencyMethodology,
  )
}

/** Builds a [Methodology] from an [InternalMeasurement.Result.Frequency]. */
fun buildStatsMethodology(frequencyResult: InternalMeasurement.Result.Frequency): Methodology {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (frequencyResult.methodologyCase) {
    InternalMeasurement.Result.Frequency.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (frequencyResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Custom direct methodology for scalar is not supported for frequency."
          }
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          CustomDirectFrequencyMethodology(
            frequencyResult.customDirectMethodology.variance.frequency.variancesMap.mapKeys {
              it.key.toInt()
            },
            frequencyResult.customDirectMethodology.variance.frequency.kPlusVariancesMap.mapKeys {
              it.key.toInt()
            },
          )
        }
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          throw MeasurementVarianceNotComputableException(
            "Frequency computed from a custom methodology doesn't have variance."
          )
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Variance case in CustomDirectMethodology should've been set."
          }
        }
      }
    }
    InternalMeasurement.Result.Frequency.MethodologyCase.DETERMINISTIC_DISTRIBUTION -> {
      DeterministicMethodology
    }
    InternalMeasurement.Result.Frequency.MethodologyCase.LIQUID_LEGIONS_DISTRIBUTION -> {
      LiquidLegionsSketchMethodology(
        decayRate = frequencyResult.liquidLegionsDistribution.decayRate,
        sketchSize = frequencyResult.liquidLegionsDistribution.maxSize,
      )
    }
    InternalMeasurement.Result.Frequency.MethodologyCase.LIQUID_LEGIONS_V2 -> {
      LiquidLegionsV2Methodology(
        decayRate = frequencyResult.liquidLegionsV2.sketchParams.decayRate,
        sketchSize = frequencyResult.liquidLegionsV2.sketchParams.maxSize,
        samplingIndicatorSize = frequencyResult.liquidLegionsV2.sketchParams.samplingIndicatorSize,
      )
    }
    InternalMeasurement.Result.Frequency.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Frequency methodology is not set.")
    }
  }
}

/** Calculates the reach result from [WeightedMeasurement]s. */
private fun calculateReachResult(
  weightedMeasurements: List<WeightedMeasurement>,
  vidSamplingInterval: InternalMetricSpec.VidSamplingInterval,
  privacyParams: InternalMetricSpec.DifferentialPrivacyParams,
  variances: Variances,
): MetricResult.ReachResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasReach() }) {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "Reach measurement result is missing."
      }
    }
  }

  return reachResult {
    value =
      weightedMeasurements.sumOf { weightedMeasurement ->
        aggregateResults(weightedMeasurement.measurement.details.resultsList).reach.value *
          weightedMeasurement.weight
      }

    val weightedMeasurementVarianceParamsList: List<WeightedReachMeasurementVarianceParams> =
      weightedMeasurements.mapNotNull { weightedMeasurement ->
        buildWeightedReachMeasurementVarianceParams(
          weightedMeasurement,
          vidSamplingInterval,
          privacyParams,
        )
      }

    // If any measurement contains insufficient data for variance calculation, univariate statistics
    // won't be computed.
    if (weightedMeasurementVarianceParamsList.size == weightedMeasurements.size) {
      univariateStatistics = univariateStatistics {
        standardDeviation =
          sqrt(
            try {
              variances.computeMetricVariance(
                ReachMetricVarianceParams(weightedMeasurementVarianceParamsList)
              )
            } catch (e: Throwable) {
              failGrpc(Status.UNKNOWN) {
                listOfNotNull(
                    "Unable to compute variance of reach metric.",
                    e.message,
                    e.cause?.message,
                  )
                  .joinToString(separator = "\n")
              }
            }
          )
      }
    }
  }
}

/**
 * Builds a nullable [WeightedReachMeasurementVarianceParams].
 *
 * @return null when measurement noise mechanism is not specified or measurement methodology is not
 *   set.
 * @throws io.grpc.StatusRuntimeException when measurement noise mechanism is unrecognized.
 */
private fun buildWeightedReachMeasurementVarianceParams(
  weightedMeasurement: WeightedMeasurement,
  vidSamplingInterval: InternalMetricSpec.VidSamplingInterval,
  privacyParams: InternalMetricSpec.DifferentialPrivacyParams,
): WeightedReachMeasurementVarianceParams? {
  val reachResult =
    if (weightedMeasurement.measurement.details.resultsList.size == 1) {
      weightedMeasurement.measurement.details.resultsList.first().reach
    } else if (weightedMeasurement.measurement.details.resultsList.size > 1) {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "No supported methodology generates more than one reach result."
      }
    } else {
      failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
        "Reach measurement should've had reach results."
      }
    }

  val statsNoiseMechanism: StatsNoiseMechanism =
    try {
      reachResult.noiseMechanism.toStatsNoiseMechanism()
    } catch (e: NoiseMechanismUnspecifiedException) {
      return null
    } catch (e: NoiseMechanismUnrecognizedException) {
      failGrpc(Status.UNKNOWN) {
        listOfNotNull(
            "Unrecognized noise mechanism should've been caught earlier.",
            e.message,
            e.cause?.message,
          )
          .joinToString(separator = "\n")
      }
    }

  val methodology: Methodology =
    try {
      buildStatsMethodology(reachResult)
    } catch (e: MeasurementVarianceNotComputableException) {
      return null
    }

  return WeightedReachMeasurementVarianceParams(
    binaryRepresentation = weightedMeasurement.binaryRepresentation,
    weight = weightedMeasurement.weight,
    measurementVarianceParams =
      ReachMeasurementVarianceParams(
        reach = max(0L, reachResult.value),
        measurementParams =
          ReachMeasurementParams(
            vidSamplingInterval = vidSamplingInterval.toStatsVidSamplingInterval(),
            dpParams = privacyParams.toNoiserDpParams(),
            noiseMechanism = statsNoiseMechanism,
          ),
      ),
    methodology = methodology,
  )
}

/** Builds a [Methodology] from an [InternalMeasurement.Result.Reach]. */
fun buildStatsMethodology(reachResult: InternalMeasurement.Result.Reach): Methodology {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (reachResult.methodologyCase) {
    InternalMeasurement.Result.Reach.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (reachResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          CustomDirectScalarMethodology(reachResult.customDirectMethodology.variance.scalar)
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Custom direct methodology for frequency is not supported for reach."
          }
        }
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          throw MeasurementVarianceNotComputableException(
            "Reach computed from a custom methodology doesn't have variance."
          )
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
            "Variance case in CustomDirectMethodology should've been set."
          }
        }
      }
    }
    InternalMeasurement.Result.Reach.MethodologyCase.DETERMINISTIC_COUNT_DISTINCT -> {
      DeterministicMethodology
    }
    InternalMeasurement.Result.Reach.MethodologyCase.LIQUID_LEGIONS_COUNT_DISTINCT -> {
      LiquidLegionsSketchMethodology(
        decayRate = reachResult.liquidLegionsCountDistinct.decayRate,
        sketchSize = reachResult.liquidLegionsCountDistinct.maxSize,
      )
    }
    InternalMeasurement.Result.Reach.MethodologyCase.LIQUID_LEGIONS_V2 -> {
      LiquidLegionsV2Methodology(
        decayRate = reachResult.liquidLegionsV2.sketchParams.decayRate,
        sketchSize = reachResult.liquidLegionsV2.sketchParams.maxSize,
        samplingIndicatorSize = reachResult.liquidLegionsV2.sketchParams.samplingIndicatorSize,
      )
    }
    InternalMeasurement.Result.Reach.MethodologyCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
      LiquidLegionsV2Methodology(
        decayRate = reachResult.reachOnlyLiquidLegionsV2.sketchParams.decayRate,
        sketchSize = reachResult.reachOnlyLiquidLegionsV2.sketchParams.maxSize,
        samplingIndicatorSize = 0L,
      )
    }
    InternalMeasurement.Result.Reach.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Reach methodology is not set.")
    }
  }
}

private operator fun Duration.times(weight: Int): Duration {
  val source = this
  return duration {
    val weightedTotalNanos: Long =
      (TimeUnit.SECONDS.toNanos(source.seconds) + source.nanos) * weight
    seconds = TimeUnit.NANOSECONDS.toSeconds(weightedTotalNanos)
    nanos = (weightedTotalNanos % NANOS_PER_SECOND).toInt()
  }
}

private operator fun Duration.plus(other: Duration): Duration {
  return Durations.add(this, other)
}

private val InternalMetric.state: Metric.State
  get() {
    val measurementStates = weightedMeasurementsList.map { it.measurement.state }
    return if (measurementStates.all { it == InternalMeasurement.State.SUCCEEDED }) {
      Metric.State.SUCCEEDED
    } else if (measurementStates.any { it == InternalMeasurement.State.FAILED }) {
      Metric.State.FAILED
    } else {
      Metric.State.RUNNING
    }
  }
