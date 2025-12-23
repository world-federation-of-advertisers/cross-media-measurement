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

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Duration as ProtoDuration
import com.google.protobuf.duration
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.util.Durations
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.File
import java.security.PrivateKey
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.collections.map
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.text.isNullOrBlank
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.jetbrains.annotations.NonBlockingExecutor
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.ApiKeyCredentials
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
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.LoadingCache
import org.wfanet.measurement.common.api.ResourceIds
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
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest.MeasurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
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
import org.wfanet.measurement.internal.reporting.v2.invalidateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2.metric as internalMetric
import org.wfanet.measurement.measurementconsumer.stats.CustomDirectFrequencyMethodology
import org.wfanet.measurement.measurementconsumer.stats.CustomDirectScalarMethodology
import org.wfanet.measurement.measurementconsumer.stats.DeterministicMethodology
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyVariances
import org.wfanet.measurement.measurementconsumer.stats.HonestMajorityShareShuffleMethodology
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
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.InvalidMetricStateTransitionException
import org.wfanet.measurement.reporting.service.api.MetricNotFoundException
import org.wfanet.measurement.reporting.service.api.ModelLineNotFoundException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.api.submitBatchRequests
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.GetMetricRequest
import org.wfanet.measurement.reporting.v2alpha.InvalidateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricKt.failure
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
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMetricsStub: InternalMetricsCoroutineStub,
  private val variances: Variances,
  internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  dataProvidersStub: DataProvidersCoroutineStub,
  measurementsStub: MeasurementsCoroutineStub,
  certificatesStub: CertificatesCoroutineStub,
  measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val kingdomModelLinesStub: ModelLinesCoroutineStub,
  private val authorization: Authorization,
  encryptionKeyPairStore: EncryptionKeyPairStore,
  private val secureRandom: Random,
  signingPrivateKeyDir: File,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val defaultVidModelLine: String,
  private val measurementConsumerModelLines: Map<String, String>,
  populationDataProvider: String,
  certificateCacheExpirationDuration: Duration = Duration.ofMinutes(60),
  dataProviderCacheExpirationDuration: Duration = Duration.ofMinutes(60),
  keyReaderContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  cacheLoaderContext: @NonBlockingExecutor CoroutineContext = Dispatchers.Default,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MetricsCoroutineImplBase(coroutineContext) {
  private data class DataProviderInfo(
    val dataProviderName: String,
    val publicKey: SignedMessage,
    val certificateName: String,
  )

  private val measurementSupplier =
    MeasurementSupplier(
      internalMeasurementsStub,
      measurementsStub,
      dataProvidersStub,
      certificatesStub,
      measurementConsumersStub,
      encryptionKeyPairStore,
      secureRandom,
      signingPrivateKeyDir,
      trustedCertificates,
      certificateCacheExpirationDuration = certificateCacheExpirationDuration,
      dataProviderCacheExpirationDuration = dataProviderCacheExpirationDuration,
      keyReaderContext,
      cacheLoaderContext,
      populationDataProvider,
    )

  private class MeasurementSupplier(
    private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
    private val measurementsStub: MeasurementsCoroutineStub,
    private val dataProvidersStub: DataProvidersCoroutineStub,
    private val certificatesStub: CertificatesCoroutineStub,
    private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
    private val encryptionKeyPairStore: EncryptionKeyPairStore,
    private val secureRandom: Random,
    private val signingPrivateKeyDir: File,
    private val trustedCertificates: Map<ByteString, X509Certificate>,
    certificateCacheExpirationDuration: Duration,
    dataProviderCacheExpirationDuration: Duration,
    private val keyReaderContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
    cacheLoaderContext: @NonBlockingExecutor CoroutineContext = Dispatchers.Default,
    private val populationDataProvider: String,
  ) {
    data class RunningMetric(
      val internalMetric: InternalMetric,
      val effectiveModelLineName: String,
    ) {
      init {
        require(internalMetric.state == InternalMetric.State.RUNNING)
      }
    }

    private data class ResourceNameApiAuthenticationKey(
      val name: String,
      val apiAuthenticationKey: String,
    )

    private val certificateCache: LoadingCache<ResourceNameApiAuthenticationKey, Certificate> =
      LoadingCache(
        Caffeine.newBuilder()
          .expireAfterWrite(certificateCacheExpirationDuration)
          .executor(
            (cacheLoaderContext[ContinuationInterceptor] as CoroutineDispatcher).asExecutor()
          )
          .buildAsync()
      ) { key ->
        getCertificate(name = key.name, apiAuthenticationKey = key.apiAuthenticationKey)
      }

    private val dataProviderCache: LoadingCache<ResourceNameApiAuthenticationKey, DataProvider> =
      LoadingCache(
        Caffeine.newBuilder()
          .expireAfterWrite(dataProviderCacheExpirationDuration)
          .executor(
            (cacheLoaderContext[ContinuationInterceptor] as CoroutineDispatcher).asExecutor()
          )
          .buildAsync()
      ) { key ->
        getDataProvider(name = key.name, apiAuthenticationKey = key.apiAuthenticationKey)
      }

    /** Creates CMM public [Measurement]s from a list of [InternalMetric]. */
    suspend fun createCmmsMeasurements(
      runningMetrics: Iterable<RunningMetric>,
      internalPrimitiveReportingSetMap: Map<String, InternalReportingSet>,
      measurementConsumerCreds: MeasurementConsumerCredentials,
    ) {
      val measurementConsumer: MeasurementConsumer =
        getMeasurementConsumer(measurementConsumerCreds)

      val dataProviderNames = mutableSetOf<String>()
      for (internalPrimitiveReportingSet in internalPrimitiveReportingSetMap.values) {
        for (eventGroupKey in internalPrimitiveReportingSet.primitive.eventGroupKeysList) {
          dataProviderNames.add(DataProviderKey(eventGroupKey.cmmsDataProviderId).toName())
        }
      }
      val dataProviderInfoMap: Map<String, DataProviderInfo> =
        buildDataProviderInfoMap(measurementConsumerCreds.callCredentials, dataProviderNames)

      val measurementConsumerSigningKey = getMeasurementConsumerSigningKey(measurementConsumerCreds)

      val cmmsCreateMeasurementRequests: Flow<CreateMeasurementRequest> = flow {
        for (runningMetric in runningMetrics) {
          for (weightedMeasurement in runningMetric.internalMetric.weightedMeasurementsList) {
            if (weightedMeasurement.measurement.cmmsMeasurementId.isBlank()) {
              emit(
                buildCreateMeasurementRequest(
                  weightedMeasurement.measurement,
                  runningMetric,
                  internalPrimitiveReportingSetMap,
                  measurementConsumer,
                  measurementConsumerCreds,
                  dataProviderInfoMap,
                  measurementConsumerSigningKey,
                )
              )
            }
          }
        }
      }

      // Create CMMS measurements.
      val callBatchCreateMeasurementsRpc:
        suspend (List<CreateMeasurementRequest>) -> BatchCreateMeasurementsResponse =
        { items ->
          batchCreateCmmsMeasurements(measurementConsumerCreds, items)
        }

      @OptIn(ExperimentalCoroutinesApi::class)
      val cmmsMeasurements: Flow<Measurement> =
        submitBatchRequests(
            cmmsCreateMeasurementRequests,
            BATCH_KINGDOM_MEASUREMENTS_LIMIT,
            callBatchCreateMeasurementsRpc,
          ) { response: BatchCreateMeasurementsResponse ->
            response.measurementsList
          }
          .map { it.asFlow() }
          .flattenMerge()

      // Set CMMS measurement IDs.
      val callBatchSetCmmsMeasurementIdsRpc: suspend (List<MeasurementIds>) -> Unit = { items ->
        batchSetCmmsMeasurementIds(
          measurementConsumerCreds.resourceKey.measurementConsumerId,
          items,
        )
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
        ) { _: Unit ->
          emptyList<Unit>()
        }
        .collect {}
    }

    /** Sets a batch of CMMS [MeasurementIds] to the [InternalMeasurement] table. */
    private suspend fun batchSetCmmsMeasurementIds(
      cmmsMeasurementConsumerId: String,
      measurementIds: List<MeasurementIds>,
    ) {
      try {
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
      measurementConsumerCreds: MeasurementConsumerCredentials,
      createMeasurementRequests: List<CreateMeasurementRequest>,
    ): BatchCreateMeasurementsResponse {
      try {
        return measurementsStub
          .withCallCredentials(measurementConsumerCreds.callCredentials)
          .batchCreateMeasurements(
            batchCreateMeasurementsRequest {
              parent = measurementConsumerCreds.resourceKey.toName()
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
              Status.NOT_FOUND.withDescription(
                "${measurementConsumerCreds.resourceKey.toName()} is not found."
              )
            else -> Status.UNKNOWN.withDescription("Unable to create CMMS Measurements.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Builds a CMMS [CreateMeasurementRequest]. */
    private suspend fun buildCreateMeasurementRequest(
      internalMeasurement: InternalMeasurement,
      runningMetric: RunningMetric,
      internalPrimitiveReportingSetMap: Map<String, InternalReportingSet>,
      measurementConsumer: MeasurementConsumer,
      measurementConsumerCreds: MeasurementConsumerCredentials,
      dataProviderInfoMap: Map<String, DataProviderInfo>,
      measurementConsumerSigningKey: SigningKeyHandle,
    ): CreateMeasurementRequest {
      val internalMetric: InternalMetric = runningMetric.internalMetric
      val packedMeasurementEncryptionPublicKey = measurementConsumer.publicKey.message

      return createMeasurementRequest {
        parent = measurementConsumer.name
        measurement = measurement {
          measurementConsumerCertificate = measurementConsumerCreds.signingCertificateKey.toName()

          if (internalMetric.metricSpec.hasPopulationCount()) {
            dataProviders +=
              buildPopulationDataProviderEntry(
                packedMeasurementEncryptionPublicKey,
                measurementConsumerSigningKey,
                internalMeasurement,
                measurementConsumerCreds,
                internalMetric,
              )
          } else {
            val eventGroupEntriesByDataProvider =
              groupEventGroupEntriesByDataProvider(
                internalMeasurement,
                internalPrimitiveReportingSetMap,
              )

            dataProviders +=
              buildDataProviderEntries(
                eventGroupEntriesByDataProvider,
                packedMeasurementEncryptionPublicKey,
                measurementConsumerSigningKey,
                dataProviderInfoMap,
              )
          }

          val unsignedMeasurementSpec: MeasurementSpec =
            buildUnsignedMeasurementSpec(
              packedMeasurementEncryptionPublicKey,
              dataProviders.map { it.value.nonceHash },
              runningMetric,
            )

          measurementSpec =
            signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
          // To help map reporting measurements to cmms measurements.
          measurementReferenceId = internalMeasurement.cmmsCreateMeasurementRequestId
        }
        requestId = internalMeasurement.cmmsCreateMeasurementRequestId
      }
    }

    /** Gets a [SigningKeyHandle] for [measurementConsumerCreds]. */
    private suspend fun getMeasurementConsumerSigningKey(
      measurementConsumerCreds: MeasurementConsumerCredentials
    ): SigningKeyHandle {
      // TODO: Factor this out to a separate class similar to EncryptionKeyPairStore.
      val signingPrivateKeyDer: ByteString =
        withContext(keyReaderContext) {
          signingPrivateKeyDir
            .resolve(measurementConsumerCreds.signingPrivateKeyPath)
            .readByteString()
        }
      val measurementConsumerCertificate: X509Certificate =
        readCertificate(getSigningCertificateDer(measurementConsumerCreds))
      val signingPrivateKey: PrivateKey =
        readPrivateKey(signingPrivateKeyDer, measurementConsumerCertificate.publicKey.algorithm)

      return SigningKeyHandle(measurementConsumerCertificate, signingPrivateKey)
    }

    /** Builds an unsigned [MeasurementSpec]. */
    private fun buildUnsignedMeasurementSpec(
      packedMeasurementEncryptionPublicKey: ProtoAny,
      nonceHashes: List<ByteString>,
      runningMetric: RunningMetric,
    ): MeasurementSpec {
      val isSingleDataProvider: Boolean = nonceHashes.size == 1
      val metric = runningMetric.internalMetric
      val metricSpec = metric.metricSpec

      return measurementSpec {
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        this.nonceHashes += nonceHashes

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metricSpec.typeCase) {
          InternalMetricSpec.TypeCase.REACH -> {
            val reachPair = metricSpec.reach.toReach(isSingleDataProvider)
            reach = reachPair.first
            vidSamplingInterval = reachPair.second
          }
          InternalMetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
            val reachAndFrequencyPair =
              metricSpec.reachAndFrequency.toReachAndFrequency(isSingleDataProvider)
            reachAndFrequency = reachAndFrequencyPair.first
            vidSamplingInterval = reachAndFrequencyPair.second
          }
          InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
            val impressionPair = metricSpec.impressionCount.toImpression()
            impression = impressionPair.first
            vidSamplingInterval = impressionPair.second
          }
          InternalMetricSpec.TypeCase.WATCH_DURATION -> {
            val durationPair = metricSpec.watchDuration.toDuration()
            duration = durationPair.first
            vidSamplingInterval = durationPair.second
          }
          InternalMetricSpec.TypeCase.POPULATION_COUNT -> {
            population = MeasurementSpec.Population.getDefaultInstance()
          }
          InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
            failGrpc(status = Status.FAILED_PRECONDITION, cause = IllegalStateException()) {
              "Unset metric type should've already raised error."
            }
        }
        modelLine = runningMetric.effectiveModelLineName

        // Add reporting metadata
        reportingMetadata = reportingMetadata {
          report = metric.details.containingReport
          this.metric =
            MetricKey(metric.cmmsMeasurementConsumerId, metric.externalMetricId).toName()
        }
      }
    }

    /** Builds a [Map] of [DataProvider] name to [DataProviderInfo]. */
    private suspend fun buildDataProviderInfoMap(
      callCredentials: ApiKeyCredentials,
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
                dataProviderCache.getValue(
                  ResourceNameApiAuthenticationKey(
                    name = dataProviderName,
                    apiAuthenticationKey = callCredentials.apiAuthenticationKey,
                  )
                )

              val certificate =
                certificateCache.getValue(
                  ResourceNameApiAuthenticationKey(
                    name = dataProvider.certificate,
                    apiAuthenticationKey = callCredentials.apiAuthenticationKey,
                  )
                )

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

    /** Build a [Measurement.DataProviderEntry] message for a Population DataProvider. */
    private suspend fun buildPopulationDataProviderEntry(
      packedMeasurementEncryptionPublicKey: ProtoAny,
      measurementConsumerSigningKey: SigningKeyHandle,
      measurement: InternalMeasurement,
      measurementConsumerCreds: MeasurementConsumerCredentials,
      metric: InternalMetric,
    ): Measurement.DataProviderEntry {
      val dataProviderInfo =
        buildDataProviderInfoMap(
            measurementConsumerCreds.callCredentials,
            listOf(populationDataProvider),
          )
          .values
          .first()

      val filtersList =
        measurement.primitiveReportingSetBasesList
          .map { primitiveReportingSetBasis ->
            val filtersList = primitiveReportingSetBasis.filtersList.filter { !it.isNullOrBlank() }
            if (filtersList.isEmpty()) {
              ""
            } else {
              buildConjunction(filtersList)
            }
          }
          .filter { it.isNotBlank() }

      val requisitionSpec = requisitionSpec {
        population =
          RequisitionSpecKt.population {
            filter =
              RequisitionSpecKt.eventFilter {
                if (filtersList.isNotEmpty()) {
                  expression = buildDisjunction(filtersList)
                }
              }
            interval = metric.timeInterval
          }
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        nonce = secureRandom.nextLong()
      }
      val encryptRequisitionSpec =
        encryptRequisitionSpec(
          signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
          dataProviderInfo.publicKey.unpack(),
        )

      return dataProviderEntry {
        key = populationDataProvider
        value =
          MeasurementKt.DataProviderEntryKt.value {
            dataProviderCertificate = dataProviderInfo.certificateName
            dataProviderPublicKey = dataProviderInfo.publicKey.message
            this.encryptedRequisitionSpec = encryptRequisitionSpec
            nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
          }
      }
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

    /** Combines event group filters. */
    private fun buildDisjunction(filters: Collection<String>): String {
      return filters.joinToString(separator = " || ") { filter -> "($filter)" }
    }

    /** Gets a [MeasurementConsumer] based on a CMMS ID. */
    private suspend fun getMeasurementConsumer(
      measurementConsumerCreds: MeasurementConsumerCredentials
    ): MeasurementConsumer {
      val measurementConsumerName = measurementConsumerCreds.resourceKey.toName()
      return try {
        measurementConsumersStub
          .withCallCredentials(measurementConsumerCreds.callCredentials)
          .getMeasurementConsumer(getMeasurementConsumerRequest { name = measurementConsumerName })
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("$measurementConsumerName not found.")
            else ->
              Status.UNKNOWN.withDescription(
                "Unable to retrieve the measurement consumer [$measurementConsumerName]."
              )
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Gets a signing certificate x509Der in ByteString. */
    private suspend fun getSigningCertificateDer(
      measurementConsumerCreds: MeasurementConsumerCredentials
    ): ByteString {
      val certificate =
        certificateCache.getValue(
          ResourceNameApiAuthenticationKey(
            name = measurementConsumerCreds.signingCertificateKey.toName(),
            apiAuthenticationKey = measurementConsumerCreds.callCredentials.apiAuthenticationKey,
          )
        )

      return certificate.x509Der
    }

    /**
     * Syncs [InternalMeasurement]s with the CMMS [Measurement]s.
     *
     * @return a boolean to indicate whether any [InternalMeasurement] was updated.
     */
    suspend fun syncInternalMeasurements(
      internalMeasurements: List<InternalMeasurement>,
      measurementConsumerCreds: MeasurementConsumerCredentials,
    ): Boolean {
      val failedMeasurements: MutableList<Measurement> = mutableListOf()

      // Most Measurements are expected to be SUCCEEDED so SUCCEEDED Measurements will be collected
      // via a Flow.
      val succeededMeasurements: Flow<Measurement> =
        getCmmsMeasurements(internalMeasurements, measurementConsumerCreds).transform { measurements
          ->
          for (measurement in measurements) {
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
            when (measurement.state) {
              Measurement.State.SUCCEEDED -> emit(measurement)
              Measurement.State.CANCELLED,
              Measurement.State.FAILED -> failedMeasurements.add(measurement)
              Measurement.State.COMPUTING,
              Measurement.State.AWAITING_REQUISITION_FULFILLMENT -> {}
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
        }

      var anyUpdate = false

      val callBatchSetInternalMeasurementResultsRpc: suspend (List<Measurement>) -> Unit =
        { items ->
          batchSetInternalMeasurementResults(items, measurementConsumerCreds)
        }
      val count =
        submitBatchRequests(
            succeededMeasurements,
            BATCH_SET_MEASUREMENT_RESULTS_LIMIT,
            callBatchSetInternalMeasurementResultsRpc,
          ) { _: Unit ->
            emptyList<Unit>()
          }
          .count()

      if (count > 0) {
        anyUpdate = true
      }

      if (failedMeasurements.isNotEmpty()) {
        val callBatchSetInternalMeasurementFailuresRpc: suspend (List<Measurement>) -> Unit =
          { items ->
            batchSetInternalMeasurementFailures(
              items,
              measurementConsumerCreds.resourceKey.measurementConsumerId,
            )
          }
        submitBatchRequests(
            failedMeasurements.asFlow(),
            BATCH_SET_MEASUREMENT_FAILURES_LIMIT,
            callBatchSetInternalMeasurementFailuresRpc,
          ) { _: Unit ->
            emptyList<Unit>()
          }
          .collect {}

        anyUpdate = true
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
    ) {
      val batchSetInternalMeasurementFailuresRequest = batchSetMeasurementFailuresRequest {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        measurementFailures +=
          failedMeasurementsList.map { measurement ->
            measurementFailure {
              cmmsMeasurementId = MeasurementKey.fromName(measurement.name)!!.measurementId
              if (measurement.hasFailure()) {
                failure = measurement.failure.toInternal()
              }
            }
          }
      }

      try {
        internalMeasurementsStub.batchSetMeasurementFailures(
          batchSetInternalMeasurementFailuresRequest
        )
      } catch (e: StatusException) {
        throw Status.INTERNAL.withDescription("Unable to set measurement failures for Measurements")
          .withCause(e)
          .asRuntimeException()
      }
    }

    /**
     * Sets a batch of succeeded [InternalMeasurement]s and stores the measurement results of the
     * given succeeded CMMS [Measurement]s.
     */
    private suspend fun batchSetInternalMeasurementResults(
      succeededMeasurementsList: List<Measurement>,
      measurementConsumerCreds: MeasurementConsumerCredentials,
    ) {
      val batchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = measurementConsumerCreds.resourceKey.measurementConsumerId
        measurementResults +=
          succeededMeasurementsList.map { measurement ->
            buildInternalMeasurementResult(measurement, measurementConsumerCreds)
          }
      }

      try {
        internalMeasurementsStub.batchSetMeasurementResults(batchSetMeasurementResultsRequest)
      } catch (e: StatusException) {
        throw Exception("Unable to set measurement results for Measurements.", e)
      }
    }

    /** Retrieves [Measurement]s from the CMMS. */
    private suspend fun getCmmsMeasurements(
      internalMeasurements: List<InternalMeasurement>,
      measurementConsumerCreds: MeasurementConsumerCredentials,
    ): Flow<List<Measurement>> {
      val measurementNames: Flow<String> = flow {
        buildSet {
          for (internalMeasurement in internalMeasurements) {
            val name =
              MeasurementKey(
                  measurementConsumerCreds.resourceKey.measurementConsumerId,
                  internalMeasurement.cmmsMeasurementId,
                )
                .toName()
            // Checks if the set already contains the name
            if (!contains(name)) {
              // If the set doesn't contain the name, emit it and add it to the set so it won't
              // get emitted again.
              emit(name)
              add(name)
            }
          }
        }
      }

      val callBatchGetMeasurementsRpc: suspend (List<String>) -> BatchGetMeasurementsResponse =
        { items ->
          batchGetCmmsMeasurements(measurementConsumerCreds, items)
        }

      return submitBatchRequests(
        measurementNames,
        BATCH_KINGDOM_MEASUREMENTS_LIMIT,
        callBatchGetMeasurementsRpc,
      ) { response: BatchGetMeasurementsResponse ->
        response.measurementsList
      }
    }

    /** Batch get CMMS measurements. */
    private suspend fun batchGetCmmsMeasurements(
      measurementConsumerCreds: MeasurementConsumerCredentials,
      measurementNames: List<String>,
    ): BatchGetMeasurementsResponse {
      try {
        return measurementsStub
          .withCallCredentials(measurementConsumerCreds.callCredentials)
          .batchGetMeasurements(
            batchGetMeasurementsRequest {
              parent = measurementConsumerCreds.resourceKey.toName()
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
            else -> Status.INTERNAL.withDescription("Unable to retrieve Measurements.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /** Builds an [InternalMeasurement.Result]. */
    private suspend fun buildInternalMeasurementResult(
      measurement: Measurement,
      measurementConsumerCreds: MeasurementConsumerCredentials,
    ): BatchSetMeasurementResultsRequest.MeasurementResult {
      val measurementSpec: MeasurementSpec = measurement.measurementSpec.unpack()
      val encryptionPrivateKeyHandle =
        encryptionKeyPairStore.getPrivateKeyHandle(
          measurementConsumerCreds.resourceKey.toName(),
          measurementSpec.measurementPublicKey.unpack<EncryptionPublicKey>().data,
        )
          ?: failGrpc(Status.FAILED_PRECONDITION) {
            "Encryption private key not found for the measurement ${measurement.name}."
          }

      val decryptedMeasurementResults: List<Measurement.Result> =
        measurement.resultsList.map {
          decryptMeasurementResultOutput(
            it,
            encryptionPrivateKeyHandle,
            measurementConsumerCreds.callCredentials,
          )
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
            } catch (e: Exception) {
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
      callCredentials: ApiKeyCredentials,
    ): Measurement.Result {
      val certificate =
        certificateCache.getValue(
          ResourceNameApiAuthenticationKey(
            name = measurementResultOutput.certificate,
            apiAuthenticationKey = callCredentials.apiAuthenticationKey,
          )
        )

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
            Status.Code.NOT_FOUND ->
              Status.FAILED_PRECONDITION.withDescription("Certificate $name not found.")
            else -> Status.INTERNAL.withDescription("Unable to retrieve Certificate $name.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }

    /**
     * Returns the [DataProvider] from the CMMS system.
     *
     * @param[name] resource name of the [DataProvider]
     * @param[apiAuthenticationKey] API key to act as the [MeasurementConsumer] client
     * @throws [StatusRuntimeException] with [Status.FAILED_PRECONDITION] when retrieving the
     *   [DataProvider] fails.
     */
    private suspend fun getDataProvider(name: String, apiAuthenticationKey: String): DataProvider {
      return try {
        dataProvidersStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getDataProvider(getDataProviderRequest { this.name = name })
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.FAILED_PRECONDITION.withDescription("$name not found")
            else -> Status.UNKNOWN.withDescription("Unable to retrieve $name")
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

    val measurementConsumerName: String = metricKey.parentKey.toName()
    authorization.check(listOf(request.name, measurementConsumerName), Permission.GET)

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[measurementConsumerName]
        ?: throw Status.INTERNAL.withDescription("Config not found for $measurementConsumerName")
          .asRuntimeException()
    val measurementConsumerCredentials =
      MeasurementConsumerCredentials.fromConfig(metricKey.parentKey, measurementConsumerConfig)

    val internalMetric: InternalMetric =
      try {
          batchGetInternalMetrics(
            metricKey.parentKey.measurementConsumerId,
            listOf(metricKey.metricId),
          )
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.NOT_FOUND ->
                Status.NOT_FOUND.withDescription("Metric ${request.name} not found")
              else -> Status.INTERNAL
            }
            .withCause(e)
            .asRuntimeException()
        }
        .single()
    return syncAndConvertInternalMetricsToPublicMetrics(
        mapOf(internalMetric.state to listOf(internalMetric)),
        measurementConsumerCredentials,
      )
      .single()
  }

  override suspend fun batchGetMetrics(request: BatchGetMetricsRequest): BatchGetMetricsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val measurementConsumerName: String = parentKey.toName()

    // TODO(@SanjayVas): Consider also allowing permission on each Metric.
    authorization.check(measurementConsumerName, Permission.GET)

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[measurementConsumerName]
        ?: throw Status.INTERNAL.withDescription("Config not found for $measurementConsumerName")
          .asRuntimeException()
    val measurementConsumerCreds =
      MeasurementConsumerCredentials.fromConfig(parentKey, measurementConsumerConfig)

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

    val internalMetricsByState: Map<InternalMetric.State, List<InternalMetric>> =
      try {
          batchGetInternalMetrics(parentKey.measurementConsumerId, metricIds)
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("Metric not found")
              else -> Status.INTERNAL
            }
            .withCause(e)
            .asRuntimeException()
        }
        .groupBy { it.state }

    return batchGetMetricsResponse {
      metrics +=
        syncAndConvertInternalMetricsToPublicMetrics(
          internalMetricsByState,
          measurementConsumerCreds,
        )
    }
  }

  override suspend fun listMetrics(request: ListMetricsRequest): ListMetricsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    val measurementConsumerName: String = parentKey.toName()
    authorization.check(measurementConsumerName, Permission.LIST)

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[measurementConsumerName]
        ?: throw Status.INTERNAL.withDescription("Config not found for $measurementConsumerName")
          .asRuntimeException()
    val measurementConsumerCreds =
      MeasurementConsumerCredentials.fromConfig(parentKey, measurementConsumerConfig)

    val listMetricsPageToken: ListMetricsPageToken = request.toListMetricsPageToken()
    val streamInternalMetricRequest: StreamMetricsRequest =
      listMetricsPageToken.toStreamMetricsRequest()

    val results: List<InternalMetric> =
      try {
        internalMetricsStub.streamMetrics(streamInternalMetricRequest).toList()
      } catch (e: StatusException) {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
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

    val subResultsByState: Map<InternalMetric.State, List<InternalMetric>> =
      results.subList(0, min(results.size, listMetricsPageToken.pageSize)).groupBy { it.state }

    return listMetricsResponse {
      metrics +=
        syncAndConvertInternalMetricsToPublicMetrics(subResultsByState, measurementConsumerCreds)

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  override suspend fun invalidateMetric(request: InvalidateMetricRequest): Metric {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val metricKey =
      MetricKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val measurementConsumerName: String = metricKey.parentKey.toName()
    authorization.check(measurementConsumerName, Permission.INVALIDATE)

    try {
      return internalMetricsStub
        .invalidateMetric(
          invalidateMetricRequest {
            cmmsMeasurementConsumerId = metricKey.cmmsMeasurementConsumerId
            externalMetricId = metricKey.metricId
          }
        )
        .toMetric(variances)
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.METRIC_NOT_FOUND ->
          MetricNotFoundException(request.name, e).asStatusRuntimeException(Status.Code.NOT_FOUND)
        InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION ->
          InvalidMetricStateTransitionException(
              request.name,
              Metric.State.FAILED,
              Metric.State.INVALID,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
        InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
        InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
        InternalErrors.Reason.INVALID_FIELD_VALUE,
        InternalErrors.Reason.BASIC_REPORT_NOT_FOUND,
        InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
        InternalErrors.Reason.REPORT_RESULT_NOT_FOUND,
        InternalErrors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
        InternalErrors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
        InternalErrors.Reason.BASIC_REPORT_STATE_INVALID,
        InternalErrors.Reason.INVALID_BASIC_REPORT,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  /**
   * Gets a batch of [InternalMetric]s.
   *
   * @throws StatusException
   */
  private suspend fun batchGetInternalMetrics(
    cmmsMeasurementConsumerId: String,
    metricIds: List<String>,
  ): List<InternalMetric> {
    val batchGetMetricsRequest = batchGetMetricsRequest {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalMetricIds += metricIds
    }

    return internalMetricsStub.batchGetMetrics(batchGetMetricsRequest).metricsList
  }

  override suspend fun createMetric(request: CreateMetricRequest): Metric {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val measurementConsumerName: String = parentKey.toName()
    grpcRequire(request.hasMetric()) { "Metric is not specified." }

    val reportingSetKey =
      ReportingSetKey.fromName(request.metric.reportingSet)
        ?: throw Status.INVALID_ARGUMENT.withDescription("metric.reporting_set is invalid")
          .asRuntimeException()
    if (reportingSetKey.parentKey != parentKey) {
      throw Status.INVALID_ARGUMENT.withDescription("metric.reporting_set has incorrect parent")
        .asRuntimeException()
    }

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[measurementConsumerName]
        ?: throw Status.INTERNAL.withDescription("Config not found for $measurementConsumerName")
          .asRuntimeException()
    val measurementConsumerCreds =
      MeasurementConsumerCredentials.fromConfig(parentKey, measurementConsumerConfig)
    val effectiveModelLineName =
      try {
        getEffectiveModelLine(
          request.metric.modelLine,
          "metric.model_line",
          measurementConsumerName,
        )
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    val requiredPermissionIds = buildSet {
      add(Permission.CREATE)
      if (effectiveModelLineName.isNotEmpty()) {
        val modelLine =
          try {
            kingdomModelLinesStub
              .withAuthenticationKey(measurementConsumerCreds.callCredentials.apiAuthenticationKey)
              .getModelLine(getModelLineRequest { name = effectiveModelLineName })
          } catch (e: StatusException) {
            throw when (e.status.code) {
              Status.Code.NOT_FOUND ->
                ModelLineNotFoundException(effectiveModelLineName, e)
                  .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
              else -> Status.INTERNAL.withCause(e).asRuntimeException()
            }
          }
        if (modelLine.type == ModelLine.Type.DEV) {
          add(Permission.CREATE_WITH_DEV_MODEL_LINE)
        }
      }
    }
    authorization.check(measurementConsumerName, requiredPermissionIds)

    val batchGetReportingSetsResponse =
      batchGetInternalReportingSets(
        parentKey.measurementConsumerId,
        listOf(reportingSetKey.reportingSetId),
      )

    val internalPrimitiveReportingSetMap: Map<String, InternalReportingSet> =
      buildInternalPrimitiveReportingSetMap(
        parentKey.measurementConsumerId,
        batchGetReportingSetsResponse.reportingSetsList,
      )

    val internalCreateMetricRequest: InternalCreateMetricRequest =
      buildInternalCreateMetricRequest(
        parentKey.measurementConsumerId,
        request,
        batchGetReportingSetsResponse.reportingSetsList.single(),
        internalPrimitiveReportingSetMap,
      )

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

    if (internalMetric.state == InternalMetric.State.RUNNING) {
      measurementSupplier.createCmmsMeasurements(
        listOf(MeasurementSupplier.RunningMetric(internalMetric, effectiveModelLineName)),
        internalPrimitiveReportingSetMap,
        measurementConsumerCreds,
      )
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

    val measurementConsumerName: String = parentKey.toName()
    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[measurementConsumerName]
        ?: throw Status.INTERNAL.withDescription("Config not found for $measurementConsumerName")
          .asRuntimeException()
    val measurementConsumerCreds =
      MeasurementConsumerCredentials.fromConfig(parentKey, measurementConsumerConfig)
    val effectiveModelLineNames =
      request.requestsList.mapIndexed { index, subRequest ->
        try {
          getEffectiveModelLine(
            subRequest.metric.modelLine,
            "requests[$index].metric.model_line",
            measurementConsumerName,
          )
        } catch (e: InvalidFieldValueException) {
          throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val requiredPermissionIds = buildSet {
      add(Permission.CREATE)
      for (effectiveModelLineName in effectiveModelLineNames) {
        if (effectiveModelLineName.isEmpty()) {
          continue
        }
        val modelLine =
          try {
            kingdomModelLinesStub
              .withAuthenticationKey(measurementConsumerCreds.callCredentials.apiAuthenticationKey)
              .getModelLine(getModelLineRequest { name = effectiveModelLineName })
          } catch (e: StatusException) {
            throw when (e.status.code) {
              Status.Code.NOT_FOUND ->
                ModelLineNotFoundException(effectiveModelLineName, e)
                  .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
              else -> StatusRuntimeException(Status.INTERNAL)
            }
          }
        if (modelLine.type == ModelLine.Type.DEV) {
          add(Permission.CREATE_WITH_DEV_MODEL_LINE)
        }
      }
    }
    authorization.check(measurementConsumerName, requiredPermissionIds)

    grpcRequire(request.requestsList.isNotEmpty()) { "Requests is empty." }
    grpcRequire(request.requestsList.size <= MAX_BATCH_SIZE) {
      "At most $MAX_BATCH_SIZE requests can be supported in a batch."
    }

    val metricIds = request.requestsList.map { it.metricId }
    grpcRequire(metricIds.size == metricIds.distinct().size) {
      "Duplicate metric IDs in the request."
    }

    val reportingSetNames =
      request.requestsList
        .map {
          grpcRequire(it.hasMetric()) { "Metric is not specified." }

          it.metric.reportingSet
        }
        .distinct()

    val callRpc: suspend (List<String>) -> BatchGetReportingSetsResponse = { items ->
      val externalReportingSetIds: List<String> =
        items.map {
          val reportingSetKey =
            grpcRequireNotNull(ReportingSetKey.fromName(it)) { "Invalid reporting set name $it." }

          if (reportingSetKey.cmmsMeasurementConsumerId != parentKey.measurementConsumerId) {
            failGrpc(Status.PERMISSION_DENIED) { "No access to the reporting set [$it]." }
          }

          reportingSetKey.reportingSetId
        }
      batchGetInternalReportingSets(parentKey.measurementConsumerId, externalReportingSetIds)
    }

    val reportingSetNameToInternalReportingSetMap: Map<String, InternalReportingSet> = buildMap {
      submitBatchRequests(reportingSetNames.asFlow(), BATCH_GET_REPORTING_SETS_LIMIT, callRpc) {
          response ->
          response.reportingSetsList
        }
        .collect { reportingSetsList ->
          for (reportingSet in reportingSetsList) {
            putIfAbsent(
              ReportingSetKey(parentKey.measurementConsumerId, reportingSet.externalReportingSetId)
                .toName(),
              reportingSet,
            )
          }
        }
    }

    val internalPrimitiveReportingSetMap: Map<String, InternalReportingSet> =
      buildInternalPrimitiveReportingSetMap(
        parentKey.measurementConsumerId,
        reportingSetNameToInternalReportingSetMap.values,
      )

    val internalCreateMetricRequests: List<InternalCreateMetricRequest> =
      coroutineScope {
          request.requestsList.map { createMetricRequest ->
            async {
              buildInternalCreateMetricRequest(
                parentKey.measurementConsumerId,
                createMetricRequest,
                reportingSetNameToInternalReportingSetMap.getValue(
                  createMetricRequest.metric.reportingSet
                ),
                internalPrimitiveReportingSetMap,
              )
            }
          }
        }
        .awaitAll()

    val internalMetrics =
      try {
        internalMetricsStub
          .batchCreateMetrics(
            internalBatchCreateMetricsRequest {
              cmmsMeasurementConsumerId = parentKey.measurementConsumerId
              requests += internalCreateMetricRequests
            }
          )
          .metricsList
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

    val internalRunningMetrics: List<MeasurementSupplier.RunningMetric> =
      internalMetrics
        .zip(effectiveModelLineNames)
        .filter { (internalMetric, _) -> internalMetric.state == InternalMetric.State.RUNNING }
        .map { (internalMetric, effectiveModelLineName) ->
          MeasurementSupplier.RunningMetric(internalMetric, effectiveModelLineName)
        }
    if (internalRunningMetrics.isNotEmpty()) {
      measurementSupplier.createCmmsMeasurements(
        internalRunningMetrics,
        internalPrimitiveReportingSetMap,
        measurementConsumerCreds,
      )
    }

    // Convert the internal metric to public and return it.
    return batchCreateMetricsResponse { metrics += internalMetrics.map { it.toMetric(variances) } }
  }

  /**
   * Returns the resource name of the effective [ModelLine], or empty string if there is none.
   *
   * @param requestModelLine resource name of the [ModelLine] from a request, which may be empty
   * @param fieldPath field path of [requestModelLine] relative to the request
   * @param measurementConsumer resource name of the [MeasurementConsumer]
   * @throws InvalidFieldValueException if [requestModelLine] is specified and invalid
   */
  private fun getEffectiveModelLine(
    requestModelLine: String,
    fieldPath: String,
    measurementConsumer: String,
  ): String {
    val key: ModelLineKey? =
      if (requestModelLine.isEmpty()) {
        val effectiveModelLine =
          measurementConsumerModelLines[measurementConsumer] ?: defaultVidModelLine.ifEmpty { null }
        if (effectiveModelLine == null) {
          null
        } else {
          checkNotNull(ModelLineKey.fromName(effectiveModelLine)) {
            "Invalid ModelLine $effectiveModelLine for $measurementConsumer in config"
          }
        }
      } else {
        ModelLineKey.fromName(requestModelLine) ?: throw InvalidFieldValueException(fieldPath)
      }

    return key?.toName() ?: ""
  }

  /** Builds an [InternalCreateMetricRequest]. */
  private fun buildInternalCreateMetricRequest(
    cmmsMeasurementConsumerId: String,
    request: CreateMetricRequest,
    internalReportingSet: InternalReportingSet,
    internalPrimitiveReportingSetMap: Map<String, InternalReportingSet>,
  ): InternalCreateMetricRequest {
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
        cmmsModelLine = request.metric.modelLine
        metricSpec =
          try {
            request.metric.metricSpec.withDefaults(metricSpecConfig, secureRandom).toInternal()
          } catch (e: MetricSpecDefaultsException) {
            failGrpc(Status.INVALID_ARGUMENT, e) {
              listOfNotNull("Invalid metric spec.", e.message, e.cause?.message)
                .joinToString(separator = "\n")
            }
          } catch (e: Exception) {
            failGrpc(Status.INTERNAL, e) { "Failed to read the metric spec." }
          }
        weightedMeasurements +=
          buildInitialInternalMeasurements(
            cmmsMeasurementConsumerId,
            request.metric,
            internalReportingSet,
            internalPrimitiveReportingSetMap,
          )
        details =
          InternalMetricKt.details {
            containingReport = request.metric.containingReport
            filters += request.metric.filtersList
          }
      }
    }
  }

  /** Build a map of external ReportingSetId to Primitive [InternalReportingSet]. */
  private suspend fun buildInternalPrimitiveReportingSetMap(
    measurementConsumerId: String,
    internalReportingSetsList: Collection<InternalReportingSet>,
  ): Map<String, InternalReportingSet> {
    // Gets all external IDs of primitive reporting sets from the metric list.
    val externalPrimitiveReportingSetIds: Flow<String> = flow {
      buildSet {
        for (internalReportingSet in internalReportingSetsList) {
          for (weightedSubsetUnion in internalReportingSet.weightedSubsetUnionsList) {
            for (primitiveReportingSetBasis in weightedSubsetUnion.primitiveReportingSetBasesList) {
              // Checks if the set already contains the ID
              if (!contains(primitiveReportingSetBasis.externalReportingSetId)) {
                // If the set doesn't contain the ID, emit it and add it to the set so it won't
                // get emitted again.
                emit(primitiveReportingSetBasis.externalReportingSetId)
                add(primitiveReportingSetBasis.externalReportingSetId)
              }
            }
          }
        }
      }
    }

    val callBatchGetInternalReportingSetsRpc:
      suspend (List<String>) -> BatchGetReportingSetsResponse =
      { items ->
        batchGetInternalReportingSets(measurementConsumerId, items)
      }

    return buildMap {
      submitBatchRequests(
          externalPrimitiveReportingSetIds,
          BATCH_GET_REPORTING_SETS_LIMIT,
          callBatchGetInternalReportingSetsRpc,
        ) { response: BatchGetReportingSetsResponse ->
          response.reportingSetsList
        }
        .collect { reportingSets: List<InternalReportingSet> ->
          for (reportingSet in reportingSets) {
            computeIfAbsent(reportingSet.externalReportingSetId) { reportingSet }
          }
        }
    }
  }

  /** Builds [InternalMeasurement]s for a [Metric] over an [InternalReportingSet]. */
  private fun buildInitialInternalMeasurements(
    cmmsMeasurementConsumerId: String,
    metric: Metric,
    internalReportingSet: InternalReportingSet,
    internalPrimitiveReportingSetMap: Map<String, InternalReportingSet>,
  ): List<WeightedMeasurement> {
    return internalReportingSet.weightedSubsetUnionsList.map { weightedSubsetUnion ->
      weightedMeasurement {
        weight = weightedSubsetUnion.weight
        binaryRepresentation = weightedSubsetUnion.binaryRepresentation
        measurement = internalMeasurement {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          timeInterval = metric.timeInterval
          val dataProviderSet = mutableSetOf<String>()
          this.primitiveReportingSetBases +=
            weightedSubsetUnion.primitiveReportingSetBasesList.map { primitiveReportingSetBasis ->
              internalPrimitiveReportingSetMap
                .getValue(primitiveReportingSetBasis.externalReportingSetId)
                .primitive
                .eventGroupKeysList
                .forEach { dataProviderSet.add(it.cmmsDataProviderId) }
              primitiveReportingSetBasis.copy { filters += metric.filtersList }
            }
          details = InternalMeasurementKt.details { dataProviderCount = dataProviderSet.size }
        }
      }
    }
  }

  /** Batch get [InternalReportingSet]s based on external IDs. */
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
      throw Exception("Unable to retrieve ReportingSets using the provided names.", e)
    }
  }

  /** Converts [InternalMetric]s to public [Metric]s after syncing [Measurement]s. */
  private suspend fun syncAndConvertInternalMetricsToPublicMetrics(
    metricsByState: Map<InternalMetric.State, List<InternalMetric>>,
    measurementConsumerCreds: MeasurementConsumerCredentials,
  ): List<Metric> {
    // Only syncs pending measurements which can only be in metrics that are still running.
    val toBeSyncedInternalMeasurements: List<InternalMeasurement> =
      if (metricsByState.containsKey(InternalMetric.State.RUNNING)) {
        metricsByState
          .getValue(InternalMetric.State.RUNNING)
          .flatMap { internalMetric -> internalMetric.weightedMeasurementsList }
          .map { weightedMeasurement -> weightedMeasurement.measurement }
          .filter { internalMeasurement ->
            internalMeasurement.state == InternalMeasurement.State.PENDING
          }
      } else {
        emptyList()
      }

    val anyMeasurementUpdated: Boolean =
      measurementSupplier.syncInternalMeasurements(
        toBeSyncedInternalMeasurements,
        measurementConsumerCreds,
      )

    return buildList {
      for (state in metricsByState.keys) {
        when (state) {
          InternalMetric.State.SUCCEEDED,
          InternalMetric.State.FAILED,
          InternalMetric.State.INVALID ->
            addAll(metricsByState.getValue(state).map { it.toMetric(variances) })
          InternalMetric.State.RUNNING -> {
            if (anyMeasurementUpdated) {
              val updatedInternalMetrics =
                try {
                  batchGetInternalMetrics(
                    measurementConsumerCreds.resourceKey.measurementConsumerId,
                    metricsByState.getValue(InternalMetric.State.RUNNING).map {
                      it.externalMetricId
                    },
                  )
                } catch (e: StatusException) {
                  throw Status.INTERNAL.withDescription("Unable to get internal Metrics")
                    .withCause(e)
                    .asRuntimeException()
                }
              addAll(updatedInternalMetrics.map { it.toMetric(variances) })
            } else {
              addAll(metricsByState.getValue(state).map { it.toMetric(variances) })
            }
          }
          InternalMetric.State.STATE_UNSPECIFIED -> {
            // Metrics created before state was tracked in the database will have the state be
            // unspecified. This calculates the correct state for those metrics.
            addAll(
              metricsByState.getValue(state).map { internalMetric ->
                internalMetric
                  .copy { this.state = internalMetric.calculateState() }
                  .toMetric(variances)
              }
            )
          }
          InternalMetric.State.UNRECOGNIZED -> error("Invalid Metric State")
        }
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
      modelLine = source.cmmsModelLine
      filters += source.details.filtersList
      state = source.state.toPublic()
      createTime = source.createTime
      containingReport = source.details.containingReport
      // The calculations can throw an error, but we still want to return the metric.
      when (state) {
        Metric.State.SUCCEEDED -> {
          try {
            result = buildMetricResult(source, variances)
          } catch (e: Exception) {
            logger.log(Level.SEVERE, "buildMetricResult exception:", e)
            state = Metric.State.FAILED
            when (e) {
              is MeasurementVarianceNotComputableException -> {
                result = buildMetricResult(source)
                failure = failure {
                  reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                  message = "Problem with variance calculation"
                }
              }
              is NoiseMechanismUnrecognizedException -> {
                result = buildMetricResult(source)
                failure = failure {
                  reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                  message = "Problem with noise mechanism"
                }
              }

              is MetricResultNotComputableException -> {
                state = Metric.State.FAILED
                result = metricResult {
                  cmmsMeasurements +=
                    source.weightedMeasurementsList.map {
                      MeasurementKey(
                          source.cmmsMeasurementConsumerId,
                          it.measurement.cmmsMeasurementId,
                        )
                        .toName()
                    }
                }
                failure = failure {
                  reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                  message = "Problem with result"
                }
              }

              else -> {
                failure = failure { reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID }
              }
            }
          }
        }
        Metric.State.FAILED -> {
          result = metricResult {
            cmmsMeasurements +=
              source.weightedMeasurementsList.map {
                MeasurementKey(source.cmmsMeasurementConsumerId, it.measurement.cmmsMeasurementId)
                  .toName()
              }
          }
          failure = failure { reason = Metric.Failure.Reason.MEASUREMENT_STATE_INVALID }
        }
        Metric.State.INVALID -> {
          result = metricResult {
            cmmsMeasurements +=
              source.weightedMeasurementsList.map {
                MeasurementKey(source.cmmsMeasurementConsumerId, it.measurement.cmmsMeasurementId)
                  .toName()
              }
          }
        }
        Metric.State.STATE_UNSPECIFIED,
        Metric.State.RUNNING,
        Metric.State.UNRECOGNIZED -> {}
      }
    }
  }

  object Permission {
    const val GET = "reporting.metrics.get"
    const val LIST = "reporting.metrics.list"
    const val CREATE = "reporting.metrics.create"
    const val CREATE_WITH_DEV_MODEL_LINE = "reporting.metrics.createWithDevModelLine"
    const val INVALIDATE = "reporting.metrics.invalidate"
  }

  companion object {
    private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX
    private val logger: Logger = Logger.getLogger(this::class.java.name)
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

/** Builds a [MetricResult] from the given [InternalMetric]. */
private fun buildMetricResult(metric: InternalMetric): MetricResult {
  return metricResult {
    cmmsMeasurements +=
      metric.weightedMeasurementsList.map {
        MeasurementKey(metric.cmmsMeasurementConsumerId, it.measurement.cmmsMeasurementId).toName()
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (metric.metricSpec.typeCase) {
      InternalMetricSpec.TypeCase.REACH -> {
        reach = calculateReachResult(metric.weightedMeasurementsList, metric.metricSpec.reach)
      }
      InternalMetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
        reachAndFrequency = reachAndFrequencyResult {
          reach =
            calculateReachResult(
              metric.weightedMeasurementsList,
              metric.metricSpec.reachAndFrequency,
            )
          frequencyHistogram =
            calculateFrequencyHistogramResults(
              metric.weightedMeasurementsList,
              metric.metricSpec.reachAndFrequency,
            )
        }
      }
      InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount =
          calculateImpressionResult(
            metric.weightedMeasurementsList,
            metric.metricSpec.impressionCount,
          )
      }
      InternalMetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration =
          calculateWatchDurationResult(
            metric.weightedMeasurementsList,
            metric.metricSpec.watchDuration,
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
          calculateReachResult(metric.weightedMeasurementsList, metric.metricSpec.reach, variances)
      }
      InternalMetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
        reachAndFrequency = reachAndFrequencyResult {
          reach =
            calculateReachResult(
              metric.weightedMeasurementsList,
              metric.metricSpec.reachAndFrequency,
              variances,
            )
          frequencyHistogram =
            calculateFrequencyHistogramResults(
              metric.weightedMeasurementsList,
              metric.metricSpec.reachAndFrequency,
              variances,
            )
        }
      }
      InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount =
          calculateImpressionResult(
            metric.weightedMeasurementsList,
            metric.metricSpec.impressionCount,
            variances,
          )
      }
      InternalMetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration =
          calculateWatchDurationResult(
            metric.weightedMeasurementsList,
            metric.metricSpec.watchDuration,
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

/**
 * Aggregates a list of [InternalMeasurement.Result]s to a [InternalMeasurement.Result]
 *
 * @throws MetricResultNotComputableException when no measurement result or reach-frequency
 *   measurement missing reach measurement result.
 */
private fun aggregateResults(
  internalMeasurementResults: List<InternalMeasurement.Result>
): InternalMeasurement.Result {
  if (internalMeasurementResults.isEmpty()) {
    throw MetricResultNotComputableException("No measurement result.")
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
    if (result.hasReach()) {
      reachValue += result.reach.value
    }
    if (result.hasFrequency()) {
      if (!result.hasReach()) {
        throw MetricResultNotComputableException(
          "Missing reach measurement in the Reach-Frequency measurement."
        )
      }
      for ((frequency, percentage) in result.frequency.relativeFrequencyDistributionMap) {
        val previousTotalReachCount =
          frequencyDistribution.getOrDefault(frequency, 0.0) * reachValue
        val currentReachCount = percentage * result.reach.value

        if (reachValue == 0L) {
          frequencyDistribution[frequency] = 0.0
        } else {
          frequencyDistribution[frequency] =
            (previousTotalReachCount + currentReachCount) / reachValue
        }
      }
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

/**
 * Calculates the watch duration result from [WeightedMeasurement]s.
 *
 * @throws MeasurementVarianceNotComputableException when metric variance computation fails.
 * @throws MetricResultNotComputableException when watch duration measurement result is missing.
 */
private fun calculateWatchDurationResult(
  weightedMeasurements: List<WeightedMeasurement>,
  watchDurationParams: InternalMetricSpec.WatchDurationParams,
  variances: Variances? = null,
): MetricResult.WatchDurationResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasWatchDuration() }) {
      throw MetricResultNotComputableException("Watch duration measurement result is missing.")
    }
  }
  return watchDurationResult {
    val watchDuration: ProtoDuration =
      weightedMeasurements
        .map { weightedMeasurement ->
          aggregateResults(weightedMeasurement.measurement.details.resultsList)
            .watchDuration
            .value * weightedMeasurement.weight
        }
        .reduce { sum, element -> sum + element }
    value = watchDuration.toDoubleSecond()

    // Only compute univariate statistics for union-only operations, i.e. single source measurement.
    if (weightedMeasurements.size == 1 && variances != null) {
      val weightedMeasurement = weightedMeasurements.single()
      val weightedMeasurementVarianceParamsList:
        List<WeightedWatchDurationMeasurementVarianceParams?> =
        buildWeightedWatchDurationMeasurementVarianceParamsPerResult(
          weightedMeasurement,
          watchDurationParams,
        )

      // If any measurement result contains insufficient data for variance calculation, univariate
      // statistics won't be computed.
      if (
        weightedMeasurementVarianceParamsList.all { it != null } &&
          weightedMeasurementVarianceParamsList.first()!!.measurementVarianceParams.duration >= 0.0
      ) {
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
                } catch (e: Exception) {
                  throw MeasurementVarianceNotComputableException(cause = e)
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
  val populationResult =
    weightedMeasurements.sumOf { weightedMeasurement ->
      aggregateResults(weightedMeasurement.measurement.details.resultsList).population.value *
        weightedMeasurement.weight
    }
  return populationCountResult { value = populationResult }
}

/** Converts [ProtoDuration] format to [Double] second. */
private fun ProtoDuration.toDoubleSecond(): Double {
  val source = this
  return source.seconds + (source.nanos.toDouble() / NANOS_PER_SECOND)
}

/**
 * Builds a list of nullable [WeightedWatchDurationMeasurementVarianceParams].
 *
 * @throws MetricResultNotComputableException when watch duration measurement has no results.
 * @throws MeasurementVarianceNotComputableException when methodology is incorrectly set.
 */
fun buildWeightedWatchDurationMeasurementVarianceParamsPerResult(
  weightedMeasurement: WeightedMeasurement,
  watchDurationParams: InternalMetricSpec.WatchDurationParams,
): List<WeightedWatchDurationMeasurementVarianceParams?> {
  val watchDurationResults: List<InternalMeasurement.Result.WatchDuration> =
    weightedMeasurement.measurement.details.resultsList.map { it.watchDuration }

  if (watchDurationResults.isEmpty()) {
    throw MetricResultNotComputableException("WatchDuration measurement should've had results.")
  }

  return watchDurationResults.map { watchDurationResult ->
    val statsNoiseMechanism: StatsNoiseMechanism =
      try {
        watchDurationResult.noiseMechanism.toStatsNoiseMechanism()
      } catch (_: NoiseMechanismUnspecifiedException) {
        return@map null
      }

    val methodology: Methodology = buildStatsMethodology(watchDurationResult) ?: return@map null

    WeightedWatchDurationMeasurementVarianceParams(
      binaryRepresentation = weightedMeasurement.binaryRepresentation,
      weight = weightedMeasurement.weight,
      measurementVarianceParams =
        WatchDurationMeasurementVarianceParams(
          duration = max(0.0, watchDurationResult.value.toDoubleSecond()),
          measurementParams =
            WatchDurationMeasurementParams(
              vidSamplingInterval =
                watchDurationParams.params.vidSamplingInterval.toStatsVidSamplingInterval(),
              dpParams = watchDurationParams.params.privacyParams.toNoiserDpParams(),
              maximumDurationPerUser =
                watchDurationParams.maximumWatchDurationPerUser.toDoubleSecond(),
              noiseMechanism = statsNoiseMechanism,
            ),
        ),
      methodology = methodology,
    )
  }
}

/**
 * Builds a [Methodology] from an [InternalMeasurement.Result.WatchDuration].
 *
 * @throws MeasurementVarianceNotComputableException when methodology is not supported for watch
 *   duration or methodology missing variance information.
 */
fun buildStatsMethodology(
  watchDurationResult: InternalMeasurement.Result.WatchDuration
): Methodology? {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (watchDurationResult.methodologyCase) {
    InternalMeasurement.Result.WatchDuration.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (watchDurationResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          CustomDirectScalarMethodology(watchDurationResult.customDirectMethodology.variance.scalar)
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          throw MeasurementVarianceNotComputableException(
            "Custom direct methodology for frequency is not supported for watch duration."
          )
        }
        // Custom methodology is allowed to explicitly state that the variance is unavailable.
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          return null
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          throw MeasurementVarianceNotComputableException(
            "Variance in CustomDirectMethodology should've been set."
          )
        }
      }
    }
    InternalMeasurement.Result.WatchDuration.MethodologyCase.DETERMINISTIC_SUM -> {
      DeterministicMethodology
    }
    InternalMeasurement.Result.WatchDuration.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Methodology not set.")
    }
  }
}

/**
 * Calculates the impression result from [WeightedMeasurement]s.
 *
 * @throws MeasurementVarianceNotComputableException when metric variance computation fails.
 * @throws MetricResultNotComputableException when impression measurement result is missing.
 */
private fun calculateImpressionResult(
  weightedMeasurements: List<WeightedMeasurement>,
  impressionParams: InternalMetricSpec.ImpressionCountParams,
  variances: Variances? = null,
): MetricResult.ImpressionCountResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasImpression() }) {
      throw MetricResultNotComputableException("Impression measurement result is missing.")
    }
  }

  return impressionCountResult {
    value =
      weightedMeasurements.sumOf { weightedMeasurement ->
        aggregateResults(weightedMeasurement.measurement.details.resultsList).impression.value *
          weightedMeasurement.weight
      }

    // Only compute univariate statistics for union-only operations, i.e. single source measurement.
    if (weightedMeasurements.size == 1 && variances != null) {
      val weightedMeasurement = weightedMeasurements.single()
      val weightedMeasurementVarianceParamsList:
        List<WeightedImpressionMeasurementVarianceParams?> =
        buildWeightedImpressionMeasurementVarianceParamsPerResult(
          weightedMeasurement,
          impressionParams,
        )

      // If any measurement result contains insufficient data for variance calculation, univariate
      // statistics won't be computed.
      if (
        weightedMeasurementVarianceParamsList.all { it != null } &&
          weightedMeasurementVarianceParamsList.first()!!.measurementVarianceParams.impression >=
            0.0
      ) {
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
                } catch (e: Exception) {
                  throw MeasurementVarianceNotComputableException(cause = e)
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
 * @throws MetricResultNotComputableException when impression measurement has no results.
 * @throws MeasurementVarianceNotComputableException when methodology incorrectly set.
 */
fun buildWeightedImpressionMeasurementVarianceParamsPerResult(
  weightedMeasurement: WeightedMeasurement,
  impressionParams: InternalMetricSpec.ImpressionCountParams,
): List<WeightedImpressionMeasurementVarianceParams?> {
  val impressionResults: List<InternalMeasurement.Result.Impression> =
    weightedMeasurement.measurement.details.resultsList.map { it.impression }

  if (impressionResults.isEmpty()) {
    throw MetricResultNotComputableException("Impression measurement should've had results.")
  }

  return impressionResults.map { impressionResult ->
    val statsNoiseMechanism: StatsNoiseMechanism =
      try {
        impressionResult.noiseMechanism.toStatsNoiseMechanism()
      } catch (_: NoiseMechanismUnspecifiedException) {
        return@map null
      }

    val methodology: Methodology = buildStatsMethodology(impressionResult) ?: return@map null

    val maxFrequencyPerUser =
      if (impressionResult.deterministicCount.customMaximumFrequencyPerUser != 0) {
        impressionResult.deterministicCount.customMaximumFrequencyPerUser
      } else {
        impressionParams.maximumFrequencyPerUser
      }

    WeightedImpressionMeasurementVarianceParams(
      binaryRepresentation = weightedMeasurement.binaryRepresentation,
      weight = weightedMeasurement.weight,
      measurementVarianceParams =
        ImpressionMeasurementVarianceParams(
          impression = max(0L, impressionResult.value),
          measurementParams =
            ImpressionMeasurementParams(
              vidSamplingInterval =
                impressionParams.params.vidSamplingInterval.toStatsVidSamplingInterval(),
              dpParams = impressionParams.params.privacyParams.toNoiserDpParams(),
              maximumFrequencyPerUser = maxFrequencyPerUser,
              noiseMechanism = statsNoiseMechanism,
            ),
        ),
      methodology = methodology,
    )
  }
}

/**
 * Builds a [Methodology] from an [InternalMeasurement.Result.Impression].
 *
 * @throws MeasurementVarianceNotComputableException when methodology is not supported for
 *   impression or methodology missing variance information.
 */
fun buildStatsMethodology(impressionResult: InternalMeasurement.Result.Impression): Methodology? {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (impressionResult.methodologyCase) {
    InternalMeasurement.Result.Impression.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (impressionResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          CustomDirectScalarMethodology(impressionResult.customDirectMethodology.variance.scalar)
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          throw MeasurementVarianceNotComputableException(
            "Custom direct methodology for frequency is not supported for impression."
          )
        }
        // Custom methodology is allowed to explicitly state that the variance is unavailable.
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          return null
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          throw MeasurementVarianceNotComputableException(
            "Variance case in CustomDirectMethodology should've been set."
          )
        }
      }
    }
    InternalMeasurement.Result.Impression.MethodologyCase.DETERMINISTIC_COUNT -> {
      DeterministicMethodology
    }
    InternalMeasurement.Result.Impression.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Methodology not set.")
    }
  }
}

/**
 * Calculates the frequency histogram result from [WeightedMeasurement]s.
 *
 * @throws MeasurementVarianceNotComputableException when metric variance computation fails.
 * @throws MetricResultNotComputableException when frequency measurement result is missing.
 */
private fun calculateFrequencyHistogramResults(
  weightedMeasurements: List<WeightedMeasurement>,
  reachAndFrequencyParams: InternalMetricSpec.ReachAndFrequencyParams,
  variances: Variances? = null,
): MetricResult.HistogramResult {
  val aggregatedFrequencyHistogramMap: MutableMap<Long, Double> =
    weightedMeasurements
      .map { weightedMeasurement ->
        if (
          weightedMeasurement.measurement.details.resultsList.any {
            !it.hasReach() || !it.hasFrequency()
          }
        ) {
          throw MetricResultNotComputableException("Reach-Frequency measurement is missing.")
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
  for (frequency in (1L..reachAndFrequencyParams.maximumFrequency)) {
    if (!aggregatedFrequencyHistogramMap.containsKey(frequency)) {
      aggregatedFrequencyHistogramMap[frequency] = 0.0
    }
  }

  val frequencyVariances: FrequencyVariances? =
    if (variances != null) {
      val weightedMeasurementVarianceParamsList: List<WeightedFrequencyMeasurementVarianceParams> =
        weightedMeasurements.mapNotNull { weightedMeasurement ->
          if (
            weightedMeasurement.measurement.details.dataProviderCount == 1 &&
              reachAndFrequencyParams.hasSingleDataProviderParams()
          ) {
            buildWeightedFrequencyMeasurementVarianceParams(
              weightedMeasurement = weightedMeasurement,
              vidSamplingInterval =
                reachAndFrequencyParams.singleDataProviderParams.vidSamplingInterval,
              reachPrivacyParams =
                reachAndFrequencyParams.singleDataProviderParams.reachPrivacyParams,
              frequencyPrivacyParams =
                reachAndFrequencyParams.singleDataProviderParams.frequencyPrivacyParams,
              maximumFrequency = reachAndFrequencyParams.maximumFrequency,
              variances = variances,
            )
          } else {
            buildWeightedFrequencyMeasurementVarianceParams(
              weightedMeasurement = weightedMeasurement,
              vidSamplingInterval =
                reachAndFrequencyParams.multipleDataProviderParams.vidSamplingInterval,
              reachPrivacyParams =
                reachAndFrequencyParams.multipleDataProviderParams.reachPrivacyParams,
              frequencyPrivacyParams =
                reachAndFrequencyParams.multipleDataProviderParams.frequencyPrivacyParams,
              maximumFrequency = reachAndFrequencyParams.maximumFrequency,
              variances = variances,
            )
          }
        }

      if (
        weightedMeasurementVarianceParamsList.size == weightedMeasurements.size &&
          weightedMeasurementVarianceParamsList.size == 1 &&
          weightedMeasurementVarianceParamsList
            .first()
            .measurementVarianceParams
            .reachMeasurementVariance >= 0.0 &&
          weightedMeasurementVarianceParamsList.first().measurementVarianceParams.totalReach > 0.0
      ) {
        try {
          variances.computeMetricVariance(
            FrequencyMetricVarianceParams(weightedMeasurementVarianceParamsList)
          )
        } catch (e: Exception) {
          throw MeasurementVarianceNotComputableException(cause = e)
        }
      } else {
        null
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
 * @throws MeasurementVarianceNotComputableException when reach measurement variance computation
 *   fails or frequency methodology incorrectly set.
 * @throws MetricResultNotComputableException when measurement has no frequency result or has more
 *   than 1 frequency result.
 */
fun buildWeightedFrequencyMeasurementVarianceParams(
  weightedMeasurement: WeightedMeasurement,
  vidSamplingInterval: InternalMetricSpec.VidSamplingInterval,
  reachPrivacyParams: InternalMetricSpec.DifferentialPrivacyParams,
  frequencyPrivacyParams: InternalMetricSpec.DifferentialPrivacyParams,
  maximumFrequency: Int,
  variances: Variances,
): WeightedFrequencyMeasurementVarianceParams? {
  // Get reach measurement variance params
  val weightedReachMeasurementVarianceParams: WeightedReachMeasurementVarianceParams =
    buildWeightedReachMeasurementVarianceParams(
      weightedMeasurement,
      vidSamplingInterval,
      reachPrivacyParams,
    ) ?: return null

  val reachMeasurementVariance: Double =
    try {
      variances.computeMeasurementVariance(
        weightedReachMeasurementVarianceParams.methodology,
        ReachMeasurementVarianceParams(
          weightedReachMeasurementVarianceParams.measurementVarianceParams.reach,
          weightedReachMeasurementVarianceParams.measurementVarianceParams.measurementParams,
        ),
      )
    } catch (e: Exception) {
      throw MeasurementVarianceNotComputableException(cause = e)
    }

  val frequencyResult: InternalMeasurement.Result.Frequency =
    if (weightedMeasurement.measurement.details.resultsList.size == 1) {
      weightedMeasurement.measurement.details.resultsList.single().frequency
    } else if (weightedMeasurement.measurement.details.resultsList.size > 1) {
      throw MetricResultNotComputableException(
        "No supported methodology generates more than one frequency result."
      )
    } else {
      throw MetricResultNotComputableException(
        "Frequency measurement should've had frequency results."
      )
    }

  val frequencyStatsNoiseMechanism: StatsNoiseMechanism =
    try {
      frequencyResult.noiseMechanism.toStatsNoiseMechanism()
    } catch (_: NoiseMechanismUnspecifiedException) {
      return null
    }

  val frequencyMethodology: Methodology = buildStatsMethodology(frequencyResult) ?: return null

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
            vidSamplingInterval = vidSamplingInterval.toStatsVidSamplingInterval(),
            dpParams = frequencyPrivacyParams.toNoiserDpParams(),
            noiseMechanism = frequencyStatsNoiseMechanism,
            maximumFrequency = maximumFrequency,
          ),
      ),
    methodology = frequencyMethodology,
  )
}

/**
 * Builds a [Methodology] from an [InternalMeasurement.Result.Frequency].
 *
 * @throws MeasurementVarianceNotComputableException when methodology not supported for frequency or
 *   methodology missing variance information.
 */
fun buildStatsMethodology(frequencyResult: InternalMeasurement.Result.Frequency): Methodology? {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (frequencyResult.methodologyCase) {
    InternalMeasurement.Result.Frequency.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (frequencyResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          throw MeasurementVarianceNotComputableException(
            "Custom direct methodology for scalar is not supported for frequency."
          )
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
        // Custom methodology is allowed to explicitly state that the variance is unavailable.
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          return null
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          throw MeasurementVarianceNotComputableException(
            "Variance case in CustomDirectMethodology should've been set."
          )
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
    InternalMeasurement.Result.Frequency.MethodologyCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
      HonestMajorityShareShuffleMethodology(
        frequencyVectorSize = frequencyResult.honestMajorityShareShuffle.frequencyVectorSize
      )
    }
    InternalMeasurement.Result.Frequency.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Methodology not set.")
    }
  }
}

/**
 * Calculates the reach result from [WeightedMeasurement]s.
 *
 * @throws MeasurementVarianceNotComputableException when metric variance computation fails.
 * @throws MetricResultNotComputableException when reach measurement result is missing.
 */
private fun calculateReachResult(
  weightedMeasurements: List<WeightedMeasurement>,
  reachParams: InternalMetricSpec.ReachParams,
  variances: Variances? = null,
): MetricResult.ReachResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasReach() }) {
      throw MetricResultNotComputableException("Reach measurement result is missing.")
    }
  }

  return reachResult {
    value =
      weightedMeasurements.sumOf { weightedMeasurement ->
        aggregateResults(weightedMeasurement.measurement.details.resultsList).reach.value *
          weightedMeasurement.weight
      }

    if (variances != null) {
      val weightedMeasurementVarianceParamsList: List<WeightedReachMeasurementVarianceParams> =
        weightedMeasurements.mapNotNull { weightedMeasurement ->
          if (
            weightedMeasurement.measurement.details.dataProviderCount == 1 &&
              reachParams.hasSingleDataProviderParams()
          ) {
            buildWeightedReachMeasurementVarianceParams(
              weightedMeasurement,
              reachParams.singleDataProviderParams.vidSamplingInterval,
              reachParams.singleDataProviderParams.privacyParams,
            )
          } else {
            buildWeightedReachMeasurementVarianceParams(
              weightedMeasurement,
              reachParams.multipleDataProviderParams.vidSamplingInterval,
              reachParams.multipleDataProviderParams.privacyParams,
            )
          }
        }

      // If any measurement contains insufficient data for variance calculation, univariate
      // statistics
      // won't be computed.
      if (
        weightedMeasurementVarianceParamsList.size == weightedMeasurements.size &&
          weightedMeasurementVarianceParamsList.all { it.measurementVarianceParams.reach >= 0.0 }
      ) {
        univariateStatistics = univariateStatistics {
          standardDeviation =
            sqrt(
              try {
                variances.computeMetricVariance(
                  ReachMetricVarianceParams(weightedMeasurementVarianceParamsList)
                )
              } catch (e: Exception) {
                throw MeasurementVarianceNotComputableException(cause = e)
              }
            )
        }
      }
    }
  }
}

/**
 * Calculates the reach result from [WeightedMeasurement]s.
 *
 * @throws MeasurementVarianceNotComputableException when metric variance computation fails.
 * @throws MetricResultNotComputableException when reach measurement result is missing.
 */
private fun calculateReachResult(
  weightedMeasurements: List<WeightedMeasurement>,
  reachAndFrequencyParams: InternalMetricSpec.ReachAndFrequencyParams,
  variances: Variances? = null,
): MetricResult.ReachResult {
  for (weightedMeasurement in weightedMeasurements) {
    if (weightedMeasurement.measurement.details.resultsList.any { !it.hasReach() }) {
      throw MetricResultNotComputableException("Reach measurement result is missing.")
    }
  }

  return reachResult {
    value =
      weightedMeasurements.sumOf { weightedMeasurement ->
        aggregateResults(weightedMeasurement.measurement.details.resultsList).reach.value *
          weightedMeasurement.weight
      }

    if (variances != null) {
      val weightedMeasurementVarianceParamsList: List<WeightedReachMeasurementVarianceParams> =
        weightedMeasurements.mapNotNull { weightedMeasurement ->
          if (
            weightedMeasurement.measurement.details.dataProviderCount == 1 &&
              reachAndFrequencyParams.hasSingleDataProviderParams()
          ) {
            buildWeightedReachMeasurementVarianceParams(
              weightedMeasurement,
              reachAndFrequencyParams.singleDataProviderParams.vidSamplingInterval,
              reachAndFrequencyParams.singleDataProviderParams.reachPrivacyParams,
            )
          } else {
            buildWeightedReachMeasurementVarianceParams(
              weightedMeasurement,
              reachAndFrequencyParams.multipleDataProviderParams.vidSamplingInterval,
              reachAndFrequencyParams.multipleDataProviderParams.reachPrivacyParams,
            )
          }
        }

      // If any measurement contains insufficient data for variance calculation, univariate
      // statistics
      // won't be computed.
      if (
        weightedMeasurementVarianceParamsList.size == weightedMeasurements.size &&
          weightedMeasurementVarianceParamsList.all { it.measurementVarianceParams.reach >= 0.0 }
      ) {
        univariateStatistics = univariateStatistics {
          standardDeviation =
            sqrt(
              try {
                variances.computeMetricVariance(
                  ReachMetricVarianceParams(weightedMeasurementVarianceParamsList)
                )
              } catch (e: Exception) {
                throw MeasurementVarianceNotComputableException(cause = e)
              }
            )
        }
      }
    }
  }
}

/**
 * Builds a nullable [WeightedReachMeasurementVarianceParams].
 *
 * @return null when measurement noise mechanism is not specified.
 * @throws MetricResultNotComputableException when reach measurement result is missing or more than
 *   one reach result is found.
 * @throws MeasurementVarianceNotComputableException when methodology incorrectly set.
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
      throw MetricResultNotComputableException(
        "No supported methodology generates more than one reach result."
      )
    } else {
      throw MetricResultNotComputableException("Reach measurement should've had reach results.")
    }

  val statsNoiseMechanism: StatsNoiseMechanism =
    try {
      reachResult.noiseMechanism.toStatsNoiseMechanism()
    } catch (_: NoiseMechanismUnspecifiedException) {
      return null
    }

  val methodology: Methodology = buildStatsMethodology(reachResult) ?: return null

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

/**
 * Builds a [Methodology] from an [InternalMeasurement.Result.Reach].
 *
 * @throws MeasurementVarianceNotComputableException when methodology not supported for reach or
 *   methodology missing variance information
 */
fun buildStatsMethodology(reachResult: InternalMeasurement.Result.Reach): Methodology? {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (reachResult.methodologyCase) {
    InternalMeasurement.Result.Reach.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (reachResult.customDirectMethodology.variance.typeCase) {
        CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
          CustomDirectScalarMethodology(reachResult.customDirectMethodology.variance.scalar)
        }
        CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
          throw MeasurementVarianceNotComputableException(
            "Custom direct methodology for frequency is not supported for reach."
          )
        }
        // Custom methodology is allowed to explicitly state that the variance is unavailable.
        CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
          return null
        }
        CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
          throw MeasurementVarianceNotComputableException(
            "Variance case in CustomDirectMethodology should've been set."
          )
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
    InternalMeasurement.Result.Reach.MethodologyCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
      HonestMajorityShareShuffleMethodology(
        frequencyVectorSize = reachResult.honestMajorityShareShuffle.frequencyVectorSize
      )
    }
    InternalMeasurement.Result.Reach.MethodologyCase.METHODOLOGY_NOT_SET -> {
      throw MeasurementVarianceNotComputableException("Methodology not set.")
    }
  }
}

private operator fun ProtoDuration.times(weight: Int): ProtoDuration {
  val source = this
  return duration {
    val weightedTotalNanos: Long =
      (TimeUnit.SECONDS.toNanos(source.seconds) + source.nanos) * weight
    seconds = TimeUnit.NANOSECONDS.toSeconds(weightedTotalNanos)
    nanos = (weightedTotalNanos % NANOS_PER_SECOND).toInt()
  }
}

private operator fun ProtoDuration.plus(other: ProtoDuration): ProtoDuration {
  return Durations.add(this, other)
}

private fun InternalMetric.calculateState(): InternalMetric.State {
  val measurementStates = weightedMeasurementsList.map { it.measurement.state }
  return if (measurementStates.all { it == InternalMeasurement.State.SUCCEEDED }) {
    InternalMetric.State.SUCCEEDED
  } else if (measurementStates.any { it == InternalMeasurement.State.FAILED }) {
    InternalMetric.State.FAILED
  } else {
    InternalMetric.State.RUNNING
  }
}
