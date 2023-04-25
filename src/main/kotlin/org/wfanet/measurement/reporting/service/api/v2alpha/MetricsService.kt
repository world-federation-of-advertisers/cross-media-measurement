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

import com.google.protobuf.ByteString
import com.google.protobuf.Duration
import com.google.protobuf.duration
import com.google.protobuf.util.Durations
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.math.min
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.api.v2.alpha.ListMetricsPageToken
import org.wfanet.measurement.api.v2.alpha.ListMetricsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listMetricsPageToken
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
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
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
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
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest.MeasurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequestKt.measurementFailure
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequestKt.measurementResult
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest as InternalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2.Metric.WeightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricResult as InternalMetricResult
import org.wfanet.measurement.internal.reporting.v2.MetricResultKt as InternalMetricResultKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.SetMetricSucceedRequest
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest as internalBatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMetricFailRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMetricSucceedRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest as internalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2.metricResult as internalMetricResult
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.setMetricSucceedRequest
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.listMetricsResponse

private const val MAX_BATCH_SIZE = 1000
private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val NANOS_PER_SECOND = 1_000_000_000

class MetricsService(
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val internalMetricsStub: InternalMetricsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificatesStub: CertificatesCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val secureRandom: SecureRandom,
  private val signingPrivateKeyDir: File,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  private val metricSpecConfig: MetricSpecConfig,
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : MetricsCoroutineImplBase() {

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
      coroutineContext
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
    private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  ) {
    /**
     * Creates CMM public [Measurement]s and [InternalMeasurement]s from a list of [InternalMetric].
     */
    suspend fun createCmmsMeasurements(
      internalMetricsList: List<InternalMetric>,
      principal: MeasurementConsumerPrincipal,
    ) = coroutineScope {
      val measurementConsumer: MeasurementConsumer = getMeasurementConsumer(principal)

      // Gets all external IDs of primitive reporting sets from the metric list.
      val externalPrimitiveReportingSetIds: Set<Long> =
        internalMetricsList
          .flatMap { internalMetric ->
            internalMetric.weightedMeasurementsList.flatMap { weightedMeasurement ->
              weightedMeasurement.measurement.primitiveReportingSetBasesList.map {
                it.externalReportingSetId
              }
            }
          }
          .toSet()

      val internalPrimitiveReportingSetMap: Map<Long, InternalReportingSet> =
        buildInternalReportingSetMap(
          principal.resourceKey.measurementConsumerId,
          externalPrimitiveReportingSetIds
        )

      val deferred = mutableListOf<Deferred<MeasurementIds>>()

      for (internalMetric in internalMetricsList) {
        for (weightedMeasurement in internalMetric.weightedMeasurementsList) {
          // If the internal measurement has a CMMS measurement ID, the CMMS measurement has been
          // created already.
          if (weightedMeasurement.measurement.cmmsMeasurementId.isNotBlank()) {
            continue
          }

          deferred.add(
            async {
              measurementIds {
                cmmsCreateMeasurementRequestId =
                  weightedMeasurement.measurement.cmmsCreateMeasurementRequestId
                val measurement =
                  createCmmsMeasurement(
                    weightedMeasurement.measurement,
                    internalMetric.metricSpec,
                    internalPrimitiveReportingSetMap,
                    measurementConsumer,
                    principal,
                  )
                cmmsMeasurementId = MeasurementKey.fromName(measurement.name)!!.measurementId
              }
            }
          )
        }
      }

      val measurementIdsList = deferred.awaitAll()
      if (measurementIdsList.isEmpty()) {
        return@coroutineScope
      }

      // Set CMMS measurement IDs.
      try {
        internalMeasurementsStub.batchSetCmmsMeasurementIds(
          batchSetCmmsMeasurementIdsRequest {
            this.cmmsMeasurementConsumerId = principal.resourceKey.measurementConsumerId
            measurementIds += measurementIdsList
          }
        )
      } catch (e: StatusException) {
        throw Exception(
          "Unable to set the CMMS measurement IDs for the measurements in the reporting database.",
          e
        )
      }
    }

    /** Creates a CMMS measurement from an [InternalMeasurement]. */
    private suspend fun createCmmsMeasurement(
      internalMeasurement: InternalMeasurement,
      metricSpec: InternalMetricSpec,
      internalPrimitiveReportingSetMap: Map<Long, InternalReportingSet>,
      measurementConsumer: MeasurementConsumer,
      principal: MeasurementConsumerPrincipal,
    ): Measurement {
      val eventGroupEntriesByDataProvider =
        groupEventGroupEntriesByDataProvider(internalMeasurement, internalPrimitiveReportingSetMap)

      val createMeasurementRequest: CreateMeasurementRequest =
        buildCreateMeasurementRequest(
          internalMeasurement,
          metricSpec,
          measurementConsumer,
          eventGroupEntriesByDataProvider,
          principal,
        )

      try {
        return measurementsStub
          .withAuthenticationKey(principal.config.apiKey)
          .createMeasurement(createMeasurementRequest)
      } catch (e: StatusException) {
        throw Exception("Unable to create a CMMS measurement.", e)
      }
    }

    /** Builds a CMMS [CreateMeasurementRequest]. */
    private suspend fun buildCreateMeasurementRequest(
      internalMeasurement: InternalMeasurement,
      metricSpec: InternalMetricSpec,
      measurementConsumer: MeasurementConsumer,
      eventGroupEntriesByDataProvider: Map<DataProviderKey, List<EventGroupEntry>>,
      principal: MeasurementConsumerPrincipal,
    ): CreateMeasurementRequest {
      val measurementConsumerSigningKey = getMeasurementConsumerSigningKey(principal)
      val measurementEncryptionPublicKey = measurementConsumer.publicKey.data

      val measurement = measurement {
        this.measurementConsumerCertificate = principal.config.signingCertificateName

        dataProviders +=
          buildDataProviderEntries(
            eventGroupEntriesByDataProvider,
            measurementEncryptionPublicKey,
            measurementConsumerSigningKey,
            principal.config.apiKey,
          )

        val unsignedMeasurementSpec: MeasurementSpec =
          buildUnsignedMeasurementSpec(
            measurementEncryptionPublicKey,
            dataProviders.map { it.value.nonceHash },
            metricSpec
          )

        this.measurementSpec =
          signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)

        this.measurementReferenceId = internalMeasurement.cmmsCreateMeasurementRequestId
      }

      return createMeasurementRequest { this.measurement = measurement }
    }

    /** Gets a [SigningKeyHandle] for a [MeasurementConsumerPrincipal]. */
    private suspend fun getMeasurementConsumerSigningKey(
      principal: MeasurementConsumerPrincipal,
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
      measurementEncryptionPublicKey: ByteString,
      nonceHashes: List<ByteString>,
      metricSpec: InternalMetricSpec,
    ): MeasurementSpec {
      return measurementSpec {
        measurementPublicKey = measurementEncryptionPublicKey
        this.nonceHashes += nonceHashes

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metricSpec.typeCase) {
          InternalMetricSpec.TypeCase.REACH -> {
            reach = metricSpec.reach.toReach()
          }
          InternalMetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
            reachAndFrequency = metricSpec.frequencyHistogram.toReachAndFrequency()
          }
          InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
            impression = metricSpec.impressionCount.toImpression()
          }
          InternalMetricSpec.TypeCase.WATCH_DURATION -> {
            duration = metricSpec.watchDuration.toDuration()
          }
          InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
            error("Unset metric type should've already raised error.")
        }
        vidSamplingInterval = metricSpec.vidSamplingInterval.toCmmsVidSamplingInterval()
      }
    }

    /**
     * Builds a [List] of [Measurement.DataProviderEntry] messages from
     * [eventGroupEntriesByDataProvider].
     */
    private suspend fun buildDataProviderEntries(
      eventGroupEntriesByDataProvider: Map<DataProviderKey, List<EventGroupEntry>>,
      measurementEncryptionPublicKey: ByteString,
      measurementConsumerSigningKey: SigningKeyHandle,
      apiAuthenticationKey: String,
    ): List<Measurement.DataProviderEntry> {
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
            certificatesStub
              .withAuthenticationKey(apiAuthenticationKey)
              .getCertificate(getCertificateRequest { name = dataProvider.certificate })
          } catch (e: StatusException) {
            throw Exception("Unable to retrieve Certificate ${dataProvider.certificate}", e)
          }
        if (
          certificate.revocationState != Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED
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
            MeasurementKt.DataProviderEntryKt.value {
              dataProviderCertificate = certificate.name
              dataProviderPublicKey = dataProvider.publicKey
              this.encryptedRequisitionSpec = encryptRequisitionSpec
              nonceHash = hashSha256(requisitionSpec.nonce)
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
      internalPrimitiveReportingSetMap: Map<Long, InternalReportingSet>,
    ): Map<DataProviderKey, List<EventGroupEntry>> {
      return measurement.primitiveReportingSetBasesList
        .flatMap { primitiveReportingSetBasis ->
          val internalPrimitiveReportingSet =
            internalPrimitiveReportingSetMap.getValue(
              primitiveReportingSetBasis.externalReportingSetId
            )

          internalPrimitiveReportingSet.primitive.eventGroupKeysList.map { internalEventGroupKey ->
            val eventGroupKey =
              EventGroupKey(
                internalEventGroupKey.cmmsMeasurementConsumerId,
                internalEventGroupKey.cmmsDataProviderId,
                internalEventGroupKey.cmmsEventGroupId
              )
            val eventGroupName = eventGroupKey.toName()
            val filtersList =
              (primitiveReportingSetBasis.filtersList + internalPrimitiveReportingSet.filter)
                .filterNotNull()
            val filter: String? = if (filtersList.isEmpty()) null else buildConjunction(filtersList)

            eventGroupKey to
              RequisitionSpecKt.eventGroupEntry {
                key = eventGroupName
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = measurement.timeInterval.toCmmsTimeInterval()
                    if (filter != null) {
                      this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                    }
                  }
              }
          }
        }
        .groupBy(
          { (eventGroupKey, _) -> DataProviderKey(eventGroupKey.cmmsDataProviderId) },
          { (_, eventGroupEntry) -> eventGroupEntry }
        )
    }

    /** Combines event group filters. */
    private fun buildConjunction(filters: Collection<String>): String {
      return filters.joinToString(separator = " AND ") { filter -> "($filter)" }
    }

    /** Gets a [MeasurementConsumer] based on a CMMS ID. */
    private suspend fun getMeasurementConsumer(
      principal: MeasurementConsumerPrincipal,
    ): MeasurementConsumer {
      return try {
        measurementConsumersStub
          .withAuthenticationKey(principal.config.apiKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest { name = principal.resourceKey.toName() }
          )
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the measurement consumer " + "[${principal.resourceKey.toName()}].",
          e
        )
      }
    }

    /**
     * Builds a map of external reporting set IDs to [InternalReportingSet]s.
     *
     * This helps reduce the number of RPCs.
     */
    private suspend fun buildInternalReportingSetMap(
      cmmsMeasurementConsumerId: String,
      externalReportingSetIds: Set<Long>,
    ): Map<Long, InternalReportingSet> {
      val batchGetReportingSetsRequest = batchGetReportingSetsRequest {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalReportingSetIds.forEach { this.externalReportingSetIds += it }
      }

      val internalReportingSetsList =
        try {
          internalReportingSetsStub
            .batchGetReportingSets(batchGetReportingSetsRequest)
            .reportingSetsList
        } catch (e: StatusException) {
          throw Exception("Unable to get reporting sets from the reporting database.", e)
        }

      return internalReportingSetsList.associateBy { it.externalReportingSetId }
    }

    /** Gets a signing certificate x509Der in ByteString. */
    private suspend fun getSigningCertificateDer(
      principal: MeasurementConsumerPrincipal,
    ): ByteString {
      // TODO: Replace this with caching certificates or having them stored alongside the private
      // key.
      return try {
        certificatesStub
          .withAuthenticationKey(principal.config.apiKey)
          .getCertificate(getCertificateRequest { name = principal.config.signingCertificateName })
          .x509Der
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the signing certificate for the measurement consumer " +
            "[$principal.config.signingCertificateName].",
          e
        )
      }
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
        getCmmsMeasurements(internalMeasurements, apiAuthenticationKey, principal).groupBy {
          measurement ->
          measurement.state
        }

      var anyUpdate = false

      for ((newState, measurementsList) in newStateToCmmsMeasurements) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (newState) {
          Measurement.State.SUCCEEDED -> {
            syncSucceededInternalMeasurements(measurementsList, apiAuthenticationKey, principal)
            anyUpdate = true
          }
          Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
          Measurement.State.COMPUTING -> {} // Do nothing.
          Measurement.State.FAILED,
          Measurement.State.CANCELLED -> {
            syncFailedInternalMeasurements(
              measurementsList,
              principal.resourceKey.measurementConsumerId
            )
            anyUpdate = true
          }
          Measurement.State.STATE_UNSPECIFIED ->
            error("The CMMS measurement state should've been set.")
          Measurement.State.UNRECOGNIZED -> error("Unrecognized CMMS measurement state.")
        }
      }

      return anyUpdate
    }

    /**
     * Syncs [InternalMeasurement]s by storing the failure states of the given failed or canceled
     * CMMS [Measurement]s.
     */
    private suspend fun syncFailedInternalMeasurements(
      failedMeasurementsList: List<Measurement>,
      cmmsMeasurementConsumerId: String,
    ) {
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

      try {
        internalMeasurementsStub
          .batchSetMeasurementFailures(batchSetInternalMeasurementFailuresRequest)
          .measurementsList
      } catch (e: StatusException) {
        throw Exception(
          "Unable to set measurement failures for the measurements in the reporting database.",
          e
        )
      }
    }

    /**
     * Syncs [InternalMeasurement]s by storing the measurement results of the given succeeded CMMS
     * [Measurement]s.
     */
    private suspend fun syncSucceededInternalMeasurements(
      succeededMeasurementsList: List<Measurement>,
      apiAuthenticationKey: String,
      principal: MeasurementConsumerPrincipal,
    ) {
      val batchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = principal.resourceKey.measurementConsumerId
        measurementResults +=
          succeededMeasurementsList.map { measurement ->
            measurementResult {
              cmmsMeasurementId = MeasurementKey.fromName(measurement.name)!!.measurementId
              result =
                buildInternalMeasurementResult(
                  measurement,
                  apiAuthenticationKey,
                  principal.resourceKey.toName()
                )
            }
          }
      }

      try {
        internalMeasurementsStub
          .batchSetMeasurementResults(batchSetMeasurementResultsRequest)
          .measurementsList
      } catch (e: StatusException) {
        throw Exception(
          "Unable to set measurement results for the measurements in the reporting database.",
          e
        )
      }
    }

    /** Retrieves [Measurement]s from the CMMS. */
    private suspend fun getCmmsMeasurements(
      internalMeasurements: List<InternalMeasurement>,
      apiAuthenticationKey: String,
      principal: MeasurementConsumerPrincipal,
    ): List<Measurement> = coroutineScope {
      val deferred =
        internalMeasurements.map { internalMeasurement ->
          val measurementResourceName =
            MeasurementKey(
                principal.resourceKey.measurementConsumerId,
                internalMeasurement.cmmsMeasurementId
              )
              .toName()
          async {
            try {
              measurementsStub
                .withAuthenticationKey(apiAuthenticationKey)
                .getMeasurement(getMeasurementRequest { name = measurementResourceName })
            } catch (e: StatusException) {
              throw Exception("Unable to retrieve the measurement [$measurementResourceName].", e)
            }
          }
        }

      deferred.awaitAll()
    }

    /** Builds an [InternalMeasurement.Result]. */
    private suspend fun buildInternalMeasurementResult(
      measurement: Measurement,
      apiAuthenticationKey: String,
      principalName: String,
    ): InternalMeasurement.Result {
      val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
      val encryptionPrivateKeyHandle =
        encryptionKeyPairStore.getPrivateKeyHandle(
          principalName,
          EncryptionPublicKey.parseFrom(measurementSpec.measurementPublicKey).data
        )
          ?: failGrpc(Status.FAILED_PRECONDITION) {
            "Encryption private key not found for the measurement ${measurement.name}."
          }

      return aggregateResults(
        measurement.resultsList
          .map {
            decryptMeasurementResultPair(it, encryptionPrivateKeyHandle, apiAuthenticationKey)
          }
          .map(Measurement.Result::toInternal)
      )
    }

    /** Decrypts a [Measurement.ResultPair] to [Measurement.Result] */
    private suspend fun decryptMeasurementResultPair(
      measurementResultPair: Measurement.ResultPair,
      encryptionPrivateKeyHandle: PrivateKeyHandle,
      apiAuthenticationKey: String,
    ): Measurement.Result {
      // TODO: Cache the certificate
      val certificate =
        try {
          certificatesStub
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

    /** Aggregates a list of [InternalMeasurement.Result]s to a [InternalMeasurement.Result] */
    private fun aggregateResults(
      internalMeasurementResults: List<InternalMeasurement.Result>
    ): InternalMeasurement.Result {
      if (internalMeasurementResults.isEmpty()) {
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
      for (result in internalMeasurementResults) {
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
      }
    }
  }

  override suspend fun listMetrics(request: ListMetricsRequest): ListMetricsResponse {
    val listMetricsPageToken: ListMetricsPageToken = request.toListMetricsPageToken()

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Metrics belonging to other MeasurementConsumers."
          }
        }
      }
    }
    val apiAuthenticationKey: String = principal.config.apiKey

    val streamInternalMetricRequest: StreamMetricsRequest =
      listMetricsPageToken.toStreamMetricsRequest()

    val results: List<InternalMetric> =
      try {
        internalMetricsStub.streamMetrics(streamInternalMetricRequest).toList()
      } catch (e: StatusException) {
        throw Exception("Unable to list metrics from the reporting database.", e)
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
        .filter { internalMetric -> internalMetric.state == InternalMetric.State.RUNNING }
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

    // If any measurement got updated, pull the list of the up-to-date internal metrics. Otherwise,
    // use the original list.
    val internalMetrics: List<InternalMetric> =
      if (anyMeasurementUpdated) {
        batchGetInternalMetrics(
          principal.resourceKey.measurementConsumerId,
          subResults.map { internalMetric -> internalMetric.externalMetricId }
        )
      } else {
        subResults
      }

    return listMetricsResponse {
      metrics +=
        refreshInternalMetrics(principal.resourceKey.measurementConsumerId, internalMetrics)
          .map(InternalMetric::toMetric)

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Refreshes a list of [InternalMetric]s. */
  private suspend fun refreshInternalMetrics(
    cmmsMeasurementConsumerId: String,
    metrics: List<InternalMetric>
  ): List<InternalMetric> {
    val setMetricSucceedRequests = mutableListOf<SetMetricSucceedRequest>()
    val failedExternalMetricIds = mutableListOf<Long>()

    for (metric in metrics) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (metric.state) {
        InternalMetric.State.RUNNING -> {
          val measurements = metric.weightedMeasurementsList.map { it.measurement }
          if (measurements.all { it.state == InternalMeasurement.State.SUCCEEDED }) {
            setMetricSucceedRequests += setMetricSucceedRequest {
              externalMetricId = metric.externalMetricId
              result = buildMetricResult(metric)
            }
          } else if (measurements.any { it.state == InternalMeasurement.State.FAILED }) {
            failedExternalMetricIds += metric.externalMetricId
          }
        }
        InternalMetric.State.SUCCEEDED,
        InternalMetric.State.FAILED -> {} // Do nothing
        InternalMetric.State.STATE_UNSPECIFIED -> error("Metric state should've been set.")
        InternalMetric.State.UNRECOGNIZED -> error("Unrecognized metric state.")
      }
    }

    val latestMetricsMap = mutableMapOf<Long, InternalMetric>()

    if (setMetricSucceedRequests.isNotEmpty()) {
      latestMetricsMap +=
        try {
          internalMetricsStub
            .batchSetMetricSucceed(
              batchSetMetricSucceedRequest {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                requests += setMetricSucceedRequests
              }
            )
            .metricsList
            .associateBy { it.externalMetricId }
        } catch (e: StatusException) {
          throw Exception("Unable to set metric results to the reporting database.", e)
        }
    }

    if (failedExternalMetricIds.isNotEmpty()) {
      latestMetricsMap +=
        try {
          internalMetricsStub
            .batchSetMetricFail(
              batchSetMetricFailRequest {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                this.externalMetricIds += failedExternalMetricIds
              }
            )
            .metricsList
            .associateBy { it.externalMetricId }
        } catch (e: StatusException) {
          throw Exception("Unable to set metric failures to the reporting database.", e)
        }
    }

    return metrics.map { metric -> latestMetricsMap.getOrDefault(metric.externalMetricId, metric) }
  }

  /** Builds an [InternalMetricResult] from the given [InternalMetric]. */
  private fun buildMetricResult(metric: InternalMetric): InternalMetricResult {
    return internalMetricResult {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (metric.metricSpec.typeCase) {
        InternalMetricSpec.TypeCase.REACH -> {
          reach = calculateReachResults(metric.weightedMeasurementsList)
        }
        InternalMetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
          frequencyHistogram =
            calculateFrequencyHistogramResults(
              metric.weightedMeasurementsList,
              metric.metricSpec.frequencyHistogram.maximumFrequencyPerUser
            )
        }
        InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
          impressionCount = calculateImpressionResults(metric.weightedMeasurementsList)
        }
        InternalMetricSpec.TypeCase.WATCH_DURATION -> {
          watchDuration = calculateWatchDurationResults(metric.weightedMeasurementsList)
        }
        InternalMetricSpec.TypeCase.TYPE_NOT_SET -> {
          error { "Metric Type should've been set." }
        }
      }
    }
  }

  /** Calculates the watch duration result by summing up [WeightedMeasurement]s. */
  private fun calculateWatchDurationResults(
    weightedMeasurements: List<WeightedMeasurement>
  ): InternalMetricResult.WatchDurationResult {
    return InternalMetricResultKt.watchDurationResult {
      val watchDuration: Duration =
        weightedMeasurements
          .map { weightedMeasurement ->
            if (!weightedMeasurement.measurement.details.result.hasWatchDuration()) {
              error("Watch duration measurement is missing.")
            }
            weightedMeasurement.measurement.details.result.watchDuration.value *
              weightedMeasurement.weight
          }
          .reduce { sum, element -> sum + element }
      value = watchDuration.seconds + (watchDuration.nanos.toDouble() / NANOS_PER_SECOND)
    }
  }

  /** Calculates the impression result by summing up [WeightedMeasurement]s. */
  private fun calculateImpressionResults(
    weightedMeasurements: List<WeightedMeasurement>
  ): InternalMetricResult.ImpressionCountResult {
    return InternalMetricResultKt.impressionCountResult {
      value =
        weightedMeasurements.sumOf { weightedMeasurement ->
          if (!weightedMeasurement.measurement.details.result.hasImpression()) {
            error("Impression measurement is missing.")
          }
          weightedMeasurement.measurement.details.result.impression.value *
            weightedMeasurement.weight
        }
    }
  }

  /** Calculates the frequency histogram result by summing up [WeightedMeasurement]s. */
  private fun calculateFrequencyHistogramResults(
    weightedMeasurements: List<WeightedMeasurement>,
    maximumFrequency: Int
  ): InternalMetricResult.HistogramResult {
    val aggregatedFrequencyHistogramMap: MutableMap<Long, Double> =
      weightedMeasurements
        .map { weightedMeasurement ->
          val result = weightedMeasurement.measurement.details.result
          if (!result.hasFrequency() || !result.hasReach()) {
            error("Reach-Frequency measurement is missing.")
          }
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
    for (frequency in (1L..maximumFrequency)) {
      if (!aggregatedFrequencyHistogramMap.containsKey(frequency)) {
        aggregatedFrequencyHistogramMap[frequency] = 0.0
      }
    }

    return InternalMetricResultKt.histogramResult {
      bins +=
        aggregatedFrequencyHistogramMap.map { (frequency, count) ->
          InternalMetricResultKt.HistogramResultKt.bin {
            label = frequency.toString()
            binResult = InternalMetricResultKt.HistogramResultKt.binResult { value = count }
          }
        }
    }
  }

  /** Calculates the reach result by summing up [WeightedMeasurement]s. */
  private fun calculateReachResults(
    weightedMeasurements: List<WeightedMeasurement>
  ): InternalMetricResult.ReachResult {
    return InternalMetricResultKt.reachResult {
      value =
        weightedMeasurements.sumOf { weightedMeasurement ->
          if (!weightedMeasurement.measurement.details.result.hasReach()) {
            error("Reach measurement is missing.")
          }
          weightedMeasurement.measurement.details.result.reach.value * weightedMeasurement.weight
        }
    }
  }

  /** Gets a batch of [InternalMetric]. */
  private suspend fun batchGetInternalMetrics(
    cmmsMeasurementConsumerId: String,
    externalMetricIds: List<Long>,
  ): List<InternalMetric> {
    val batchGetMetricsRequest = batchGetMetricsRequest {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalMetricIds += externalMetricIds
    }

    return try {
      internalMetricsStub.batchGetMetrics(batchGetMetricsRequest).metricsList
    } catch (e: StatusException) {
      throw Exception("Unable to get metrics from the reporting database.", e)
    }
  }

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

    val internalCreateMetricRequest: InternalCreateMetricRequest =
      buildInternalCreateMetricRequest(principal.resourceKey.measurementConsumerId, request)

    val internalMetric =
      try {
        internalMetricsStub.createMetric(internalCreateMetricRequest)
      } catch (e: StatusException) {
        throw Exception("Unable to create the metric in the reporting database.", e)
      }

    if (internalMetric.state == InternalMetric.State.RUNNING) {
      measurementSupplier.createCmmsMeasurements(listOf(internalMetric), principal)
    }

    // Convert the internal metric to public and return it.
    return internalMetric.toMetric()
  }

  override suspend fun batchCreateMetrics(
    request: BatchCreateMetricsRequest,
  ): BatchCreateMetricsResponse {
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

    grpcRequire(request.requestsList.isNotEmpty()) { "Requests is empty." }
    grpcRequire(request.requestsList.size <= MAX_BATCH_SIZE) {
      "At most $MAX_BATCH_SIZE requests can be supported in a batch."
    }

    val internalCreateMetricRequestsList: List<InternalCreateMetricRequest> =
      request.requestsList.map { createMetricRequest ->
        buildInternalCreateMetricRequest(
          principal.resourceKey.measurementConsumerId,
          createMetricRequest
        )
      }

    val internalMetrics =
      try {
        internalMetricsStub
          .batchCreateMetrics(
            internalBatchCreateMetricsRequest {
              cmmsMeasurementConsumerId = principal.resourceKey.measurementConsumerId
              requests += internalCreateMetricRequestsList
            }
          )
          .metricsList
      } catch (e: StatusException) {
        throw Exception("Unable to create the metric in the reporting database.", e)
      }

    measurementSupplier.createCmmsMeasurements(internalMetrics, principal)

    // Convert the internal metric to public and return it.
    return batchCreateMetricsResponse { metrics += internalMetrics.map { it.toMetric() } }
  }

  /** Builds an [InternalCreateMetricRequest]. */
  private suspend fun buildInternalCreateMetricRequest(
    cmmsMeasurementConsumerId: String,
    request: CreateMetricRequest,
  ): InternalCreateMetricRequest {
    grpcRequire(request.hasMetric()) { "Metric is not specified." }
    grpcRequire(request.metric.reportingSet.isNotBlank()) {
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

    return internalCreateMetricRequest {
      requestId = request.requestId
      metric = internalMetric {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalReportingSetId = internalReportingSet.externalReportingSetId
        timeInterval = request.metric.timeInterval.toInternal()
        metricSpec = buildInternalMetricSpec(request.metric.metricSpec)
        weightedMeasurements +=
          buildInitialInternalMeasurements(
            cmmsMeasurementConsumerId,
            request.metric,
            internalReportingSet
          )
        details = InternalMetricKt.details { filters += request.metric.filtersList }
      }
    }
  }

  /** Builds an [InternalMetricSpec] given a [MetricSpec]. */
  private fun buildInternalMetricSpec(
    metricSpec: MetricSpec,
  ): InternalMetricSpec {
    return internalMetricSpec {
      val defaultVidSamplingInterval: MetricSpecConfig.VidSamplingInterval =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metricSpec.typeCase) {
          MetricSpec.TypeCase.REACH -> {
            reach = buildInternalReachParams(metricSpec.reach)
            metricSpecConfig.reachVidSamplingInterval
          }
          MetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
            frequencyHistogram =
              buildInternalFrequencyHistogramParams(metricSpec.frequencyHistogram)
            metricSpecConfig.frequencyHistogramVidSamplingInterval
          }
          MetricSpec.TypeCase.IMPRESSION_COUNT -> {
            impressionCount = buildInternalImpressionCountParams(metricSpec.impressionCount)
            metricSpecConfig.impressionCountVidSamplingInterval
          }
          MetricSpec.TypeCase.WATCH_DURATION -> {
            watchDuration = buildInternalWatchDurationParams(metricSpec.watchDuration)
            metricSpecConfig.watchDurationVidSamplingInterval
          }
          MetricSpec.TypeCase.TYPE_NOT_SET ->
            failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Metric is not specified." }
        }

      vidSamplingInterval =
        if (metricSpec.hasVidSamplingInterval()) {
          metricSpec.vidSamplingInterval.toInternal()
        } else defaultVidSamplingInterval.toInternal()

      grpcRequire(vidSamplingInterval.start >= 0) {
        "vidSamplingInterval.start cannot be negative."
      }
      grpcRequire(vidSamplingInterval.start < 1) {
        "vidSamplingInterval.start must be smaller than 1."
      }
      grpcRequire(vidSamplingInterval.width > 0) {
        "vidSamplingInterval.width must be greater than 0."
      }
      grpcRequire(vidSamplingInterval.start + vidSamplingInterval.width <= 1) {
        "vidSamplingInterval start + width cannot be greater than 1."
      }
    }
  }

  /** Builds an [InternalMetricSpec.ReachParams] given a [MetricSpec.ReachParams]. */
  private fun buildInternalReachParams(
    reachParams: MetricSpec.ReachParams,
  ): InternalMetricSpec.ReachParams {
    grpcRequire(reachParams.hasPrivacyParams()) { "privacyParams in reach is not set." }

    return InternalMetricSpecKt.reachParams {
      privacyParams =
        buildInternalDifferentialPrivacyParams(
          reachParams.privacyParams,
          metricSpecConfig.reachParams.privacyParams.epsilon,
          metricSpecConfig.reachParams.privacyParams.delta
        )
    }
  }

  /**
   * Builds an [InternalMetricSpec.FrequencyHistogramParams] given a
   * [MetricSpec.FrequencyHistogramParams].
   */
  private fun buildInternalFrequencyHistogramParams(
    frequencyHistogramParams: MetricSpec.FrequencyHistogramParams
  ): InternalMetricSpec.FrequencyHistogramParams {
    grpcRequire(frequencyHistogramParams.hasReachPrivacyParams()) {
      "reachPrivacyParams in frequency histogram is not set."
    }
    grpcRequire(frequencyHistogramParams.hasFrequencyPrivacyParams()) {
      "frequencyPrivacyParams in frequency histogram is not set."
    }

    return InternalMetricSpecKt.frequencyHistogramParams {
      reachPrivacyParams =
        buildInternalDifferentialPrivacyParams(
          frequencyHistogramParams.reachPrivacyParams,
          metricSpecConfig.frequencyHistogramParams.reachPrivacyParams.epsilon,
          metricSpecConfig.frequencyHistogramParams.reachPrivacyParams.delta
        )
      frequencyPrivacyParams =
        buildInternalDifferentialPrivacyParams(
          frequencyHistogramParams.frequencyPrivacyParams,
          metricSpecConfig.frequencyHistogramParams.frequencyPrivacyParams.epsilon,
          metricSpecConfig.frequencyHistogramParams.frequencyPrivacyParams.delta
        )
      maximumFrequencyPerUser =
        if (frequencyHistogramParams.hasMaximumFrequencyPerUser()) {
          frequencyHistogramParams.maximumFrequencyPerUser
        } else {
          metricSpecConfig.frequencyHistogramParams.maximumFrequencyPerUser
        }
    }
  }

  /**
   * Builds an [InternalMetricSpec.WatchDurationParams] given a [MetricSpec.WatchDurationParams].
   */
  private fun buildInternalWatchDurationParams(
    watchDurationParams: MetricSpec.WatchDurationParams
  ): InternalMetricSpec.WatchDurationParams {
    grpcRequire(watchDurationParams.hasPrivacyParams()) {
      "privacyParams in watch duration is not set."
    }

    return InternalMetricSpecKt.watchDurationParams {
      privacyParams =
        buildInternalDifferentialPrivacyParams(
          watchDurationParams.privacyParams,
          metricSpecConfig.watchDurationParams.privacyParams.epsilon,
          metricSpecConfig.watchDurationParams.privacyParams.delta
        )
      maximumWatchDurationPerUser =
        if (watchDurationParams.hasMaximumWatchDurationPerUser()) {
          watchDurationParams.maximumWatchDurationPerUser
        } else {
          metricSpecConfig.watchDurationParams.maximumWatchDurationPerUser
        }
    }
  }

  /**
   * Builds an [InternalMetricSpec.ImpressionCountParams] given a
   * [MetricSpec.ImpressionCountParams].
   */
  private fun buildInternalImpressionCountParams(
    impressionCountParams: MetricSpec.ImpressionCountParams
  ): InternalMetricSpec.ImpressionCountParams {
    grpcRequire(impressionCountParams.hasPrivacyParams()) {
      "privacyParams in impression count is not set."
    }

    return InternalMetricSpecKt.impressionCountParams {
      privacyParams =
        buildInternalDifferentialPrivacyParams(
          impressionCountParams.privacyParams,
          metricSpecConfig.impressionCountParams.privacyParams.epsilon,
          metricSpecConfig.impressionCountParams.privacyParams.delta
        )
      maximumFrequencyPerUser =
        if (impressionCountParams.hasMaximumFrequencyPerUser()) {
          impressionCountParams.maximumFrequencyPerUser
        } else {
          metricSpecConfig.impressionCountParams.maximumFrequencyPerUser
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
        measurement = internalMeasurement {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          timeInterval = metric.timeInterval.toInternal()
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
            this.externalReportingSetIds += apiIdToExternalId(reportingSetKey.reportingSetId)
          }
        )
        .reportingSetsList
        .first()
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve a reporting set from the reporting database using the provided " +
          "reportingSet [$reportingSetName].",
        e
      )
    }
  }
}

/**
 * Build an [InternalMetricSpec.DifferentialPrivacyParams] given
 * [MetricSpec.DifferentialPrivacyParams]. If any field in the given
 * [MetricSpec.DifferentialPrivacyParams] is unspecified, it will use the provided default value.
 */
private fun buildInternalDifferentialPrivacyParams(
  dpParams: MetricSpec.DifferentialPrivacyParams,
  defaultEpsilon: Double,
  defaultDelta: Double
): InternalMetricSpec.DifferentialPrivacyParams {
  return InternalMetricSpecKt.differentialPrivacyParams {
    epsilon = if (dpParams.hasEpsilon()) dpParams.epsilon else defaultEpsilon
    delta = if (dpParams.hasDelta()) dpParams.delta else defaultDelta
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

  val isValidPageSize =
    source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE

  return if (pageToken.isNotBlank()) {
    ListMetricsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
        "Arguments must be kept the same when using a page token."
      }

      if (isValidPageSize) {
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
