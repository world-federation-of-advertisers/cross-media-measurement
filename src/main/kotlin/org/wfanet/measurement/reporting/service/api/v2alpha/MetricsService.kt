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
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.TimeInterval as CmmsTimeInterval
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval as cmmsTimeInterval
import org.wfanet.measurement.api.withAuthenticationKey
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
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.internal.reporting.v2alpha.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2alpha.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.Metric.WeightedMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MetricResult as InternalMetricResult
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2alpha.batchCreateMetricsRequest as internalBatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.copy
import org.wfanet.measurement.internal.reporting.v2alpha.getMetricByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.v2alpha.getReportingSetRequest as getInternalReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2alpha.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResult as MetricResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.bin
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.doubleResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.histogramResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.integerResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.timeInterval

private const val MAX_BATCH_SIZE = 1000
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
  private val certificatesStub: CertificatesCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val secureRandom: SecureRandom,
  private val signingPrivateKeyDir: File,
  private val trustedCertificates: Map<ByteString, X509Certificate>
) : MetricsCoroutineImplBase() {

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

      val deferred = mutableListOf<Deferred<Measurement>>()
      val measurementRequestIdToExternalId = mutableMapOf<String, Long>()

      for (internalMetric in internalMetricsList) {
        for (weightedMeasurement in internalMetric.weightedMeasurementsList) {
          if (weightedMeasurement.measurement.cmmsMeasurementId.isNotBlank()) continue

          measurementRequestIdToExternalId[
            weightedMeasurement.measurement.cmmsCreateMeasurementRequestId] =
            weightedMeasurement.measurement.externalMeasurementId

          deferred.add(
            async {
              createCmmsMeasurement(
                weightedMeasurement.measurement,
                internalMetric.metricSpec,
                internalPrimitiveReportingSetMap,
                measurementConsumer,
                principal,
              )
            }
          )
        }
      }

      // Set CMMs measurement IDs.
      val measurements = deferred.awaitAll()
      if (measurements.isEmpty()) return@coroutineScope

      try {
        internalMeasurementsStub
          .batchSetCmmsMeasurementIds(
            batchSetCmmsMeasurementIdsRequest {
              this.cmmsMeasurementConsumerId = principal.resourceKey.measurementConsumerId
              measurementIds +=
                measurements.map { measurement ->
                  measurementIds {
                    externalMeasurementId =
                      measurementRequestIdToExternalId.getValue(measurement.measurementReferenceId)
                    cmmsMeasurementId = MeasurementKey.fromName(measurement.name)!!.measurementId
                  }
                }
            }
          )
          .toList()
      } catch (e: StatusException) {
        throw Exception(
          "Unable to set the CMMs measurement IDs for the measurements in the reporting database.",
          e
        )
      }
    }

    /** Creates a CMMs measurement from an [InternalMeasurement]. */
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
        throw Exception("Unable to create a CMMs measurement.", e)
      }
    }

    /** Builds a CMMs [CreateMeasurementRequest]. */
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
      principal: MeasurementConsumerPrincipal
    ): SigningKeyHandle {
      // TODO: Factor this out to a separate class similar to EncryptionKeyPairStore.
      val signingPrivateKeyDer: ByteString =
        signingPrivateKeyDir.resolve(principal.config.signingPrivateKeyPath).readByteString()
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
      metricSpec: InternalMetricSpec
    ): MeasurementSpec {
      return measurementSpec {
        measurementPublicKey = measurementEncryptionPublicKey
        this.nonceHashes += nonceHashes

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metricSpec.typeCase) {
          InternalMetricSpec.TypeCase.REACH -> {
            reachAndFrequency = REACH_ONLY_MEASUREMENT_SPEC
            vidSamplingInterval = buildReachOnlyVidSamplingInterval(secureRandom)
          }
          InternalMetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
            reachAndFrequency =
              buildReachAndFrequencyMeasurementSpec(
                metricSpec.frequencyHistogram.maximumFrequencyPerUser
              )
            vidSamplingInterval = buildReachAndFrequencyVidSamplingInterval(secureRandom)
          }
          InternalMetricSpec.TypeCase.IMPRESSION_COUNT -> {
            impression =
              buildImpressionMeasurementSpec(metricSpec.impressionCount.maximumFrequencyPerUser)
            vidSamplingInterval = buildImpressionVidSamplingInterval(secureRandom)
          }
          InternalMetricSpec.TypeCase.WATCH_DURATION -> {
            duration =
              buildDurationMeasurementSpec(
                metricSpec.watchDuration.maximumWatchDurationPerUser,
              )
            vidSamplingInterval = buildDurationVidSamplingInterval(secureRandom)
          }
          InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
            error("Unset metric type should've already raised error.")
        }
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
            val filter: String? =
              combineEventGroupFilters(
                primitiveReportingSetBasis.filtersList + internalPrimitiveReportingSet.filter
              )

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

    /** Combines two event group filters. */
    private fun combineEventGroupFilters(filters: List<String?>): String? {
      val nonNullFilters = filters.filterNotNull()
      return if (nonNullFilters.isEmpty()) null
      else {
        nonNullFilters.joinToString(separator = " AND ") { filter -> "($filter)" }
      }
    }

    /** Get a [MeasurementConsumer] based on a CMMs ID. */
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
        throw Exception(
          "Unable to retrieve the measurement consumer " + "[${principal.resourceKey.toName()}].",
          e
        )
      }
    }

    /**
     * Builds a map of external reporting set IDs to [InternalReportingSet]s to minimize grpc calls.
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
        internalReportingSetsStub.batchGetReportingSets(batchGetReportingSetsRequest).toList()

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

    /** Gets a signing certificate x509Der in ByteString. */
    private suspend fun getSigningCertificateDer(
      principal: MeasurementConsumerPrincipal
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
  }

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

    val uncreatedInternalMetric: InternalMetric =
      createInitialInternalMetric(principal.resourceKey.measurementConsumerId, request)

    val initialInternalMetric =
      if (uncreatedInternalMetric.state != InternalMetric.State.STATE_UNSPECIFIED)
        uncreatedInternalMetric
      else if (uncreatedInternalMetric.state == InternalMetric.State.UNRECOGNIZED)
        error("Unrecognized metric state.")
      else
        try {
          internalMetricsStub.createMetric(uncreatedInternalMetric)
        } catch (e: StatusException) {
          throw Exception("Unable to create the metric in the reporting database.", e)
        }

    if (initialInternalMetric.state == InternalMetric.State.RUNNING) {
      measurementSupplier.createCmmsMeasurements(listOf(initialInternalMetric), principal)
    }

    // Convert the internal metric to public and return it.
    return initialInternalMetric.toMetric()
  }

  override suspend fun batchCreateMetrics(
    request: BatchCreateMetricsRequest
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

    val uncreatedInternalMetricsList: List<InternalMetric> =
      request.requestsList.map { createMetricRequest ->
        createInitialInternalMetric(
          principal.resourceKey.measurementConsumerId,
          createMetricRequest
        )
      }

    val initialInternalMetrics =
      try {
        internalMetricsStub
          .batchCreateMetrics(
            internalBatchCreateMetricsRequest {
              cmmsMeasurementConsumerId = principal.resourceKey.measurementConsumerId
              metrics += uncreatedInternalMetricsList
            }
          )
          .toList()
      } catch (e: StatusException) {
        throw Exception("Unable to create the metric in the reporting database.", e)
      }

    measurementSupplier.createCmmsMeasurements(initialInternalMetrics, principal)

    // Convert the internal metric to public and return it.
    return batchCreateMetricsResponse { metrics += initialInternalMetrics.map { it.toMetric() } }
  }

  /** Creates an initial [InternalMetric] for caching. */
  private suspend fun createInitialInternalMetric(
    cmmsMeasurementConsumerId: String,
    request: CreateMetricRequest,
  ): InternalMetric {
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

    // Check if there's any existing metric using the unique request ID.
    val existingInternalMetric: InternalMetric? =
      if (request.requestId.isBlank()) null
      else getInternalMetricByIdempotencyKey(cmmsMeasurementConsumerId, request.requestId)

    if (existingInternalMetric != null) return existingInternalMetric

    val internalReportingSet: InternalReportingSet =
      getInternalReportingSet(cmmsMeasurementConsumerId, request.metric.reportingSet)

    return internalMetric {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
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
      details = InternalMetricKt.details { filters += request.metric.filtersList }
    }
  }

  /** Builds [InternalMeasurement]s for a [Metric] over an [InternalReportingSet]. */
  private fun buildInitialInternalMeasurements(
    cmmsMeasurementConsumerId: String,
    metric: Metric,
    internalReportingSet: InternalReportingSet
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

  /** Gets an [InternalMetric] using an idempotency key. */
  private suspend fun getInternalMetricByIdempotencyKey(
    cmmsMeasurementConsumerId: String,
    metricIdempotencyKey: String,
  ): InternalMetric? {
    return try {
      internalMetricsStub.getMetricByIdempotencyKey(
        getMetricByIdempotencyKeyRequest {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
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

  /** Gets an [InternalReportingSet] based on a reporting set name. */
  private suspend fun getInternalReportingSet(
    cmmsMeasurementConsumerId: String,
    reportingSetName: String,
  ): InternalReportingSet {
    val reportingSetKey =
      grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
        "Invalid reporting set name $reportingSetName."
      }

    grpcRequire(reportingSetKey.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
      "No access to the reporting set [$reportingSetName]."
    }

    return try {
      internalReportingSetsStub.getReportingSet(
        getInternalReportingSetRequest {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
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

/** Converts an [InternalTimeInterval] to a [CmmsTimeInterval]. */
private fun InternalTimeInterval.toCmmsTimeInterval(): CmmsTimeInterval {
  val source = this
  return cmmsTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Converts a [MetricSpec] to an [InternalMetricSpec]. */
private fun MetricSpec.toInternal(): InternalMetricSpec {
  val source = this
  return internalMetricSpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.typeCase) {
      MetricSpec.TypeCase.REACH -> reach = InternalMetricSpecKt.reachParams {}
      MetricSpec.TypeCase.FREQUENCY_HISTOGRAM ->
        frequencyHistogram =
          InternalMetricSpecKt.frequencyHistogramParams {
            maximumFrequencyPerUser = source.frequencyHistogram.maximumFrequencyPerUser
          }
      MetricSpec.TypeCase.IMPRESSION_COUNT ->
        impressionCount =
          InternalMetricSpecKt.impressionCountParams {
            maximumFrequencyPerUser = source.impressionCount.maximumFrequencyPerUser
          }
      MetricSpec.TypeCase.WATCH_DURATION ->
        watchDuration =
          InternalMetricSpecKt.watchDurationParams {
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

/** Converts an [InternalMetric] to a public [Metric]. */
private fun InternalMetric.toMetric(): Metric {
  val source = this
  return metric {
    name =
      MetricKey(
          cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId,
          metricId = externalIdToApiId(source.externalMetricId)
        )
        .toName()
    reportingSet =
      ReportingSetKey(
          source.cmmsMeasurementConsumerId,
          externalIdToApiId(source.externalReportingSetId)
        )
        .toName()
    timeInterval = source.timeInterval.toTimeInterval()
    metricSpec = source.metricSpec.toMetricSpec()
    filters += source.details.filtersList
    state = source.state.toState()
    createTime = source.createTime
    if (source.details.hasResult()) {
      result = source.details.result.toResult()
    }
  }
}

/** Converts an [InternalMetricResult] to a public [MetricResult]. */
private fun InternalMetricResult.toResult(): MetricResult {
  val source = this

  return metricResult {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.resultCase) {
      InternalMetricResult.ResultCase.REACH -> {
        reach = source.reach.toIntegerResult()
      }
      InternalMetricResult.ResultCase.FREQUENCY_HISTOGRAM -> {
        frequencyHistogram = source.frequencyHistogram.toHistogramResult()
      }
      InternalMetricResult.ResultCase.IMPRESSION_COUNT -> {
        impressionCount = source.impressionCount.toIntegerResult()
      }
      InternalMetricResult.ResultCase.WATCH_DURATION -> {
        watchDuration = source.watchDuration.toDoubleResult()
      }
      InternalMetricResult.ResultCase
        .RESULT_NOT_SET -> {} // No action if the result hasn't been set yet.
    }
  }
}

/** Converts an [InternalMetricResult.DoubleResult] to a public [MetricResult.DoubleResult]. */
private fun InternalMetricResult.DoubleResult.toDoubleResult(): MetricResult.DoubleResult {
  val source = this
  return doubleResult { value = source.value }
}

/**
 * Converts an [InternalMetricResult.HistogramResult] to a public [MetricResult.HistogramResult].
 */
private fun InternalMetricResult.HistogramResult.toHistogramResult(): MetricResult.HistogramResult {
  val source = this
  return histogramResult {
    bins +=
      source.binsList.map { internalBin ->
        bin {
          label = internalBin.label
          value = internalBin.value.toDoubleResult()
        }
      }
  }
}

/** Converts an [InternalMetricResult.IntegerResult] to a public [MetricResult.IntegerResult]. */
private fun InternalMetricResult.IntegerResult.toIntegerResult(): MetricResult.IntegerResult {
  val source = this
  return integerResult { value = source.value }
}

/** Converts an [InternalMetric.State] to a public [Metric.State]. */
private fun InternalMetric.State.toState(): Metric.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
    InternalMetric.State.RUNNING -> Metric.State.RUNNING
    InternalMetric.State.SUCCEEDED -> Metric.State.SUCCEEDED
    InternalMetric.State.FAILED -> Metric.State.FAILED
    InternalMetric.State.STATE_UNSPECIFIED -> error("Metric state should've been set.")
    InternalMetric.State.UNRECOGNIZED -> error("Unrecognized metric state.")
  }
}

/** Converts an [InternalMetricSpec] to a public [MetricSpec]. */
private fun InternalMetricSpec.toMetricSpec(): MetricSpec {
  val source = this
  return metricSpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.typeCase) {
      InternalMetricSpec.TypeCase.REACH -> reach = MetricSpecKt.reachParams {}
      InternalMetricSpec.TypeCase.FREQUENCY_HISTOGRAM ->
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
            maximumFrequencyPerUser = source.frequencyHistogram.maximumFrequencyPerUser
          }
      InternalMetricSpec.TypeCase.IMPRESSION_COUNT ->
        impressionCount =
          MetricSpecKt.impressionCountParams {
            maximumFrequencyPerUser = source.impressionCount.maximumFrequencyPerUser
          }
      InternalMetricSpec.TypeCase.WATCH_DURATION ->
        watchDuration =
          MetricSpecKt.watchDurationParams {
            maximumWatchDurationPerUser = source.watchDuration.maximumWatchDurationPerUser
          }
      InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
        failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Metric is not specified." }
    }
  }
}

/** Converts an [InternalTimeInterval] to a public [TimeInterval]. */
private fun InternalTimeInterval.toTimeInterval(): TimeInterval {
  val source = this
  return timeInterval {
    startTime = source.startTime
    endTime = source.endTime
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
): MeasurementSpec.Duration {
  return MeasurementSpecKt.duration {
    privacyParams = differentialPrivacyParams {
      epsilon = WATCH_DURATION_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    this.maximumWatchDurationPerUser = maximumWatchDurationPerUser
  }
}
