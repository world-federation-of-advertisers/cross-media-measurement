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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
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
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
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
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest.MeasurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest as InternalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2.Metric.WeightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest as internalBatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest as internalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse

private const val MAX_BATCH_SIZE = 1000
private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

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
        withContext(Dispatchers.IO) {
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

    /** Get a [MeasurementConsumer] based on a CMMS ID. */
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
        internalReportingSetsStub
          .batchGetReportingSets(batchGetReportingSetsRequest)
          .reportingSetsList

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
