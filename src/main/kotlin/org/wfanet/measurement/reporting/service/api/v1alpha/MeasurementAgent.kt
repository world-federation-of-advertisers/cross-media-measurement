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
import io.grpc.Status
import io.grpc.StatusException
import java.security.PrivateKey
import java.security.SecureRandom
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value as dataProviderEntryValue
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.TimeInterval as MeasurementTimeInterval
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval as measurementTimeInterval
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.Measurement.Result as InternalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.frequency as internalFrequency
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.impression as internalImpression
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.reach as internalReach
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.watchDuration as internalWatchDuration
import org.wfanet.measurement.internal.reporting.MeasurementKt.failure as internalFailure
import org.wfanet.measurement.internal.reporting.MeasurementKt.result as internalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric.Details as InternalMetricDetails
import org.wfanet.measurement.internal.reporting.Metric.Details.MetricTypeCase as InternalMetricTypeCase
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest as SetInternalMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.getReportingSetRequest
import org.wfanet.measurement.internal.reporting.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest as setInternalMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest as setInternalMeasurementResultRequest
import org.wfanet.measurement.reporting.v1alpha.TimeInterval

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

data class MeasurementInfo(
  val measurementConsumerReferenceId: String,
  val idempotencyKey: String,
  val eventGroupFilters: Map<String, String>,
)

data class SigningConfig(
  val signingCertificateName: String,
  val signingCertificateDer: ByteString,
  val signingPrivateKey: PrivateKey,
)

data class WeightedMeasurementInfo(
  val reportingMeasurementId: String,
  val weightedMeasurement: WeightedMeasurement,
  val timeInterval: TimeInterval,
  var kingdomMeasurementId: String? = null,
)

data class SetOperationResult(
  val weightedMeasurementInfoList: List<WeightedMeasurementInfo>,
  val internalMetricDetails: InternalMetricDetails,
)

class MeasurementAgent(
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificateStub: CertificatesCoroutineStub,
  private val measurementConsumer: MeasurementConsumer,
  private val apiAuthenticationKey: String,
  private val secureRandom: SecureRandom,
) {
  private val measurementConsumerReferenceId: String =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(measurementConsumer.name)) {
        "Invalid measurement consumer name [${measurementConsumer.name}]"
      }
      .measurementConsumerId

  suspend fun createMeasurements(
    namedSetOperationResults: Map<String, SetOperationResult>,
    measurementInfo: MeasurementInfo,
    signingConfig: SigningConfig,
  ) = coroutineScope {
    namedSetOperationResults.forEach { (_, setOperationResult) ->
      setOperationResult.weightedMeasurementInfoList.forEach { weightedMeasurementInfo ->
        launch {
          createMeasurement(
            weightedMeasurementInfo,
            measurementInfo,
            setOperationResult.internalMetricDetails,
            signingConfig,
          )
        }
      }
    }
  }

  private suspend fun createMeasurement(
    weightedMeasurementInfo: WeightedMeasurementInfo,
    measurementInfo: MeasurementInfo,
    internalMetricDetails: InternalMetricDetails,
    signingConfig: SigningConfig,
  ) {
    val dataProviderNameToInternalEventGroupEntriesList =
      aggregateInternalEventGroupEntryByDataProviderName(
        weightedMeasurementInfo.weightedMeasurement.reportingSets,
        weightedMeasurementInfo.timeInterval.toMeasurementTimeInterval(),
        measurementInfo.eventGroupFilters
      )

    val createMeasurementRequest: CreateMeasurementRequest =
      buildCreateMeasurementRequest(
        dataProviderNameToInternalEventGroupEntriesList,
        internalMetricDetails,
        weightedMeasurementInfo.reportingMeasurementId,
        signingConfig,
      )

    try {
      val measurement =
        measurementsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .createMeasurement(createMeasurementRequest)
      weightedMeasurementInfo.kingdomMeasurementId =
        checkNotNull(MeasurementKey.fromName(measurement.name)).measurementId
    } catch (e: StatusException) {
      throw Exception(
        "Unable to create the measurement [${createMeasurementRequest.measurement.name}].",
        e
      )
    }

    try {
      internalMeasurementsStub.createMeasurement(
        internalMeasurement {
          this.measurementConsumerReferenceId = measurementInfo.measurementConsumerReferenceId
          this.measurementReferenceId = weightedMeasurementInfo.kingdomMeasurementId!!
          state = InternalMeasurement.State.PENDING
        }
      )
    } catch (e: StatusException) {
      throw Exception(
        "Unable to create the measurement [${createMeasurementRequest.measurement.name}] " +
          "in the reporting database.",
        e
      )
    }
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
            try {
              internalReportingSetsStub.getReportingSet(
                getReportingSetRequest {
                  this.measurementConsumerReferenceId = reportingSetKey.measurementConsumerId
                  externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
                }
              )
            } catch (e: StatusException) {
              throw Exception(
                "Unable to retrieve the reporting set [$reportingSetName] from the reporting " +
                  "database.",
                e
              )
            }
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
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = timeInterval

                val filter =
                  combineEventGroupFilters(
                    internalReportingSet.filter,
                    eventGroupFilters[eventGroupName]
                  )
                if (filter != null) {
                  this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                }
              }
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

  /** Builds a [CreateMeasurementRequest]. */
  private suspend fun buildCreateMeasurementRequest(
    dataProviderNameToInternalEventGroupEntriesList: Map<String, List<EventGroupEntry>>,
    internalMetricDetails: InternalMetricDetails,
    measurementReferenceId: String,
    signingConfig: SigningConfig,
  ): CreateMeasurementRequest {
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
          dataProviderNameToInternalEventGroupEntriesList,
          measurementEncryptionPublicKey,
          measurementConsumerSigningKey,
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
          try {
            dataProvidersStub
              .withAuthenticationKey(apiAuthenticationKey)
              .getDataProvider(getDataProviderRequest { name = dataProviderName })
          } catch (e: StatusException) {
            throw Exception("Unable to retrieve the data provider [$dataProviderName].", e)
          }

        key = dataProvider.name
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

  suspend fun syncMeasurements(
    measurementsMap: Map<String, InternalMeasurement>,
    encryptionKeyPairStore: EncryptionKeyPairStore,
    principalName: String,
  ) = coroutineScope {
    for ((measurementReferenceId, internalMeasurement) in measurementsMap) {
      // Measurement with SUCCEEDED state is already synced
      if (internalMeasurement.state == InternalMeasurement.State.SUCCEEDED) continue

      launch {
        syncMeasurement(
          measurementReferenceId,
          encryptionKeyPairStore,
          principalName,
        )
      }
    }
  }

  private suspend fun syncMeasurement(
    measurementReferenceId: String,
    encryptionKeyPairStore: EncryptionKeyPairStore,
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
            measurementReferenceId,
            measurement.resultsList,
            encryptionPrivateKeyHandle,
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
          this.measurementConsumerReferenceId = this@MeasurementAgent.measurementConsumerReferenceId
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
    measurementReferenceId: String,
    resultsList: List<Measurement.ResultPair>,
    privateKeyHandle: PrivateKeyHandle,
  ): SetInternalMeasurementResultRequest {

    return setInternalMeasurementResultRequest {
      this.measurementConsumerReferenceId = this@MeasurementAgent.measurementConsumerReferenceId
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
    if (!verifyResult(signedResult, readCertificate(certificate.x509Der))) {
      error("Signature of the result is invalid.")
    }
    return Measurement.Result.parseFrom(signedResult.data)
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

/** Converts an [TimeInterval] to a [MeasurementTimeInterval] for measurement request. */
private fun TimeInterval.toMeasurementTimeInterval(): MeasurementTimeInterval {
  val source = this
  return measurementTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
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

private operator fun Duration.plus(other: Duration): Duration {
  return Durations.add(this, other)
}
