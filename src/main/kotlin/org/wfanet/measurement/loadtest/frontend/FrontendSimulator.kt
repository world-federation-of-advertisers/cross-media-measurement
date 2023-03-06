// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.frontend

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.nio.file.Paths
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.random.Random
import kotlinx.coroutines.time.delay
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchProtos
import org.wfanet.estimation.Estimators
import org.wfanet.estimation.ValueHistogram
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.Failure
import org.wfanet.measurement.api.v2alpha.Measurement.Result
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.loadtest.config.TestIdentifiers
import org.wfanet.measurement.loadtest.storage.SketchStore

private const val DATA_PROVIDER_WILDCARD = "dataProviders/-"

data class MeasurementConsumerData(
  // The MC's public API resource name
  val name: String,
  /** The MC's consent signaling signing key. */
  val signingKey: SigningKeyHandle,
  /** The MC's encryption public key. */
  val encryptionKey: PrivateKeyHandle,
  /** An API key for the MC. */
  val apiAuthenticationKey: String
)

/** A simulator performing frontend operations. */
class FrontendSimulator(
  private val measurementConsumerData: MeasurementConsumerData,
  private val outputDpParams: DifferentialPrivacyParams,
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val measurementsClient: MeasurementsCoroutineStub,
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val certificatesClient: CertificatesCoroutineStub,
  private val sketchStore: SketchStore,
  private val resultPollingDelay: Duration,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  /** Map of event template names to filter expressions. */
  private val eventTemplateFilters: Map<String, String> = emptyMap(),
) {
  /** Cache of resource name to [Certificate]. */
  private val certificateCache = mutableMapOf<String, Certificate>()

  /** A sequence of operations done in the simulator involving a reach and frequency measurement. */
  suspend fun executeReachAndFrequency(runId: String) {
    logger.info { "Creating reach and frequency Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val createdReachAndFrequencyMeasurement =
      createMeasurement(measurementConsumer, runId, ::newReachAndFrequencyMeasurementSpec)
    logger.info {
      "Created reach and frequency Measurement ${createdReachAndFrequencyMeasurement.name}"
    }

    // Get the CMMS computed result and compare it with the expected result.
    val reachAndFrequencyResult: Result = pollForResult {
      getReachAndFrequencyResult(createdReachAndFrequencyMeasurement.name)
    }
    logger.info("Got reach and frequency result from Kingdom: $reachAndFrequencyResult")

    val liquidLegionV2Protocol =
      createdReachAndFrequencyMeasurement.protocolConfig.protocolsList
        .first { it.hasLiquidLegionsV2() }
        .liquidLegionsV2
    val expectedResult =
      getExpectedResult(createdReachAndFrequencyMeasurement.name, liquidLegionV2Protocol)
    logger.info("Expected result: $expectedResult")

    assertDpResultsEqual(
      expectedResult,
      reachAndFrequencyResult,
      liquidLegionV2Protocol.maximumFrequency.toLong()
    )
    logger.info("Reach and frequency result is equal to the expected result")
  }

  /**
   * A sequence of operations done in the simulator involving a reach and frequency measurement with
   * invalid params.
   */
  suspend fun executeInvalidReachAndFrequency(runId: String) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)

    val invalidMeasurement =
      createMeasurement(measurementConsumer, runId, ::newInvalidReachAndFrequencyMeasurementSpec)
    logger.info(
      "Created invalid reach and frequency measurement ${invalidMeasurement.name}, state=${invalidMeasurement.state.name}"
    )

    var failure = getFailure(invalidMeasurement.name)
    var attempts = 0
    while (failure == null) {
      attempts += 1
      assertThat(attempts).isLessThan(10)
      logger.info("Computation not done yet, wait for another 5 seconds...")
      delay(Duration.ofSeconds(5))
      failure = getFailure(invalidMeasurement.name)
    }
    assertThat(failure.message).contains("reach_privacy_params.delta")
    logger.info("Receive failed Measurement from Kingdom: ${failure.message}. Test passes.")
  }

  /**
   * A sequence of operations done in the simulator involving a direct reach and frequency
   * measurement.
   */
  suspend fun executeDirectReachAndFrequency(runId: String) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val createdReachAndFrequencyMeasurement =
      createMeasurement(measurementConsumer, runId, ::newReachAndFrequencyMeasurementSpec, 1)
    logger.info(
      "Created direct reach and frequency measurement ${createdReachAndFrequencyMeasurement.name}."
    )

    // Get the CMMS computed result and compare it with the expected result.
    val reachAndFrequencyResult = pollForResult {
      getReachAndFrequencyResult(createdReachAndFrequencyMeasurement.name)
    }
    logger.info("Got direct reach and frequency result from Kingdom: $reachAndFrequencyResult")

    // For InProcessLifeOfAMeasurementIntegrationTest, EdpSimulator sets to those values with seeded
    // random VIDs and Laplace publisher noise.
    val expectedReachValue = 948L
    val expectedFrequencyMap =
      mapOf(
        1L to 0.947389665261748,
        2L to 0.04805005905234108,
        3L to 0.0038138458821366963,
        4L to 9.558853281715655E-5
      )

    assertThat(reachAndFrequencyResult.reach.value).isEqualTo(expectedReachValue)
    reachAndFrequencyResult.frequency.relativeFrequencyDistributionMap.forEach {
      (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedFrequencyMap[frequency])
    }

    logger.info("Direct reach and frequency result is equal to the expected result")
  }

  /** A sequence of operations done in the simulator involving a reach-only measurement. */
  suspend fun executeReachOnly(runId: String) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val createdReachOnlyMeasurement =
      createMeasurement(measurementConsumer, runId, ::newReachOnlyMeasurementSpec)
    logger.info("Created reach-only measurement ${createdReachOnlyMeasurement.name}.")

    // Get the CMMS computed result and compare it with the expected result.
    var reachOnlyResult = getReachAndFrequencyResult(createdReachOnlyMeasurement.name)
    var nAttempts = 0
    while (reachOnlyResult == null && (nAttempts < 4)) {
      nAttempts++
      logger.info("Computation not done yet, wait for another 30 seconds.  Attempt $nAttempts")
      delay(Duration.ofSeconds(30))
      reachOnlyResult = getReachAndFrequencyResult(createdReachOnlyMeasurement.name)
    }
    checkNotNull(reachOnlyResult) { "Timed out waiting for response to reach-only request" }
    logger.info("Actual result: $reachOnlyResult")

    val liquidLegionV2Protocol =
      createdReachOnlyMeasurement.protocolConfig.protocolsList
        .first { it.hasLiquidLegionsV2() }
        .liquidLegionsV2
    val expectedResultWithFrequencies =
      getExpectedResult(createdReachOnlyMeasurement.name, liquidLegionV2Protocol)
    val expectedResult = result {
      reach = reach { value = expectedResultWithFrequencies.reach.value }
      frequency = frequency { relativeFrequencyDistribution.putAll(mapOf(1L to 1.0)) }
    }

    logger.info("Expected result: $expectedResult")

    assertDpResultsEqual(
      expectedResult,
      reachOnlyResult,
      liquidLegionV2Protocol.maximumFrequency.toLong()
    )
    logger.info("Reach-only result is equal to the expected result. Correctness Test passes.")
  }

  /** A sequence of operations done in the simulator involving an impression measurement. */
  suspend fun executeImpression(runId: String) {
    logger.info { "Creating impression Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val createdImpressionMeasurement =
      createMeasurement(measurementConsumer, runId, ::newImpressionMeasurementSpec)
    logger.info("Created impression Measurement ${createdImpressionMeasurement.name}.")

    val impressionResults = pollForResults {
      getImpressionResults(createdImpressionMeasurement.name)
    }

    impressionResults.forEach {
      val result = parseAndVerifyResult(it)
      assertThat(result.impression.value)
        .isEqualTo(
          // EdpSimulator sets it to this value.
          apiIdToExternalId(DataProviderCertificateKey.fromName(it.certificate)!!.dataProviderId)
        )
    }
    logger.info("Impression result is equal to the expected result")
  }

  /** A sequence of operations done in the simulator involving a duration measurement. */
  suspend fun executeDuration(runId: String) {
    logger.info { "Creating duration Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val createdDurationMeasurement =
      createMeasurement(measurementConsumer, runId, ::newDurationMeasurementSpec)
    logger.info("Created duration Measurement ${createdDurationMeasurement.name}.")

    val durationResults = pollForResults { getDurationResults(createdDurationMeasurement.name) }

    durationResults.forEach {
      val result = parseAndVerifyResult(it)
      assertThat(result.watchDuration.value.seconds)
        .isEqualTo(
          // EdpSimulator sets it to this value.
          apiIdToExternalId(DataProviderCertificateKey.fromName(it.certificate)!!.dataProviderId)
        )
    }
    logger.info("Duration result is equal to the expected result")
  }

  /** Compare two [Result]s within the differential privacy error range. */
  private fun assertDpResultsEqual(
    expectedResult: Result,
    actualResult: Result,
    maximumFrequency: Long
  ) {
    val reachRatio = expectedResult.reach.value.toDouble() / actualResult.reach.value.toDouble()
    assertThat(reachRatio).isWithin(0.10).of(1.0)
    (1L..maximumFrequency).forEach {
      val expected = expectedResult.frequency.relativeFrequencyDistributionMap.getOrDefault(it, 0.0)
      val actual = actualResult.frequency.relativeFrequencyDistributionMap.getOrDefault(it, 0.0)
      assertThat(actual).isWithin(0.05).of(expected)
    }
  }

  /** Creates a Measurement on behalf of the [MeasurementConsumer]. */
  private suspend fun createMeasurement(
    measurementConsumer: MeasurementConsumer,
    runId: String,
    newMeasurementSpec:
      (
        serializedMeasurementPublicKey: ByteString, nonceHashes: MutableList<ByteString>
      ) -> MeasurementSpec,
    maxDataProviders: Int = 20
  ): Measurement {
    val eventGroups: List<EventGroup> =
      listEventGroups(measurementConsumer.name).filter {
        it.eventGroupReferenceId.startsWith(TestIdentifiers.EVENT_GROUP_REFERENCE_ID_PREFIX)
      }
    check(eventGroups.isNotEmpty()) { "No event groups found for ${measurementConsumer.name}" }
    val nonceHashes = mutableListOf<ByteString>()

    val dataProviderEntries =
      eventGroups
        .groupBy { extractDataProviderKey(it.name) }
        .entries
        .take(maxDataProviders)
        .map { (dataProviderKey, eventGroups) ->
          val nonce = Random.Default.nextLong()
          nonceHashes.add(hashSha256(nonce))
          createDataProviderEntry(dataProviderKey, eventGroups, measurementConsumer, nonce)
        }

    val request = createMeasurementRequest {
      measurement = measurement {
        measurementConsumerCertificate = measurementConsumer.certificate
        measurementSpec =
          signMeasurementSpec(
            newMeasurementSpec(measurementConsumer.publicKey.data, nonceHashes),
            measurementConsumerData.signingKey
          )
        dataProviders += dataProviderEntries
        this.measurementReferenceId = runId
      }
    }
    try {
      return measurementsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .createMeasurement(request)
    } catch (e: StatusException) {
      throw Exception("Error creating Measurement", e)
    }
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getImpressionResults(measurementName: String): List<Measurement.ResultPair> {
    val measurement = getMeasurement(measurementName)
    logger.info("Current Measurement state is: " + measurement.state)
    if (measurement.state == Measurement.State.FAILED) {
      val failure: Failure = measurement.failure
      throw Exception("Measurement failed with reason ${failure.reason}: ${failure.message}")
    }
    return measurement.resultsList.toList()
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getDurationResults(measurementName: String): List<Measurement.ResultPair> {
    val measurement = getMeasurement(measurementName)
    logger.info("Current Measurement state is: " + measurement.state)
    if (measurement.state == Measurement.State.FAILED) {
      val failure: Failure = measurement.failure
      throw Exception("Measurement failed with reason ${failure.reason}: ${failure.message}")
    }
    return measurement.resultsList.toList()
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getReachAndFrequencyResult(measurementName: String): Result? {
    val measurement = getMeasurement(measurementName)
    logger.info("Current Measurement state is: " + measurement.state)
    if (measurement.state == Measurement.State.FAILED) {
      val failure: Failure = measurement.failure
      throw Exception("Measurement failed with reason ${failure.reason}: ${failure.message}")
    }
    if (measurement.state != Measurement.State.SUCCEEDED) {
      return null
    }

    val resultPair = measurement.resultsList[0]
    val result = parseAndVerifyResult(resultPair)
    assertThat(result.hasReach()).isTrue()
    assertThat(result.hasFrequency()).isTrue()

    return result
  }

  private suspend fun getMeasurement(measurementName: String) =
    try {
      measurementsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .getMeasurement(getMeasurementRequest { name = measurementName })
    } catch (e: StatusException) {
      throw Exception("Error fetching measurement $measurementName", e)
    }

  /** Gets the failure of an invalid [Measurement] if it is failed */
  private suspend fun getFailure(measurementName: String): Failure? {
    val measurement = getMeasurement(measurementName)
    logger.info("Current Measurement state is: " + measurement.state)
    if (measurement.state != Measurement.State.FAILED) {
      return null
    }
    return measurement.failure
  }

  private suspend fun parseAndVerifyResult(resultPair: Measurement.ResultPair): Result {
    val certificate =
      certificateCache.getOrPut(resultPair.certificate) {
        try {
          certificatesClient
            .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
            .getCertificate(getCertificateRequest { name = resultPair.certificate })
        } catch (e: StatusException) {
          throw Exception("Error fetching certificate ${resultPair.certificate}", e)
        }
      }

    val signedResult =
      decryptResult(resultPair.encryptedResult, measurementConsumerData.encryptionKey)
    val x509Certificate: X509Certificate = readCertificate(certificate.x509Der)
    val trustedIssuer =
      checkNotNull(trustedCertificates[checkNotNull(x509Certificate.authorityKeyIdentifier)]) {
        "Issuer of ${certificate.name} not trusted"
      }
    try {
      verifyResult(signedResult, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw Exception("Certificate path is invalid for ${certificate.name}", e)
    } catch (e: SignatureException) {
      throw Exception("Measurement result signature is invalid", e)
    }
    return Result.parseFrom(signedResult.data)
  }

  /** Gets the expected result of a [Measurement] using raw sketches. */
  suspend fun getExpectedResult(
    measurementName: String,
    protocolConfig: ProtocolConfig.LiquidLegionsV2
  ): Result {
    val requisitions = listRequisitions(measurementName)
    require(requisitions.isNotEmpty()) { "Requisition list is empty." }

    val anySketches =
      requisitions.map {
        val storedSketch =
          sketchStore.get(it)?.read()?.flatten() ?: error("Sketch blob not found for ${it.name}.")
        SketchProtos.toAnySketch(Sketch.parseFrom(storedSketch))
      }

    val combinedAnySketch = anySketches[0]
    if (anySketches.size > 1) {
      combinedAnySketch.apply { mergeAll(anySketches.subList(1, anySketches.size)) }
    }

    val expectedReach =
      estimateCardinality(
        combinedAnySketch,
        protocolConfig.sketchParams.decayRate,
        protocolConfig.sketchParams.maxSize
      )
    val expectedFrequency =
      estimateFrequency(combinedAnySketch, protocolConfig.maximumFrequency.toLong())
    return result {
      reach = reach { value = expectedReach }
      frequency = frequency { relativeFrequencyDistribution.putAll(expectedFrequency) }
    }
  }

  /** Estimates the cardinality of an [AnySketch]. */
  private fun estimateCardinality(anySketch: AnySketch, decayRate: Double, indexSize: Long): Long {
    val activeRegisterCount = anySketch.toList().size.toLong()
    return Estimators.EstimateCardinalityLiquidLegions(decayRate, indexSize, activeRegisterCount)
  }

  /** Estimates the relative frequency histogram of an [AnySketch]. */
  private fun estimateFrequency(anySketch: AnySketch, maximumFrequency: Long): Map<Long, Double> {
    val valueIndex = anySketch.getValueIndex("SamplingIndicator").asInt
    val actualHistogram =
      ValueHistogram.calculateHistogram(anySketch, "Frequency") { it.values[valueIndex] != -1L }
    val result = mutableMapOf<Long, Double>()
    actualHistogram.forEach {
      val key = minOf(it.key, maximumFrequency)
      result[key] = result.getOrDefault(key, 0.0) + it.value
    }
    return result
  }

  private suspend fun getMeasurementConsumer(name: String): MeasurementConsumer {
    val request = getMeasurementConsumerRequest { this.name = name }
    try {
      return measurementConsumersClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .getMeasurementConsumer(request)
    } catch (e: StatusException) {
      throw Exception("Error getting MC $name", e)
    }
  }

  private fun newReachAndFrequencyMeasurementSpec(
    serializedMeasurementPublicKey: ByteString,
    nonceHashes: List<ByteString>
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = serializedMeasurementPublicKey
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = outputDpParams
        frequencyPrivacyParams = outputDpParams
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      this.nonceHashes += nonceHashes
    }
  }

  private fun newReachOnlyMeasurementSpec(
    serializedMeasurementPublicKey: ByteString,
    nonceHashes: List<ByteString>
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = serializedMeasurementPublicKey
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = outputDpParams
        frequencyPrivacyParams = outputDpParams
        maximumFrequencyPerUser = 1
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      this.nonceHashes += nonceHashes
    }
  }

  private fun newInvalidReachAndFrequencyMeasurementSpec(
    serializedMeasurementPublicKey: ByteString,
    nonceHashes: List<ByteString>
  ): MeasurementSpec {
    return newReachAndFrequencyMeasurementSpec(serializedMeasurementPublicKey, nonceHashes).copy {
      val invalidPrivacyParams = differentialPrivacyParams {
        epsilon = 1.0
        delta = 0.0
      }
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = invalidPrivacyParams
        frequencyPrivacyParams = invalidPrivacyParams
      }
    }
  }

  private fun newImpressionMeasurementSpec(
    serializedMeasurementPublicKey: ByteString,
    nonceHashes: List<ByteString>
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = serializedMeasurementPublicKey
      impression = impression {
        privacyParams = outputDpParams
        maximumFrequencyPerUser = 1
      }
      this.nonceHashes += nonceHashes
    }
  }

  private fun newDurationMeasurementSpec(
    serializedMeasurementPublicKey: ByteString,
    nonceHashes: List<ByteString>
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = serializedMeasurementPublicKey
      duration = duration {
        privacyParams = outputDpParams
        maximumWatchDurationPerUser = 1
      }
      this.nonceHashes += nonceHashes
    }
  }

  private suspend fun listEventGroups(measurementConsumer: String): List<EventGroup> {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_WILDCARD
      filter = ListEventGroupsRequestKt.filter { measurementConsumers += measurementConsumer }
    }
    try {
      return eventGroupsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .listEventGroups(request)
        .eventGroupsList
    } catch (e: StatusException) {
      throw Exception("Error listing event groups for MC $measurementConsumer", e)
    }
  }

  private suspend fun listRequisitions(measurement: String): List<Requisition> {
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_WILDCARD
      filter = ListRequisitionsRequestKt.filter { this.measurement = measurement }
    }
    try {
      return requisitionsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .listRequisitions(request)
        .requisitionsList
    } catch (e: StatusException) {
      throw Exception("Error listing requisitions for measurement $measurement", e)
    }
  }

  private fun extractDataProviderKey(eventGroupName: String): DataProviderKey {
    val eventGroupKey = EventGroupKey.fromName(eventGroupName) ?: error("Invalid eventGroup name.")
    return DataProviderKey(eventGroupKey.dataProviderId)
  }

  private suspend fun getDataProvider(key: DataProviderKey): DataProvider {
    val name = key.toName()
    val request = GetDataProviderRequest.newBuilder().also { it.name = name }.build()
    try {
      return dataProvidersClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .getDataProvider(request)
    } catch (e: StatusException) {
      throw Exception("Error fetching DataProvider $name", e)
    }
  }

  private fun createFilterExpression(): String = eventTemplateFilters.values.joinToString(" && ")

  private suspend fun createDataProviderEntry(
    dataProviderKey: DataProviderKey,
    eventGroups: Iterable<EventGroup>,
    measurementConsumer: MeasurementConsumer,
    nonce: Long
  ): DataProviderEntry {
    val dataProvider = getDataProvider(dataProviderKey)

    val eventFilterExpression = createFilterExpression()
    val requisitionSpec = requisitionSpec {
      for (eventGroup in eventGroups) {
        this.eventGroups += eventGroupEntry {
          key = eventGroup.name
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = timeInterval {
                startTime =
                  LocalDate.now()
                    .minusDays(1)
                    .atStartOfDay()
                    .toInstant(ZoneOffset.UTC)
                    .toProtoTime()
                endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              }
              filter = eventFilter { expression = eventFilterExpression }
            }
        }
      }
      measurementPublicKey = measurementConsumer.publicKey.data
      this.nonce = nonce
    }
    val signedRequisitionSpec =
      signRequisitionSpec(requisitionSpec, measurementConsumerData.signingKey)
    return dataProvider.toDataProviderEntry(signedRequisitionSpec, hashSha256(nonce))
  }

  private fun DataProvider.toDataProviderEntry(
    signedRequisitionSpec: SignedData,
    nonceHash: ByteString
  ): DataProviderEntry {
    val source = this
    return dataProviderEntry {
      key = source.name
      this.value =
        MeasurementKt.DataProviderEntryKt.value {
          dataProviderCertificate = source.certificate
          dataProviderPublicKey = source.publicKey
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedRequisitionSpec,
              EncryptionPublicKey.parseFrom(source.publicKey.data),
            )
          this.nonceHash = nonceHash
        }
    }
  }

  private suspend inline fun pollForResult(getResult: () -> Result?): Result {
    while (true) {
      val result = getResult()
      if (result != null) {
        return result
      }

      logger.info("Result not yet available. Waiting for ${resultPollingDelay.seconds} seconds.")
      delay(resultPollingDelay)
    }
  }

  private suspend inline fun <T> pollForResults(getResults: () -> List<T>): List<T> {
    while (true) {
      val result = getResults()
      if (result.isNotEmpty()) {
        return result
      }

      logger.info("Result not yet available. Waiting for ${resultPollingDelay.seconds} seconds.")
      delay(resultPollingDelay)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    init {
      loadLibrary(
        name = "estimators",
        directoryPath =
          Paths.get("any_sketch_java", "src", "main", "java", "org", "wfanet", "estimation")
      )
    }
  }
}
