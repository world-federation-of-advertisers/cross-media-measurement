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

package org.wfanet.measurement.loadtest.measurementconsumer

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.util.Durations
import com.google.type.interval
import io.grpc.StatusException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import java.util.logging.Logger
import kotlin.math.log2
import kotlin.random.Random
import kotlinx.coroutines.time.delay
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
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.loadtest.config.TestIdentifiers
import org.wfanet.measurement.loadtest.config.VidSampling
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.MeasurementResults

data class MeasurementConsumerData(
  // The MC's public API resource name
  val name: String,
  /** The MC's consent signaling signing key. */
  val signingKey: SigningKeyHandle,
  /** The MC's encryption private key. */
  val encryptionKey: PrivateKeyHandle,
  /** An API key for the MC. */
  val apiAuthenticationKey: String
)

/** Simulator for MeasurementConsumer operations on the CMMS public API. */
class MeasurementConsumerSimulator(
  private val measurementConsumerData: MeasurementConsumerData,
  private val outputDpParams: DifferentialPrivacyParams,
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val measurementsClient: MeasurementsCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val certificatesClient: CertificatesCoroutineStub,
  private val resultPollingDelay: Duration,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  private val eventQuery: EventQuery<Message>,
  private val expectedDirectNoiseMechanism: NoiseMechanism,
) {
  /** Cache of resource name to [Certificate]. */
  private val certificateCache = mutableMapOf<String, Certificate>()

  data class RequisitionInfo(
    val dataProviderEntry: DataProviderEntry,
    val requisitionSpec: RequisitionSpec,
    val eventGroups: List<EventGroup>,
  )

  data class MeasurementInfo(
    val measurement: Measurement,
    val measurementSpec: MeasurementSpec,
    val requisitions: List<RequisitionInfo>,
  )

  private val MeasurementInfo.sampledVids: Sequence<Long>
    get() {
      val vidSamplingInterval = measurementSpec.vidSamplingInterval

      return requisitions.asSequence().flatMap {
        val eventGroupsMap: Map<String, RequisitionSpec.EventGroupEntry.Value> =
          it.requisitionSpec.eventGroupsMap
        it.eventGroups.flatMap { eventGroup ->
          eventQuery
            .getUserVirtualIds(
              EventQuery.EventGroupSpec(eventGroup, eventGroupsMap.getValue(eventGroup.name))
            )
            .filter { vid ->
              VidSampling.sampler.vidIsInSamplingBucket(
                vid,
                vidSamplingInterval.start,
                vidSamplingInterval.width
              )
            }
        }
      }
    }

  data class ExecutionResult(
    val actualResult: Result,
    val expectedResult: Result,
    val measurementInfo: MeasurementInfo,
  )

  /** A sequence of operations done in the simulator involving a reach and frequency measurement. */
  suspend fun testReachAndFrequency(runId: String) {
    logger.info { "Creating reach and frequency Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo: MeasurementInfo =
      createMeasurement(measurementConsumer, runId, ::newReachAndFrequencyMeasurementSpec)
    val measurementName = measurementInfo.measurement.name
    logger.info { "Created reach and frequency Measurement $measurementName" }

    // Get the CMMS computed result and compare it with the expected result.
    val reachAndFrequencyResult: Result = pollForResult {
      getReachAndFrequencyResult(measurementName)
    }
    logger.info("Got reach and frequency result from Kingdom: $reachAndFrequencyResult")

    val expectedResult = getExpectedResult(measurementInfo)
    logger.info("Expected result: $expectedResult")

    assertDpResultsEqual(expectedResult, reachAndFrequencyResult)
    logger.info("Reach and frequency result is equal to the expected result")
  }

  /**
   * A sequence of operations done in the simulator involving a reach and frequency measurement with
   * invalid params.
   */
  suspend fun testInvalidReachAndFrequency(runId: String) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)

    val invalidMeasurement =
      createMeasurement(measurementConsumer, runId, ::newInvalidReachAndFrequencyMeasurementSpec)
        .measurement
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
    assertThat(failure.message).contains("delta")
    logger.info("Receive failed Measurement from Kingdom: ${failure.message}. Test passes.")
  }

  /**
   * A sequence of operations done in the simulator involving a direct reach and frequency
   * measurement.
   */
  suspend fun testDirectReachAndFrequency(runId: String) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(measurementConsumer, runId, ::newReachAndFrequencyMeasurementSpec, 1)
    val measurementName = measurementInfo.measurement.name
    logger.info("Created direct reach and frequency measurement $measurementName.")

    // Get the CMMS computed result and compare it with the expected result.
    val reachAndFrequencyResult = pollForResult { getReachAndFrequencyResult(measurementName) }
    logger.info("Got direct reach and frequency result from Kingdom: $reachAndFrequencyResult")

    val expectedResult = getExpectedResult(measurementInfo)
    logger.info("Expected result: $expectedResult")

    // TODO(@riemanli): Use variance rather than fixed tolerance values.
    assertThat(reachAndFrequencyResult)
      .reachValue()
      .isWithinPercent(0.5)
      .of(expectedResult.reach.value)
    assertThat(reachAndFrequencyResult.reach.hasDeterministicCountDistinct()).isTrue()
    assertThat(reachAndFrequencyResult.reach.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)

    assertThat(reachAndFrequencyResult)
      .frequencyDistribution()
      .isWithin(0.01)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)
    assertThat(reachAndFrequencyResult.frequency.hasDeterministicDistribution()).isTrue()
    assertThat(reachAndFrequencyResult.frequency.noiseMechanism)
      .isEqualTo(expectedDirectNoiseMechanism)

    logger.info("Direct reach and frequency result is equal to the expected result")
  }

  /** A sequence of operations done in the simulator involving a direct reach measurement. */
  suspend fun testDirectReach(runId: String) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(measurementConsumer, runId, ::newReachMeasurementSpec, 1)
    val measurementName = measurementInfo.measurement.name
    logger.info("Created direct reach measurement $measurementName.")

    // Get the CMMS computed result and compare it with the expected result.
    val reachResult = pollForResult { getReachResult(measurementName) }
    logger.info("Got direct reach result from Kingdom: $reachResult")

    val expectedResult = getExpectedResult(measurementInfo)
    logger.info("Expected result: $expectedResult")

    // TODO(@riemanli): Use variance rather than fixed tolerance values.
    assertThat(reachResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
    assertThat(reachResult.reach.hasDeterministicCountDistinct()).isTrue()
    assertThat(reachResult.reach.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)

    logger.info("Direct reach result is equal to the expected result")
  }

  suspend fun executeReachOnly(runId: String): ExecutionResult {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(measurementConsumer, runId, ::newReachOnlyMeasurementSpec)
    val measurementName = measurementInfo.measurement.name
    logger.info("Created reach-only measurement $measurementName.")

    // Get the CMMS computed result and compare it with the expected result.
    var reachOnlyResult = getReachResult(measurementName)
    var nAttempts = 0
    while (reachOnlyResult == null && (nAttempts < 4)) {
      nAttempts++
      logger.info("Computation not done yet, wait for another 30 seconds.  Attempt $nAttempts")
      delay(Duration.ofSeconds(30))
      reachOnlyResult = getReachResult(measurementName)
    }
    checkNotNull(reachOnlyResult) { "Timed out waiting for response to reach-only request" }

    val expectedResult: Result = getExpectedResult(measurementInfo)
    return ExecutionResult(reachOnlyResult, expectedResult, measurementInfo)
  }

  /** A sequence of operations done in the simulator involving a reach-only measurement. */
  suspend fun testReachOnly(runId: String) {
    val result = executeReachOnly(runId)

    logger.info("Actual result: ${result.actualResult}")
    logger.info("Expected result: ${result.expectedResult}")

    assertDpResultsEqual(result.actualResult, result.expectedResult)
    logger.info("Reach-only result is equal to the expected result. Correctness Test passes.")
  }

  /** A sequence of operations done in the simulator involving an impression measurement. */
  suspend fun testImpression(runId: String) {
    logger.info { "Creating impression Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(measurementConsumer, runId, ::newImpressionMeasurementSpec)
    val measurementName = measurementInfo.measurement.name
    logger.info("Created impression Measurement $measurementName.")

    val impressionResults = pollForResults { getImpressionResults(measurementName) }

    impressionResults.forEach {
      val result = parseAndVerifyResult(it)
      assertThat(result.impression.value)
        .isEqualTo(
          // EdpSimulator sets it to this value.
          apiIdToExternalId(DataProviderCertificateKey.fromName(it.certificate)!!.dataProviderId)
        )
      assertThat(result.impression.customDirectMethodology)
        .isEqualTo(customDirectMethodology { scalar = 0.0 })
      assertThat(result.impression.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)
    }
    logger.info("Impression result is equal to the expected result")
  }

  /** A sequence of operations done in the simulator involving a duration measurement. */
  suspend fun testDuration(runId: String) {
    logger.info { "Creating duration Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(measurementConsumer, runId, ::newDurationMeasurementSpec)
    val measurementName = measurementInfo.measurement.name
    logger.info("Created duration Measurement $measurementName.")

    val durationResults = pollForResults { getDurationResults(measurementName) }

    durationResults.forEach {
      val result = parseAndVerifyResult(it)
      val externalDataProviderId =
        apiIdToExternalId(DataProviderCertificateKey.fromName(it.certificate)!!.dataProviderId)
      assertThat(result.watchDuration.value.seconds)
        .isEqualTo(
          // EdpSimulator sets it to this value.
          log2(externalDataProviderId.toDouble()).toLong()
        )
      // EdpSimulator hasn't had an implementation for watch duration.
      assertThat(result.watchDuration.customDirectMethodology)
        .isEqualTo(customDirectMethodology { scalar = 0.0 })
      assertThat(result.watchDuration.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)
    }
    logger.info("Duration result is equal to the expected result")
  }

  /** Compare two [Result]s within the differential privacy error range. */
  private fun assertDpResultsEqual(expectedResult: Result, actualResult: Result) {
    // TODO(@riemanli): Use variance rather than fixed tolerance values.
    assertThat(actualResult).reachValue().isWithinPercent(10.0).of(expectedResult.reach.value)
    assertThat(actualResult)
      .frequencyDistribution()
      .isWithin(0.05)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)
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
  ): MeasurementInfo {
    val eventGroups: List<EventGroup> =
      listEventGroups(measurementConsumer.name).filter {
        it.eventGroupReferenceId.startsWith(
          TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
        )
      }
    check(eventGroups.isNotEmpty()) { "No event groups found for ${measurementConsumer.name}" }
    val nonceHashes = mutableListOf<ByteString>()

    val requisitions: List<RequisitionInfo> =
      eventGroups
        .groupBy { extractDataProviderKey(it.name) }
        .entries
        .take(maxDataProviders)
        .map { (dataProviderKey, eventGroups) ->
          val nonce = Random.Default.nextLong()
          nonceHashes.add(Hashing.hashSha256(nonce))
          buildRequisitionInfo(dataProviderKey, eventGroups, measurementConsumer, nonce)
        }

    val measurementSpec = newMeasurementSpec(measurementConsumer.publicKey.data, nonceHashes)
    val request = createMeasurementRequest {
      parent = measurementConsumer.name
      measurement = measurement {
        measurementConsumerCertificate = measurementConsumer.certificate
        this.measurementSpec =
          signMeasurementSpec(measurementSpec, measurementConsumerData.signingKey)
        dataProviders += requisitions.map { it.dataProviderEntry }
        this.measurementReferenceId = runId
      }
    }
    val measurement: Measurement =
      try {
        measurementsClient
          .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
          .createMeasurement(request)
      } catch (e: StatusException) {
        throw Exception("Error creating Measurement", e)
      }

    return MeasurementInfo(measurement, measurementSpec, requisitions)
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getImpressionResults(measurementName: String): List<Measurement.ResultPair> {
    return checkNotFailed(getMeasurement(measurementName)).resultsList.toList()
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getDurationResults(measurementName: String): List<Measurement.ResultPair> {
    return checkNotFailed(getMeasurement(measurementName)).resultsList.toList()
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getReachAndFrequencyResult(measurementName: String): Result? {
    val measurement = checkNotFailed(getMeasurement(measurementName))
    if (measurement.state != Measurement.State.SUCCEEDED) {
      return null
    }

    val resultPair = measurement.resultsList[0]
    val result = parseAndVerifyResult(resultPair)
    assertThat(result.hasReach()).isTrue()
    assertThat(result.hasFrequency()).isTrue()

    return result
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getReachResult(measurementName: String): Result? {
    val measurement = checkNotFailed(getMeasurement(measurementName))
    if (measurement.state != Measurement.State.SUCCEEDED) {
      return null
    }

    val resultPair = measurement.resultsList[0]
    val result = parseAndVerifyResult(resultPair)
    assertThat(result.hasReach()).isTrue()
    assertThat(result.hasFrequency()).isFalse()

    return result
  }

  /** Gets [Measurement] with logging state. */
  private suspend fun getMeasurement(measurementName: String): Measurement {
    val measurement: Measurement =
      try {
        measurementsClient
          .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementName })
      } catch (e: StatusException) {
        throw Exception("Error fetching measurement $measurementName", e)
      }

    logger.info("Current Measurement state is: " + measurement.state)

    return measurement
  }

  /** Checks if the given [Measurement] is failed. */
  private fun checkNotFailed(measurement: Measurement): Measurement {
    check(measurement.state != Measurement.State.FAILED) {
      val failure: Failure = measurement.failure
      "Measurement failed with reason ${failure.reason}: ${failure.message}"
    }
    return measurement
  }

  /** Gets the failure of an invalid [Measurement] if it is failed */
  private suspend fun getFailure(measurementName: String): Failure? {
    val measurement = getMeasurement(measurementName)
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
  private fun getExpectedResult(measurementInfo: MeasurementInfo): Result {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementInfo.measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH -> getExpectedReachResult(measurementInfo)
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
        getExpectedReachAndFrequencyResult(measurementInfo)
      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> getExpectedImpressionResult()
      MeasurementSpec.MeasurementTypeCase.DURATION -> getExpectedDurationResult()
      MeasurementSpec.MeasurementTypeCase.POPULATION -> getExpectedPopulationResult()
      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
        error("measurement_type not set")
    }
  }

  private fun getExpectedDurationResult(): Result {
    TODO("Not yet implemented")
  }

  private fun getExpectedImpressionResult(): Result {
    TODO("Not yet implemented")
  }

  private fun getExpectedPopulationResult(): Result {
    TODO("Not yet implemented")
  }

  private fun getExpectedReachResult(measurementInfo: MeasurementInfo): Result {
    val reach = MeasurementResults.computeReach(measurementInfo.sampledVids.asIterable())
    return result { this.reach = reach { value = reach.toLong() } }
  }

  private fun getExpectedReachAndFrequencyResult(measurementInfo: MeasurementInfo): Result {
    val (reach, relativeFrequencyDistribution) =
      MeasurementResults.computeReachAndFrequency(
        measurementInfo.sampledVids.asIterable(),
        measurementInfo.measurementSpec.reachAndFrequency.maximumFrequency
      )
    return result {
      this.reach = reach { value = reach.toLong() }
      frequency = frequency {
        this.relativeFrequencyDistribution.putAll(
          relativeFrequencyDistribution.mapKeys { it.key.toLong() }
        )
      }
    }
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

  private fun newReachMeasurementSpec(
    serializedMeasurementPublicKey: ByteString,
    nonceHashes: List<ByteString>
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = serializedMeasurementPublicKey
      reach = MeasurementSpecKt.reach { privacyParams = outputDpParams }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      this.nonceHashes += nonceHashes
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
        maximumFrequency = 10
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
      reach = MeasurementSpecKt.reach { privacyParams = outputDpParams }
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
    val invalidPrivacyParams = differentialPrivacyParams {
      epsilon = 1.0
      delta = 0.0
    }
    return newReachAndFrequencyMeasurementSpec(serializedMeasurementPublicKey, nonceHashes).copy {
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = invalidPrivacyParams
        frequencyPrivacyParams = invalidPrivacyParams
        maximumFrequency = 10
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
        maximumWatchDurationPerUser = Durations.fromMinutes(1)
      }
      this.nonceHashes += nonceHashes
    }
  }

  private suspend fun listEventGroups(measurementConsumer: String): List<EventGroup> {
    val request = listEventGroupsRequest { parent = measurementConsumer }
    try {
      return eventGroupsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .listEventGroups(request)
        .eventGroupsList
    } catch (e: StatusException) {
      throw Exception("Error listing event groups for MC $measurementConsumer", e)
    }
  }

  private fun extractDataProviderKey(eventGroupName: String): DataProviderKey {
    val eventGroupKey = EventGroupKey.fromName(eventGroupName) ?: error("Invalid eventGroup name.")
    return eventGroupKey.parentKey
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

  private suspend fun buildRequisitionInfo(
    dataProviderKey: DataProviderKey,
    eventGroups: List<EventGroup>,
    measurementConsumer: MeasurementConsumer,
    nonce: Long
  ): RequisitionInfo {
    val dataProvider = getDataProvider(dataProviderKey)

    val requisitionSpec = requisitionSpec {
      for (eventGroup in eventGroups) {
        events =
          RequisitionSpecKt.events {
            this.eventGroups += eventGroupEntry {
              key = eventGroup.name
              value =
                RequisitionSpecKt.EventGroupEntryKt.value {
                  collectionInterval = interval {
                    startTime = EVENT_RANGE.start.toProtoTime()
                    endTime = EVENT_RANGE.endExclusive.toProtoTime()
                  }
                  filter = eventFilter { expression = FILTER_EXPRESSION }
                }
            }
          }
      }
      measurementPublicKey = measurementConsumer.publicKey.data
      this.nonce = nonce
    }
    val signedRequisitionSpec =
      signRequisitionSpec(requisitionSpec, measurementConsumerData.signingKey)
    val dataProviderEntry =
      dataProvider.toDataProviderEntry(signedRequisitionSpec, Hashing.hashSha256(nonce))

    return RequisitionInfo(dataProviderEntry, requisitionSpec, eventGroups)
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
    private const val FILTER_EXPRESSION =
      "person.gender == ${Person.Gender.MALE_VALUE} && " +
        "(video_ad.viewed_fraction > 0.25 || video_ad.viewed_fraction == 0.25)"

    /**
     * Date range for events.
     *
     * TODO(@SanjayVas): Make this configurable.
     */
    private val EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private val RequisitionSpec.eventGroupsMap: Map<String, RequisitionSpec.EventGroupEntry.Value>
  get() = events.eventGroupsList.associate { it.key to it.value }
