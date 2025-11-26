// Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.util.Durations
import io.grpc.StatusException
import java.lang.IllegalStateException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.logging.Logger
import kotlin.math.log2
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.random.Random
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.time.delay
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.Failure
import org.wfanet.measurement.api.v2alpha.Measurement.Result
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.population
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.coerceAtMost
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.eventdataprovider.noiser.DpParams as NoiserDpParams
import org.wfanet.measurement.measurementconsumer.stats.DeterministicMethodology
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.HonestMajorityShareShuffleMethodology
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsV2Methodology
import org.wfanet.measurement.measurementconsumer.stats.Methodology
import org.wfanet.measurement.measurementconsumer.stats.NoiseMechanism as StatsNoiseMechanism
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.measurementconsumer.stats.VidSamplingInterval as StatsVidSamplingInterval
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

data class MeasurementConsumerData(
  // The MC's public API resource name
  val name: String,
  /** The MC's consent signaling signing key. */
  val signingKey: SigningKeyHandle,
  /** The MC's encryption private key. */
  val encryptionKey: PrivateKeyHandle,
  /** An API key for the MC. */
  val apiAuthenticationKey: String,
)

data class PopulationData(
  val populationDataProviderName: String,
  val populationSpec: PopulationSpec,
)

/** Simulator for MeasurementConsumer operations on the CMMS public API. */
abstract class MeasurementConsumerSimulator(
  private val measurementConsumerData: MeasurementConsumerData,
  private val outputDpParams: DifferentialPrivacyParams,
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val measurementsClient: MeasurementsCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val certificatesClient: CertificatesCoroutineStub,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  private val expectedDirectNoiseMechanism: NoiseMechanism,
  private val initialResultPollingDelay: Duration,
  private val maximumResultPollingDelay: Duration,
  private val reportName: String =
    ReportKey(
        MeasurementConsumerKey.fromName(measurementConsumerData.name)!!.measurementConsumerId,
        "some-report-id",
      )
      .toName(),
  private val modelLineName: String = "some-model-line",
  private val onMeasurementsCreated: (() -> Unit)? = null,
) {
  /** Cache of resource name to [Certificate]. */
  private val certificateCache = mutableMapOf<String, Certificate>()

  private lateinit var populationModelLineName: String

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

  private data class MeasurementComputationInfo(
    val methodology: Methodology,
    val noiseMechanism: NoiseMechanism,
  )

  protected abstract fun Flow<EventGroup>.filterEventGroups(): Flow<EventGroup>

  protected abstract fun getFilteredVids(measurementInfo: MeasurementInfo): Sequence<Long>

  protected abstract fun getFilteredVids(
    measurementInfo: MeasurementInfo,
    targetDataProviderId: String,
  ): Sequence<Long>

  data class ExecutionResult(
    val actualResult: Result,
    val expectedResult: Result,
    val measurementInfo: MeasurementInfo,
  )

  /** A sequence of operations done in the simulator involving a reach and frequency measurement. */
  suspend fun testReachAndFrequency(
    runId: String,
    requiredCapabilities: DataProvider.Capabilities =
      DataProvider.Capabilities.getDefaultInstance(),
    vidSamplingInterval: VidSamplingInterval = DEFAULT_VID_SAMPLING_INTERVAL,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ) {
    logger.info { "Creating reach and frequency Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo: MeasurementInfo =
      createMeasurement(
        measurementConsumer,
        runId,
        ::newReachAndFrequencyMeasurementSpec,
        requiredCapabilities,
        vidSamplingInterval = vidSamplingInterval,
        eventGroupFilter = eventGroupFilter,
      )
    val measurementName = measurementInfo.measurement.name
    logger.info { "Created reach and frequency Measurement $measurementName" }

    onMeasurementsCreated?.invoke()

    // Get the CMMS computed result and compare it with the expected result.
    val reachAndFrequencyResult: Result = pollForResult {
      getReachAndFrequencyResult(measurementName)
    }
    logger.info("Got reach and frequency result from Kingdom: $reachAndFrequencyResult")

    val expectedResult = getExpectedResult(measurementInfo)
    logger.info("Expected result: $expectedResult")

    val protocol = measurementInfo.measurement.protocolConfig.protocolsList.first()

    val reachVariance: Double =
      computeReachVariance(
        reachAndFrequencyResult,
        measurementInfo.measurementSpec.vidSamplingInterval,
        measurementInfo.measurementSpec.reachAndFrequency.reachPrivacyParams,
        protocol,
      )
    val reachTolerance = computeErrorMargin(reachVariance)
    if (expectedResult.reach.value.toDouble() < reachTolerance) {
      throw IllegalStateException("Expected result cannot be less than tolerance")
    }

    if (requiredCapabilities.honestMajorityShareShuffleSupported) {
      assertThat(protocol.protocolCase)
        .isEqualTo(ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE)
    } else {
      assertThat(protocol.protocolCase)
        .isEqualTo(ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2)
    }
    assertThat(reachAndFrequencyResult)
      .reachValue()
      .isWithin(reachTolerance)
      .of(expectedResult.reach.value)

    val frequencyTolerance: Map<Long, Double> =
      computeRelativeFrequencyTolerance(
        reachAndFrequencyResult,
        reachVariance,
        measurementInfo.measurementSpec,
        protocol,
      )
    assertThat(reachAndFrequencyResult)
      .frequencyDistribution()
      .isWithin(frequencyTolerance)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)

    logger.info("Reach and frequency result is equal to the expected result")
  }

  /**
   * A sequence of operations done in the simulator involving a reach and frequency measurement with
   * invalid params.
   */
  suspend fun testInvalidReachAndFrequency(
    runId: String,
    requiredCapabilities: DataProvider.Capabilities =
      DataProvider.Capabilities.getDefaultInstance(),
    vidSamplingInterval: VidSamplingInterval = DEFAULT_VID_SAMPLING_INTERVAL,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)

    val invalidMeasurement =
      createMeasurement(
          measurementConsumer,
          runId,
          ::newInvalidReachAndFrequencyMeasurementSpec,
          requiredCapabilities,
          vidSamplingInterval,
          eventGroupFilter = eventGroupFilter,
        )
        .measurement
    logger.info(
      "Created invalid reach and frequency measurement ${invalidMeasurement.name}, state=${invalidMeasurement.state.name}"
    )

    onMeasurementsCreated?.invoke()

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
   * 1. Requisitions are all created before results are checked for any since requistions may be
   *    grouped.
   * 2. Poll for requisition results.
   *
   * @numMeasurements - The number of incremental measurements to request within the time period.
   */
  suspend fun testDirectReachAndFrequency(
    runId: String,
    numMeasurements: Int,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ) {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    logger.info("Creating measurements...")
    val measurementInfos =
      (1..numMeasurements).map { measurementNumber ->
        val measurementInfo =
          createMeasurement(
            measurementConsumer,
            runId,
            ::newReachAndFrequencyMeasurementSpec,
            DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
            DEFAULT_VID_SAMPLING_INTERVAL,
            measurementNumber.toDouble() / numMeasurements,
            1,
            eventGroupFilter = eventGroupFilter,
          )
        val measurementName = measurementInfo.measurement.name
        logger.info("Created direct reach and frequency measurement $measurementName.")
        measurementInfo
      }

    onMeasurementsCreated?.invoke()

    measurementInfos.forEachIndexed { measurementNumber, measurementInfo ->
      val measurementName = measurementInfo.measurement.name
      // Get the CMMS computed result and compare it with the expected result.
      logger.info("Polling for result for $measurementNumber/$numMeasurements: $measurementInfo")
      val reachAndFrequencyResult = pollForResult { getReachAndFrequencyResult(measurementName) }
      logger.info("Got direct reach and frequency result from Kingdom: $reachAndFrequencyResult")

      val expectedResult = getExpectedResult(measurementInfo)
      logger.info("Expected result: $expectedResult")
      assertThat(reachAndFrequencyResult.reach.hasDeterministicCountDistinct()).isTrue()
      assertThat(reachAndFrequencyResult.reach.noiseMechanism)
        .isEqualTo(expectedDirectNoiseMechanism)
      assertThat(reachAndFrequencyResult.frequency.hasDeterministicDistribution()).isTrue()
      assertThat(reachAndFrequencyResult.frequency.noiseMechanism)
        .isEqualTo(expectedDirectNoiseMechanism)

      val protocol = measurementInfo.measurement.protocolConfig.protocolsList.first()

      val reachVariance: Double =
        computeReachVariance(
          reachAndFrequencyResult,
          measurementInfo.measurementSpec.vidSamplingInterval,
          measurementInfo.measurementSpec.reachAndFrequency.reachPrivacyParams,
          protocol,
        )
      val reachTolerance = computeErrorMargin(reachVariance)
      if (expectedResult.reach.value.toDouble() < reachTolerance) {
        throw IllegalStateException("Expected result cannot be less than tolerance")
      }

      assertThat(reachAndFrequencyResult)
        .reachValue()
        .isWithin(reachTolerance)
        .of(expectedResult.reach.value)

      val frequencyTolerance: Map<Long, Double> =
        computeRelativeFrequencyTolerance(
          reachAndFrequencyResult,
          reachVariance,
          measurementInfo.measurementSpec,
          protocol,
        )

      assertThat(reachAndFrequencyResult)
        .frequencyDistribution()
        .isWithin(frequencyTolerance)
        .of(expectedResult.frequency.relativeFrequencyDistributionMap)

      logger.info(
        "Direct reach and frequency result is equal to the expected result for measurement: $measurementNumber"
      )
    }
  }

  /**
   * A sequence of operations done in the simulator involving a direct reach measurement.
   *
   * @numMeasurements - The number of incremental measurements to request within the time period.
   */
  suspend fun testDirectReachOnly(
    runId: String,
    numMeasurements: Int,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ) {
    // Create new measurements on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    logger.info("Creating measurements...")
    val measurementInfos =
      (1..numMeasurements).map { measurementNumber ->
        val measurementInfo =
          createMeasurement(
            measurementConsumer,
            runId,
            ::newReachMeasurementSpec,
            DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
            DEFAULT_VID_SAMPLING_INTERVAL,
            measurementNumber.toDouble() / numMeasurements,
            1,
            eventGroupFilter = eventGroupFilter,
          )
        val measurementName = measurementInfo.measurement.name
        logger.info("Created direct reach measurement $measurementName.")
        measurementInfo
      }
    onMeasurementsCreated?.invoke()
    measurementInfos.forEachIndexed { measurementNumber, measurementInfo ->
      val measurementName = measurementInfo.measurement.name
      // Get the CMMS computed result and compare it with the expected result.
      val reachResult = pollForResult { getReachResult(measurementName) }
      logger.info("Got direct reach result from Kingdom: $reachResult")

      val expectedResult = getExpectedResult(measurementInfo)
      logger.info("Expected result: $expectedResult")

      val protocol = measurementInfo.measurement.protocolConfig.protocolsList.first()

      val reachVariance: Double =
        computeReachVariance(
          reachResult,
          measurementInfo.measurementSpec.vidSamplingInterval,
          measurementInfo.measurementSpec.reach.privacyParams,
          protocol,
        )
      val reachTolerance = computeErrorMargin(reachVariance)
      if (expectedResult.reach.value.toDouble() < reachTolerance) {
        throw IllegalStateException("Expected result cannot be less than tolerance")
      }

      assertThat(reachResult).reachValue().isWithin(reachTolerance).of(expectedResult.reach.value)
      assertThat(reachResult.reach.hasDeterministicCountDistinct()).isTrue()
      assertThat(reachResult.reach.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)
      assertThat(reachResult.hasFrequency()).isFalse()

      logger.info(
        "Direct reach result is equal to the expected result for measurement: $measurementNumber"
      )
    }
  }

  suspend fun executeReachOnly(
    runId: String,
    requiredCapabilities: DataProvider.Capabilities =
      DataProvider.Capabilities.getDefaultInstance(),
    vidSamplingInterval: VidSamplingInterval = DEFAULT_VID_SAMPLING_INTERVAL,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ): ExecutionResult {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(
        measurementConsumer,
        runId,
        ::newReachOnlyMeasurementSpec,
        requiredCapabilities,
        vidSamplingInterval = vidSamplingInterval,
        eventGroupFilter = eventGroupFilter,
      )
    val measurementName = measurementInfo.measurement.name
    logger.info("Created reach-only measurement $measurementName.")

    onMeasurementsCreated?.invoke()

    // Get the CMMS computed result and compare it with the expected result.
    var reachOnlyResult = getReachResult(measurementName)
    var attemptCount = 0
    while (reachOnlyResult == null && (attemptCount < 6)) {
      attemptCount++
      logger.info("Computation not done yet, wait for another 30 seconds.  Attempt $attemptCount")
      delay(Duration.ofSeconds(30))
      reachOnlyResult = getReachResult(measurementName)
    }
    checkNotNull(reachOnlyResult) { "Timed out waiting for response to reach-only request" }

    val expectedResult: Result = getExpectedResult(measurementInfo)
    return ExecutionResult(reachOnlyResult, expectedResult, measurementInfo)
  }

  suspend fun executeReachAndFrequency(
    runId: String,
    requiredCapabilities: DataProvider.Capabilities =
      DataProvider.Capabilities.getDefaultInstance(),
    vidSamplingInterval: VidSamplingInterval = DEFAULT_VID_SAMPLING_INTERVAL,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ): ExecutionResult {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(
        measurementConsumer,
        runId,
        ::newReachAndFrequencyMeasurementSpec,
        requiredCapabilities,
        vidSamplingInterval = vidSamplingInterval,
        eventGroupFilter = eventGroupFilter,
      )
    val measurementName = measurementInfo.measurement.name
    logger.info("Created reach-and-frequency measurement $measurementName.")

    onMeasurementsCreated?.invoke()

    // Get the CMMS computed result and compare it with the expected result.
    var reachAndFrequencyResult = getReachAndFrequencyResult(measurementName)
    var attemptCount = 0
    while (reachAndFrequencyResult == null && (attemptCount < 4)) {
      attemptCount++
      logger.info("Computation not done yet, wait for another 30 seconds.  Attempt $attemptCount")
      delay(Duration.ofSeconds(30))
      reachAndFrequencyResult = getReachAndFrequencyResult(measurementName)
    }
    checkNotNull(reachAndFrequencyResult) {
      "Timed out waiting for response to reach-and-frequency request"
    }

    val expectedResult: Result = getExpectedResult(measurementInfo)
    return ExecutionResult(reachAndFrequencyResult, expectedResult, measurementInfo)
  }

  /** A sequence of operations done in the simulator involving a reach-only measurement. */
  suspend fun testReachOnly(
    runId: String,
    requiredCapabilities: DataProvider.Capabilities =
      DataProvider.Capabilities.getDefaultInstance(),
    vidSamplingInterval: VidSamplingInterval = DEFAULT_VID_SAMPLING_INTERVAL,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ) {
    logger.info { "Creating reach only Measurement..." }
    val result =
      executeReachOnly(runId, requiredCapabilities, vidSamplingInterval, eventGroupFilter)

    val protocol = result.measurementInfo.measurement.protocolConfig.protocolsList.first()

    val reachVariance: Double =
      computeReachVariance(
        result.actualResult,
        result.measurementInfo.measurementSpec.vidSamplingInterval,
        result.measurementInfo.measurementSpec.reach.privacyParams,
        protocol,
      )
    val reachTolerance = computeErrorMargin(reachVariance)
    if (result.expectedResult.reach.value.toDouble() < reachTolerance) {
      throw IllegalStateException("Expected result cannot be less than tolerance")
    }

    if (requiredCapabilities.honestMajorityShareShuffleSupported) {
      assertThat(protocol.protocolCase)
        .isEqualTo(ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE)
    } else {
      assertThat(protocol.protocolCase)
        .isEqualTo(ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2)
    }
    assertThat(result.actualResult)
      .reachValue()
      .isWithin(reachTolerance)
      .of(result.expectedResult.reach.value)
    logger.info("Actual result: ${result.actualResult}")
    logger.info("Expected result: ${result.expectedResult}")

    assertThat(result.actualResult)
      .reachValue()
      .isWithin(reachTolerance)
      .of(result.expectedResult.reach.value)
    logger.info("Reach-only result is equal to the expected result. Correctness Test passes.")
  }

  /** A sequence of operations done in the simulator involving an impression measurement. */
  suspend fun testImpression(runId: String, eventGroupFilter: ((EventGroup) -> Boolean)? = null) {
    logger.info { "Creating impression Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(
        measurementConsumer,
        runId,
        ::newImpressionMeasurementSpec,
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
        DEFAULT_VID_SAMPLING_INTERVAL,
        eventGroupFilter = eventGroupFilter,
      )
    val measurementName = measurementInfo.measurement.name
    logger.info("Created impression Measurement $measurementName.")

    onMeasurementsCreated?.invoke()

    val impressionResults: List<Measurement.ResultOutput> = pollForResults {
      getImpressionResults(measurementName)
    }
    logger.info("Got impression result from Kingdom: $impressionResults")

    val protocol = measurementInfo.measurement.protocolConfig.protocolsList.first()

    impressionResults.forEach {
      val result = parseAndVerifyResult(it)
      val dataProviderId = DataProviderCertificateKey.fromName(it.certificate)!!.dataProviderId

      val expectedResult =
        getExpectedImpressionResultByDataProvider(measurementInfo, dataProviderId)

      val variance = computeImpressionVariance(result, measurementInfo.measurementSpec, protocol)
      val tolerance = computeErrorMargin(variance)
      if (expectedResult.impression.value.toDouble() < tolerance) {
        throw IllegalStateException("Expected impressions cannot be less than tolerance")
      }
      assertThat(result.impression.hasDeterministicCount()).isTrue()
      assertThat(result.impression.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)
      assertThat(result).impressionValue().isWithin(tolerance).of(expectedResult.impression.value)
    }
    logger.info("Impression result is equal to the expected result")
  }

  /** A sequence of operations done in the simulator involving a duration measurement. */
  suspend fun testDuration(runId: String, eventGroupFilter: ((EventGroup) -> Boolean)? = null) {
    logger.info { "Creating duration Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val measurementInfo =
      createMeasurement(
        measurementConsumer,
        runId,
        ::newDurationMeasurementSpec,
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
        DEFAULT_VID_SAMPLING_INTERVAL,
        eventGroupFilter = eventGroupFilter,
      )
    val measurementName = measurementInfo.measurement.name
    logger.info("Created duration Measurement $measurementName.")

    onMeasurementsCreated?.invoke()

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
        .isEqualTo(
          customDirectMethodology { variance = CustomDirectMethodologyKt.variance { scalar = 0.0 } }
        )
      assertThat(result.watchDuration.noiseMechanism).isEqualTo(expectedDirectNoiseMechanism)
    }
    logger.info("Duration result is equal to the expected result")
  }

  /** A sequence of operations done in the simulator involving a population measurement. */
  suspend fun testPopulation(
    runId: String,
    populationData: PopulationData,
    modelLineName: String,
    populationFilterExpression: String,
    eventMessageDescriptor: Descriptors.Descriptor,
  ) {
    logger.info { "Creating population Measurement..." }
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    populationModelLineName = modelLineName
    val measurementInfo: MeasurementInfo =
      createPopulationMeasurement(
        measurementConsumer,
        runId,
        populationData,
        populationFilterExpression,
        ::newPopulationMeasurementSpec,
      )

    onMeasurementsCreated?.invoke()

    val measurementName = measurementInfo.measurement.name
    logger.info { "Created population Measurement $measurementName" }

    // Get the CMMS computed result and compare it with the expected result.
    val populationResult: Result = pollForResult { getPopulationResult(measurementName) }
    logger.info("Got population result from Kingdom: $populationResult")

    val expectedResult =
      getExpectedPopulationResult(
        measurementInfo,
        populationData.populationSpec,
        eventMessageDescriptor,
      )
    logger.info("Expected result: $expectedResult")

    assertThat(populationResult.population.value).isEqualTo(expectedResult.population.value)

    logger.info("Population result is equal to the expected result")
  }

  /** Computes the tolerance values of an impression [Result] for testing. */
  private fun computeImpressionVariance(
    result: Result,
    measurementSpec: MeasurementSpec,
    protocol: ProtocolConfig.Protocol,
  ): Double {
    val measurementComputationInfo: MeasurementComputationInfo =
      buildMeasurementComputationInfo(result, protocol, result.impression.noiseMechanism)

    val maxFrequencyPerUser =
      if (result.impression.deterministicCount.customMaximumFrequencyPerUser != 0) {
        result.impression.deterministicCount.customMaximumFrequencyPerUser
      } else {
        measurementSpec.impression.maximumFrequencyPerUser
      }

    return VariancesImpl.computeMeasurementVariance(
      measurementComputationInfo.methodology,
      ImpressionMeasurementVarianceParams(
        impression = max(0L, result.impression.value),
        measurementParams =
          ImpressionMeasurementParams(
            vidSamplingInterval = measurementSpec.vidSamplingInterval.toStatsVidSamplingInterval(),
            dpParams = measurementSpec.impression.privacyParams.toNoiserDpParams(),
            maximumFrequencyPerUser = maxFrequencyPerUser,
            noiseMechanism = measurementComputationInfo.noiseMechanism.toStatsNoiseMechanism(),
          ),
      ),
    )
  }

  /** Computes the tolerance values of a relative frequency distribution [Result] for testing. */
  private fun computeRelativeFrequencyTolerance(
    result: Result,
    reachVariance: Double,
    measurementSpec: MeasurementSpec,
    protocol: ProtocolConfig.Protocol,
  ): Map<Long, Double> {
    val measurementComputationInfo: MeasurementComputationInfo =
      buildMeasurementComputationInfo(result, protocol, result.frequency.noiseMechanism)

    return VariancesImpl.computeMeasurementVariance(
        measurementComputationInfo.methodology,
        FrequencyMeasurementVarianceParams(
          totalReach = max(0L, result.reach.value),
          reachMeasurementVariance = reachVariance,
          relativeFrequencyDistribution =
            result.frequency.relativeFrequencyDistributionMap.mapKeys { it.key.toInt() },
          measurementParams =
            FrequencyMeasurementParams(
              vidSamplingInterval =
                measurementSpec.vidSamplingInterval.toStatsVidSamplingInterval(),
              dpParams =
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.toNoiserDpParams(),
              noiseMechanism = measurementComputationInfo.noiseMechanism.toStatsNoiseMechanism(),
              maximumFrequency = measurementSpec.reachAndFrequency.maximumFrequency,
            ),
        ),
      )
      .relativeVariances
      .mapKeys { it.key.toLong() }
      .mapValues { computeErrorMargin(it.value) }
  }

  /** Computes the variance value of a reach [Result]. */
  private fun computeReachVariance(
    result: Result,
    vidSamplingInterval: VidSamplingInterval,
    privacyParams: DifferentialPrivacyParams,
    protocol: ProtocolConfig.Protocol,
  ): Double {
    val measurementComputationInfo: MeasurementComputationInfo =
      buildMeasurementComputationInfo(result, protocol, result.reach.noiseMechanism)

    return VariancesImpl.computeMeasurementVariance(
      measurementComputationInfo.methodology,
      ReachMeasurementVarianceParams(
        reach = max(0L, result.reach.value),
        measurementParams =
          ReachMeasurementParams(
            vidSamplingInterval = vidSamplingInterval.toStatsVidSamplingInterval(),
            dpParams = privacyParams.toNoiserDpParams(),
            noiseMechanism = measurementComputationInfo.noiseMechanism.toStatsNoiseMechanism(),
          ),
      ),
    )
  }

  /** Computes the margin of error, i.e. half width, of a 99.9% confidence interval. */
  private fun computeErrorMargin(variance: Double): Double {
    return CONFIDENCE_INTERVAL_MULTIPLIER * sqrt(variance)
  }

  /** Builds a [MeasurementComputationInfo] from a [ProtocolConfig.Protocol]. */
  private fun buildMeasurementComputationInfo(
    result: Result,
    protocol: ProtocolConfig.Protocol,
    directNoiseMechanism: NoiseMechanism,
  ): MeasurementComputationInfo {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (protocol.protocolCase) {
      ProtocolConfig.Protocol.ProtocolCase.DIRECT -> {
        MeasurementComputationInfo(DeterministicMethodology, directNoiseMechanism)
      }
      ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2 -> {
        MeasurementComputationInfo(
          LiquidLegionsV2Methodology(
            decayRate = protocol.liquidLegionsV2.sketchParams.decayRate,
            sketchSize = protocol.liquidLegionsV2.sketchParams.maxSize,
            samplingIndicatorSize = protocol.liquidLegionsV2.sketchParams.samplingIndicatorSize,
          ),
          protocol.liquidLegionsV2.noiseMechanism,
        )
      }
      ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        MeasurementComputationInfo(
          LiquidLegionsV2Methodology(
            decayRate = protocol.reachOnlyLiquidLegionsV2.sketchParams.decayRate,
            sketchSize = protocol.reachOnlyLiquidLegionsV2.sketchParams.maxSize,
            samplingIndicatorSize = 0L,
          ),
          protocol.reachOnlyLiquidLegionsV2.noiseMechanism,
        )
      }
      ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
        MeasurementComputationInfo(
          HonestMajorityShareShuffleMethodology(
            frequencyVectorSize = result.reach.honestMajorityShareShuffle.frequencyVectorSize
          ),
          protocol.honestMajorityShareShuffle.noiseMechanism,
        )
      }
      ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE -> {
        error("TrusTEE is not implemented.")
      }
      ProtocolConfig.Protocol.ProtocolCase.PROTOCOL_NOT_SET -> {
        error("Protocol is not set.")
      }
    }
  }

  /**
   * Creates a Measurement on behalf of the [MeasurementConsumer].
   *
   * @timePercentage - the percentage of the time period that will be inclued in the requisition
   * spec.
   */
  protected suspend fun createMeasurement(
    measurementConsumer: MeasurementConsumer,
    runId: String,
    newMeasurementSpec:
      (
        packedMeasurementPublicKey: ProtoAny,
        nonceHashes: List<ByteString>,
        vidSamplingInterval: VidSamplingInterval,
      ) -> MeasurementSpec,
    requiredCapabilities: DataProvider.Capabilities =
      DataProvider.Capabilities.getDefaultInstance(),
    vidSamplingInterval: VidSamplingInterval,
    timePercentage: Double = 1.0,
    maxDataProviders: Int = 20,
    eventGroupFilter: ((EventGroup) -> Boolean)? = null,
  ): MeasurementInfo {
    val eventGroups: List<EventGroup> =
      listEventGroups(measurementConsumer.name).filterEventGroups().toList()
    check(eventGroups.isNotEmpty()) { "No event groups found for ${measurementConsumer.name}" }
    val nonceHashes = mutableListOf<ByteString>()
    val keyToDataProviderMap: Map<DataProviderKey, DataProvider> =
      eventGroups
        .groupBy { extractDataProviderKey(it.name) }
        .entries
        .associate { it.key to getDataProvider(it.key.toName()) }

    val requisitions: List<RequisitionInfo> =
      eventGroups
        .filter { eventGroupFilter?.invoke(it) ?: true }
        .groupBy { extractDataProviderKey(it.name) }
        .entries
        .filter {
          val dataProvider = keyToDataProviderMap.getValue(it.key)
          if (requiredCapabilities.honestMajorityShareShuffleSupported) {
            dataProvider.capabilities.honestMajorityShareShuffleSupported
          } else {
            true
          }
        }
        .take(maxDataProviders)
        .map { (dataProviderKey, eventGroups) ->
          val nonce = Random.nextLong()
          nonceHashes.add(Hashing.hashSha256(nonce))
          val dataProvider = keyToDataProviderMap.getValue(dataProviderKey)
          buildRequisitionInfo(
            dataProvider,
            eventGroups,
            measurementConsumer,
            nonce,
            timePercentage,
          )
        }
    val measurementSpec =
      newMeasurementSpec(measurementConsumer.publicKey.message, nonceHashes, vidSamplingInterval)
    return createMeasurementInfo(measurementConsumer, measurementSpec, requisitions, runId)
  }

  private suspend fun createPopulationMeasurement(
    measurementConsumer: MeasurementConsumer,
    runId: String,
    populationData: PopulationData,
    populationFilterExpression: String,
    newMeasurementSpec:
      (packedMeasurementPublicKey: ProtoAny, nonceHashes: List<ByteString>) -> MeasurementSpec,
  ): MeasurementInfo {
    val nonce = Random.nextLong()
    val nonceHashes = mutableListOf<ByteString>()
    nonceHashes.add(Hashing.hashSha256(nonce))
    val populationDataProvider = getDataProvider(populationData.populationDataProviderName)
    val requisitions =
      listOf(
        buildPopulationMeasurementRequisitionInfo(
          populationDataProvider,
          measurementConsumer,
          populationFilterExpression,
          nonce,
        )
      )
    val measurementSpec = newMeasurementSpec(measurementConsumer.publicKey.message, nonceHashes)

    return createMeasurementInfo(measurementConsumer, measurementSpec, requisitions, runId)
  }

  private suspend fun createMeasurementInfo(
    measurementConsumer: MeasurementConsumer,
    measurementSpec: MeasurementSpec,
    requisitions: List<RequisitionInfo>,
    runId: String,
  ): MeasurementInfo {
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
  private suspend fun getImpressionResults(
    measurementName: String
  ): List<Measurement.ResultOutput> {
    return checkNotFailed(getMeasurement(measurementName)).resultsList.toList()
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getDurationResults(measurementName: String): List<Measurement.ResultOutput> {
    return checkNotFailed(getMeasurement(measurementName)).resultsList.toList()
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getReachAndFrequencyResult(measurementName: String): Result? {
    val measurement = checkNotFailed(getMeasurement(measurementName))
    if (measurement.state != Measurement.State.SUCCEEDED) {
      return null
    }

    val resultOutput = measurement.resultsList[0]
    val result = parseAndVerifyResult(resultOutput)
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

    val resultOutput = measurement.resultsList[0]
    val result = parseAndVerifyResult(resultOutput)
    assertThat(result.hasReach()).isTrue()
    assertThat(result.hasFrequency()).isFalse()

    return result
  }

  /** Gets the result of a [Measurement] if it is succeeded. */
  private suspend fun getPopulationResult(measurementName: String): Result? {
    val measurement = checkNotFailed(getMeasurement(measurementName))
    if (measurement.state != Measurement.State.SUCCEEDED) {
      return null
    }

    val resultOutput = measurement.resultsList[0]
    val result = parseAndVerifyResult(resultOutput)
    assertThat(result.hasPopulation()).isTrue()

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

    logger.info("Current Measurement ${measurement.name} state is: " + measurement.state)

    return measurement
  }

  /** Checks if the given [Measurement] is in an unsuccessful terminal state. */
  private fun checkNotFailed(measurement: Measurement): Measurement {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    when (measurement.state) {
      Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
      Measurement.State.COMPUTING,
      Measurement.State.SUCCEEDED -> return measurement
      Measurement.State.FAILED -> {
        val failure: Failure = measurement.failure
        error("Measurement failed with reason ${failure.reason}: ${failure.message}")
      }
      Measurement.State.CANCELLED -> error("Measurement cancelled")
      Measurement.State.STATE_UNSPECIFIED,
      Measurement.State.UNRECOGNIZED -> error("Unexpected Measurement state ${measurement.state}")
    }
  }

  /** Gets the failure of an invalid [Measurement] if it is failed */
  private suspend fun getFailure(measurementName: String): Failure? {
    val measurement = getMeasurement(measurementName)
    if (measurement.state != Measurement.State.FAILED) {
      return null
    }
    return measurement.failure
  }

  private suspend fun parseAndVerifyResult(resultOutput: Measurement.ResultOutput): Result {
    val certificate =
      certificateCache.getOrPut(resultOutput.certificate) {
        try {
          certificatesClient
            .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
            .getCertificate(getCertificateRequest { name = resultOutput.certificate })
        } catch (e: StatusException) {
          throw Exception("Error fetching certificate ${resultOutput.certificate}", e)
        }
      }

    val signedResult =
      decryptResult(resultOutput.encryptedResult, measurementConsumerData.encryptionKey)
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
    return signedResult.unpack()
  }

  /** Gets the expected result of a [Measurement] using raw sketches. */
  private fun getExpectedResult(measurementInfo: MeasurementInfo): Result {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementInfo.measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH -> getExpectedReachResult(measurementInfo)
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
        getExpectedReachAndFrequencyResult(measurementInfo)
      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> error("Should not be reached.")
      MeasurementSpec.MeasurementTypeCase.DURATION -> getExpectedDurationResult()
      MeasurementSpec.MeasurementTypeCase.POPULATION -> error("Should not be reached.")
      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
        error("measurement_type not set")
    }
  }

  private fun getExpectedDurationResult(): Result {
    TODO("Not yet implemented")
  }

  /** Gets the expected result of impression from a specific data provider. */
  private fun getExpectedImpressionResultByDataProvider(
    measurementInfo: MeasurementInfo,
    targetDataProviderId: String,
  ): Result {
    return result {
      impression =
        MeasurementKt.ResultKt.impression {
          value =
            MeasurementResults.computeImpression(
              getFilteredVids(measurementInfo, targetDataProviderId),
              measurementInfo.measurementSpec.impression.maximumFrequencyPerUser,
            )
        }
    }
  }

  private fun getExpectedPopulationResult(
    measurementInfo: MeasurementInfo,
    populationSpec: PopulationSpec,
    eventMessageDescriptor: Descriptors.Descriptor,
  ): Result {
    val requisition = measurementInfo.requisitions[0]
    val requisitionSpec = requisition.requisitionSpec
    val requisitionFilterExpression = requisitionSpec.population.filter.expression

    return result {
      population =
        MeasurementKt.ResultKt.population {
          value =
            MeasurementResults.computePopulation(
              populationSpec,
              requisitionFilterExpression,
              eventMessageDescriptor,
            )
        }
    }
  }

  private fun getExpectedReachResult(measurementInfo: MeasurementInfo): Result {
    val reach = MeasurementResults.computeReach(getFilteredVids(measurementInfo))
    return result { this.reach = reach { value = reach.toLong() } }
  }

  private fun getExpectedReachAndFrequencyResult(measurementInfo: MeasurementInfo): Result {
    val (reach, relativeFrequencyDistribution) =
      MeasurementResults.computeReachAndFrequency(
        getFilteredVids(measurementInfo),
        measurementInfo.measurementSpec.reachAndFrequency.maximumFrequency,
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

  protected suspend fun getMeasurementConsumer(name: String): MeasurementConsumer {
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
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
    vidSamplingInterval: VidSamplingInterval,
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = packedMeasurementPublicKey
      reach = MeasurementSpecKt.reach { privacyParams = outputDpParams }
      this.vidSamplingInterval = vidSamplingInterval
      this.nonceHashes += nonceHashes
      this.reportingMetadata = reportingMetadata { report = reportName }
      this.modelLine = modelLineName
    }
  }

  protected fun newReachAndFrequencyMeasurementSpec(
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
    vidSamplingInterval: VidSamplingInterval,
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = packedMeasurementPublicKey
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = outputDpParams
        frequencyPrivacyParams = outputDpParams
        maximumFrequency = 10
      }
      this.vidSamplingInterval = vidSamplingInterval
      this.nonceHashes += nonceHashes
      this.modelLine = modelLineName
      this.reportingMetadata = reportingMetadata { report = reportName }
    }
  }

  private fun newReachOnlyMeasurementSpec(
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
    vidSamplingInterval: VidSamplingInterval,
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = packedMeasurementPublicKey
      reach = MeasurementSpecKt.reach { privacyParams = outputDpParams }
      this.vidSamplingInterval = vidSamplingInterval
      this.nonceHashes += nonceHashes
      this.modelLine = modelLineName
      this.reportingMetadata = reportingMetadata { report = reportName }
    }
  }

  private fun newInvalidReachAndFrequencyMeasurementSpec(
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
    vidSamplingInterval: VidSamplingInterval,
  ): MeasurementSpec {
    val invalidPrivacyParams = differentialPrivacyParams {
      epsilon = 1.0
      delta = 0.0
    }
    return newReachAndFrequencyMeasurementSpec(
        packedMeasurementPublicKey,
        nonceHashes,
        vidSamplingInterval,
      )
      .copy {
        reachAndFrequency = reachAndFrequency {
          reachPrivacyParams = invalidPrivacyParams
          frequencyPrivacyParams = invalidPrivacyParams
          maximumFrequency = 10
        }
      }
  }

  private fun newImpressionMeasurementSpec(
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
    vidSamplingInterval: VidSamplingInterval,
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = packedMeasurementPublicKey
      impression = impression {
        privacyParams = outputDpParams
        maximumFrequencyPerUser = 2
      }
      this.vidSamplingInterval = vidSamplingInterval
      this.nonceHashes += nonceHashes
      this.modelLine = modelLineName
      this.reportingMetadata = reportingMetadata { report = reportName }
    }
  }

  private fun newDurationMeasurementSpec(
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
    vidSamplingInterval: VidSamplingInterval,
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = packedMeasurementPublicKey
      duration = duration {
        privacyParams = outputDpParams
        maximumWatchDurationPerUser = Durations.fromMinutes(1)
      }
      this.nonceHashes += nonceHashes
      this.modelLine = modelLineName
      this.reportingMetadata = reportingMetadata { report = reportName }
    }
  }

  private fun newPopulationMeasurementSpec(
    packedMeasurementPublicKey: ProtoAny,
    nonceHashes: List<ByteString>,
  ): MeasurementSpec {
    return measurementSpec {
      measurementPublicKey = packedMeasurementPublicKey
      population = MeasurementSpecKt.population {}
      this.nonceHashes += nonceHashes
      this.modelLine = populationModelLineName
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun listEventGroups(measurementConsumer: String): Flow<EventGroup> {
    return eventGroupsClient
      .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
      .listResources { pageToken: String ->
        val response =
          try {
            listEventGroups(
              listEventGroupsRequest {
                parent = measurementConsumer
                this.pageToken = pageToken
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing event groups for MC $measurementConsumer", e)
          }
        ResourceList(response.eventGroupsList, response.nextPageToken)
      }
      .flattenConcat()
  }

  private fun extractDataProviderKey(eventGroupName: String): DataProviderKey {
    val eventGroupKey = EventGroupKey.fromName(eventGroupName) ?: error("Invalid eventGroup name.")
    return eventGroupKey.parentKey
  }

  private suspend fun getDataProvider(name: String): DataProvider {
    val request = GetDataProviderRequest.newBuilder().also { it.name = name }.build()
    try {
      return dataProvidersClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .getDataProvider(request)
    } catch (e: StatusException) {
      throw Exception("Error fetching DataProvider $name", e)
    }
  }

  protected abstract fun buildRequisitionInfo(
    dataProvider: DataProvider,
    eventGroups: List<EventGroup>,
    measurementConsumer: MeasurementConsumer,
    nonce: Long,
    percentage: Double = 1.0,
  ): RequisitionInfo

  private fun buildPopulationMeasurementRequisitionInfo(
    dataProvider: DataProvider,
    measurementConsumer: MeasurementConsumer,
    populationFilterExpression: String,
    nonce: Long,
  ): RequisitionInfo {
    val requisitionSpec = requisitionSpec {
      population = population { filter = eventFilter { expression = populationFilterExpression } }
      measurementPublicKey = measurementConsumer.publicKey.message
      this.nonce = nonce
    }
    val signedRequisitionSpec =
      signRequisitionSpec(requisitionSpec, measurementConsumerData.signingKey)
    val dataProviderEntry =
      dataProvider.toDataProviderEntry(signedRequisitionSpec, Hashing.hashSha256(nonce))

    return RequisitionInfo(dataProviderEntry, requisitionSpec, listOf())
  }

  protected fun DataProvider.toDataProviderEntry(
    signedRequisitionSpec: SignedMessage,
    nonceHash: ByteString,
  ): DataProviderEntry {
    val source = this
    return dataProviderEntry {
      key = source.name
      this.value =
        MeasurementKt.DataProviderEntryKt.value {
          dataProviderCertificate = source.certificate
          dataProviderPublicKey = source.publicKey.message
          encryptedRequisitionSpec =
            encryptRequisitionSpec(signedRequisitionSpec, source.publicKey.unpack())
          this.nonceHash = nonceHash
        }
    }
  }

  private suspend inline fun pollForResult(getResult: () -> Result?): Result {
    return pollForResult(getResult) { it != null }!!
  }

  private suspend inline fun <T> pollForResults(getResults: () -> List<T>): List<T> {
    return pollForResult(getResults) { it.isNotEmpty() }
  }

  private suspend inline fun <T> pollForResult(getResult: () -> T, done: (T) -> Boolean): T {
    val backoff =
      ExponentialBackoff(initialDelay = initialResultPollingDelay, randomnessFactor = 0.0)
    var attempt = 1
    while (true) {
      val result = getResult()
      if (done(result)) {
        return result
      }

      val resultPollingDelay =
        backoff.durationForAttempt(attempt).coerceAtMost(maximumResultPollingDelay)
      logger.info { "Result not yet available. Waiting for ${resultPollingDelay.seconds} seconds." }
      delay(resultPollingDelay)
      attempt++
    }
  }

  companion object {
    // For a 99.9999% Confidence Interval.
    private const val CONFIDENCE_INTERVAL_MULTIPLIER = 5.0
    private val DEFAULT_VID_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0.2f
      width = 0.5f
    }
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/** Converts a [NoiseMechanism] to a [StatsNoiseMechanism]. */
private fun NoiseMechanism.toStatsNoiseMechanism(): StatsNoiseMechanism {
  return when (this) {
    NoiseMechanism.NONE -> StatsNoiseMechanism.NONE
    NoiseMechanism.GEOMETRIC,
    NoiseMechanism.CONTINUOUS_LAPLACE -> StatsNoiseMechanism.LAPLACE
    NoiseMechanism.DISCRETE_GAUSSIAN,
    NoiseMechanism.CONTINUOUS_GAUSSIAN -> StatsNoiseMechanism.GAUSSIAN
    NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
    NoiseMechanism.UNRECOGNIZED -> {
      error("Invalid NoiseMechanism.")
    }
  }
}

/** Converts a [VidSamplingInterval] to a [StatsVidSamplingInterval]. */
private fun VidSamplingInterval.toStatsVidSamplingInterval(): StatsVidSamplingInterval {
  val source = this
  return StatsVidSamplingInterval(source.start.toDouble(), source.width.toDouble())
}

/** Converts a [DifferentialPrivacyParams] to [NoiserDpParams]. */
fun DifferentialPrivacyParams.toNoiserDpParams(): NoiserDpParams {
  val source = this
  return NoiserDpParams(source.epsilon, source.delta)
}

val RequisitionSpec.eventGroupsMap: Map<String, RequisitionSpec.EventGroupEntry.Value>
  get() = events.eventGroupsList.associate { it.key to it.value }
