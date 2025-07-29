// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.abs
import kotlin.math.pow
import kotlin.math.sqrt
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator.MeasurementInfo
import org.wfanet.measurement.measurementconsumer.stats.HonestMajorityShareShuffleMethodology
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsV2Methodology
import org.wfanet.measurement.measurementconsumer.stats.Methodology
import org.wfanet.measurement.measurementconsumer.stats.NoiseMechanism as StatsNoiseMechanism
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementParams
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl.computeMeasurementVariance
import org.wfanet.measurement.measurementconsumer.stats.VidSamplingInterval as StatsVidSamplingInterval
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt

/**
 * Test the Measurement results are accurate w.r.t to the variance.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessReachMeasurementAccuracyTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<
      (
        String, ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
      ) -> InProcessDuchy.DuchyDependencies
    >,
) {

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(
      kingdomDataServicesRule,
      duchyDependenciesRule,
      SYNTHETIC_POPULATION_SPEC,
      SYNTHETIC_EVENT_GROUP_SPECS,
      useEdpSimulators = true,
    )

  private lateinit var mcSimulator: EventQueryMeasurementConsumerSimulator

  private val publicMeasurementsClient by lazy {
    MeasurementsGrpcKt.MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(
      inProcessCmmsComponents.kingdom.publicApiChannel
    )
  }
  private val publicCertificatesClient by lazy {
    CertificatesGrpcKt.CertificatesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicEventGroupsClient by lazy {
    EventGroupsGrpcKt.EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicDataProvidersClient by lazy {
    DataProvidersGrpcKt.DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicRequisitionsClient by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  @Before
  fun startDaemons() {
    inProcessCmmsComponents.startDaemons()
    initMcSimulator()
  }

  private fun initMcSimulator() {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    mcSimulator =
      EventQueryMeasurementConsumerSimulator(
        MeasurementConsumerData(
          measurementConsumerData.name,
          InProcessCmmsComponents.MC_ENTITY_CONTENT.signingKey,
          InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
          measurementConsumerData.apiAuthenticationKey,
        ),
        OUTPUT_DP_PARAMS,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicMeasurementConsumersClient,
        publicCertificatesClient,
        InProcessCmmsComponents.TRUSTED_CERTIFICATES,
        inProcessCmmsComponents.eventQuery,
        NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
  }

  @After
  fun stopEdpSimulators() {
    inProcessCmmsComponents.stopEdpSimulators()
  }

  @After
  fun stopDuchyDaemons() {
    inProcessCmmsComponents.stopDuchyDaemons()
  }

  private fun getReachVariance(
    methodology: Methodology,
    measurementInfo: MeasurementInfo,
    reach: Long,
  ): Double {
    val reachMeasurementParams =
      ReachMeasurementParams(
        StatsVidSamplingInterval(
          measurementInfo.measurementSpec.vidSamplingInterval.start.toDouble(),
          measurementInfo.measurementSpec.vidSamplingInterval.width.toDouble(),
        ),
        DpParams(OUTPUT_DP_PARAMS.epsilon, OUTPUT_DP_PARAMS.delta),
        StatsNoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)
    return computeMeasurementVariance(methodology, reachMeasurementVarianceParams)
  }

  private fun getStandardDeviation(nums: List<Double>): Double {
    val mean = nums.average()
    val standardDeviation = nums.fold(0.0) { acc, num -> acc + (num - mean).pow(2.0) }

    return sqrt(standardDeviation / nums.size)
  }

  data class ReachResult(
    val actualReach: Long,
    val expectedReach: Long,
    val lowerBound: Double,
    val upperBound: Double,
    val withinInterval: Boolean,
  )

  @Test
  fun `reach-only llv2 results should be accurate with respect to the variance`() = runBlocking {
    val reachResults = mutableListOf<ReachResult>()
    var expectedReach = -1L
    var expectedStandardDeviation = 0.0
    val liquidLegionsMethodology =
      LiquidLegionsV2Methodology(
        RoLlv2ProtocolConfig.protocolConfig.sketchParams.decayRate,
        RoLlv2ProtocolConfig.protocolConfig.sketchParams.maxSize,
        RoLlv2ProtocolConfig.protocolConfig.sketchParams.samplingIndicatorSize,
      )
    var summary = ""
    for (round in 1..DEFAULT_TEST_ROUND_NUMBER) {
      val executionResult =
        mcSimulator.executeReachOnly(
          round.toString(),
          DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
        )

      if (expectedReach == -1L) {
        expectedReach = executionResult.expectedResult.reach.value
        val expectedVariance =
          getReachVariance(liquidLegionsMethodology, executionResult.measurementInfo, expectedReach)
        expectedStandardDeviation = sqrt(expectedVariance)
      } else if (expectedReach != executionResult.expectedResult.reach.value) {
        logger.log(
          Level.WARNING,
          "expected result not consistent. round=$round, prev_expected_result=$expectedReach, " +
            "current_expected_result=${executionResult.expectedResult.reach.value}",
        )
      }

      // The general formula for confidence interval is result +/- multiplier * sqrt(variance).
      // The multiplier for 95% confidence interval is 1.96.
      val reach = executionResult.actualResult.reach.value
      val reachVariance =
        getReachVariance(liquidLegionsMethodology, executionResult.measurementInfo, reach)
      val intervalLowerBound = reach - sqrt(reachVariance) * MULTIPLIER
      val intervalUpperBound = reach + sqrt(reachVariance) * MULTIPLIER
      val withinInterval = reach >= intervalLowerBound && reach <= intervalUpperBound

      val reachResult =
        ReachResult(reach, expectedReach, intervalLowerBound, intervalUpperBound, withinInterval)
      reachResults += reachResult

      val message =
        "round=$round, actual_result=${reachResult.actualReach}, " +
          "expected_result=${reachResult.expectedReach}, " +
          "interval=(${"%.2f".format(reachResult.lowerBound)}, " +
          "${"%.2f".format(reachResult.upperBound)}), accurate=${reachResult.withinInterval}"
      summary += message + "\n"
      logger.log(Level.INFO, message)
    }

    logger.log(Level.INFO, "Accuracy Test Complete.\n$summary")

    val averageReach = reachResults.map { it.actualReach }.average()
    val withinIntervalNumber = reachResults.map { if (it.withinInterval) 1 else 0 }.sum()
    val withinIntervalPercentage = withinIntervalNumber.toDouble() / reachResults.size * 100
    val offsetPercentage = (averageReach - expectedReach) / expectedReach * 100
    val averageDispersionRatio =
      abs(averageReach - expectedReach) * sqrt(DEFAULT_TEST_ROUND_NUMBER.toDouble()) /
        expectedStandardDeviation

    logger.log(
      Level.INFO,
      "average_reach=$averageReach, offset_percentage=${"%.2f".format(offsetPercentage)}%, " +
        "number_of_rounds_within_interval=$withinIntervalNumber out of $DEFAULT_TEST_ROUND_NUMBER " +
        "(${"%.2f".format(withinIntervalPercentage)}%) ",
    )

    val standardDeviation = getStandardDeviation(reachResults.map { it.actualReach.toDouble() })
    logger.log(
      Level.INFO,
      "std=${"%.2f".format(standardDeviation)}, " +
        "expected_std=${"%.2f".format(expectedStandardDeviation)}, " +
        "ratio=${"%.2f".format(standardDeviation / expectedStandardDeviation)}",
    )

    assertThat(withinIntervalPercentage).isAtLeast(COVERAGE_TEST_THRESHOLD)
    assertThat(averageDispersionRatio).isLessThan(AVERAGE_TEST_THRESHOLD)
    assertThat(standardDeviation)
      .isGreaterThan(expectedStandardDeviation * STANDARD_DEVIATION_TEST_LOWER_THRESHOLD)
    assertThat(standardDeviation)
      .isLessThan(expectedStandardDeviation * STANDARD_DEVIATION_TEST_UPPER_THRESHOLD)
  }

  @Test
  fun `reach-only hmss results should be accurate with respect to the variance`() = runBlocking {
    val reachResults = mutableListOf<ReachResult>()
    var expectedReach = -1L
    var expectedStandardDeviation = 0.0
    var summary = ""
    for (round in 1..DEFAULT_TEST_ROUND_NUMBER) {
      val executionResult =
        mcSimulator.executeReachOnly(
          round.toString(),
          DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
        )
      val honestMajorityShareShuffleMethodology =
        HonestMajorityShareShuffleMethodology(
          frequencyVectorSize =
            executionResult.actualResult.reach.honestMajorityShareShuffle.frequencyVectorSize
        )

      if (expectedReach == -1L) {
        expectedReach = executionResult.expectedResult.reach.value
        val expectedVariance =
          getReachVariance(
            honestMajorityShareShuffleMethodology,
            executionResult.measurementInfo,
            expectedReach,
          )
        expectedStandardDeviation = sqrt(expectedVariance)
      } else if (expectedReach != executionResult.expectedResult.reach.value) {
        logger.log(
          Level.WARNING,
          "expected result not consistent. round=$round, prev_expected_result=$expectedReach, " +
            "current_expected_result=${executionResult.expectedResult.reach.value}",
        )
      }

      // The general formula for confidence interval is result +/- multiplier * sqrt(variance).
      // The multiplier for 95% confidence interval is 1.96.
      val reach = executionResult.actualResult.reach.value
      val reachVariance =
        getReachVariance(
          honestMajorityShareShuffleMethodology,
          executionResult.measurementInfo,
          reach,
        )
      val intervalLowerBound = reach - sqrt(reachVariance) * MULTIPLIER
      val intervalUpperBound = reach + sqrt(reachVariance) * MULTIPLIER
      val withinInterval = reach >= intervalLowerBound && reach <= intervalUpperBound

      val reachResult =
        ReachResult(reach, expectedReach, intervalLowerBound, intervalUpperBound, withinInterval)
      reachResults += reachResult

      val message =
        "round=$round, actual_result=${reachResult.actualReach}, " +
          "expected_result=${reachResult.expectedReach}, " +
          "interval=(${"%.2f".format(reachResult.lowerBound)}, " +
          "${"%.2f".format(reachResult.upperBound)}), accurate=${reachResult.withinInterval}"
      summary += message + "\n"
      logger.log(Level.INFO, message)
    }

    logger.log(Level.INFO, "Accuracy Test Complete.\n$summary")

    val averageReach = reachResults.map { it.actualReach }.average()
    val withinIntervalNumber = reachResults.map { if (it.withinInterval) 1 else 0 }.sum()
    val withinIntervalPercentage = withinIntervalNumber.toDouble() / reachResults.size * 100
    val offsetPercentage = (averageReach - expectedReach) / expectedReach * 100
    val averageDispersionRatio =
      abs(averageReach - expectedReach) * sqrt(DEFAULT_TEST_ROUND_NUMBER.toDouble()) /
        expectedStandardDeviation

    logger.log(
      Level.INFO,
      "average_reach=$averageReach, offset_percentage=${"%.2f".format(offsetPercentage)}%, " +
        "number_of_rounds_within_interval=$withinIntervalNumber out of $DEFAULT_TEST_ROUND_NUMBER " +
        "(${"%.2f".format(withinIntervalPercentage)}%) ",
    )

    val standardDeviation = getStandardDeviation(reachResults.map { it.actualReach.toDouble() })
    logger.log(
      Level.INFO,
      "std=${"%.2f".format(standardDeviation)}, " +
        "expected_std=${"%.2f".format(expectedStandardDeviation)}, " +
        "ratio=${"%.2f".format(standardDeviation / expectedStandardDeviation)}",
    )

    assertThat(withinIntervalPercentage).isAtLeast(COVERAGE_TEST_THRESHOLD)
    assertThat(averageDispersionRatio).isLessThan(AVERAGE_TEST_THRESHOLD)
    assertThat(standardDeviation)
      .isGreaterThan(expectedStandardDeviation * STANDARD_DEVIATION_TEST_LOWER_THRESHOLD)
    assertThat(standardDeviation)
      .isLessThan(expectedStandardDeviation * STANDARD_DEVIATION_TEST_UPPER_THRESHOLD)
  }

  @Test
  fun `reach from reach-and-frequency hmss results should be accurate with respect to the variance`() =
    runBlocking {
      val reachResults = mutableListOf<ReachResult>()
      var expectedReach = -1L
      var expectedStandardDeviation = 0.0
      var summary = ""
      for (round in 1..DEFAULT_TEST_ROUND_NUMBER) {
        val executionResult =
          mcSimulator.executeReachAndFrequency(
            round.toString(),
            DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
          )
        val honestMajorityShareShuffleMethodology =
          HonestMajorityShareShuffleMethodology(
            frequencyVectorSize =
              executionResult.actualResult.reach.honestMajorityShareShuffle.frequencyVectorSize
          )

        if (expectedReach == -1L) {
          expectedReach = executionResult.expectedResult.reach.value
          val expectedVariance =
            getReachVariance(
              honestMajorityShareShuffleMethodology,
              executionResult.measurementInfo,
              expectedReach,
            )
          expectedStandardDeviation = sqrt(expectedVariance)
        } else if (expectedReach != executionResult.expectedResult.reach.value) {
          logger.log(
            Level.WARNING,
            "expected result not consistent. round=$round, prev_expected_result=$expectedReach, " +
              "current_expected_result=${executionResult.expectedResult.reach.value}",
          )
        }

        // The general formula for confidence interval is result +/- multiplier * sqrt(variance).
        // The multiplier for 95% confidence interval is 1.96.
        val reach = executionResult.actualResult.reach.value
        val reachVariance =
          getReachVariance(
            honestMajorityShareShuffleMethodology,
            executionResult.measurementInfo,
            reach,
          )
        val intervalLowerBound = reach - sqrt(reachVariance) * MULTIPLIER
        val intervalUpperBound = reach + sqrt(reachVariance) * MULTIPLIER
        val withinInterval = reach >= intervalLowerBound && reach <= intervalUpperBound

        val reachResult =
          ReachResult(reach, expectedReach, intervalLowerBound, intervalUpperBound, withinInterval)
        reachResults += reachResult

        val message =
          "round=$round, actual_result=${reachResult.actualReach}, " +
            "expected_result=${reachResult.expectedReach}, " +
            "interval=(${"%.2f".format(reachResult.lowerBound)}, " +
            "${"%.2f".format(reachResult.upperBound)}), accurate=${reachResult.withinInterval}"
        summary += message + "\n"
        logger.log(Level.INFO, message)
      }

      logger.log(Level.INFO, "Accuracy Test Complete.\n$summary")

      val averageReach = reachResults.map { it.actualReach }.average()
      val withinIntervalNumber = reachResults.map { if (it.withinInterval) 1 else 0 }.sum()
      val withinIntervalPercentage = withinIntervalNumber.toDouble() / reachResults.size * 100
      val offsetPercentage = (averageReach - expectedReach) / expectedReach * 100
      val averageDispersionRatio =
        abs(averageReach - expectedReach) * sqrt(DEFAULT_TEST_ROUND_NUMBER.toDouble()) /
          expectedStandardDeviation

      logger.log(
        Level.INFO,
        "average_reach=$averageReach, offset_percentage=${"%.2f".format(offsetPercentage)}%, " +
          "number_of_rounds_within_interval=$withinIntervalNumber out of $DEFAULT_TEST_ROUND_NUMBER " +
          "(${"%.2f".format(withinIntervalPercentage)}%) ",
      )

      val standardDeviation = getStandardDeviation(reachResults.map { it.actualReach.toDouble() })
      logger.log(
        Level.INFO,
        "std=${"%.2f".format(standardDeviation)}, " +
          "expected_std=${"%.2f".format(expectedStandardDeviation)}, " +
          "ratio=${"%.2f".format(standardDeviation / expectedStandardDeviation)}",
      )

      assertThat(withinIntervalPercentage).isAtLeast(COVERAGE_TEST_THRESHOLD)
      assertThat(averageDispersionRatio).isLessThan(AVERAGE_TEST_THRESHOLD)
      assertThat(standardDeviation)
        .isGreaterThan(expectedStandardDeviation * STANDARD_DEVIATION_TEST_LOWER_THRESHOLD)
      assertThat(standardDeviation)
        .isLessThan(expectedStandardDeviation * STANDARD_DEVIATION_TEST_UPPER_THRESHOLD)
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val SYNTHETIC_POPULATION_SPEC = SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL
    private val SYNTHETIC_EVENT_GROUP_SPECS =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL_36K

    private const val DEFAULT_TEST_ROUND_NUMBER = 30
    // Multiplier for 95% confidence interval
    private const val MULTIPLIER = 1.96

    private const val COVERAGE_TEST_THRESHOLD = 80
    private const val AVERAGE_TEST_THRESHOLD = 2.58
    private const val STANDARD_DEVIATION_TEST_LOWER_THRESHOLD = 0.67
    private const val STANDARD_DEVIATION_TEST_UPPER_THRESHOLD = 1.35
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.0033
      delta = 0.00001
    }

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
