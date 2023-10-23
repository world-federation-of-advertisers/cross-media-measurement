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

import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.abs
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
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
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator.MeasurementInfo
import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsSketchMethodology
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
        String,
        ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
      ) -> InProcessDuchy.DuchyDependencies
    >,
) {

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(
      kingdomDataServicesRule,
      duchyDependenciesRule,
      SYNTHETIC_EVENT_GROUP_SPECS
    )

  private lateinit var mcSimulator: MeasurementConsumerSimulator

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
    val eventQuery =
      MetadataSyntheticGeneratorEventQuery(
        SyntheticGenerationSpecs.POPULATION_SPEC,
        InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY
      )
    mcSimulator =
      MeasurementConsumerSimulator(
        MeasurementConsumerData(
          measurementConsumerData.name,
          InProcessCmmsComponents.MC_ENTITY_CONTENT.signingKey,
          InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
          measurementConsumerData.apiAuthenticationKey
        ),
        OUTPUT_DP_PARAMS,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicMeasurementConsumersClient,
        publicCertificatesClient,
        RESULT_POLLING_DELAY,
        InProcessCmmsComponents.TRUSTED_CERTIFICATES,
        eventQuery,
        NoiseMechanism.CONTINUOUS_GAUSSIAN
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

  private fun getReachVariance(measurementInfo: MeasurementInfo, reach: Long): Double {
    val liquidLegionsSketchMethodology =
      LiquidLegionsSketchMethodology(
        RoLlv2ProtocolConfig.protocolConfig.sketchParams.decayRate,
        RoLlv2ProtocolConfig.protocolConfig.sketchParams.maxSize,
      )
    val reachMeasurementParams =
      ReachMeasurementParams(
        StatsVidSamplingInterval(
          measurementInfo.measurementSpec.vidSamplingInterval.start.toDouble(),
          measurementInfo.measurementSpec.vidSamplingInterval.width.toDouble()
        ),
        DpParams(OUTPUT_DP_PARAMS.epsilon, OUTPUT_DP_PARAMS.delta),
        StatsNoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)
    return computeMeasurementVariance(
      liquidLegionsSketchMethodology,
      reachMeasurementVarianceParams
    )
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

    var summary = ""
    for (round in 1..DEFAULT_TEST_ROUND_NUMBER) {
      val executionResult = mcSimulator.executeReachOnly(round.toString())

      if (expectedReach == -1L) {
        expectedReach = executionResult.expectedResult.reach.value
        val expectedVariance = getReachVariance(executionResult.measurementInfo, expectedReach)
        expectedStandardDeviation = sqrt(expectedVariance)
      } else if (expectedReach != executionResult.expectedResult.reach.value) {
        logger.log(
          Level.WARNING,
          "expected result not consistent. round=$round, prev_expected_result=$expectedReach, " +
            "current_expected_result=${executionResult.expectedResult.reach.value}"
        )
      }

      // The general formula for confidence interval is result +/- multiplier * sqrt(variance).
      // The multiplier for 95% confidence interval is 1.96.
      val reach = executionResult.actualResult.reach.value
      val reachVariance = getReachVariance(executionResult.measurementInfo, reach)
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
        "(${"%.2f".format(withinIntervalPercentage)}%) "
    )

    val standardDeviation = getStandardDeviation(reachResults.map { it.actualReach.toDouble() })
    val variance = standardDeviation.pow(2.0)
    val standardDeviationOffset =
      abs(standardDeviation - expectedStandardDeviation) / expectedStandardDeviation
    logger.log(
      Level.INFO,
      "standard_deviation=${"%.2f".format(standardDeviation)}, " +
        "expected_standard_deviation=${"%.2f".format(expectedStandardDeviation)}, " +
        "offset=${"%.2f".format(standardDeviationOffset * 100)}%"
    )

    assertTrue(withinIntervalPercentage >= COVERAGE_TEST_THRESHOLD)
    assertTrue(averageDispersionRatio < AVERAGE_TEST_THRESHOLD)
    val expectedVariance = expectedStandardDeviation.pow(2.0)
    assertTrue(
      (variance > expectedVariance * VARIANCE_TEST_LOWER_THRESHOLD) and
        (variance < expectedVariance * VARIANCE_TEST_UPPER_THRESHOLD)
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val SYNTHETIC_EVENT_GROUP_SPECS = SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_2M

    private const val DEFAULT_TEST_ROUND_NUMBER = 30
    // Multiplier for 95% confidence interval
    private const val MULTIPLIER = 1.96
    private const val CONTRIBUTOR_COUNT = 1

    private const val COVERAGE_TEST_THRESHOLD = 90
    private const val AVERAGE_TEST_THRESHOLD = 4
    private const val VARIANCE_TEST_LOWER_THRESHOLD = 0.5
    private const val VARIANCE_TEST_UPPER_THRESHOLD = 1.5
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.0033
      delta = 0.00001
    }
    private val RESULT_POLLING_DELAY = Duration.ofSeconds(10)

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
