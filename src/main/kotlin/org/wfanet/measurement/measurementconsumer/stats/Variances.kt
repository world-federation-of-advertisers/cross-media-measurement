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

package org.wfanet.measurement.measurementconsumer.stats

import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random
import kotlin.random.asJavaRandom
import org.apache.commons.numbers.core.Precision
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter

/** The interface of Variance calculations. */
interface Variances {
  /** Computes variance of a reach metric. */
  fun computeMetricVariance(params: ReachMetricVarianceParams): Double

  /** Computes variance of a reach measurement based on the methodology. */
  fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: ReachMeasurementVarianceParams,
  ): Double

  /**
   * Computes variance of a frequency metric.
   *
   * Currently, only support variance of frequency metrics that are computed on union-only set
   * expression. That is, metrics that are composed of single source measurement.
   */
  fun computeMetricVariance(params: FrequencyMetricVarianceParams): FrequencyVariances

  /** Computes variance of a frequency measurement based on the methodology. */
  fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: FrequencyMeasurementVarianceParams,
  ): FrequencyVariances

  /**
   * Computes variance of an impression metric.
   *
   * Currently, only support variance of impression metrics that are computed on union-only set
   * expression. That is, metrics that are composed of single source measurement.
   */
  fun computeMetricVariance(params: ImpressionMetricVarianceParams): Double

  /** Computes variance of an impression measurement based on the methodology. */
  fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: ImpressionMeasurementVarianceParams,
  ): Double

  /**
   * Computes variance of a watch duration metric.
   *
   * Currently, only support variance of watch duration metrics that are computed on union-only set
   * expression. That is, metrics that are composed of single source measurement.
   */
  fun computeMetricVariance(params: WatchDurationMetricVarianceParams): Double

  /** Computes variance of a watch duration measurement based on the methodology. */
  fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: WatchDurationMeasurementVarianceParams,
  ): Double
}

/** Default implementation of [Variances]. */
object VariancesImpl : Variances {
  private const val TOLERANCE = 1E-6
  private val EQUIVALENCE = Precision.doubleEquivalenceOfEpsilon(TOLERANCE)

  /**
   * Computes the variance of a reach measurement that is computed using the deterministic count
   * distinct methodology.
   */
  private fun computeDeterministicVariance(params: ReachMeasurementVarianceParams): Double {
    return computeDeterministicScalarMeasurementVariance(
      params.reach.toDouble(),
      params.measurementParams.vidSamplingInterval.width,
      params.measurementParams.dpParams,
      1.0,
      params.measurementParams.noiseMechanism,
    )
  }

  /**
   * Computes the variance of an impression measurement that is computed using the deterministic
   * count methodology.
   */
  private fun computeDeterministicVariance(params: ImpressionMeasurementVarianceParams): Double {
    return computeDeterministicScalarMeasurementVariance(
      params.impression.toDouble(),
      params.measurementParams.vidSamplingInterval.width,
      params.measurementParams.dpParams,
      params.measurementParams.maximumFrequencyPerUser.toDouble(),
      params.measurementParams.noiseMechanism,
    )
  }

  /**
   * Computes the variance of a watch duration measurement that is computed using the deterministic
   * sum methodology.
   */
  private fun computeDeterministicVariance(params: WatchDurationMeasurementVarianceParams): Double {
    return computeDeterministicScalarMeasurementVariance(
      params.duration,
      params.measurementParams.vidSamplingInterval.width,
      params.measurementParams.dpParams,
      params.measurementParams.maximumDurationPerUser,
      params.measurementParams.noiseMechanism,
    )
  }

  /**
   * Computes [FrequencyVariances] of a reach-and-frequency measurement that is computed using the
   * deterministic distribution methodology.
   *
   * Note that the reach measurement can be computed using any methodology.
   */
  private fun computeDeterministicVariance(
    params: FrequencyMeasurementVarianceParams
  ): FrequencyVariances {
    return frequencyVariance(
      params,
      ::deterministicFrequencyRelativeVariance,
      ::frequencyCountVariance,
    )
  }

  /**
   * Outputs the variance of the given [reachRatio] at a certain frequency computed from the
   * deterministic distribution methodology.
   *
   * Different types of frequency histograms have different values of [multiplier].
   */
  private fun deterministicFrequencyRelativeVariance(
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams
  ): Double {

    val (
      totalReach: Long,
      reachMeasurementVariance: Double,
      reachRatio: Double,
      measurementParams: FrequencyMeasurementParams,
      multiplier: Int) =
      relativeFrequencyMeasurementVarianceParams

    require(totalReach >= 0) { "Total reach must be non-negative, but got $totalReach." }
    require(reachRatio >= 0.0 && reachRatio <= 1.0) {
      "Reach ratio must be greater than or equal to 0 and less than or equal to 1, but got " +
        "$reachRatio."
    }

    // When reach is too small, we have little info to estimate frequency, and thus the estimate of
    // relative frequency is equivalent to a uniformly random guess at probability.
    if (
      isReachTooSmallForComputingRelativeFrequencyVariance(totalReach, reachMeasurementVariance)
    ) {
      return if (measurementParams.maximumFrequency == multiplier) 0.0
      else VARIANCE_OF_UNIFORMLY_RANDOM_PROBABILITY
    }

    val frequencyNoiseVariance: Double =
      computeDirectNoiseVariance(measurementParams.dpParams, measurementParams.noiseMechanism)
    val varPart1 =
      reachRatio * (1.0 - reachRatio) * (1.0 - measurementParams.vidSamplingInterval.width) /
        (totalReach * measurementParams.vidSamplingInterval.width)
    var varPart2 = (1.0 - 2.0 * reachRatio) * multiplier
    varPart2 += reachRatio.pow(2) * measurementParams.maximumFrequency
    varPart2 *=
      frequencyNoiseVariance / (totalReach * measurementParams.vidSamplingInterval.width).pow(2)
    return max(0.0, varPart1 + varPart2)
  }

  /**
   * Outputs the variance of the reach count and the reach ratio at a certain frequency.
   *
   * Reach count = [totalReach] * [reachRatio]
   */
  private fun frequencyCountVariance(
    totalReach: Long,
    totalReachVariance: Double,
    reachRatio: Double,
    reachRatioVariance: Double,
  ): Double {
    require(totalReach >= 0) { "Total reach must be non-negative, but got $totalReach." }
    require(totalReachVariance >= 0) {
      "Total reach variance must not be negative, but got $totalReachVariance."
    }
    require(reachRatio >= 0.0 && reachRatio <= 1.0) {
      "Reach ratio must be greater than or equal to 0 and less than or equal to 1, but got " +
        "$reachRatio."
    }
    require(reachRatioVariance >= 0) {
      "Reach ratio variance must not be negative, but got $reachRatioVariance."
    }
    val variance =
      reachRatioVariance * totalReachVariance +
        reachRatioVariance * totalReach.toDouble().pow(2) +
        totalReachVariance * reachRatio.pow(2)
    return max(0.0, variance)
  }

  /**
   * Computes the variance of a scalar measurement result that is deterministic methodology based.
   */
  private fun computeDeterministicScalarMeasurementVariance(
    measurementValue: Double,
    vidSamplingIntervalWidth: Double,
    dpParams: DpParams,
    maximumFrequencyPerUser: Double,
    noiseMechanism: NoiseMechanism,
  ): Double {
    require(measurementValue >= 0.0) {
      "The scalar measurement value ($measurementValue) cannot be negative."
    }
    val noiseVariance: Double = computeDirectNoiseVariance(dpParams, noiseMechanism)
    val variance =
      (maximumFrequencyPerUser *
        measurementValue *
        vidSamplingIntervalWidth *
        (1 - vidSamplingIntervalWidth) + maximumFrequencyPerUser.pow(2) * noiseVariance) /
        vidSamplingIntervalWidth.pow(2.0)

    return max(0.0, variance)
  }

  /**
   * Computes the variance of a reach measurement which is computed using the Liquid Legions Count
   * Distinct methodology.
   */
  private fun computeLiquidLegionsSketchVariance(
    sketchParams: LiquidLegionsSketchParams,
    varianceParams: ReachMeasurementVarianceParams,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    val noiseVariance: Double =
      computeDirectNoiseVariance(
        varianceParams.measurementParams.dpParams,
        varianceParams.measurementParams.noiseMechanism,
      )

    val variance =
      LiquidLegions.inflatedReachCovariance(
        sketchParams = sketchParams,
        reach = varianceParams.reach,
        otherReach = varianceParams.reach,
        overlapReach = varianceParams.reach,
        samplingWidth = varianceParams.measurementParams.vidSamplingInterval.width,
        otherSamplingWidth = varianceParams.measurementParams.vidSamplingInterval.width,
        overlapSamplingWidth = varianceParams.measurementParams.vidSamplingInterval.width,
        inflation = noiseVariance,
      )

    return max(0.0, variance)
  }

  /**
   * Computes [FrequencyVariances] of a reach-and-frequency measurement that is computed using the
   * Liquid Legions distribution methodology.
   *
   * Note that the reach can be computed using any methodology.
   */
  private fun computeLiquidLegionsSketchVariance(
    sketchParams: LiquidLegionsSketchParams,
    params: FrequencyMeasurementVarianceParams,
  ): FrequencyVariances {
    verifyLiquidLegionsSketchParams(sketchParams)
    return frequencyVariance(
      params,
      constructLiquidLegionsSketchFrequencyRelativeVariance(sketchParams, params.measurementParams),
      ::frequencyCountVariance,
    )
  }

  /**
   * Constructs a function object of [LiquidLegions.liquidLegionsFrequencyRelativeVariance] with the
   * custom [LiquidLegionsSketchParams] for Liquid Legions sketch.
   */
  private fun constructLiquidLegionsSketchFrequencyRelativeVariance(
    sketchParams: LiquidLegionsSketchParams,
    measurementParams: FrequencyMeasurementParams,
  ): (
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams
  ) -> Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    val frequencyNoiseVariance: Double =
      computeDirectNoiseVariance(measurementParams.dpParams, measurementParams.noiseMechanism)
    return { relativeFrequencyMeasurementVarianceParams ->
      LiquidLegions.liquidLegionsFrequencyRelativeVariance(
        sketchParams = sketchParams,
        collisionResolution = true,
        frequencyNoiseVariance = frequencyNoiseVariance,
        relativeFrequencyMeasurementVarianceParams = relativeFrequencyMeasurementVarianceParams,
      )
    }
  }

  /** Computes the variance of a reach measurement which is computed using Liquid Legions V2. */
  private fun computeLiquidLegionsV2Variance(
    sketchParams: LiquidLegionsSketchParams,
    varianceParams: ReachMeasurementVarianceParams,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    val distributedGaussianNoiseVariance: Double =
      computeDistributedNoiseVariance(
        varianceParams.measurementParams.dpParams,
        varianceParams.measurementParams.noiseMechanism,
      )
    require(distributedGaussianNoiseVariance >= 0.0) {
      "Distributed Gaussian noise variance must not be negative, but got " +
        "$distributedGaussianNoiseVariance."
    }

    val variance =
      LiquidLegions.inflatedReachCovariance(
        sketchParams = sketchParams,
        reach = varianceParams.reach,
        otherReach = varianceParams.reach,
        overlapReach = varianceParams.reach,
        samplingWidth = varianceParams.measurementParams.vidSamplingInterval.width,
        otherSamplingWidth = varianceParams.measurementParams.vidSamplingInterval.width,
        overlapSamplingWidth = varianceParams.measurementParams.vidSamplingInterval.width,
        inflation = distributedGaussianNoiseVariance,
      )

    return max(0.0, variance)
  }

  /**
   * Computes [FrequencyVariances] of a reach-and-frequency measurement that is computed using the
   * Liquid Legions V2.
   */
  private fun computeLiquidLegionsV2Variance(
    sketchParams: LiquidLegionsSketchParams,
    params: FrequencyMeasurementVarianceParams,
  ): FrequencyVariances {
    verifyLiquidLegionsSketchParams(sketchParams)
    return frequencyVariance(
      params,
      constructLiquidLegionsV2FrequencyRelativeVariance(sketchParams, params.measurementParams),
      ::frequencyCountVariance,
    )
  }

  /**
   * Constructs a function object of [LiquidLegions.liquidLegionsFrequencyRelativeVariance] with the
   * custom [LiquidLegionsSketchParams] for Liquid Legions V2.
   */
  private fun constructLiquidLegionsV2FrequencyRelativeVariance(
    sketchParams: LiquidLegionsSketchParams,
    measurementParams: FrequencyMeasurementParams,
  ): (
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams
  ) -> Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    val frequencyNoiseVariance: Double =
      computeDistributedNoiseVariance(measurementParams.dpParams, measurementParams.noiseMechanism)
    require(frequencyNoiseVariance >= 0.0) {
      "Frequency noise variance must not be negative, but got $frequencyNoiseVariance."
    }
    return { relativeFrequencyMeasurementVarianceParams ->
      LiquidLegions.liquidLegionsFrequencyRelativeVariance(
        sketchParams = sketchParams,
        collisionResolution = false,
        frequencyNoiseVariance = frequencyNoiseVariance,
        relativeFrequencyMeasurementVarianceParams = relativeFrequencyMeasurementVarianceParams,
      )
    }
  }

  /** Computes the noise variance based on the [DpParams] and the [NoiseMechanism]. */
  private fun computeDirectNoiseVariance(
    dpParams: DpParams,
    noiseMechanism: NoiseMechanism,
  ): Double {
    return when (noiseMechanism) {
      NoiseMechanism.NONE -> 0.0
      NoiseMechanism.LAPLACE -> {
        LaplaceNoiser(dpParams, Random.asJavaRandom()).variance
      }
      NoiseMechanism.GAUSSIAN -> {
        GaussianNoiser(dpParams, Random.asJavaRandom()).variance
      }
    }
  }

  /** Computes the noise variance based on the [DpParams] and the [NoiseMechanism]. */
  private fun computeDistributedNoiseVariance(
    dpParams: DpParams,
    noiseMechanism: NoiseMechanism,
  ): Double {
    return when (noiseMechanism) {
      NoiseMechanism.NONE -> 0.0
      NoiseMechanism.LAPLACE -> {
        error("Laplace is not supported for distributed noises.")
      }
      NoiseMechanism.GAUSSIAN -> {
        // By passing 1 to contributorCount, the function called below outputs the total distributed
        // sigma of the noiser as sigmaDistributed = sigma / sqrt(contributorCount).
        AcdpParamsConverter.computeMpcSigmaDistributedDiscreteGaussian(
            dpParams,
            contributorCount = 1,
          )
          .pow(2)
      }
    }
  }

  /** Common function that computes [FrequencyVariances]. */
  private fun frequencyVariance(
    params: FrequencyMeasurementVarianceParams,
    frequencyRelativeVarianceFun:
      (
        relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams
      ) -> Double,
    frequencyCountVarianceFun:
      (
        totalReach: Long, totalReachVariance: Double, reachRatio: Double, reachRatioVariance: Double,
      ) -> Double,
  ): FrequencyVariances {
    require(params.totalReach >= 0.0) {
      "The total reach value (${params.totalReach}) cannot be negative."
    }
    require(params.reachMeasurementVariance >= 0.0) {
      "The reach variance value (${params.reachMeasurementVariance}) cannot be negative."
    }

    val maximumFrequency = params.measurementParams.maximumFrequency

    var suffixSum = 0.0
    // There is no estimate of zero-frequency reach.
    val kPlusRelativeFrequencyDistribution: Map<Int, Double> =
      (maximumFrequency downTo 1).associateWith { frequency ->
        suffixSum += params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0)
        require(EQUIVALENCE.compare(suffixSum, 1.0) <= 0) {
          "kPlus relative frequency must not exceed 1, but got $suffixSum."
        }
        min(1.0, suffixSum)
      }

    val relativeVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyRelativeVarianceFun(
          RelativeFrequencyMeasurementVarianceParams(
            params.totalReach,
            params.reachMeasurementVariance,
            params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
            params.measurementParams,
            1,
          )
        )
      }

    val kPlusRelativeVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyRelativeVarianceFun(
          RelativeFrequencyMeasurementVarianceParams(
            params.totalReach,
            params.reachMeasurementVariance,
            kPlusRelativeFrequencyDistribution.getValue(frequency),
            params.measurementParams,
            maximumFrequency - frequency + 1,
          )
        )
      }

    val countVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyCountVarianceFun(
          params.totalReach,
          params.reachMeasurementVariance,
          params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
          relativeVariances.getValue(frequency),
        )
      }

    val kPlusCountVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyCountVarianceFun(
          params.totalReach,
          params.reachMeasurementVariance,
          kPlusRelativeFrequencyDistribution.getValue(frequency),
          kPlusRelativeVariances.getValue(frequency),
        )
      }

    return FrequencyVariances(
      relativeVariances,
      kPlusRelativeVariances,
      countVariances,
      kPlusCountVariances,
    )
  }

  /**
   * Computes [ReachVariance] of a reach-and-frequency measurement that is computed using the Honest
   * Majority Share Shuffle methodology.
   */
  private fun computeFrequencyVectorBasedVariance(
    frequencyVectorSize: Long,
    reachParams: ReachMeasurementVarianceParams,
  ): Double {
    val reachNoiseVariance: Double =
      computeDistributedNoiseVariance(
        reachParams.measurementParams.dpParams,
        reachParams.measurementParams.noiseMechanism,
      )

    val variance =
      FrequencyVectorBasedVariance.reachVariance(
        frequencyVectorSize = frequencyVectorSize,
        vidSamplingIntervalWidth = reachParams.measurementParams.vidSamplingInterval.width,
        reach = reachParams.reach,
        reachNoiseVariance = reachNoiseVariance,
      )
    return max(0.0, variance)
  }

  /**
   * Computes [FrequencyVariances] of a reach-and-frequency measurement that is computed using the
   * Honest Majority Share Shuffle methodology.
   */
  private fun computeFrequencyVectorBasedVariance(
    frequencyVectorSize: Long,
    frequencyParams: FrequencyMeasurementVarianceParams,
  ): FrequencyVariances {
    require(frequencyParams.totalReach >= 0.0) {
      "The total reach value (${frequencyParams.totalReach}) cannot be negative."
    }
    require(frequencyParams.reachMeasurementVariance >= 0.0) {
      "The reach variance value (${frequencyParams.reachMeasurementVariance}) cannot be negative."
    }

    val maximumFrequency = frequencyParams.measurementParams.maximumFrequency

    val frequencyNoiseVariance: Double =
      computeDistributedNoiseVariance(
        frequencyParams.measurementParams.dpParams,
        frequencyParams.measurementParams.noiseMechanism,
      )

    var suffixSum = 0.0
    // There is no estimate of zero-frequency reach
    val kPlusRelativeFrequencyDistribution: Map<Int, Double> =
      (maximumFrequency downTo 1).associateWith { frequency ->
        suffixSum += frequencyParams.relativeFrequencyDistribution.getOrDefault(frequency, 0.0)
        require(EQUIVALENCE.compare(suffixSum, 1.0) <= 0) {
          "kPlus relative frequency must not exceed 1, but got $suffixSum."
        }
        min(1.0, suffixSum)
      }

    val countVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        FrequencyVectorBasedVariance.frequencyCountVariance(
          frequencyVectorSize,
          frequency,
          frequencyNoiseVariance,
          RelativeFrequencyMeasurementVarianceParams(
            frequencyParams.totalReach,
            frequencyParams.reachMeasurementVariance,
            frequencyParams.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
            frequencyParams.measurementParams,
            0,
          ),
        )
      }

    val kPlusCountVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        FrequencyVectorBasedVariance.kPlusFrequencyCountVariance(
          frequencyVectorSize,
          frequency,
          frequencyNoiseVariance,
          RelativeFrequencyMeasurementVarianceParams(
            frequencyParams.totalReach,
            frequencyParams.reachMeasurementVariance,
            kPlusRelativeFrequencyDistribution.getValue(frequency),
            frequencyParams.measurementParams,
            0,
          ),
        )
      }

    val relativeVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        FrequencyVectorBasedVariance.frequencyRelativeVariance(
          frequencyVectorSize,
          frequency,
          frequencyNoiseVariance,
          RelativeFrequencyMeasurementVarianceParams(
            frequencyParams.totalReach,
            frequencyParams.reachMeasurementVariance,
            frequencyParams.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
            frequencyParams.measurementParams,
            0,
          ),
        )
      }

    val kPlusRelativeVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        FrequencyVectorBasedVariance.kPlusFrequencyRelativeVariance(
          frequencyVectorSize,
          frequency,
          frequencyNoiseVariance,
          RelativeFrequencyMeasurementVarianceParams(
            frequencyParams.totalReach,
            frequencyParams.reachMeasurementVariance,
            kPlusRelativeFrequencyDistribution.getValue(frequency),
            frequencyParams.measurementParams,
            0,
          ),
        )
      }

    return FrequencyVariances(
      relativeVariances,
      kPlusRelativeVariances,
      countVariances,
      kPlusCountVariances,
    )
  }

  /**
   * Common function that computes [FrequencyVariances] with known [relativeVariances] and
   * [kPlusRelativeVariances].
   */
  private fun frequencyVariance(
    params: FrequencyMeasurementVarianceParams,
    relativeVariances: Map<Int, Double>,
    kPlusRelativeVariances: Map<Int, Double>,
    frequencyCountVarianceFun:
      (
        totalReach: Long, totalReachVariance: Double, reachRatio: Double, reachRatioVariance: Double,
      ) -> Double,
  ): FrequencyVariances {
    require(params.totalReach >= 0.0) {
      "The total reach value (${params.totalReach}) cannot be negative."
    }
    require(params.reachMeasurementVariance >= 0.0) {
      "The reach variance value (${params.reachMeasurementVariance}) cannot be negative."
    }

    val maximumFrequency = params.measurementParams.maximumFrequency

    var suffixSum = 0.0
    // There is no estimate of zero-frequency reach
    val kPlusRelativeFrequencyDistribution: Map<Int, Double> =
      (maximumFrequency downTo 1).associateWith { frequency ->
        suffixSum += params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0)
        require(EQUIVALENCE.compare(suffixSum, 1.0) <= 0) {
          "kPlus relative frequency must not exceed 1, but got $suffixSum."
        }
        min(1.0, suffixSum)
      }

    val countVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyCountVarianceFun(
          params.totalReach,
          params.reachMeasurementVariance,
          params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
          relativeVariances.getValue(frequency),
        )
      }

    val kPlusCountVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyCountVarianceFun(
          params.totalReach,
          params.reachMeasurementVariance,
          kPlusRelativeFrequencyDistribution.getValue(frequency),
          kPlusRelativeVariances.getValue(frequency),
        )
      }

    return FrequencyVariances(
      relativeVariances,
      kPlusRelativeVariances,
      countVariances,
      kPlusCountVariances,
    )
  }

  /** Computes variance of a reach metric. */
  override fun computeMetricVariance(params: ReachMetricVarianceParams): Double {
    require(params.weightedMeasurementVarianceParamsList.isNotEmpty()) {
      "Invalid params: number of measurements must be greater than 0."
    }

    // Sum of weighted measurement variances = Sum_i a_i^2 * msmt_var_i
    var metricVariance: Double =
      params.weightedMeasurementVarianceParamsList.sumOf { weightedMeasurementVarianceParams ->
        weightedMeasurementVarianceParams.weight.square() *
          computeMeasurementVariance(
            weightedMeasurementVarianceParams.methodology,
            weightedMeasurementVarianceParams.measurementVarianceParams,
          )
      }

    val weightedMeasurementVarianceParamsMap =
      params.weightedMeasurementVarianceParamsList.associateBy { weightedMeasurementVarianceParams
        ->
        weightedMeasurementVarianceParams.binaryRepresentation
      }
    val numberMeasurements = params.weightedMeasurementVarianceParamsList.size

    // For every two distinct measurements in the list
    for (index in 0 until numberMeasurements) {
      for (otherIndex in index + 1 until numberMeasurements) {
        val weightedMeasurementVarianceParams = params.weightedMeasurementVarianceParamsList[index]
        val otherWeightedMeasurementVarianceParams =
          params.weightedMeasurementVarianceParamsList[otherIndex]
        val unionWeightedMeasurementVarianceParams =
          weightedMeasurementVarianceParamsMap.getValue(
            weightedMeasurementVarianceParams.binaryRepresentation or
              otherWeightedMeasurementVarianceParams.binaryRepresentation
          )

        // Add weighted measurement covariance = 2 * a_i * a_j * cov(msmt_i, msmt_j)
        metricVariance +=
          2 *
            weightedMeasurementVarianceParams.weight *
            otherWeightedMeasurementVarianceParams.weight *
            Covariances.computeMeasurementCovariance(
              weightedMeasurementVarianceParams,
              otherWeightedMeasurementVarianceParams,
              unionWeightedMeasurementVarianceParams,
            )
      }
    }

    return max(0.0, metricVariance)
  }

  /** Computes variance of a reach measurement based on the methodology. */
  override fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: ReachMeasurementVarianceParams,
  ): Double {
    return when (methodology) {
      is CustomDirectScalarMethodology -> {
        methodology.variance
      }
      is CustomDirectFrequencyMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Custom direct methodology for frequency cannot be used for reach."
        )
      }
      is DeterministicMethodology -> {
        computeDeterministicVariance(measurementVarianceParams)
      }
      is LiquidLegionsSketchMethodology -> {
        computeLiquidLegionsSketchVariance(
          LiquidLegionsSketchParams(methodology.decayRate, methodology.sketchSize),
          measurementVarianceParams,
        )
      }
      is LiquidLegionsV2Methodology -> {
        computeLiquidLegionsV2Variance(
          LiquidLegionsSketchParams(methodology.decayRate, methodology.sketchSize),
          measurementVarianceParams,
        )
      }
      is HonestMajorityShareShuffleMethodology -> {
        computeFrequencyVectorBasedVariance(
          methodology.frequencyVectorSize,
          measurementVarianceParams,
        )
      }
      is TrusTeeMethodology -> {
        computeFrequencyVectorBasedVariance(
          methodology.frequencyVectorSize,
          measurementVarianceParams,
        )
      }
    }
  }

  /**
   * Computes variance of a frequency metric.
   *
   * Currently, only support variance of frequency metrics that are computed on union-only set
   * expression. That is, metrics that are composed of single source measurement.
   */
  override fun computeMetricVariance(params: FrequencyMetricVarianceParams): FrequencyVariances {
    require(params.weightedMeasurementVarianceParamsList.isNotEmpty()) {
      "Invalid params: number of measurements must be greater than 0."
    }

    require(params.weightedMeasurementVarianceParamsList.size == 1) {
      "Only support variance calculation of frequency metrics computed on union-only set " +
        "expressions. Expected exactly 1 weighted measurement variance params, but got " +
        "${params.weightedMeasurementVarianceParamsList.size}."
    }

    val weightedMeasurementVarianceParams = params.weightedMeasurementVarianceParamsList.first()

    val coefficient = weightedMeasurementVarianceParams.weight.square()

    val frequencyVariances: FrequencyVariances =
      computeMeasurementVariance(
        weightedMeasurementVarianceParams.methodology,
        weightedMeasurementVarianceParams.measurementVarianceParams,
      )

    return FrequencyVariances(
      relativeVariances = frequencyVariances.relativeVariances.mapValues { coefficient * it.value },
      kPlusRelativeVariances =
        frequencyVariances.kPlusRelativeVariances.mapValues { coefficient * it.value },
      countVariances = frequencyVariances.countVariances.mapValues { coefficient * it.value },
      kPlusCountVariances =
        frequencyVariances.kPlusCountVariances.mapValues { coefficient * it.value },
    )
  }

  /** Computes variance of a frequency measurement based on the methodology. */
  override fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: FrequencyMeasurementVarianceParams,
  ): FrequencyVariances {
    return when (methodology) {
      is CustomDirectScalarMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Custom direct methodology for scalar cannot be used for frequency."
        )
      }
      is CustomDirectFrequencyMethodology -> {
        computeCustomDirectMethodologyVariance(methodology, measurementVarianceParams)
      }
      is DeterministicMethodology -> {
        computeDeterministicVariance(measurementVarianceParams)
      }
      is LiquidLegionsSketchMethodology -> {
        computeLiquidLegionsSketchVariance(
          LiquidLegionsSketchParams(methodology.decayRate, methodology.sketchSize),
          measurementVarianceParams,
        )
      }
      is LiquidLegionsV2Methodology -> {
        computeLiquidLegionsV2Variance(
          LiquidLegionsSketchParams(methodology.decayRate, methodology.sketchSize),
          measurementVarianceParams,
        )
      }
      is HonestMajorityShareShuffleMethodology -> {
        computeFrequencyVectorBasedVariance(
          methodology.frequencyVectorSize,
          measurementVarianceParams,
        )
      }
      is TrusTeeMethodology -> {
        computeFrequencyVectorBasedVariance(
          methodology.frequencyVectorSize,
          measurementVarianceParams,
        )
      }
    }
  }

  /**
   * Computes [FrequencyVariances] of a reach-and-frequency measurement where the frequency
   * distribution result is computed using a custom direct frequency methodology.
   *
   * Note that the reach can be computed using any methodology.
   */
  private fun computeCustomDirectMethodologyVariance(
    methodology: CustomDirectFrequencyMethodology,
    params: FrequencyMeasurementVarianceParams,
  ): FrequencyVariances {
    return frequencyVariance(
      params,
      methodology.relativeVariances,
      methodology.kPlusRelativeVariances,
      ::frequencyCountVariance,
    )
  }

  /**
   * Computes variance of an impression metric.
   *
   * Currently, only support variance of impression metrics that are computed on union-only set
   * expression. That is, metrics that are composed of single source measurement.
   */
  override fun computeMetricVariance(params: ImpressionMetricVarianceParams): Double {
    require(params.weightedMeasurementVarianceParamsList.isNotEmpty()) {
      "Invalid params: number of measurements must be greater than 0."
    }

    require(params.weightedMeasurementVarianceParamsList.size == 1) {
      "Only support variance calculation of impression metrics computed on union-only set " +
        "expressions. Expected exactly 1 weighted measurement variance params, but got " +
        "${params.weightedMeasurementVarianceParamsList.size}."
    }

    val weightedMeasurementVarianceParams = params.weightedMeasurementVarianceParamsList.first()

    return weightedMeasurementVarianceParams.weight.square() *
      computeMeasurementVariance(
        weightedMeasurementVarianceParams.methodology,
        weightedMeasurementVarianceParams.measurementVarianceParams,
      )
  }

  /** Computes variance of an impression measurement based on the methodology. */
  override fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: ImpressionMeasurementVarianceParams,
  ): Double {
    return when (methodology) {
      is CustomDirectScalarMethodology -> {
        methodology.variance
      }
      is CustomDirectFrequencyMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Custom direct methodology for frequency cannot be used for impression."
        )
      }
      is DeterministicMethodology -> {
        computeDeterministicVariance(measurementVarianceParams)
      }
      is LiquidLegionsSketchMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology LIQUID_LEGIONS_SKETCH is not supported for impression.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
      is LiquidLegionsV2Methodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology LIQUID_LEGIONS_V2 is not supported for impression.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
      is HonestMajorityShareShuffleMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology HONEST_MAJORITY_SHARE_SHUFFLE is not supported for impression.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
      is TrusTeeMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology HONEST_MAJORITY_SHARE_SHUFFLE is not supported for impression.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
    }
  }

  /**
   * Computes variance of a watch duration metric.
   *
   * Currently, only support variance of watch duration metrics that are computed on union-only set
   * expression. That is, metrics that are composed of single source measurement.
   */
  override fun computeMetricVariance(params: WatchDurationMetricVarianceParams): Double {
    require(params.weightedMeasurementVarianceParamsList.isNotEmpty()) {
      "Invalid params: number of measurements must be greater than 0."
    }

    require(params.weightedMeasurementVarianceParamsList.size == 1) {
      "Only support variance calculation of watch duration metrics computed on union-only set " +
        "expressions. Expected exactly 1 weighted measurement variance params, but got " +
        "${params.weightedMeasurementVarianceParamsList.size}."
    }

    val weightedMeasurementVarianceParams = params.weightedMeasurementVarianceParamsList.first()

    return weightedMeasurementVarianceParams.weight.square() *
      computeMeasurementVariance(
        weightedMeasurementVarianceParams.methodology,
        weightedMeasurementVarianceParams.measurementVarianceParams,
      )
  }

  /** Computes variance of a watch duration measurement based on the methodology. */
  override fun computeMeasurementVariance(
    methodology: Methodology,
    measurementVarianceParams: WatchDurationMeasurementVarianceParams,
  ): Double {
    return when (methodology) {
      is CustomDirectScalarMethodology -> {
        methodology.variance
      }
      is CustomDirectFrequencyMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Custom direct methodology for frequency cannot be used for watch duration."
        )
      }
      is DeterministicMethodology -> {
        computeDeterministicVariance(measurementVarianceParams)
      }
      is LiquidLegionsSketchMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology LIQUID_LEGIONS_SKETCH is not supported for watch duration.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
      is LiquidLegionsV2Methodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology LIQUID_LEGIONS_V2 is not supported for watch duration.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
      is HonestMajorityShareShuffleMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology HONEST_MAJORITY_SHARE_SHUFFLE is not supported for watch duration.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
      is TrusTeeMethodology -> {
        throw UnsupportedMethodologyUsageException(
          "Methodology HONEST_MAJORITY_SHARE_SHUFFLE is not supported for watch duration.",
          IllegalArgumentException("Invalid methodology"),
        )
      }
    }
  }
}

/** Outputs the square of an [Int] value. */
private fun Int.square(): Int {
  return this * this
}
