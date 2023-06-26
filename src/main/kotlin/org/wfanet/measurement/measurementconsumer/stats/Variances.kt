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
import kotlin.math.pow
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter

/** Contains functions that compute different variances. */
object Variances {
  /**
   * Computes the variance of reach that is computed using the deterministic count distinct
   * methodology.
   */
  fun computeDeterministicVariance(params: ReachVarianceParams): Double {
    return computeDeterministicScalarMeasurementVariance(
      params.reach.toDouble(),
      params.measurementParams.vidSamplingIntervalWidth,
      params.measurementParams.dpParams,
      1.0,
      params.measurementParams.noiseMechanism
    )
  }

  /**
   * Computes the variance of impression that is computed using the deterministic count methodology.
   */
  fun computeDeterministicVariance(params: ImpressionVarianceParams): Double {
    return computeDeterministicScalarMeasurementVariance(
      params.impression.toDouble(),
      params.measurementParams.vidSamplingIntervalWidth,
      params.measurementParams.dpParams,
      params.measurementParams.maximumFrequencyPerUser.toDouble(),
      params.measurementParams.noiseMechanism
    )
  }

  /**
   * Computes the variance of watch duration that is computed using the deterministic sum
   * methodology.
   */
  fun computeDeterministicVariance(params: WatchDurationVarianceParams): Double {
    return computeDeterministicScalarMeasurementVariance(
      params.duration,
      params.measurementParams.vidSamplingIntervalWidth,
      params.measurementParams.dpParams,
      params.measurementParams.maximumDurationPerUser,
      params.measurementParams.noiseMechanism
    )
  }

  /**
   * Computes [FrequencyVariances] of reach-and-frequency that is computed using the deterministic
   * distribution methodology.
   *
   * Note that the reach can be computed using any methodology.
   */
  fun computeDeterministicVariance(params: FrequencyVarianceParams): FrequencyVariances {
    return frequencyVariance(
      params,
      ::deterministicFrequencyRelativeVariance,
      ::deterministicFrequencyCountVariance
    )
  }

  /**
   * Outputs the variance of the given [reachRatio] at a certain frequency computed from the
   * deterministic distribution methodology.
   *
   * Different types of frequency histograms have different values of [multiplier].
   */
  private fun deterministicFrequencyRelativeVariance(
    totalReach: Int,
    reachRatio: Double,
    measurementParams: FrequencyMeasurementParams,
    multiplier: Int
  ): Double {
    val frequencyNoiseVariance: Double =
      computeNoiseVariance(measurementParams.dpParams, measurementParams.noiseMechanism)
    val varPart1 =
      reachRatio * (1.0 - reachRatio) * (1.0 - measurementParams.vidSamplingIntervalWidth) /
        (totalReach * measurementParams.vidSamplingIntervalWidth)
    var varPart2 = (1.0 - 2.0 * reachRatio) * multiplier
    varPart2 += reachRatio.pow(2) * measurementParams.maximumFrequency
    varPart2 *=
      frequencyNoiseVariance / (totalReach * measurementParams.vidSamplingIntervalWidth).pow(2)
    return max(0.0, varPart1 + varPart2)
  }

  /**
   * Outputs the variance of the given reach count at a certain frequency and the reach ratio
   * computed using the deterministic distribution methodology.
   *
   * Reach count = [totalReach] * [reachRatio]
   */
  private fun deterministicFrequencyCountVariance(
    totalReach: Int,
    totalReachVariance: Double,
    reachRatio: Double,
    reachRatioVariance: Double,
  ): Double {
    val variance =
      reachRatioVariance * totalReachVariance +
        reachRatioVariance * totalReach.toDouble().pow(2) +
        totalReachVariance * reachRatio.pow(2)
    return max(0.0, variance)
  }

  /** Computes the variance of a scalar measurement result. */
  private fun computeDeterministicScalarMeasurementVariance(
    measurementValue: Double,
    vidSamplingIntervalWidth: Double,
    dpParams: DpParams,
    maximumFrequencyPerUser: Double,
    noiseMechanism: NoiseMechanism,
  ): Double {
    if (measurementValue < 0.0) {
      throw IllegalArgumentException("The scalar measurement value cannot be negative.")
    }
    val noiseVariance: Double = computeNoiseVariance(dpParams, noiseMechanism)
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
  fun computeLiquidLegionsSketchVariance(
    sketchParams: LiquidLegionsSketchParams,
    varianceParams: ReachVarianceParams,
  ): Double {
    val noiseVariance: Double =
      computeNoiseVariance(
        varianceParams.measurementParams.dpParams,
        varianceParams.measurementParams.noiseMechanism
      )

    val variance =
      LiquidLegions.inflatedReachCovariance(
        sketchParams,
        varianceParams.reach,
        varianceParams.reach,
        varianceParams.reach,
        varianceParams.measurementParams.vidSamplingIntervalWidth,
        varianceParams.measurementParams.vidSamplingIntervalWidth,
        varianceParams.measurementParams.vidSamplingIntervalWidth,
        noiseVariance
      )

    return max(0.0, variance)
  }

  /**
   * Computes [FrequencyVariances] of reach-and-frequency that is computed using the Liquid Legions
   * distribution methodology.
   *
   * Note that the reach can be computed using any methodology.
   */
  fun computeLiquidLegionsSketchVariance(
    sketchParams: LiquidLegionsSketchParams,
    params: FrequencyVarianceParams
  ): FrequencyVariances {
    return frequencyVariance(
      params,
      constructLiquidLegionsSketchFrequencyRelativeVariance(sketchParams, params.measurementParams),
      LiquidLegions::liquidLegionsFrequencyCountVariance
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
    totalReach: Int,
    reachRatio: Double,
    measurementParams: FrequencyMeasurementParams,
    multiplier: Int
  ) -> Double {
    val frequencyNoiseVariance: Double =
      computeNoiseVariance(measurementParams.dpParams, measurementParams.noiseMechanism)
    return { tr, rr, freqParams, m ->
      LiquidLegions.liquidLegionsFrequencyRelativeVariance(
        sketchParams,
        true,
        frequencyNoiseVariance,
        tr,
        rr,
        freqParams,
        m
      )
    }
  }

  /** Computes the variance of a reach measurement which is computed using Liquid Legions V2. */
  fun computeLiquidLegionsV2Variance(
    sketchParams: LiquidLegionsSketchParams,
    varianceParams: ReachVarianceParams,
  ): Double {
    val distributedGaussianNoiseVariance: Double =
      computeDistributedNoiseVariance(
        varianceParams.measurementParams.dpParams,
        varianceParams.measurementParams.noiseMechanism
      )

    val variance =
      LiquidLegions.inflatedReachCovariance(
        sketchParams,
        varianceParams.reach,
        varianceParams.reach,
        varianceParams.reach,
        varianceParams.measurementParams.vidSamplingIntervalWidth,
        varianceParams.measurementParams.vidSamplingIntervalWidth,
        varianceParams.measurementParams.vidSamplingIntervalWidth,
        distributedGaussianNoiseVariance
      )

    return max(0.0, variance)
  }

  /**
   * Computes [FrequencyVariances] of reach-and-frequency that is computed using the Liquid Legions
   * V2.
   */
  fun computeLiquidLegionsV2Variance(
    sketchParams: LiquidLegionsSketchParams,
    params: FrequencyVarianceParams
  ): FrequencyVariances {
    return frequencyVariance(
      params,
      constructLiquidLegionsV2FrequencyRelativeVariance(sketchParams, params.measurementParams),
      LiquidLegions::liquidLegionsFrequencyCountVariance
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
    totalReach: Int,
    reachRatio: Double,
    measurementParams: FrequencyMeasurementParams,
    multiplier: Int
  ) -> Double {
    val frequencyNoiseVariance: Double =
      computeDistributedNoiseVariance(measurementParams.dpParams, measurementParams.noiseMechanism)
    return { tr, rr, freqParams, m ->
      LiquidLegions.liquidLegionsFrequencyRelativeVariance(
        sketchParams,
        false,
        frequencyNoiseVariance,
        tr,
        rr,
        freqParams,
        m
      )
    }
  }

  /** Computes the noise variance based on the [DpParams] and the [NoiseMechanism]. */
  private fun computeNoiseVariance(
    dpParams: DpParams,
    noiseMechanism: NoiseMechanism,
  ): Double {
    return when (noiseMechanism) {
      NoiseMechanism.NONE -> 0.0
      NoiseMechanism.LAPLACE -> {
        LaplaceNoiser.computeVariance(dpParams)
      }
      NoiseMechanism.GAUSSIAN -> {
        GaussianNoiser.getSigma(dpParams).pow(2)
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
        AcdpParamsConverter.computeLlv2SigmaDistributedDiscreteGaussian(dpParams, 1).pow(2)
      }
    }
  }

  /** Common function that computes [FrequencyVariances]. */
  private fun frequencyVariance(
    params: FrequencyVarianceParams,
    frequencyRelativeVarianceFun:
      (
        totalReach: Int,
        reachRatio: Double,
        measurementParams: FrequencyMeasurementParams,
        multiplier: Int
      ) -> Double,
    frequencyCountVarianceFun:
      (
        totalReach: Int,
        totalReachVariance: Double,
        reachRatio: Double,
        reachRatioVariance: Double,
      ) -> Double
  ): FrequencyVariances {
    if (params.totalReach < 0.0) {
      throw IllegalArgumentException("The total reach value cannot be negative.")
    }
    if (params.reachVariance < 0.0) {
      throw IllegalArgumentException("The reach variance value cannot be negative.")
    }

    val maximumFrequency = params.measurementParams.maximumFrequency

    var suffixSum = 0.0
    // There is no estimate of zero-frequency reach
    val kPlusRelativeFrequencyDistribution: Map<Int, Double> =
      (maximumFrequency downTo 1).associateWith { frequency ->
        suffixSum += params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0)
        suffixSum
      }

    val relativeVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyRelativeVarianceFun(
          params.totalReach,
          params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
          params.measurementParams,
          1
        )
      }

    val kPlusRelativeVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyRelativeVarianceFun(
          params.totalReach,
          kPlusRelativeFrequencyDistribution.getValue(frequency),
          params.measurementParams,
          maximumFrequency - frequency + 1
        )
      }

    val countVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyCountVarianceFun(
          params.totalReach,
          params.reachVariance,
          params.relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
          relativeVariances.getValue(frequency)
        )
      }

    val kPlusCountVariances: Map<Int, Double> =
      (1..maximumFrequency).associateWith { frequency ->
        frequencyCountVarianceFun(
          params.totalReach,
          params.reachVariance,
          kPlusRelativeFrequencyDistribution.getValue(frequency),
          kPlusRelativeVariances.getValue(frequency)
        )
      }

    return FrequencyVariances(
      relativeVariances,
      kPlusRelativeVariances,
      countVariances,
      kPlusCountVariances
    )
  }
}
