/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

/** The parameters that are used to compute Share Shuffle sketch. */
data class ShareShuffleSketchParams(val sketchSize: Long)

/** Functions to compute statistics of Share Shuffle sketch based measurements. */
object HonestMajorityShareShuffle {
  /** Calculates the variance of the reach. */
  fun reachVariance(
    sketchSize: Long,
    vidSamplingIntervalWidth: Double,
    reach: Long,
    reachNoiseVariance: Double,
  ): Double {
    val vidUniverseSize: Long = (sketchSize / vidSamplingIntervalWidth).toLong()
    require(vidUniverseSize > 0) { "Vid universe size must be positive." }
    if (vidUniverseSize == 1L) {
      return 0.0
    }
    require(vidSamplingIntervalWidth > 0.0 && vidSamplingIntervalWidth <= 1.0) {
      "Vid sampling width must be greater than 0 and less than or equal to 1."
    }
    require(reach <= vidUniverseSize) {
      "Reach must be less than or equal to the size of the Vid universe."
    }
    require(reachNoiseVariance >= 0) { "Reach noise variance must be a non-negative value." }

    val reachVariance =
      (vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth) *
        (vidUniverseSize - reach) *
        reach / (vidUniverseSize - 1) + reachNoiseVariance) / vidSamplingIntervalWidth.pow(2.0)

    return max(0.0, reachVariance)
  }

  /** Outputs the variance of the given [kReach]. */
  fun frequencyCountVariance(
    sketchParams: ShareShuffleSketchParams,
    frequencyNoiseVariance: Double,
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams,
  ): Double {
    val vidSamplingIntervalWidth =
      relativeFrequencyMeasurementVarianceParams.measurementParams.vidSamplingInterval.width
    val vidUniverseSize = (sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    val kReach =
      (relativeFrequencyMeasurementVarianceParams.reachRatio *
          relativeFrequencyMeasurementVarianceParams.totalReach)
        .toLong()

    var kReachVariance =
      if (vidUniverseSize == 1L) 0.0
      else
        (vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - kReach) *
          kReach / (vidUniverseSize - 1) + frequencyNoiseVariance) /
          vidSamplingIntervalWidth.pow(2.0)

    return max(0.0, kReachVariance)
  }

  /** Outputs the variance of the given [kPlusReach]. */
  fun kPlusFrequencyCountVariance(
    sketchParams: ShareShuffleSketchParams,
    frequencyNoiseVariance: Double,
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams,
  ): Double {
    val (
      totalReach: Long,
      reachMeasurementVariance: Double,
      kReachRatio: Double,
      frequencyMeasurementParams: FrequencyMeasurementParams,
    ) = relativeFrequencyMeasurementVarianceParams
    val vidSamplingIntervalWidth = frequencyMeasurementParams.vidSamplingInterval.width
    val vidUniverseSize = (sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    val kPlusReach = (kReachRatio * totalReach).toLong()

    // Gets the reach noise variance from the reach measurement variance and total reach.
    val reachNoiseVariance =
      reachMeasurementVariance * vidSamplingIntervalWidth.pow(2.0) -
        vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - totalReach) *
          totalReach / (vidUniverseSize - 1)

    val kPlusReachVariance =
      if (vidUniverseSize == 1L) 0.0
      else
        (vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - kPlusReach) *
          kPlusReach / (vidUniverseSize - 1) +
          relativeFrequencyMeasurementVarianceParams.multiplier * frequencyNoiseVariance +
          reachNoiseVariance) / vidSamplingIntervalWidth.pow(2.0)

    return max(0.0, kPlusReachVariance)
  }

  /**
   * Outputs the variance of the given [kReachRatio].
   *
   * kReachRatio = kReach / reach.
   */
  fun frequencyRelativeVariance(
    sketchParams: ShareShuffleSketchParams,
    frequencyNoiseVariance: Double,
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams,
  ): Double {
    val (
      totalReach: Long,
      reachMeasurementVariance: Double,
      kReachRatio: Double,
      frequencyMeasurementParams: FrequencyMeasurementParams,
    ) = relativeFrequencyMeasurementVarianceParams

    // When reach is too small, we have little info to estimate frequency, and thus the estimate of
    // relative frequency is equivalent to a uniformly random guess of a probability in [0, 1].
    if (
      isReachTooSmallForComputingRelativeFrequencyVariance(totalReach, reachMeasurementVariance)
    ) {
      return VARIANCE_OF_UNIFORMLY_RANDOM_PROBABILITY
    }

    val vidSamplingIntervalWidth = frequencyMeasurementParams.vidSamplingInterval.width
    val vidUniverseSize = (sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    val kReach = (kReachRatio * totalReach).toLong()

    var kReachVariance =
      frequencyCountVariance(
        sketchParams,
        frequencyNoiseVariance,
        relativeFrequencyMeasurementVarianceParams,
      )

    val covarianceBetweenReachAndKReach =
      if (vidUniverseSize == 1L) 0.0
      else
        vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - totalReach) *
          kReach / (vidUniverseSize - 1) / vidSamplingIntervalWidth.pow(2.0)

    val kReachRatioVariance =
      (kReachRatio / totalReach).pow(2.0) * reachMeasurementVariance +
        (1.0 / totalReach).pow(2.0) * kReachVariance -
        2.0 * kReachRatio / totalReach.toDouble().pow(2.0) * covarianceBetweenReachAndKReach

    return max(0.0, kReachRatioVariance)
  }

  /**
   * Outputs the variance of the given [kPlusReachRatio].
   *
   * kPlusReachRatio = kPlusReach / reach.
   */
  fun kPlusFrequencyRelativeVariance(
    sketchParams: ShareShuffleSketchParams,
    frequencyNoiseVariance: Double,
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams,
  ): Double {
    val (
      totalReach: Long,
      reachMeasurementVariance: Double,
      kPlusReachRatio: Double,
      frequencyMeasurementParams: FrequencyMeasurementParams,
      multiplier: Int,
    ) = relativeFrequencyMeasurementVarianceParams

    // When reach is too small, we have little infomation to estimate frequency, and thus the
    // estimate variance of k+ relative frequency is equivalent to a uniformly random guess of a
    // probability in [0, 1] (except for when k is 1 as the 1+ relative variance is always 0.0).
    if (
      isReachTooSmallForComputingRelativeFrequencyVariance(totalReach, reachMeasurementVariance)
    ) {
      // When frequency = 1, the multiplier = 0.
      if (multiplier == 0) return 0.0 else return VARIANCE_OF_UNIFORMLY_RANDOM_PROBABILITY
    }

    val vidSamplingIntervalWidth = frequencyMeasurementParams.vidSamplingInterval.width
    val vidUniverseSize = (sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    val kPlusReach = (kPlusReachRatio * totalReach).toLong()

    // Gets the reach noise variance from the reach measurement variance and total reach.
    val reachNoiseVariance =
      reachMeasurementVariance * vidSamplingIntervalWidth.pow(2.0) -
        vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - totalReach) *
          totalReach / (vidUniverseSize - 1)

    val kPlusReachVariance =
      kPlusFrequencyCountVariance(
        sketchParams,
        frequencyNoiseVariance,
        relativeFrequencyMeasurementVarianceParams,
      )

    val covarianceBetweenReachAndKPlusReach =
      if (vidUniverseSize == 1L) 0.0
      else
        (vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - totalReach) *
          kPlusReach / (vidUniverseSize - 1) + reachNoiseVariance) /
          vidSamplingIntervalWidth.pow(2.0)

    val kPlusReachRatioVariance =
      (kPlusReachRatio / totalReach).pow(2.0) * reachMeasurementVariance +
        (1.0 / totalReach).pow(2.0) * kPlusReachVariance -
        2.0 * kPlusReachRatio / totalReach.toDouble().pow(2.0) * covarianceBetweenReachAndKPlusReach

    return max(0.0, kPlusReachRatioVariance)
  }
}
