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

import kotlin.math.ceil
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
    val vidUniverseSize: Long = ceil(sketchSize / vidSamplingIntervalWidth).toLong()
    require(vidUniverseSize > 1) { "Vid universe size is too small." }

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

  /**
   * Outputs the variance of the given [kReach] when k < maximumFrequency, and the variance of
   * [kPlusReach] when k is the maximumFrequency.
   */
  fun frequencyCountVariance(
    sketchParams: ShareShuffleSketchParams,
    frequency: Int,
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

    val vidUniverseSize = ceil(sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    require(vidUniverseSize > 1) { "Vid universe size is too small." }
    require(totalReach <= vidUniverseSize) {
      "Reach must be less than or equal to the size of the Vid universe."
    }

    val kReach = (kReachRatio * totalReach).toLong()
    require(kReach <= vidUniverseSize) {
      "kReach must be less than or equal to the size of the Vid universe."
    }

    var kReachVariance =
      (vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth) *
        (vidUniverseSize - kReach) *
        kReach / (vidUniverseSize - 1) + frequencyNoiseVariance) / vidSamplingIntervalWidth.pow(2.0)

    if (frequency == frequencyMeasurementParams.maximumFrequency) {
      val reachNoiseVariance =
        reachMeasurementVariance * vidSamplingIntervalWidth.pow(2.0) -
          vidSamplingIntervalWidth *
            (1.0 - vidSamplingIntervalWidth) *
            (vidUniverseSize - totalReach) *
            totalReach / (vidUniverseSize - 1)
      kReachVariance +=
        ((reachNoiseVariance + (frequency - 2) * frequencyNoiseVariance) /
          vidSamplingIntervalWidth.pow(2.0))
    }
    return max(0.0, kReachVariance)
  }

  /** Outputs the variance of the given [kPlusReach]. */
  fun kPlusFrequencyCountVariance(
    sketchParams: ShareShuffleSketchParams,
    frequency: Int,
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

    val vidUniverseSize = ceil(sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    require(vidUniverseSize > 1) { "Vid universe size is too small." }
    require(totalReach <= vidUniverseSize) {
      "Reach must be less than or equal to the size of the Vid universe."
    }

    val kPlusReach = (kReachRatio * totalReach).toLong()
    require(kPlusReach <= vidUniverseSize) {
      "kPlusReach must be less than or equal to the size of the Vid universe."
    }

    // Gets the reach noise variance from the reach measurement variance and total reach.
    val reachNoiseVariance =
      reachMeasurementVariance * vidSamplingIntervalWidth.pow(2.0) -
        vidSamplingIntervalWidth *
          (1.0 - vidSamplingIntervalWidth) *
          (vidUniverseSize - totalReach) *
          totalReach / (vidUniverseSize - 1)

    val kPlusReachVariance =
      (vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth) *
        (vidUniverseSize - kPlusReach) *
        kPlusReach / (vidUniverseSize - 1) +
        (frequency - 1) * frequencyNoiseVariance +
        reachNoiseVariance) / vidSamplingIntervalWidth.pow(2.0)

    return max(0.0, kPlusReachVariance)
  }

  /**
   * Outputs the variance of the given [kReachRatio] when k < maximumFrequency, and the variance of
   * [kPlusReachRatio] when k is the maximumFrequency.
   */
  fun frequencyRelativeVariance(
    sketchParams: ShareShuffleSketchParams,
    frequency: Int,
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

    val vidUniverseSize = ceil(sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    require(vidUniverseSize > 1) { "Vid universe size is too small." }
    require(totalReach <= vidUniverseSize) {
      "Reach must be less than or equal to the size of the Vid universe."
    }

    val kReach = (kReachRatio * totalReach).toLong()
    require(kReach <= vidUniverseSize) {
      "kReach must be less than or equal to the size of the Vid universe."
    }

    var kReachVariance =
      frequencyCountVariance(
        sketchParams,
        frequency,
        frequencyNoiseVariance,
        relativeFrequencyMeasurementVarianceParams,
      )

    var covarianceBetweenReachAndKReach =
      vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth) *
        (vidUniverseSize - totalReach) *
        kReach / (vidUniverseSize - 1) / vidSamplingIntervalWidth.pow(2.0)

    if (frequency == frequencyMeasurementParams.maximumFrequency) {
      val reachNoiseVariance =
        reachMeasurementVariance * vidSamplingIntervalWidth.pow(2.0) -
          vidSamplingIntervalWidth *
            (1.0 - vidSamplingIntervalWidth) *
            (vidUniverseSize - totalReach) *
            totalReach / (vidUniverseSize - 1)
      covarianceBetweenReachAndKReach += reachNoiseVariance / vidSamplingIntervalWidth.pow(2.0)
    }

    val kReachRatioVariance =
      (kReachRatio / totalReach).pow(2.0) * reachMeasurementVariance +
        (1.0 / totalReach).pow(2.0) * kReachVariance -
        2.0 * kReachRatio / totalReach.toDouble().pow(2.0) * covarianceBetweenReachAndKReach

    return max(0.0, kReachRatioVariance)
  }

  /** Outputs the variance of the given [kPlusReachRatio]. */
  fun kPlusFrequencyRelativeVariance(
    sketchParams: ShareShuffleSketchParams,
    frequency: Int,
    frequencyNoiseVariance: Double,
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams,
  ): Double {
    val (
      totalReach: Long,
      reachMeasurementVariance: Double,
      kPlusReachRatio: Double,
      frequencyMeasurementParams: FrequencyMeasurementParams,
    ) = relativeFrequencyMeasurementVarianceParams

    // When reach is too small, we have little infomation to estimate frequency, and thus the
    // estimate variance of k+ relative frequency is equivalent to a uniformly random guess of a
    // probability in [0, 1] (except for when k is 1 as the 1+ relative variance is always 0.0).
    if (
      isReachTooSmallForComputingRelativeFrequencyVariance(totalReach, reachMeasurementVariance)
    ) {
      if (frequency == 1) return 0.0 else return VARIANCE_OF_UNIFORMLY_RANDOM_PROBABILITY
    }

    val vidSamplingIntervalWidth = frequencyMeasurementParams.vidSamplingInterval.width

    val vidUniverseSize = ceil(sketchParams.sketchSize / vidSamplingIntervalWidth).toLong()
    require(vidUniverseSize > 1) { "Vid universe size is too small." }
    require(totalReach <= vidUniverseSize) {
      "Reach must be less than or equal to the size of the Vid universe."
    }

    val kPlusReach = (kPlusReachRatio * totalReach).toLong()
    require(kPlusReach <= vidUniverseSize) {
      "kPlusReach must be less than or equal to the size of the Vid universe."
    }

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
        frequency,
        frequencyNoiseVariance,
        relativeFrequencyMeasurementVarianceParams,
      )

    val covarianceBetweenReachAndKPlusReach =
      (vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth) *
        (vidUniverseSize - totalReach) *
        kPlusReach / (vidUniverseSize - 1) + reachNoiseVariance) / vidSamplingIntervalWidth.pow(2.0)

    val kPlusReachRatioVariance =
      (kPlusReachRatio / totalReach).pow(2.0) * reachMeasurementVariance +
        (1.0 / totalReach).pow(2.0) * kPlusReachVariance -
        2.0 * kPlusReachRatio / totalReach.toDouble().pow(2.0) * covarianceBetweenReachAndKPlusReach

    return max(0.0, kPlusReachRatioVariance)
  }
}
