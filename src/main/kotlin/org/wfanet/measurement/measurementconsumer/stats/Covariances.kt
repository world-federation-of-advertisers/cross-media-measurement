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

/** Functions to compute different covariances. */
object Covariances {
  /**
   * Outputs the covariance of two reach measurements that are computed using deterministic count
   * distinct methodology.
   *
   * Precisely, computes the covariance between any two reach measurements when at least one reach
   * is computed using deterministic count distinct methodology.
   */
  fun computeDeterministicCovariance(
    reachMeasurementCovarianceParams: ReachMeasurementCovarianceParams
  ): Double {
    val overlapReach =
      reachMeasurementCovarianceParams.reach + reachMeasurementCovarianceParams.otherReach -
        reachMeasurementCovarianceParams.unionReach

    val overlapSamplingWidth =
      reachMeasurementCovarianceParams.samplingWidth +
        reachMeasurementCovarianceParams.otherSamplingWidth -
        reachMeasurementCovarianceParams.unionSamplingWidth
    require(overlapSamplingWidth >= 0.0 && overlapSamplingWidth <= 1.0) {
      "Overlap sampling width must be greater than or equal to 0 and less than or equal to 1, but" +
        " got $overlapSamplingWidth."
    }

    return overlapReach *
      (overlapSamplingWidth /
        reachMeasurementCovarianceParams.samplingWidth /
        reachMeasurementCovarianceParams.otherSamplingWidth - 1.0)
  }

  /**
   * Calculates the covariance between two reach measurements that are computed using Liquid Legions
   * sketch.
   *
   * Precisely, the covariance between any two reach measurements when both measurements are
   * computed from Liquid Legions sketch.
   */
  fun computeLiquidLegionsCovariance(
    sketchParams: LiquidLegionsSketchParams,
    reachMeasurementCovarianceParams: ReachMeasurementCovarianceParams,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    return LiquidLegions.inflatedReachCovariance(
      sketchParams = sketchParams,
      reach = reachMeasurementCovarianceParams.reach,
      otherReach = reachMeasurementCovarianceParams.otherReach,
      overlapReach =
        reachMeasurementCovarianceParams.reach + reachMeasurementCovarianceParams.otherReach -
          reachMeasurementCovarianceParams.unionReach,
      samplingWidth = reachMeasurementCovarianceParams.samplingWidth,
      otherSamplingWidth = reachMeasurementCovarianceParams.otherSamplingWidth,
      overlapSamplingWidth =
        reachMeasurementCovarianceParams.samplingWidth +
          reachMeasurementCovarianceParams.otherSamplingWidth -
          reachMeasurementCovarianceParams.unionSamplingWidth,
      inflation = 0.0,
    )
  }

  /**
   * Computes the covariance of two reach measurements based on their methodologies.
   *
   * TODO(@ple13): Add support for Honest Majority Share Shuffle.
   */
  fun computeMeasurementCovariance(
    weightedMeasurementVarianceParams: WeightedReachMeasurementVarianceParams,
    otherWeightedMeasurementVarianceParams: WeightedReachMeasurementVarianceParams,
    unionWeightedMeasurementVarianceParams: WeightedReachMeasurementVarianceParams,
  ): Double {
    val unionSamplingWidth =
      computeUnionSamplingWidth(
        weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
          .vidSamplingInterval,
        otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
          .vidSamplingInterval,
      )
    require(unionSamplingWidth >= 0.0 && unionSamplingWidth <= 1.0) {
      "The union sampling width must be greater than or equal to 0 and less than or equal to 1, " +
        "but got $unionSamplingWidth."
    }

    val liquidLegionsSketchParams =
      when (val methodology = weightedMeasurementVarianceParams.methodology) {
        is CustomDirectScalarMethodology,
        is CustomDirectFrequencyMethodology -> {
          // Custom direct methodology must guarantee independence.
          return 0.0
        }
        is DeterministicMethodology,
        is HonestMajorityShareShuffleMethodology,
        is TrusTeeMethodology -> {
          return computeDeterministicCovariance(
            ReachMeasurementCovarianceParams(
              reach = weightedMeasurementVarianceParams.measurementVarianceParams.reach,
              otherReach = otherWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
              unionReach = unionWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
              samplingWidth =
                weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                  .vidSamplingInterval
                  .width,
              otherSamplingWidth =
                otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                  .vidSamplingInterval
                  .width,
              unionSamplingWidth = unionSamplingWidth,
            )
          )
        }
        is LiquidLegionsSketchMethodology -> {
          LiquidLegionsSketchParams(methodology.decayRate, methodology.sketchSize)
        }
        is LiquidLegionsV2Methodology -> {
          LiquidLegionsSketchParams(methodology.decayRate, methodology.sketchSize)
        }
      }

    when (val otherMethodology = otherWeightedMeasurementVarianceParams.methodology) {
      is CustomDirectScalarMethodology,
      is CustomDirectFrequencyMethodology -> {
        // Custom direct methodology must guarantee independence.
        return 0.0
      }
      is DeterministicMethodology,
      is HonestMajorityShareShuffleMethodology,
      is TrusTeeMethodology -> {
        return computeDeterministicCovariance(
          ReachMeasurementCovarianceParams(
            reach = weightedMeasurementVarianceParams.measurementVarianceParams.reach,
            otherReach = otherWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
            unionReach = unionWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
            samplingWidth =
              weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                .vidSamplingInterval
                .width,
            otherSamplingWidth =
              otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                .vidSamplingInterval
                .width,
            unionSamplingWidth = unionSamplingWidth,
          )
        )
      }
      is LiquidLegionsSketchMethodology -> {
        if (
          liquidLegionsSketchParams.decayRate != otherMethodology.decayRate ||
            liquidLegionsSketchParams.sketchSize != otherMethodology.sketchSize
        ) {
          throw IllegalArgumentException(
            "Covariance calculation for Liquid Legions based measurements requires two " +
              "measurements using the same decay rate and sketch size."
          )
        }
      }
      is LiquidLegionsV2Methodology -> {
        if (
          liquidLegionsSketchParams.decayRate != otherMethodology.decayRate ||
            liquidLegionsSketchParams.sketchSize != otherMethodology.sketchSize
        ) {
          throw IllegalArgumentException(
            "Covariance calculation for Liquid Legions based measurements requires two " +
              "measurements using the same decay rate and sketch size."
          )
        }
      }
    }
    return computeLiquidLegionsCovariance(
      sketchParams = liquidLegionsSketchParams,
      ReachMeasurementCovarianceParams(
        reach = weightedMeasurementVarianceParams.measurementVarianceParams.reach,
        otherReach = otherWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
        unionReach = unionWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
        samplingWidth =
          weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
            .vidSamplingInterval
            .width,
        otherSamplingWidth =
          otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
            .vidSamplingInterval
            .width,
        unionSamplingWidth = unionSamplingWidth,
      ),
    )
  }

  /** Returns a list of non-wrapping interval represented by pairs of <start, end> */
  private fun VidSamplingInterval.toNonWrappingIntervals(): List<ClosedRange<Double>> {
    val start = this.start
    val end = this.start + this.width

    return if (end > 1.0) {
      listOf(0.0..(end - 1.0), start..1.0)
    } else {
      listOf(start..end)
    }
  }

  /** Computes the width of the union of two sampling intervals. */
  fun computeUnionSamplingWidth(
    vidSamplingInterval: VidSamplingInterval,
    otherVidSamplingInterval: VidSamplingInterval,
  ): Double {
    val intervals: List<ClosedRange<Double>> =
      buildList {
          addAll(vidSamplingInterval.toNonWrappingIntervals())
          addAll(otherVidSamplingInterval.toNonWrappingIntervals())
        }
        .sortedWith(compareBy { it.start })

    var currentStart = intervals[0].start
    var currentEnd = intervals[0].endInclusive
    var totalWidth = 0.0

    for (i in 1 until intervals.size) {
      val interval = intervals[i]
      if (interval.start <= currentEnd) {
        // Overlap
        currentEnd = maxOf(currentEnd, interval.endInclusive)
      } else {
        // No overlap
        totalWidth += currentEnd - currentStart
        currentStart = interval.start
        currentEnd = interval.endInclusive
      }
    }
    totalWidth += currentEnd - currentStart

    return totalWidth
  }
}
