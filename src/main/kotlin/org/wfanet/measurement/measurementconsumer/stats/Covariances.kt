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

class MeasurementSketchParamsNotMatchCovarianceException(
  message: String? = null,
  cause: Throwable? = null
) : Exception(message, cause)

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
      inflation = 0.0
    )
  }

  /** Computes the covariance of two reach measurements based on their methodologies. */
  fun computeMeasurementCovariance(
    weightedMeasurementVarianceParams: WeightedReachMeasurementVarianceParams,
    otherWeightedMeasurementVarianceParams: WeightedReachMeasurementVarianceParams,
    unionWeightedMeasurementVarianceParams: WeightedReachMeasurementVarianceParams,
  ): Double {
    val unionSamplingWidth =
      computeUnionSamplingWidth(
        weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
          .vidSamplingIntervalStart,
        weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
          .vidSamplingIntervalWidth,
        otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
          .vidSamplingIntervalStart,
        otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
          .vidSamplingIntervalWidth
      )

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val liquidLegionsSketchParams =
      when (weightedMeasurementVarianceParams.methodology.typeCase) {
        Methodology.TypeCase.DETERMINISTIC -> {
          return computeDeterministicCovariance(
            ReachMeasurementCovarianceParams(
              reach = weightedMeasurementVarianceParams.measurementVarianceParams.reach,
              otherReach = otherWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
              unionReach = unionWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
              samplingWidth =
                weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                  .vidSamplingIntervalWidth,
              otherSamplingWidth =
                otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                  .vidSamplingIntervalWidth,
              unionSamplingWidth = unionSamplingWidth
            )
          )
        }
        Methodology.TypeCase.LIQUID_LEGIONS_SKETCH -> {
          LiquidLegionsSketchParams(
            weightedMeasurementVarianceParams.methodology.liquidLegionsSketch.decayRate,
            weightedMeasurementVarianceParams.methodology.liquidLegionsSketch.maxSize.toDouble()
          )
        }
        Methodology.TypeCase.LIQUID_LEGIONS_V2 -> {
          LiquidLegionsSketchParams(
            weightedMeasurementVarianceParams.methodology.liquidLegionsV2.decayRate,
            weightedMeasurementVarianceParams.methodology.liquidLegionsV2.maxSize.toDouble()
          )
        }
        Methodology.TypeCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
          LiquidLegionsSketchParams(
            weightedMeasurementVarianceParams.methodology.reachOnlyLiquidLegionsV2.decayRate,
            weightedMeasurementVarianceParams.methodology.reachOnlyLiquidLegionsV2.maxSize
              .toDouble()
          )
        }
        Methodology.TypeCase.TYPE_NOT_SET -> {
          error("Methodology is not set.")
        }
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (otherWeightedMeasurementVarianceParams.methodology.typeCase) {
      Methodology.TypeCase.DETERMINISTIC -> {
        return computeDeterministicCovariance(
          ReachMeasurementCovarianceParams(
            reach = weightedMeasurementVarianceParams.measurementVarianceParams.reach,
            otherReach = otherWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
            unionReach = unionWeightedMeasurementVarianceParams.measurementVarianceParams.reach,
            samplingWidth =
              weightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                .vidSamplingIntervalWidth,
            otherSamplingWidth =
              otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
                .vidSamplingIntervalWidth,
            unionSamplingWidth = unionSamplingWidth
          )
        )
      }
      Methodology.TypeCase.LIQUID_LEGIONS_SKETCH -> {
        if (
          liquidLegionsSketchParams.decayRate !=
            otherWeightedMeasurementVarianceParams.methodology.liquidLegionsSketch.decayRate ||
            liquidLegionsSketchParams.sketchSize !=
              otherWeightedMeasurementVarianceParams.methodology.liquidLegionsSketch.maxSize
                .toDouble()
        ) {
          throw MeasurementSketchParamsNotMatchCovarianceException(
            "Covariance calculation for Liquid Legions based measurements requires two " +
              "measurements using the same decay rate and sketch size.",
            IllegalArgumentException("Measurements in the covariance calculation are not valid.")
          )
        }
      }
      Methodology.TypeCase.LIQUID_LEGIONS_V2 -> {
        if (
          liquidLegionsSketchParams.decayRate !=
            otherWeightedMeasurementVarianceParams.methodology.liquidLegionsV2.decayRate ||
            liquidLegionsSketchParams.sketchSize !=
              otherWeightedMeasurementVarianceParams.methodology.liquidLegionsV2.maxSize.toDouble()
        ) {
          throw MeasurementSketchParamsNotMatchCovarianceException(
            "Covariance calculation for Liquid Legions based measurements requires two " +
              "measurements using the same decay rate and sketch size.",
            IllegalArgumentException("Measurements in the covariance calculation are not valid.")
          )
        }
      }
      Methodology.TypeCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        if (
          liquidLegionsSketchParams.decayRate !=
            otherWeightedMeasurementVarianceParams.methodology.reachOnlyLiquidLegionsV2.decayRate ||
            liquidLegionsSketchParams.sketchSize !=
              otherWeightedMeasurementVarianceParams.methodology.reachOnlyLiquidLegionsV2.maxSize
                .toDouble()
        ) {
          throw MeasurementSketchParamsNotMatchCovarianceException(
            "Covariance calculation for Liquid Legions based measurements requires two " +
              "measurements using the same decay rate and sketch size.",
            IllegalArgumentException("Measurements in the covariance calculation are not valid.")
          )
        }
      }
      Methodology.TypeCase.TYPE_NOT_SET -> {
        error("Methodology is not set.")
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
            .vidSamplingIntervalWidth,
        otherSamplingWidth =
          otherWeightedMeasurementVarianceParams.measurementVarianceParams.measurementParams
            .vidSamplingIntervalWidth,
        unionSamplingWidth = unionSamplingWidth
      )
    )
  }

  /** Computes the width of the union of two sampling intervals. */
  private fun computeUnionSamplingWidth(
    vidSamplingIntervalStart: Double,
    vidSamplingIntervalWidth: Double,
    otherVidSamplingIntervalStart: Double,
    otherVidSamplingIntervalWidth: Double,
  ): Double {
    return max(
      vidSamplingIntervalStart + vidSamplingIntervalWidth,
      otherVidSamplingIntervalStart + otherVidSamplingIntervalWidth
    ) -
      min(vidSamplingIntervalStart, otherVidSamplingIntervalStart) -
      max(
        0.0,
        otherVidSamplingIntervalStart - vidSamplingIntervalStart - vidSamplingIntervalWidth
      ) -
      max(
        0.0,
        vidSamplingIntervalStart - otherVidSamplingIntervalStart - otherVidSamplingIntervalWidth
      )
  }
}
