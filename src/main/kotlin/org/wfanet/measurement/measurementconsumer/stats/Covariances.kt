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

/** Contains the functions that compute different covariances. */
object Covariances {
  /**
   * Outputs the covariance of two reaches that are computed using deterministic count distinct
   * methodology.
   *
   * Precisely, computes the covariance between any two reach measurements when at least one reach
   * is a direct measurement.
   */
  fun computeDeterministicCovariance(reachCovarianceParams: ReachCovarianceParams): Double {
    val overlapReach =
      reachCovarianceParams.reach + reachCovarianceParams.otherReach -
        reachCovarianceParams.unionReach
    val overlapSamplingWidth =
      reachCovarianceParams.samplingWidth + reachCovarianceParams.otherSamplingWidth -
        reachCovarianceParams.unionSamplingWidth
    return overlapReach *
      (overlapSamplingWidth /
        reachCovarianceParams.samplingWidth /
        reachCovarianceParams.otherSamplingWidth - 1.0)
  }

  /**
   * Outputs the covariance of two impressions that are computed using deterministic count
   * methodology.
   *
   * Precisely, computes the covariance between any two impression measurements when at least one
   * impression is a direct measurement.
   */
  fun computeDeterministicCovariance(
    impressionCovarianceParams: ImpressionCovarianceParams
  ): Double {
    TODO()
  }

  /**
   * Outputs the covariance of two watch durations that are computed using deterministic sum
   * methodology.
   *
   * Precisely, computes the covariance between any two watch duration measurements when at least
   * one watch duration is a direct measurement.
   */
  fun computeDeterministicCovariance(
    watchDurationCovarianceParams: WatchDurationCovarianceParams
  ): Double {
    TODO()
  }

  /**
   * Calculates the covariance between two reaches that are computed using Liquid Legions.
   *
   * Precisely, the covariance between any two reach results when both reaches are from Liquid
   * Legions.
   */
  fun computeLiquidLegionsCovariance(
    sketchParams: LiquidLegionsSketchParams,
    reachCovarianceParams: ReachCovarianceParams,
  ): Double {
    return LiquidLegions.inflatedReachCovariance(
      sketchParams,
      reachCovarianceParams.reach,
      reachCovarianceParams.otherReach,
      reachCovarianceParams.reach + reachCovarianceParams.otherReach -
        reachCovarianceParams.unionReach,
      reachCovarianceParams.samplingWidth,
      reachCovarianceParams.otherSamplingWidth,
      reachCovarianceParams.samplingWidth + reachCovarianceParams.otherSamplingWidth -
        reachCovarianceParams.unionSamplingWidth,
      0.0
    )
  }

  /**
   * Calculates the covariance between two impressions that are computed using Liquid Legions.
   *
   * Precisely, the covariance between any two impression results when both impressions are from
   * Liquid Legions.
   */
  fun computeLiquidLegionsCovariance(
    sketchParams: LiquidLegionsSketchParams,
    impressionCovarianceParams: ImpressionCovarianceParams,
  ): Double {
    TODO()
  }

  /**
   * Calculates the covariance between two watch durations that are computed using Liquid Legions.
   *
   * Precisely, the covariance between any two watch duration results when both watch durations are
   * from Liquid Legions.
   */
  fun computeLiquidLegionsCovariance(
    sketchParams: LiquidLegionsSketchParams,
    watchDurationCovarianceParams: WatchDurationCovarianceParams,
  ): Double {
    TODO()
  }
}
