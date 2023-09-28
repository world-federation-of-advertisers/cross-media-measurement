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
}
