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

/** Computes statistics for direct reach measurements. */
class DirectReachMeasurementStatistics : DirectMeasurementStatistics(), ReachMeasurementStatistics {
  /** Outputs the variance of the given [reach]. */
  override fun variance(reach: Int, params: ReachMeasurementParams): Double {
    return calculateScalarMeasurementVariance(
      reach.toDouble(),
      params.vidSamplingIntervalWidth,
      params.dpParams,
      1.0
    )
  }

  /**
   * Outputs the covariance of the given two reaches.
   *
   * Precisely, computes the covariance between any two reach measurements when at least one reach
   * is a direct measurement.
   */
  override fun covariance(
    thisReach: Int,
    thatReach: Int,
    unionReach: Int,
    thisSamplingWidth: Double,
    thatSamplingWidth: Double,
    unionSamplingWidth: Double
  ): Double {
    val overlapReach = thisReach + thatReach - unionReach
    val overlapSamplingWidth = thisSamplingWidth + thatSamplingWidth - unionSamplingWidth
    return overlapReach * (overlapSamplingWidth / thisSamplingWidth / thatSamplingWidth - 1.0)
  }
}
