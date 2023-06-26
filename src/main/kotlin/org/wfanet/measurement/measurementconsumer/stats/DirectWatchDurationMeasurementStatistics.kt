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

/** Computes statistics for direct watch duration measurements. */
class DirectWatchDurationMeasurementStatistics : DirectMeasurementStatistics() {

  /** Outputs the variance of the given [duration]. */
  fun variance(duration: Double, params: WatchDurationMeasurementParams): Double {
    return calculateScalarMeasurementVariance(
      duration,
      params.vidSamplingIntervalWidth,
      params.dpParams,
      params.maximumDurationPerUser
    )
  }
}
