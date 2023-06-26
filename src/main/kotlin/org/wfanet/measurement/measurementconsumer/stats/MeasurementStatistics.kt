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

import org.wfanet.measurement.eventdataprovider.noiser.DpParams

/** Noise mechanism enums. */
enum class NoiseMechanism {
  NONE,
  LAPLACE,
  GAUSSIAN,
}

/** The parameters used to compute a reach measurement. */
data class ReachMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val dpParams: DpParams,
  val noiseMechanism: NoiseMechanism
)

/** The parameters used to compute a reach-frequency measurement. */
data class FrequencyMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val frequencyDpParams: DpParams,
  val frequencyNoiseMechanism: NoiseMechanism,
  val maximumFrequency: Int,
)

/** The parameters used to compute an impression measurement. */
data class ImpressionMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val dpParams: DpParams,
  val maximumFrequencyPerUser: Int,
  val noiseMechanism: NoiseMechanism
)

/** The parameters used to compute a watch duration measurement. */
data class WatchDurationMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val dpParams: DpParams,
  val maximumDurationPerUser: Double,
  val noiseMechanism: NoiseMechanism
)

/** The parameters used to compute the variance of a reach. */
data class ReachVarianceParams(val reach: Int, val reachMeasurementParams: ReachMeasurementParams)

/** The parameters used to compute the variance of a reach-frequency. */
data class FrequencyVarianceParams(
  val totalReach: Int,
  val reachVariance: Double,
  val relativeFrequencyDistribution: Map<Int, Double>,
  val frequencyMeasurementParams: FrequencyMeasurementParams
)

/** The parameters used to compute the variance of an impression. */
data class ImpressionVarianceParams(
  val impression: Int,
  val impressionMeasurementParams: ImpressionMeasurementParams
)

/** The parameters used to compute the variance of a watch duration. */
data class WatchDurationVarianceParams(
  val duration: Double,
  val watchDurationMeasurementParams: WatchDurationMeasurementParams
)

typealias FrequencyVariance = Map<Int, Double>

/** Wraps different types of variances of reach-frequency. */
data class FrequencyVariances(
  val relativeVariances: FrequencyVariance,
  val kPlusRelativeVariances: FrequencyVariance,
  val countVariances: FrequencyVariance,
  val kPlusCountVariances: FrequencyVariance,
)

/** The parameters used to compute the covariance of two reaches. */
data class ReachCovarianceParams(
  val reach: Int,
  val otherReach: Int,
  val unionReach: Int,
  val samplingWidth: Double,
  val otherSamplingWidth: Double,
  val unionSamplingWidth: Double
)

/** The parameters used to compute the covariance of two impressions. */
data class ImpressionCovarianceParams(
  val impression: Int,
  val otherImpression: Int,
  val unionImpression: Int,
  val samplingWidth: Double,
  val otherSamplingWidth: Double,
  val unionSamplingWidth: Double
)

/** The parameters used to compute the covariance of two watch durations. */
data class WatchDurationCovarianceParams(
  val watchDuration: Int,
  val otherWatchDuration: Int,
  val unionWatchDuration: Int,
  val samplingWidth: Double,
  val otherSamplingWidth: Double,
  val unionSamplingWidth: Double
)
