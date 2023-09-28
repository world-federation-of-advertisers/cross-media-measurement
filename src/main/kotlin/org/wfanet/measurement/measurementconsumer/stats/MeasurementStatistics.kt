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

data class VidSamplingInterval(val start: Double, val width: Double)

/** The parameters used to compute a reach measurement. */
data class ReachMeasurementParams(
  val vidSamplingInterval: VidSamplingInterval,
  val dpParams: DpParams,
  val noiseMechanism: NoiseMechanism
)

/** The parameters used to compute a reach-and-frequency measurement. */
data class FrequencyMeasurementParams(
  val vidSamplingInterval: VidSamplingInterval,
  val dpParams: DpParams,
  val noiseMechanism: NoiseMechanism,
  val maximumFrequency: Int,
)

/** The parameters used to compute an impression measurement. */
data class ImpressionMeasurementParams(
  val vidSamplingInterval: VidSamplingInterval,
  val dpParams: DpParams,
  val maximumFrequencyPerUser: Int,
  val noiseMechanism: NoiseMechanism
)

/** The parameters used to compute a watch duration measurement. */
data class WatchDurationMeasurementParams(
  val vidSamplingInterval: VidSamplingInterval,
  val dpParams: DpParams,
  val maximumDurationPerUser: Double,
  val noiseMechanism: NoiseMechanism
)

/** The parameters used to compute the variance of a reach measurement. */
data class ReachMeasurementVarianceParams(
  val reach: Int,
  val measurementParams: ReachMeasurementParams
)

/** The parameters used to compute the variance of a reach-and-frequency measurement. */
data class FrequencyMeasurementVarianceParams(
  val totalReach: Int,
  val reachMeasurementVariance: Double,
  val relativeFrequencyDistribution: Map<Int, Double>,
  val measurementParams: FrequencyMeasurementParams
)

/** The parameters used to compute the variance of an impression measurement. */
data class ImpressionMeasurementVarianceParams(
  val impression: Int,
  val measurementParams: ImpressionMeasurementParams
)

/** The parameters used to compute the variance of a watch duration measurement. */
data class WatchDurationMeasurementVarianceParams(
  val duration: Double,
  val measurementParams: WatchDurationMeasurementParams
)

typealias FrequencyVariance = Map<Int, Double>

/** A data class that wraps different types of variances of a reach-and-frequency result. */
data class FrequencyVariances(
  val relativeVariances: FrequencyVariance,
  val kPlusRelativeVariances: FrequencyVariance,
  val countVariances: FrequencyVariance,
  val kPlusCountVariances: FrequencyVariance,
)

/** The parameters used to compute the covariance of two reach measurements. */
data class ReachMeasurementCovarianceParams(
  val reach: Int,
  val otherReach: Int,
  val unionReach: Int,
  val samplingWidth: Double,
  val otherSamplingWidth: Double,
  val unionSamplingWidth: Double
)
