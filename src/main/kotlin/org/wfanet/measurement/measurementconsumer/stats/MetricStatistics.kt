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

sealed interface BaseMethodology

data class CustomDirectScalarMethodology(val variance: Double) : BaseMethodology

data class CustomDirectFrequencyMethodology(
  val relativeVariances: FrequencyVariance,
  val kPlusRelativeVariances: FrequencyVariance
) : BaseMethodology

object DeterministicBaseMethodology : BaseMethodology

data class LiquidLegionsSketchBaseMethodology(val decayRate: Double, val sketchSize: Double) :
  BaseMethodology

data class LiquidLegionsV2BaseMethodology(
  val decayRate: Double,
  val sketchSize: Double,
  val samplingIndicatorSize: Double
) : BaseMethodology

data class WeightedReachMeasurementVarianceParams(
  val binaryRepresentation: Int,
  val weight: Int,
  val measurementVarianceParams: ReachMeasurementVarianceParams,
  val baseMethodology: BaseMethodology
)

data class WeightedFrequencyMeasurementVarianceParams(
  val binaryRepresentation: Int,
  val weight: Int,
  val measurementVarianceParams: FrequencyMeasurementVarianceParams,
  val baseMethodology: BaseMethodology
)

data class WeightedImpressionMeasurementVarianceParams(
  val binaryRepresentation: Int,
  val weight: Int,
  val measurementVarianceParams: ImpressionMeasurementVarianceParams,
  val baseMethodology: BaseMethodology
)

data class WeightedWatchDurationMeasurementVarianceParams(
  val binaryRepresentation: Int,
  val weight: Int,
  val measurementVarianceParams: WatchDurationMeasurementVarianceParams,
  val baseMethodology: BaseMethodology
)

/** The parameters used to compute the variance of a reach metric. */
data class ReachMetricVarianceParams(
  val weightedMeasurementVarianceParamsList: List<WeightedReachMeasurementVarianceParams>
)

/** The parameters used to compute the variance of a reach-and-frequency metric. */
data class FrequencyMetricVarianceParams(
  val weightedMeasurementVarianceParamsList: List<WeightedFrequencyMeasurementVarianceParams>
)

/** The parameters used to compute the variance of an impression metric. */
data class ImpressionMetricVarianceParams(
  val weightedMeasurementVarianceParamsList: List<WeightedImpressionMeasurementVarianceParams>
)

/** The parameters used to compute the variance of a watch duration metric. */
data class WatchDurationMetricVarianceParams(
  val weightedMeasurementVarianceParamsList: List<WeightedWatchDurationMeasurementVarianceParams>
)
