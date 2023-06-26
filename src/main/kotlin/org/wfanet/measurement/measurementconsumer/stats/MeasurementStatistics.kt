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

/** Interface that contains necessary information about computing the measurement. */
sealed interface MeasurementParams

data class ReachMeasurementParams(val vidSamplingIntervalWidth: Double, val dpParams: DpParams) :
  MeasurementParams

data class FrequencyMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val reachDpParams: DpParams,
  val frequencyDpParams: DpParams,
  val maximumFrequency: Int
) : MeasurementParams

data class ImpressionMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val dpParams: DpParams,
  val maximumFrequencyPerUser: Int
) : MeasurementParams

data class WatchDurationMeasurementParams(
  val vidSamplingIntervalWidth: Double,
  val dpParams: DpParams,
  val maximumDurationPerUser: Double
) : MeasurementParams

data class FrequencyVariances(
  val relativeVariances: Map<Int, Double>,
  val kPlusRelativeVariances: Map<Int, Double>,
  val countVariances: Map<Int, Double>,
  val kPlusCountVariances: Map<Int, Double>,
)

/** Interface of measurement statistics. */
interface MeasurementStatistics
