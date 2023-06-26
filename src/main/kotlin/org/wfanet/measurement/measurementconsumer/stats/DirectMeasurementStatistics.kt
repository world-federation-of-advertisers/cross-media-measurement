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
import kotlin.math.pow
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser

/** Class that computes statistics of direct measurements. */
abstract class DirectMeasurementStatistics : MeasurementStatistics {
  /** Calculates variance of a scalar measurement. */
  protected fun calculateScalarMeasurementVariance(
    measurementValue: Double,
    vidSamplingIntervalWidth: Double,
    dpParams: DpParams,
    maximumFrequency: Double,
  ): Double {
    assert(measurementValue >= 0) { "The scalar measurement value cannot be negative." }
    val gaussianNoiseVariance: Double = GaussianNoiser.getSigma(dpParams).pow(2)
    val variance =
      (maximumFrequency *
        measurementValue *
        vidSamplingIntervalWidth *
        (1 - vidSamplingIntervalWidth) + maximumFrequency.pow(2) * gaussianNoiseVariance) /
        vidSamplingIntervalWidth.pow(2.0)

    return max(0.0, variance)
  }
}
