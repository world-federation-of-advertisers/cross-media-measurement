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

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class MpcReachMeasurementStatisticsTest {
  private lateinit var mpcReachMeasurementStatistics: MpcReachMeasurementStatistics

  @Before
  fun initService() {
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(DECAY_RATE, SKETCH_SIZE)
  }

  @Test
  fun `variance returns a value when reach is small, sampling width is small, and small decay rate`() {
    val reach = 2
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 432817.78878559935
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is small, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)

    val reach = 2
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 433242.3223399124
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is small, sampling width is large, and small decay rate`() {
    val reach = 2
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 4328.084473679164
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is small, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val reach = 2
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 4336.579244624804
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large, sampling width is small, and small decay rate`() {
    val reach = 3e6.toInt()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 362456073.197418
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val reach = 3e6.toInt()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 45186835212.94076
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large, sampling width is large, and small decay rate`() {
    val reach = 3e6.toInt()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 4.94250670279621e+29
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val reach = 3e6.toInt()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = mpcReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 45186557325.27274
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when small reaches overlap and small sampling widths not overlap`() {
    val decayRate = 1e-3
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val covariance = mpcReachMeasurementStatistics.covariance(5, 5, 5, 0.01, 0.01, 0.02)
    val expect = -4.849999813860015
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when small reaches not overlap and large sampling widths overlap `() {
    val decayRate = 15.0
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val covariance = mpcReachMeasurementStatistics.covariance(1, 1, 2, 0.5, 0.4, 0.5)
    val expect = 0.000562525536043962
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when large reaches overlap and small sampling widths overlap`() {
    val decayRate = 15.0
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val covariance =
      mpcReachMeasurementStatistics.covariance(
        1e6.toInt(),
        3e8.toInt(),
        3e8.toInt(),
        0.02,
        0.01,
        0.02
      )
    val expect = 156079036.5929788
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when large reaches not overlap and large sampling widths not overlap`() {
    val decayRate = 100.0
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val covariance =
      mpcReachMeasurementStatistics.covariance(3e8.toInt(), 3e8.toInt(), 6e8.toInt(), 0.3, 0.4, 0.7)
    val expect = 428562.31521871715
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when one reach is small`() {
    val decayRate = 1.0
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val covariance =
      mpcReachMeasurementStatistics.covariance(1, 3e6.toInt(), 3e6.toInt(), 0.5, 0.4, 0.7)
    val expect = 0.0002056053253447586
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when large reaches not overlap and large sampling widths overlap`() {
    val decayRate = 100.0
    mpcReachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, SKETCH_SIZE)
    val covariance =
      mpcReachMeasurementStatistics.covariance(
        3e8.toInt(),
        3e8.toInt(),
        6e8.toInt(),
        0.7,
        0.7,
        0.7,
      )
    val expect = 214283.93565983986
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val PERCENTAGE_ERROR_TOLERANCE = 1e-2
    const val DECAY_RATE = 1e-3
    const val SKETCH_SIZE = 1e5
  }
}
