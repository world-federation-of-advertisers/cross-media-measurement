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
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class DirectReachMeasurementStatisticsTest {
  private lateinit var directReachMeasurementStatistics: DirectReachMeasurementStatistics

  @Before
  fun initService() {
    directReachMeasurementStatistics = DirectReachMeasurementStatistics()
  }

  @Test
  fun `variance returns a value when reach is small and vid sampling interval width is large`() {
    val reach = 0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 2.5e-7
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is small and vid sampling interval width is small`() {
    val reach = 0
    val vidSamplingIntervalWidth = 1e-4
    val dpParams = DpParams(1e-3, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 1701291910758399.5
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large and vid sampling interval width is large`() {
    val reach = 3e8.toInt()
    val vidSamplingIntervalWidth = 0.9
    val dpParams = DpParams(1e-2, 1e-15)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 33906671.712079
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large and vid sampling interval width is small`() {
    val reach = 3e8.toInt()
    val vidSamplingIntervalWidth = 1e-4
    val dpParams = DpParams(1e-2, 1e-15)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 49440108678400.01
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance throws IllegalArgumentException when reach is negative`() {
    val reach = -1
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    assertThrows(IllegalArgumentException::class.java) {
      directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    }
  }

  @Test
  fun `covariance returns a value when small reaches overlap and small sampling widths overlap `() {
    val covariance = directReachMeasurementStatistics.covariance(1, 2, 2, 2e-4, 3e-4, 4e-4)
    val expect = 1665.6666666666665
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when small reaches not overlap and large sampling widths not overlap `() {
    val covariance = directReachMeasurementStatistics.covariance(1, 2, 3, 0.5, 0.5, 1.0)
    val expect = 0.0
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when large reaches overlap and small sampling widths not overlap`() {
    val covariance =
      directReachMeasurementStatistics.covariance(
        3e8.toInt(),
        3e8.toInt(),
        4e8.toInt(),
        1e-4,
        1e-4,
        2e-4
      )
    val expect = -2e+8
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when one reach is small`() {
    val covariance =
      directReachMeasurementStatistics.covariance(1, 3e6.toInt(), 3e6.toInt(), 0.5, 0.4, 0.7)
    val expect = 2.220446049250313e-16
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when large reaches not overlap and large sampling widths overlap`() {
    val covariance =
      directReachMeasurementStatistics.covariance(
        3e8.toInt(),
        3e8.toInt(),
        6e8.toInt(),
        0.7,
        0.7,
        0.7,
      )
    val expect = 0.0
    assertThat(covariance).isEqualTo(expect)
  }

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val ERROR_TOLERANCE = 1e-2
  }
}
