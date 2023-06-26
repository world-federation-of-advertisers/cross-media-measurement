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
class DirectImpressionMeasurementStatisticsTest {
  private lateinit var directImpressionMeasurementStatistics:
    DirectImpressionMeasurementStatistics

  @Before
  fun initService() {
    directImpressionMeasurementStatistics = DirectImpressionMeasurementStatistics()
  }

  @Test
  fun `variance returns a value when impressions is 0`() {
    val impressions = 0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val maximumFrequencyPerUser = 1
    val impressionMeasurementParams =
      ImpressionMeasurementParams(vidSamplingIntervalWidth, dpParams, maximumFrequencyPerUser)

    val variance =
      directImpressionMeasurementStatistics.variance(impressions, impressionMeasurementParams)
    val expect = 2.5e-7
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when impressions is small and sampling width is small`() {
    val impressions = 1
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 1
    val impressionMeasurementParams =
      ImpressionMeasurementParams(vidSamplingIntervalWidth, dpParams, maximumFrequencyPerUser)

    val variance =
      directImpressionMeasurementStatistics.variance(impressions, impressionMeasurementParams)
    val expect = 2102185919.1600006
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when impressions is small and sampling width is large`() {
    val impressions = 1
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 1
    val impressionMeasurementParams =
      ImpressionMeasurementParams(vidSamplingIntervalWidth, dpParams, maximumFrequencyPerUser)

    val variance =
      directImpressionMeasurementStatistics.variance(impressions, impressionMeasurementParams)
    val expect = 210218.58201600003
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when impressions is large and sampling width is small`() {
    val impressions = 3e8.toInt()
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 200
    val impressionMeasurementParams =
      ImpressionMeasurementParams(vidSamplingIntervalWidth, dpParams, maximumFrequencyPerUser)

    val variance =
      directImpressionMeasurementStatistics.variance(impressions, impressionMeasurementParams)
    val expect = 90027432806400.0
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when impressions is large and sampling width is large`() {
    val impressions = 3e8.toInt()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 200
    val impressionMeasurementParams =
      ImpressionMeasurementParams(vidSamplingIntervalWidth, dpParams, maximumFrequencyPerUser)

    val variance =
      directImpressionMeasurementStatistics.variance(impressions, impressionMeasurementParams)
    val expect = 8408743280.640002
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variance throws IllegalArgumentException when impressions is negative`() {
    val impressions = -1
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val impressionMeasurementParams =
      ImpressionMeasurementParams(vidSamplingIntervalWidth, dpParams, 1)

    assertThrows(IllegalArgumentException::class.java) {
      directImpressionMeasurementStatistics.variance(impressions, impressionMeasurementParams)
    }
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
