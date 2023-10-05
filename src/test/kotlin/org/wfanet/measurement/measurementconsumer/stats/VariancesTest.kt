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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class VariancesTest {
  @Test
  fun `computeMeasurementVariance returns a value for deterministic reach when reach is small and vid sampling interval width is large`() {
    val reach = 0L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)
    val expected = 2.5e-7
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic reach when reach is small and vid sampling interval width is small`() {
    val reach = 0L
    val vidSamplingIntervalWidth = 1e-4
    val dpParams = DpParams(1e-3, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)
    val expected = 1701291910758399.5
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic reach when reach is large and vid sampling interval width is large`() {
    val reach = 3e8.toLong()
    val vidSamplingIntervalWidth = 0.9
    val dpParams = DpParams(1e-2, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)
    val expected = 33906671.712079
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic reach when reach is large and vid sampling interval width is small`() {
    val reach = 3e8.toLong()
    val vidSamplingIntervalWidth = 1e-4
    val dpParams = DpParams(1e-2, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)
    val expected = 49440108678400.01
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance for deterministic reach throws IllegalArgumentException when reach is negative`() {
    val reach = -1L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)
    }
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic impression when impressions is 0`() {
    val impressions = 0L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val maximumFrequencyPerUser = 1
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumFrequencyPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams
      )
    val expected = 2.5e-7
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic impression when impressions is small and sampling width is small`() {
    val impressions = 1L
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 1
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumFrequencyPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams
      )
    val expected = 2102185919.1600006
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic impression when impressions is small and sampling width is large`() {
    val impressions = 1L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 1
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumFrequencyPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams
      )
    val expected = 210218.58201600003
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic impression when impressions is large and sampling width is small`() {
    val impressions = 3e8.toLong()
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 200
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumFrequencyPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams
      )
    val expected = 90027432806400.0
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic impression when impressions is large and sampling width is large`() {
    val impressions = 3e8.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 200
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumFrequencyPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams
      )
    val expected = 8408743280.640002
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance for deterministic impression throws IllegalArgumentException when impressions is negative`() {
    val impressions = -1L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        1,
        NoiseMechanism.GAUSSIAN
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams
      )
    }
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic watch duration when watchDuration is 0`() {
    val watchDuration = 0.0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val maximumDurationPerUser = 1.0
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumDurationPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams
      )
    val expected = 2.5e-7
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic watch duration when watch duration is small and sampling width is small`() {
    val watchDuration = 1.0
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumDurationPerUser = 1.0
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumDurationPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams
      )
    val expected = 2102185919.1600006
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic watch duration when watchDuration is small and sampling width is large`() {
    val watchDuration = 1.0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumDurationPerUser = 1.0
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumDurationPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams
      )
    val expected = 210218.58201600003
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic watch duration when watchDuration is large and sampling width is small`() {
    val watchDuration = 3e8
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumDurationPerUser = 200.0
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumDurationPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams
      )
    val expected = 90027432806400.0
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for deterministic watch duration when watchDuration is large and sampling width is large`() {
    val watchDuration = 3e8
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumDurationPerUser = 200.0
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumDurationPerUser,
        NoiseMechanism.GAUSSIAN
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams
      )
    val expected = 8408743280.640002
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance for deterministic watch duration throws IllegalArgumentException when watchDuration is negative`() {
    val watchDuration = -1.0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        1.0,
        NoiseMechanism.GAUSSIAN
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams
      )
    }
  }

  @Test
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is small and sampling width is small`() {
    val vidSamplingIntervalWidth = 1e-4
    val totalReach = 1L
    val reachDpParams = DpParams(0.05, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(130523240799.76, 110944754739.79, 104418592319.84, 110944753539.91, 130523238400.0)
    val expectedRKPlus =
      listOf(0.0, 130523240799.75995, 215363345459.78998, 215363344259.90997, 130523238400.0)
    val expectedNK =
      listOf(
        2.5828737279268425e+23,
        2.195442669924104e+23,
        2.06629897600801e+23,
        2.1954426461785614e+23,
        2.582873680435757e+23
      )
    val expectedNKPlus =
      listOf(
        1978861168399.0,
        2.5828737279307992e+23,
        4.261741614272709e+23,
        4.2617415905271664e+23,
        2.582873680435757e+23
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is small and sampling width is large`() {
    val vidSamplingIntervalWidth = 0.9
    val totalReach = 10L
    val reachDpParams = DpParams(0.05, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        16.116646716049377,
        13.69921637530864,
        12.89296181728395,
        13.697883041975308,
        16.113980049382715
      )
    val expectedRKPlus =
      listOf(0.0, 16.116646716049377, 26.590400414814813, 26.58906708148148, 16.113980049382715)
    val expectedNK =
      listOf(
        399274.49027152435,
        338261.103357843,
        317260.89827872894,
        336273.87503418204,
        395300.03362420265
      )
    val expectedNKPlus =
      listOf(
        24431.495782716047,
        404160.78942806745,
        654501.1302572051,
        652513.9019335442,
        395300.03362420265
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is large and sampling width is small`() {
    val vidSamplingIntervalWidth = 0.1
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.05, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        7.201450258204445e-09,
        6.301232719473777e-09,
        4.8011602065635574e-09,
        2.701232719473778e-09,
        1.4502582044444444e-12
      )
    val expectedRKPlus =
      listOf(
        0.0,
        7.201450258204445e-09,
        6.302392926037333e-09,
        2.702392926037334e-09,
        1.4502582044444444e-12
      )
    val expectedNK =
      listOf(
        1080447160.4819105,
        810289059.2826936,
        540183586.0096896,
        270130740.66289777,
        130523.24231856702
      )
    val expectedNKPlus =
      listOf(
        2701978861.1583996,
        1620842932.7135906,
        810393477.8765483,
        270235159.25675255,
        130523.24231856702
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is large and sampling width is large`() {
    val vidSamplingIntervalWidth = 0.9
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.05, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        8.890679331116596e-11,
        7.77929965367133e-11,
        5.927358279708091e-11,
        3.3348552092268854e-11,
        1.790442227709191e-14
      )
    val expectedRKPlus =
      listOf(
        0.0,
        8.890679331116596e-11,
        7.780732007453497e-11,
        3.336287563009053e-11,
        1.790442227709191e-14
      )
    val expectedNK =
      listOf(
        13338853.595851457,
        10003568.425519641,
        6668933.002434713,
        3334947.3265966796,
        1611.398005535523
      )
    val expectedNKPlus =
      listOf(
        33357763.718004923,
        20010406.339452446,
        10004857.543924069,
        3336236.445001108,
        1611.398005535523
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for deterministic reach-frequency when maximum frequency is 1`() {
    val vidSamplingIntervalWidth = 1e-3
    val totalReach = 100L
    val reachDpParams = DpParams(0.05, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(DeterministicMethodology, reachMeasurementVarianceParams)

    val maximumFrequency = 1
    val relativeFrequencyDistribution = mapOf(1 to 1.0)
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 19788711484.000004
    val expectedNKPlus = 19788711484.000004

    assertThat(rKVars.getValue(1))
      .isWithin(computeErrorTolerance(rKVars.getValue(1), expectedRK))
      .of(expectedRK)
    assertThat(rKPlusVars.getValue(1))
      .isWithin(computeErrorTolerance(rKPlusVars.getValue(1), expectedRKPlus))
      .of(expectedRKPlus)
    assertThat(nKVars.getValue(1))
      .isWithin(computeErrorTolerance(nKVars.getValue(1), expectedNK))
      .of(expectedNK)
    assertThat(nKPlusVars.getValue(1))
      .isWithin(computeErrorTolerance(nKPlusVars.getValue(1), expectedNKPlus))
      .of(expectedNKPlus)
  }

  @Test
  fun `computeMeasurementVariance for deterministic reach-frequency throws IllegalArgumentException when reach is negative`() {
    val vidSamplingIntervalWidth = 1e-3
    val totalReach = -1L
    val reachMeasurementVariance = 0.1

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )
    }
  }

  @Test
  fun `computeMeasurementVariance for deterministic reach-frequency throws IllegalArgumentException when reach variance is negative`() {
    val vidSamplingIntervalWidth = 1e-3
    val totalReach = 10L
    val reachMeasurementVariance = -0.1

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.2, 1e-15)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams
      )
    }
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )
    val expected = 252107.369636947
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )
    val expected = 252354.6749380062
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val expected = 2520.9441397473865
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val expected = 2525.8928386525
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val expected = 289553744.8898575
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val expected = 28923114340.800056
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val expected = 2.8788216360657764e+29
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val expected = 28922934034.98562
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )
    val expected = 432817.78878559935
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )
    val expected = 433242.3223399124
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val expected = 4328.084473679164
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val expected = 4336.579244624804
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val expected = 362456073.197418
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val expected = 45186835212.94076
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val expected = 4.94250670279621e+29
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 1e5
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val expected = 45186557325.27274
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, small sampling width, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val vidSamplingIntervalWidth = 1e-2
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(4033588.314191154, 2268893.4267325234, 1008397.0785477886, 252099.26963694714, 0.0)
    val expectedNKPlus =
      listOf(25209926.963694707, 9075573.706930095, 2268893.426732524, 252099.26963694714, 0.0)

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5
    val vidSamplingIntervalWidth = 1e-2
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(4037545.1990081007, 2271119.174442056, 1009386.2997520252, 252346.5749380063, 0.0)
    val expectedNKPlus =
      listOf(25234657.493800625, 9084476.697768226, 2271119.1744420566, 252346.5749380063, 0.0)

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, large sampling width, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1.0
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        3.047425140662341,
        2.5903113959637496,
        2.4379399717258194,
        2.590310867948552,
        3.047424084631945
      )
    val expectedRKPlus =
      listOf(0.0, 3.0474251406623356, 5.028250663669304, 5.028250135654106, 3.047424084631945)
    val expectedNK =
      listOf(
        8391.77774412893,
        7017.029139893044,
        6491.543194894815,
        6815.319909134243,
        7988.3592826113245
      )
    val expectedNKPlus =
      listOf(
        2521.348083089697,
        8896.047360746856,
        13407.7165659821,
        13206.007335223296,
        7988.3592826113245
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, large sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1.0
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        3.062431068650644,
        2.60306772923472,
        2.449937810218249,
        2.6030413116012294,
        3.062378233383659
      )
    val expectedRKPlus =
      listOf(0.0, 3.062431068650639, 5.052970315941649, 5.052943898308155, 3.062378233383659)
    val expectedNK =
      listOf(
        8511.182542906958,
        7117.383664496014,
        6584.86219003212,
        6913.618119515269,
        8103.65145294546
      )
    val expectedNKPlus =
      listOf(
        2546.195484478623,
        9020.42163980267,
        13600.304826852385,
        13396.539281871635,
        8103.65145294546
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when large total reach, small sampling width, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.01
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        2.4296711936002306e-06,
        2.1252005145602013e-06,
        1.6238436215468216e-06,
        9.256005145600891e-07,
        3.04711936000057e-08
      )
    val expectedRKPlus =
      listOf(
        0.0,
        2.4296711936002306e-06,
        2.1495774694402064e-06,
        9.499774694400937e-07,
        3.04711936000057e-08
      )
    val expectedNK =
      listOf(
        4.606184563605248e+32,
        2.5910006531914206e+32,
        1.1515754019878232e+32,
        2.8790880999445434e+31,
        8.77211314124454e+25
      )
    val expectedNKPlus =
      listOf(
        2.8788216360657756e+33,
        1.0363827835736802e+33,
        2.591001354960473e+32,
        2.879095117635056e+31,
        8.77211314124454e+25
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when large total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.01
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        3.2095281397907705e-05,
        2.7979977962975644e-05,
        2.1948284986304392e-05,
        1.4000202467893943e-05,
        4.1357304077443056e-06
      )
    val expectedRKPlus =
      listOf(
        0.0,
        3.2095281397907705e-05,
        3.128856228917109e-05,
        1.7308786794089384e-05,
        4.1357304077443056e-06
      )
    val expectedNK =
      listOf(
        49179305632090.01,
        28559605109729.0,
        13552056002615.002,
        4156658310739.75,
        373412034105.87726
      )
    val expectedNKPlus =
      listOf(
        289259040349856.06,
        107031113702056.0,
        28858334737014.0,
        4455387938024.376,
        373412034105.87726
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when large total reach, large sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.99
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        1.9964862800346414e-05,
        1.7425393384806477e-05,
        1.3543836882879667e-05,
        8.320193294565975e-06,
        1.754462619865412e-06
      )
    val expectedRKPlus =
      listOf(
        3.3881317890172014e-21,
        1.9964862800346414e-05,
        1.8828963480698805e-05,
        9.723763390458304e-06,
        1.754462619865412e-06
      )
    val expectedNK =
      listOf(
        48084027390574.0,
        27606621636808.0,
        12793216675394.498,
        3643812506329.2495,
        158409129613.70135
      )
    val expectedNKPlus =
      listOf(
        289258842034032.0,
        105935795797375.98,
        27733348940498.0,
        3770539810020.25,
        158409129613.70135
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when maximum frequency is 1`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 100L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 1
    val relativeFrequencyDistribution = mapOf(1 to 1.0)
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 253034.8083089697
    val expectedNKPlus = 253034.8083089697

    assertThat(rKVars.getValue(1))
      .isWithin(computeErrorTolerance(rKVars.getValue(1), expectedRK))
      .of(expectedRK)
    assertThat(rKPlusVars.getValue(1))
      .isWithin(computeErrorTolerance(rKPlusVars.getValue(1), expectedRKPlus))
      .of(expectedRKPlus)
    assertThat(nKVars.getValue(1))
      .isWithin(computeErrorTolerance(nKVars.getValue(1), expectedNK))
      .of(expectedNK)
    assertThat(nKPlusVars.getValue(1))
      .isWithin(computeErrorTolerance(nKPlusVars.getValue(1), expectedNKPlus))
      .of(expectedNKPlus)
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when reach is less than 3`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1e-3
    val totalReach = 1L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(403738839.74081016, 227103097.35420564, 100934709.93520254, 25233677.483800635, 0.0)
    val expectedNKPlus =
      listOf(2523367748.380063, 908412389.416823, 227103097.35420576, 25233677.483800635, 0.0)

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, small sampling width, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1e-2
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(6924955.020569592, 3895287.199070394, 1731238.755142398, 432809.6887855995, 0.0)
    val expectedNKPlus =
      listOf(43280968.87855994, 15581148.79628158, 3895287.199070395, 432809.6887855995, 0.0)

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1e-2
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(6931747.5574386, 3899108.001059211, 1732936.88935965, 433234.2223399125, 0.0)
    val expectedNKPlus =
      listOf(43323422.233991235, 15596432.004236847, 3899108.001059212, 433234.2223399125, 0.0)

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, large sampling width, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1.0
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        4.809657736908107,
        4.088209119574676,
        3.84772595911162,
        4.088208255518936,
        4.809656008796624
      )
    val expectedRKPlus =
      listOf(0.0, 4.809657736908105, 7.935933926611972, 7.935933062556234, 4.809656008796624)
    val expectedNK =
      listOf(
        21993.508778450716,
        18495.358884218687,
        17213.873575034922,
        18149.05285089939,
        21300.89671181211
      )
    val expectedNKPlus =
      listOf(
        4328.777582607534,
        22859.264294972218,
        35536.07625366837,
        35189.77022034907,
        21300.89671181211
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, large sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1.0
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        4.852333214777005,
        4.1244853983815455,
        3.8818550207757827,
        4.124442081959716,
        4.852246581933344
      )
    val expectedRKPlus =
      listOf(0.0, 4.852333214777005, 8.00628266392822, 8.006239347506389, 4.852246581933344)
    val expectedNK =
      listOf(
        22396.225589524824,
        18835.716311720287,
        17532.244237694387,
        18485.809367447113,
        21696.411700978482
      )
    val expectedNKPlus =
      listOf(
        4371.415731789478,
        23270.508735882726,
        36192.84567250306,
        35842.938728229885,
        21696.411700978482
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when large total reach, small sampling width, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.01
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(
        7.908010724473936e+32,
        4.448256032516588e+32,
        1.977002681118484e+32,
        4.94250670279621e+31,
        0.0
      )
    val expectedNKPlus =
      listOf(
        4.942506702796209e+33,
        1.779302413006636e+33,
        4.44825603251659e+32,
        4.94250670279621e+31,
        0.0
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when large total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.01
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        0.0007208782342626791,
        0.0006187464791082771,
        0.0005447026941568097,
        0.0004987468794082772,
        0.00048087903486267896
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0007208782342626791,
        0.0010034497069984203,
        0.0008834501072984204,
        0.00048087903486267896
      )
    val expectedNK =
      listOf(
        137508046270732.0,
        96637366321740.02,
        67345201977040.01,
        49631553236626.37,
        43496420100501.89
      )
    val expectedNKPlus =
      listOf(
        451895273252719.94,
        227887100921272.0,
        131434502402141.0,
        84428689317028.02,
        43496420100501.89
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when large total reach, large sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.99
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK =
      listOf(
        0.0007208782342626792,
        0.0006187464791082771,
        0.00054470269415681,
        0.0004987468794082774,
        0.0004808790348626793
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0007208782342626785,
        0.0010034497069984208,
        0.0008834501072984206,
        0.0004808790348626793
      )
    val expectedNK =
      listOf(
        137507997147242.02,
        96637338624596.98,
        67345189584765.0,
        49631550027739.75,
        43496419953523.766
      )
    val expectedNKPlus =
      listOf(
        451894967607984.06,
        227886990668836.03,
        131434474587415.98,
        84428685990558.75,
        43496419953523.766
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when maximum frequency is 1`() {
    val decayRate = 1e-3
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 100L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 1
    val relativeFrequencyDistribution = mapOf(1 to 1.0)
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 433777.75826075335
    val expectedNKPlus = 433777.75826075335

    assertThat(rKVars.getValue(1))
      .isWithin(computeErrorTolerance(rKVars.getValue(1), expectedRK))
      .of(expectedRK)
    assertThat(rKPlusVars.getValue(1))
      .isWithin(computeErrorTolerance(rKPlusVars.getValue(1), expectedRKPlus))
      .of(expectedRKPlus)
    assertThat(nKVars.getValue(1))
      .isWithin(computeErrorTolerance(nKVars.getValue(1), expectedNK))
      .of(expectedNK)
    assertThat(nKPlusVars.getValue(1))
      .isWithin(computeErrorTolerance(nKPlusVars.getValue(1), expectedNKPlus))
      .of(expectedNKPlus)
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when reach is less than 3`() {
    val decayRate = 100.0
    val sketchSize = 1e5

    val vidSamplingIntervalWidth = 1e-3
    val totalReach = 1L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        reachMeasurementVarianceParams
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyDpParams = DpParams(0.3, 1e-9)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        frequencyDpParams,
        NoiseMechanism.GAUSSIAN,
        maximumFrequency,
      )
    val frequencyMeasurementVarianceParams =
      FrequencyMeasurementVarianceParams(
        totalReach,
        reachMeasurementVariance,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      Variances.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0.0),
        frequencyMeasurementVarianceParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(692479821.8969592, 389519899.81703943, 173119955.4742398, 43279988.86855995, 0.0)
    val expectedNKPlus =
      listOf(4327998886.855994, 1558079599.2681584, 389519899.8170396, 43279988.86855995, 0.0)

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1])
        )
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKVars.getValue(frequency))
        .isWithin(computeErrorTolerance(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .of(expectedNK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(nKPlusVars.getValue(frequency))
        .isWithin(
          computeErrorTolerance(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1])
        )
        .of(expectedNKPlus[frequency - 1])
    }
  }

  @Test
  fun `computeMetricVariance returns a value for reach when sampling intervals are fully overlapped`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = LiquidLegionsV2Methodology(0.0, 1e6, 1e8)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = DeterministicMethodology
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = Variances.computeMetricVariance(params)

    val expected = 27396.052940381534
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance returns a value for reach when sampling intervals are partially overlapped`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = LiquidLegionsV2Methodology(0.0, 1e6, 1e8)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.3, 0.8),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = DeterministicMethodology
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = Variances.computeMetricVariance(params)

    val expected = 21253.74748438153
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance returns a value for reach when sampling intervals are not overlapped`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = LiquidLegionsV2Methodology(0.0, 1e6, 1e8)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.5, 1.0),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = DeterministicMethodology
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = Variances.computeMetricVariance(params)

    val expected = 19836.523148381533
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance returns a value for reach when one is from custom direct methodology`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = LiquidLegionsV2Methodology(0.0, 1e6, 1e8)
      )

    val varianceSingleMeasurement =
      Variances.computeMetricVariance(
        ReachMetricVarianceParams(listOf(weightedReachMeasurementVarianceParams))
      )

    val varianceOtherSingleMeasurement = 1e4

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.5, 1.0),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = CustomDirectScalarMethodology(varianceOtherSingleMeasurement)
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = Variances.computeMetricVariance(params)

    val expected = varianceSingleMeasurement + varianceOtherSingleMeasurement
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance returns a value for reach intersection`() {
    val unionWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 4L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.1, 1.0),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = LiquidLegionsV2Methodology(0.0, 1e6, 1e8)
      )

    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = DeterministicMethodology
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = DeterministicMethodology
      )

    val params =
      ReachMetricVarianceParams(
        listOf(
          unionWeightedReachMeasurementVarianceParams,
          weightedReachMeasurementVarianceParams,
          otherWeightedReachMeasurementVarianceParams
        )
      )

    val variance = Variances.computeMetricVariance(params)

    val expected = 10554.13919363766
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance for reach throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(ReachMetricVarianceParams(listOf()))
    }
  }

  @Test
  fun `computeMetricVariance for reach throws UnsupportedMethodologyException when using CustomDirectFrequencyMethodology`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2L,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN
              )
          ),
        methodology = CustomDirectFrequencyMethodology(mapOf(), mapOf())
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        ReachMetricVarianceParams(listOf(weightedReachMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for reach-frequency throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(FrequencyMetricVarianceParams(listOf()))
    }
  }

  @Test
  fun `computeMetricVariance for reach-frequency throws IllegalArgumentException when there are two measurements`() {
    val weightedFrequencyMeasurementVarianceParams =
      WeightedFrequencyMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          FrequencyMeasurementVarianceParams(
            totalReach = 2L,
            reachMeasurementVariance = 100.0,
            relativeFrequencyDistribution = mapOf(1 to 1.0),
            measurementParams =
              FrequencyMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumFrequency = 10
              )
          ),
        methodology = DeterministicMethodology
      )

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(
        FrequencyMetricVarianceParams(
          listOf(
            weightedFrequencyMeasurementVarianceParams,
            weightedFrequencyMeasurementVarianceParams
          )
        )
      )
    }
  }

  @Test
  fun `computeMetricVariance for reach-frequency throws UnsupportedMethodologyException when CustomDirectScalarMethodology is used`() {
    val weightedFrequencyMeasurementVarianceParams =
      WeightedFrequencyMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          FrequencyMeasurementVarianceParams(
            totalReach = 2L,
            reachMeasurementVariance = 100.0,
            relativeFrequencyDistribution = mapOf(1 to 1.0),
            measurementParams =
              FrequencyMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumFrequency = 10
              )
          ),
        methodology = CustomDirectScalarMethodology(0.0)
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        FrequencyMetricVarianceParams(listOf(weightedFrequencyMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for impression throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(ImpressionMetricVarianceParams(listOf()))
    }
  }

  @Test
  fun `computeMetricVariance for impression throws IllegalArgumentException when there are two measurements`() {
    val weightedImpressionMeasurementVarianceParams =
      WeightedImpressionMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ImpressionMeasurementVarianceParams(
            impression = 2L,
            measurementParams =
              ImpressionMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumFrequencyPerUser = 10
              )
          ),
        methodology = DeterministicMethodology
      )

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(
        ImpressionMetricVarianceParams(
          listOf(
            weightedImpressionMeasurementVarianceParams,
            weightedImpressionMeasurementVarianceParams
          )
        )
      )
    }
  }

  @Test
  fun `computeMetricVariance for impression throws UnsupportedMethodologyException when using CustomDirectFrequencyMethodology`() {
    val weightedImpressionMeasurementVarianceParams =
      WeightedImpressionMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ImpressionMeasurementVarianceParams(
            impression = 2L,
            measurementParams =
              ImpressionMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumFrequencyPerUser = 10
              )
          ),
        methodology = CustomDirectFrequencyMethodology(mapOf(), mapOf())
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        ImpressionMetricVarianceParams(listOf(weightedImpressionMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for impression throws UnsupportedMethodologyException when using Liquid Legions Sketch methodology`() {
    val weightedImpressionMeasurementVarianceParams =
      WeightedImpressionMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ImpressionMeasurementVarianceParams(
            impression = 2L,
            measurementParams =
              ImpressionMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumFrequencyPerUser = 10
              )
          ),
        methodology = LiquidLegionsSketchMethodology(1.0, 1.0)
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        ImpressionMetricVarianceParams(listOf(weightedImpressionMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for impression throws UnsupportedMethodologyException when using Liquid Legions V2 methodology`() {
    val weightedImpressionMeasurementVarianceParams =
      WeightedImpressionMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ImpressionMeasurementVarianceParams(
            impression = 2L,
            measurementParams =
              ImpressionMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumFrequencyPerUser = 10
              )
          ),
        methodology = LiquidLegionsV2Methodology(1.0, 1.0, 1.0)
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        ImpressionMetricVarianceParams(listOf(weightedImpressionMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for watch duration throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(WatchDurationMetricVarianceParams(listOf()))
    }
  }

  @Test
  fun `computeMetricVariance for watch duration throws IllegalArgumentException when there are two measurements`() {
    val weightedWatchDurationMeasurementVarianceParams =
      WeightedWatchDurationMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          WatchDurationMeasurementVarianceParams(
            duration = 1.0,
            measurementParams =
              WatchDurationMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumDurationPerUser = 10.0
              )
          ),
        methodology = DeterministicMethodology
      )

    assertFailsWith<IllegalArgumentException> {
      Variances.computeMetricVariance(
        WatchDurationMetricVarianceParams(
          listOf(
            weightedWatchDurationMeasurementVarianceParams,
            weightedWatchDurationMeasurementVarianceParams
          )
        )
      )
    }
  }

  @Test
  fun `computeMetricVariance for watch duration throws UnsupportedMethodologyException when using CustomDirectFrequencyMethodology`() {
    val weightedWatchDurationMeasurementVarianceParams =
      WeightedWatchDurationMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          WatchDurationMeasurementVarianceParams(
            duration = 1.0,
            measurementParams =
              WatchDurationMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumDurationPerUser = 10.0
              )
          ),
        methodology = CustomDirectFrequencyMethodology(mapOf(), mapOf())
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        WatchDurationMetricVarianceParams(listOf(weightedWatchDurationMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for watch duration throws UnsupportedMethodologyException when using Liquid Legions sketch methodology`() {
    val weightedWatchDurationMeasurementVarianceParams =
      WeightedWatchDurationMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          WatchDurationMeasurementVarianceParams(
            duration = 1.0,
            measurementParams =
              WatchDurationMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumDurationPerUser = 10.0
              )
          ),
        methodology = LiquidLegionsSketchMethodology(1.0, 1.0)
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        WatchDurationMetricVarianceParams(listOf(weightedWatchDurationMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for watch duration throws UnsupportedMethodologyException when using Liquid Legions V2 methodology`() {
    val weightedWatchDurationMeasurementVarianceParams =
      WeightedWatchDurationMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          WatchDurationMeasurementVarianceParams(
            duration = 1.0,
            measurementParams =
              WatchDurationMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
                maximumDurationPerUser = 10.0
              )
          ),
        methodology = LiquidLegionsV2Methodology(1.0, 1.0, 1.0)
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      Variances.computeMetricVariance(
        WatchDurationMetricVarianceParams(listOf(weightedWatchDurationMeasurementVarianceParams))
      )
    }
  }

  companion object {
    fun computeErrorTolerance(actual: Double, expected: Double): Double {
      return if (expected == 0.0 || actual == 0.0) {
        ERROR_TOLERANCE_PERCENTAGE
      } else {
        abs(expected * ERROR_TOLERANCE_PERCENTAGE)
      }
    }

    private const val ERROR_TOLERANCE_PERCENTAGE = 5e-3
  }
}
