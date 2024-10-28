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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )
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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )
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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )
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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )
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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )
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
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        impressionMeasurementVariancesParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams,
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
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        watchDurationMeasurementVarianceParams,
      )
    }
  }

  @Test
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is small and sampling width is small`() {
    val vidSamplingIntervalWidth = 5e-2
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.5270081502877656,
        0.4480709277446008,
        0.4209985202302125,
        0.4457909277446008,
        0.5224481502877656,
      )
    val expectedRKPlus =
      listOf(0.0, 0.5270081502877656, 0.8660294479748132, 0.8637494479748131, 0.5224481502877656)
    val expectedNK =
      listOf(
        599711.6131995119,
        505012.9301397436,
        469784.2425013215,
        494025.5502842458,
        577736.8534885163,
      )
    val expectedNKPlus =
      listOf(
        105826.2014523311,
        620876.8534899781,
        967202.4129305566,
        956215.0330750588,
        577736.8534885163,
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
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is small and sampling width is large`() {
    val vidSamplingIntervalWidth = 0.9
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0016391609576782886,
        0.0013939534806932123,
        0.0013077732105870755,
        0.0013806201473598788,
        0.001612494291011622,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0016391609576782882,
        0.0026839489135025095,
        0.002670615580169177,
        0.001612494291011622,
      )
    val expectedNK =
      listOf(
        1700.4372667720345,
        1428.6003082911895,
        1323.4327071114915,
        1384.9344632329405,
        1613.1055766555369,
      )
    val expectedNKPlus =
      listOf(
        379.0932143590467,
        1776.2559096438433,
        2719.0847696156193,
        2675.418924557371,
        1613.1055766555369,
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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        7.201450258204445e-09,
        6.301232719473777e-09,
        4.8011602065635574e-09,
        2.701232719473778e-09,
        1.4502582044444444e-12,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        7.201450258204445e-09,
        6.302392926037333e-09,
        2.702392926037334e-09,
        1.4502582044444444e-12,
      )
    val expectedNK =
      listOf(
        1080447160.4819105,
        810289059.2826936,
        540183586.0096896,
        270130740.66289777,
        130523.24231856702,
      )
    val expectedNKPlus =
      listOf(
        2701978861.1583996,
        1620842932.7135906,
        810393477.8765483,
        270235159.25675255,
        130523.24231856702,
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
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        8.890679331116596e-11,
        7.77929965367133e-11,
        5.927358279708091e-11,
        3.3348552092268854e-11,
        1.790442227709191e-14,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        8.890679331116596e-11,
        7.780732007453497e-11,
        3.336287563009053e-11,
        1.790442227709191e-14,
      )
    val expectedNK =
      listOf(
        13338853.595851457,
        10003568.425519641,
        6668933.002434713,
        3334947.3265966796,
        1611.398005535523,
      )
    val expectedNKPlus =
      listOf(
        33357763.718004923,
        20010406.339452446,
        10004857.543924069,
        3336236.445001108,
        1611.398005535523,
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
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is too small`() {
    val vidSamplingIntervalWidth = 5e-2
    val totalReach = 1L
    val reachDpParams = DpParams(0.5, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedNK =
      listOf(
        21132.41568673391,
        15053.25158507073,
        10710.991512454175,
        8105.63546888424,
        7237.183454360929,
      )
    val expectedNKPlus =
      listOf(
        86845.20145233112,
        38501.45597720013,
        15053.251585070733,
        8105.63546888424,
        7237.183454360929,
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
    val vidSamplingIntervalWidth = 0.9
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 379.0932143590467
    val expectedNKPlus = 379.0932143590467

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
        frequencyMeasurementParams,
      )

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )
    }
  }

  @Test
  fun `computeMeasurementVariance for LiquidLegionsSketch throws InvalINVALID_ARGUMENT when sketch size is invalid`() {
    val decayRate = 1e-3
    val sketchSize = 0L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )
    }
  }

  @Test
  fun `computeMeasurementVariance for LiquidLegionsSketch throws InvalINVALID_ARGUMENT when decay rate is invalid`() {
    val decayRate = -5.0
    val sketchSize = 10L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )
    }
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )
    val expected = 252107.369636947
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )
    val expected = 252354.6749380062
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )

    val expected = 2520.9441397473865
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is small, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )

    val expected = 2525.8928386525
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )

    val expected = 289553744.8898575
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )

    val expected = 28923114340.800056
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )

    val expected = 2.8788216360657764e+29
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsSketch reach when reach is large, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
      )

    val expected = 28922934034.98562
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance for LiquidLegionsV2 throws InvalINVALID_ARGUMENT when sketch size is invalid`() {
    val decayRate = 1e-3
    val sketchSize = 0L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )
    }
  }

  @Test
  fun `computeMeasurementVariance for LiquidLegionsV2 throws InvalINVALID_ARGUMENT when decay rate is invalid`() {
    val decayRate = -5.0
    val sketchSize = 10L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )
    }
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )
    val expected = 432817.78878559935
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )
    val expected = 433242.3223399124
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )

    val expected = 4328.084473679164
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is small, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )

    val expected = 4336.579244624804
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is small, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )

    val expected = 362456073.197418
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is small, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )

    val expected = 45186835212.94076
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for HonestMajorityShareShuffle reach when reach is small, sampling width is small`() {
    val frequencyVectorSize = 10_000_000L
    val reach = 2L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )
    // Expected variance = variance from DP noise + variance from Hmss sketch sampling.
    val expected = 432791.13137641125 + 17.99999974
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for HonestMajorityShareShuffle reach when reach is small, sampling width is large`() {
    val frequencyVectorSize = 10_000_000L
    val reach = 2L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )
    // The variance of the measurement comes entirely from the reach DP noise when sampling interval
    // width is equal to 1.
    val expected = 4327.9113137641125
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for HonestMajorityShareShuffle reach when reach is large, sampling width is small`() {
    val frequencyVectorSize = 10_000_000L
    val reach = 90_000_000L
    val vidSamplingIntervalWidth = 0.1
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    // Expected variance = variance from DP noise + variance from Hmss sketch sampling.
    val expected = 252102.58106484052 + 8.1E7
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for HonestMajorityShareShuffle reach when reach is large, sampling width is large`() {
    val frequencyVectorSize = 10_000_000L
    val reach = 9_000_000L
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    // The variance of the measurement comes entirely from the reach DP noise when sampling interval
    // width is equal to 1.
    val expected = 4327.91131376411
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for HonestMajorityShareShuffle reach when reach is half, sampling width is half`() {
    val frequencyVectorSize = 10_000_000L
    val reach = 10_000_000L
    val vidSamplingIntervalWidth = 0.5
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )
    // Expected variance = variance from DP noise + variance from Hmss sketch sampling.
    val expected = 17311.6452550564 + 5.0E6
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is large, and small decay rate`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )

    val expected = 4.94250670279621e+29
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns a value for LiquidLegionsV2 reach when reach is large, sampling width is large, and large decay rate`() {
    val decayRate = 1e2
    val sketchSize = 100000L
    val reach = 3e6.toLong()
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(reach, reachMeasurementParams)

    val variance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
      )

    val expected = 45186557325.27274
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, small sampling width, and small decay rate`() {
    val decayRate = 1e-2
    val sketchSize = 100000L
    val vidSamplingIntervalWidth = 1e-1
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.03260806784967439,
        0.027770887646892062,
        0.02579829441483908,
        0.026690288153515446,
        0.030446868862921157,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.032608067849674405,
        0.05212838273722897,
        0.05104778324385237,
        0.030446868862921157,
      )
    val expectedNK =
      listOf(
        36541.340051824605,
        30175.900914382,
        27141.9691905643,
        27439.544880371428,
        31068.62798380339,
      )
    val expectedNKPlus =
      listOf(
        20421.118627387797,
        40625.56377730211,
        55030.80330142469,
        52294.447267414136,
        31068.62798380339,
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
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 100000L
    val vidSamplingIntervalWidth = 1e-1
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0341805974697979,
        0.02910901042381123,
        0.02704846424526232,
        0.027998958934151177,
        0.031960494490477796,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0341805974697979,
        0.05467740601619346,
        0.05356735452653342,
        0.031960494490477796,
      )
    val expectedNK =
      listOf(
        38421.913999295386,
        31710.604101578978,
        28512.945060473943,
        28828.936875980187,
        32658.579548097718,
      )
    val expectedNKPlus =
      listOf(
        21842.123182039944,
        42790.338635703316,
        57837.46774005718,
        54955.80051445836,
        32658.579548097718,
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
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when small total reach, large sampling width, and small decay rate`() {
    val decayRate = 1e-2
    val sketchSize = 100000L
    val vidSamplingIntervalWidth = 1.0
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0003084195788141115,
        0.00026218666238117175,
        0.0002465755543090122,
        0.00026158625459763276,
        0.0003072187632470337,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0003084195788141115,
        0.0005079616729787987,
        0.0005073612651952599,
        0.0003072187632470337,
      )
    val expectedNK =
      listOf(
        327.8586634488892,
        273.1319033763575,
        251.45587557046386,
        262.8305800311573,
        307.25601675845763,
      )
    val expectedNKPlus =
      listOf(
        121.26053444866557,
        352.1107703385642,
        518.9367167831224,
        508.635393437924,
        307.25601675845763,
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
    val sketchSize = 100000L
    val vidSamplingIntervalWidth = 1.0
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0005405568786010254,
        0.00046100419546627093,
        0.00042428097671869016,
        0.000430387222358283,
        0.0004793229323850494,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.000540556878601025,
        0.0008444625413743105,
        0.0008138455682663226,
        0.0004793229323850494,
      )
    val expectedNK =
      listOf(
        635.9104781849892,
        514.7338108340482,
        448.29111332634784,
        436.582385661839,
        479.6076278405448,
      )
    val expectedNKPlus =
      listOf(
        593.9533376357285,
        754.7011457121116,
        898.4199131064815,
        820.2684879342742,
        479.6076278405448,
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
    val decayRate = 1e-2
    val sketchSize = 3_000_000L

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        7.92374327869203e-08,
        6.933190871070413e-08,
        5.282946173981984e-08,
        2.9730091874267457e-08,
        3.3799114047009024e-11,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        7.92374327869203e-08,
        6.935894800194172e-08,
        2.9757131165505068e-08,
        3.3799114047009024e-11,
      )
    val expectedNK =
      listOf(
        1947873584904.0,
        1097907668595.9999,
        489940606067.0,
        123972397310.87502,
        3042330.23507461,
      )
    val expectedNKPlus =
      listOf(
        12129632842688.0,
        4373800153436.0005,
        1097910102459.9999,
        123974831175.0,
        3042330.23507461,
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
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.01
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        3.2095281397907705e-05,
        2.7979977962975644e-05,
        2.1948284986304392e-05,
        1.4000202467893943e-05,
        4.1357304077443056e-06,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        3.2095281397907705e-05,
        3.128856228917109e-05,
        1.7308786794089384e-05,
        4.1357304077443056e-06,
      )
    val expectedNK =
      listOf(
        49179305632090.01,
        28559605109729.0,
        13552056002615.002,
        4156658310739.75,
        373412034105.87726,
      )
    val expectedNKPlus =
      listOf(
        289259040349856.06,
        107031113702056.0,
        28858334737014.0,
        4455387938024.376,
        373412034105.87726,
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
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.99
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        1.9964862800346414e-05,
        1.7425393384806477e-05,
        1.3543836882879667e-05,
        8.320193294565975e-06,
        1.754462619865412e-06,
      )
    val expectedRKPlus =
      listOf(
        3.3881317890172014e-21,
        1.9964862800346414e-05,
        1.8828963480698805e-05,
        9.723763390458304e-06,
        1.754462619865412e-06,
      )
    val expectedNK =
      listOf(
        48084027390574.0,
        27606621636808.0,
        12793216675394.498,
        3643812506329.2495,
        158409129613.70135,
      )
    val expectedNKPlus =
      listOf(
        289258842034032.0,
        105935795797375.98,
        27733348940498.0,
        3770539810020.25,
        158409129613.70135,
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
    val decayRate = 1e-2
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 20421.118627387797
    val expectedNKPlus = 20421.118627387797

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
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is zero and zero relative frequencies `() {
    val vidSamplingIntervalWidth = 5e-2
    val totalReach = 0L
    val reachDpParams = DpParams(0.5, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution = (1..maximumFrequency).associateWith { 0.0 }
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedNK =
      listOf(
        7244.988593451251,
        7244.988593451251,
        7244.988593451251,
        7244.988593451251,
        7244.988593451251,
      )
    val expectedNKPlus =
      listOf(0.0, 7244.988593451251, 7244.988593451251, 7244.988593451251, 7244.988593451251)

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
  fun `computeMeasurementVariance returns for deterministic reach-frequency when total reach is zero and regular relative frequencies `() {
    val vidSamplingIntervalWidth = 5e-2
    val totalReach = 0L
    val reachDpParams = DpParams(0.5, 1e-15)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      mapOf(
        1 to 0.4351145038167939,
        2 to 0.0,
        3 to 0.3816793893129771,
        4 to 0.1832061068702290,
        5 to 0.0,
      )
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        DeterministicMethodology,
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedNK =
      listOf(
        23699.39297634332,
        7243.321926784585,
        19905.73424562018,
        10160.74172504431,
        7243.321926784585,
      )
    val expectedNKPlus =
      listOf(
        86919.86312141502,
        34979.06986996207,
        34979.06986996207,
        10160.74172504431,
        7243.321926784585,
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
  fun `computeMeasurementVariance returns for LiquidLegionsSketch reach-frequency when reach is too small`() {
    val decayRate = 100.0
    val sketchSize = 100000L
    val vidSamplingIntervalWidth = 1e-1
    val totalReach = 1L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsSketchMethodology(decayRate, sketchSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedNK =
      listOf(
        2777.413131500171,
        1978.455244356285,
        1407.7710392535107,
        1065.360516191846,
        951.2236751712913,
      )
    val expectedNKPlus =
      listOf(
        11413.684102055491,
        5060.14995191127,
        1978.4552443562857,
        1065.360516191846,
        951.2236751712913,
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
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, small sampling width, and small decay rate`() {
    val decayRate = 1e-2
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.05034342984868496,
        0.042845920790728145,
        0.039986714975769756,
        0.04176581240380985,
        0.04818321307484837,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.05034342984868497,
        0.08139249125060682,
        0.08031238286368855,
        0.04818321307484837,
      )
    val expectedNK =
      listOf(
        55886.2748486677,
        46346.59750952415,
        42094.47733130149,
        43129.91431399983,
        49452.90845761906,
      )
    val expectedNKPlus =
      listOf(
        26351.405432392614,
        61156.55593514616,
        85908.92427561936,
        82692.2410800951,
        49452.90845761906,
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
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 1e-1
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.05532921404123075,
        0.04708679728314121,
        0.043959556043144256,
        0.045947490321239945,
        0.05305060011742826,
      )
    val expectedRKPlus =
      listOf(0.0, 0.0553292140412307, 0.0895272773770838, 0.08838797041518255, 0.05305060011742826)
    val expectedNK =
      listOf(
        61439.77763656265,
        50977.015322569,
        46342.14128092452,
        47535.15551162926,
        54556.058014683185,
      )
    val expectedNKPlus =
      listOf(
        28377.77318112482,
        67115.33227278745,
        94621.86173431553,
        91180.00192337578,
        54556.058014683185,
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
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when small total reach, large sampling width, and small decay rate`() {
    val decayRate = 1e-2
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 1.0
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0004929886184465759,
        0.0004191004472982807,
        0.00039407024612424105,
        0.00041789801492445744,
        0.0004905837536989296,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0004929886184465762,
        0.0008115674502574244,
        0.000810365017883601,
        0.0004905837536989296,
      )
    val expectedNK =
      listOf(
        522.1406505274354,
        435.52422094502253,
        401.4074465952872,
        419.79032747817297,
        490.6728635936937,
      )
    val expectedNKPlus =
      listOf(
        181.6405335321324,
        558.4687572338153,
        828.0625118199823,
        812.3286183531291,
        490.6728635936937,
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
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 1.0
    val totalReach = 1000L
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0013421266113186165,
        0.0011442914928402196,
        0.0010551206318847811,
        0.0010746140284523004,
        0.0012027716825427776,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0013421266113186165,
        0.002106508838874441,
        0.0020368313744865223,
        0.0012027716825427776,
      )
    val expectedNK =
      listOf(
        1461.8600223995454,
        1211.9304731251468,
        1085.5879979647943,
        1082.832596918464,
        1203.6642699861634,
      )
    val expectedNKPlus =
      listOf(
        742.1087944958126,
        1610.2817812986902,
        2174.861889114065,
        2045.7640129073934,
        1203.6642699861634,
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
    val decayRate = 1e-2
    val sketchSize = 3_000_000L

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.5, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0004352090325906098,
        0.0003743314887583703,
        0.000324680233771944,
        0.00028625526763133123,
        0.00025905659033653163,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.00043520903259060994,
        0.0005815767610275956,
        0.0004935005399005565,
        0.00025905659033653163,
      )
    val expectedNK =
      listOf(
        41575204563504.01,
        35045372066235.004,
        29826056933018.504,
        25917259163848.25,
        23318978758726.46,
      )
    val expectedNKPlus =
      listOf(
        14999149157200.002,
        44575034394936.01,
        53700555073216.01,
        44572442170829.375,
        23318978758726.46,
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
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.01
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0007208782342626791,
        0.0006187464791082771,
        0.0005447026941568097,
        0.0004987468794082772,
        0.00048087903486267896,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0007208782342626791,
        0.0010034497069984203,
        0.0008834501072984204,
        0.00048087903486267896,
      )
    val expectedNK =
      listOf(
        137508046270732.0,
        96637366321740.02,
        67345201977040.01,
        49631553236626.37,
        43496420100501.89,
      )
    val expectedNKPlus =
      listOf(
        451895273252719.94,
        227887100921272.0,
        131434502402141.0,
        84428689317028.02,
        43496420100501.89,
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
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.99
    val totalReach = 3e8.toLong()
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.0007208782342626792,
        0.0006187464791082771,
        0.00054470269415681,
        0.0004987468794082774,
        0.0004808790348626793,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0007208782342626785,
        0.0010034497069984208,
        0.0008834501072984206,
        0.0004808790348626793,
      )
    val expectedNK =
      listOf(
        137507997147242.02,
        96637338624596.98,
        67345189584765.0,
        49631550027739.75,
        43496419953523.766,
      )
    val expectedNKPlus =
      listOf(
        451894967607984.06,
        227886990668836.03,
        131434474587415.98,
        84428685990558.75,
        43496419953523.766,
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
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 0.1
    val totalReach = 100L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
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
  fun `computeMeasurementVariance returns for LiquidLegionsV2 reach-frequency when reach is too small`() {
    val decayRate = 1e-2
    val sketchSize = 100000L

    val vidSamplingIntervalWidth = 1e-2
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      VariancesImpl.computeMeasurementVariance(
        LiquidLegionsV2Methodology(decayRate, sketchSize, 0L),
        frequencyMeasurementVarianceParams,
      )

    val expectedRK =
      listOf(
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedNK =
      listOf(
        10531710.756153584,
        7502042.935890221,
        5337994.492844966,
        4039565.427017812,
        3606755.738408761,
      )
    val expectedNKPlus =
      listOf(
        43280968.86090512,
        19187904.528334606,
        7502042.935890224,
        4039565.427017812,
        3606755.738408761,
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
  fun `computeMeasurementVariance returns for HonestMajorityShareShuffle reach-frequency when small total reach, small sampling width`() {
    val frequencyVectorSize = 10_000_000L
    val vidSamplingIntervalWidth = 0.1
    val totalReach = 10_000L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    // FrequencyDistribution = {1/15, 2/15, 3/15, 4/15, 5/15}
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { frequency -> frequency / 15.0 }
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
        frequencyMeasurementParams,
      )

    val (relativeVariances, kPlusRelativeVariances, countVariances, kPlusCountVariances) =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRelativeVariances =
      listOf(
        5.5611419682E-04,
        6.6181968148E-04,
        7.9799548885E-04,
        9.6464161893E-04,
        4.0470322809E-03,
      )
    val expectedKPlusRelativeVariances =
      listOf(0.0, 5.5611419682E-04, 1.2788745237E-03, 2.3511029170E-03, 4.0470322809E-03)
    val expectedCountVariances =
      listOf(
        5.4087863546E+04,
        6.0087743606E+04,
        6.6087543666E+04,
        7.2087263726E+04,
        6.5514174562E+05,
      )
    val expectedKPlusCountVariances =
      listOf(
        5.2278213228E+05,
        5.6487119570E+05,
        6.0096117907E+05,
        6.3105160238E+05,
        6.5514174562E+05,
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(relativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            relativeVariances.getValue(frequency),
            expectedRelativeVariances[frequency - 1],
          )
        )
        .of(expectedRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusRelativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusRelativeVariances.getValue(frequency),
            expectedKPlusRelativeVariances[frequency - 1],
          )
        )
        .of(expectedKPlusRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(countVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            countVariances.getValue(frequency),
            expectedCountVariances[frequency - 1],
          )
        )
        .of(expectedCountVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusCountVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusCountVariances.getValue(frequency),
            expectedKPlusCountVariances[frequency - 1],
          )
        )
        .of(expectedKPlusCountVariances[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for HonestMajorityShareShuffle reach-frequency when small total reach, large sampling width`() {
    val frequencyVectorSize = 10_000_000L
    val vidSamplingIntervalWidth = 1.0
    val totalReach = 10_000L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { frequency -> frequency / 15.0 }
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
        frequencyMeasurementParams,
      )

    val (relativeVariances, kPlusRelativeVariances, countVariances, kPlusCountVariances) =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRelativeVariances =
      listOf(
        5.0011419626E-06,
        5.5781968044E-06,
        6.5399548741E-06,
        7.8864161717E-06,
        3.8470322789E-05,
      )
    val expectedKPlusRelativeVariances =
      listOf(0.0, 5.0011419626E-06, 1.1348745223E-05, 2.1351029148E-05, 3.8470322789E-05)
    val expectedCountVariances =
      listOf(
        4.8087903486E+02,
        4.8087903486E+02,
        4.8087903486E+02,
        4.8087903486E+02,
        6.2514274532E+03,
      )
    val expectedKPlusCountVariances =
      listOf(
        4.3279113138E+03,
        4.8087903486E+03,
        5.2896693835E+03,
        5.7705484184E+03,
        6.2514274532E+03,
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(relativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            relativeVariances.getValue(frequency),
            expectedRelativeVariances[frequency - 1],
          )
        )
        .of(expectedRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusRelativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusRelativeVariances.getValue(frequency),
            expectedKPlusRelativeVariances[frequency - 1],
          )
        )
        .of(expectedKPlusRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(countVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            countVariances.getValue(frequency),
            expectedCountVariances[frequency - 1],
          )
        )
        .of(expectedCountVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusCountVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusCountVariances.getValue(frequency),
            expectedKPlusCountVariances[frequency - 1],
          )
        )
        .of(expectedKPlusCountVariances[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for HonestMajorityShareShuffle reach-frequency when large total reach, small sampling width`() {
    val frequencyVectorSize = 10_000_000L
    val vidSamplingIntervalWidth = 0.1
    val totalReach = 90_000_000L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { frequency -> frequency / 15.0 }
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
        frequencyMeasurementParams,
      )

    val (relativeVariances, kPlusRelativeVariances, countVariances, kPlusCountVariances) =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRelativeVariances =
      listOf(
        6.2283965338E-09,
        1.1562442334E-08,
        1.6008074178E-08,
        1.9565292067E-08,
        2.2269716670E-08,
      )
    val expectedKPlusRelativeVariances =
      listOf(0.0, 6.2283965338E-09, 1.6014010957E-08, 2.4026359535E-08, 2.2269716670E-08)
    val expectedCountVariances =
      listOf(
        5.0808088411E+07,
        9.5088088854E+07,
        1.3288808923E+08,
        1.6420808955E+08,
        1.8962514464E+08,
      )
    val expectedKPlusCountVariances =
      listOf(
        8.1432791941E+07,
        1.2144088024E+08,
        1.8196896875E+08,
        2.2413705708E+08,
        1.8962514464E+08,
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(relativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            relativeVariances.getValue(frequency),
            expectedRelativeVariances[frequency - 1],
          )
        )
        .of(expectedRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusRelativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusRelativeVariances.getValue(frequency),
            expectedKPlusRelativeVariances[frequency - 1],
          )
        )
        .of(expectedKPlusRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(countVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            countVariances.getValue(frequency),
            expectedCountVariances[frequency - 1],
          )
        )
        .of(expectedCountVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusCountVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusCountVariances.getValue(frequency),
            expectedKPlusCountVariances[frequency - 1],
          )
        )
        .of(expectedKPlusCountVariances[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for HonestMajorityShareShuffle reach-frequency when large total reach, large sampling width`() {
    val frequencyVectorSize = 10_000_000L
    val vidSamplingIntervalWidth = 1.0
    val totalReach = 9_000_000L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { frequency -> frequency / 15.0 }
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
        frequencyMeasurementParams,
      )

    val (relativeVariances, kPlusRelativeVariances, countVariances, kPlusCountVariances) =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRelativeVariances =
      listOf(
        6.1742493365E-12,
        6.8866627215E-12,
        8.0740183631E-12,
        9.7363162614E-12,
        4.7494225665E-11,
      )
    val expectedKPlusRelativeVariances =
      listOf(0.0, 6.1742493365E-12, 1.4010796571E-11, 2.6359295244E-11, 4.7494225665E-11)
    val expectedCountVariances =
      listOf(
        4.8087903486E+02,
        4.8087903486E+02,
        4.8087903486E+02,
        4.8087903486E+02,
        6.2514274532E+03,
      )
    val expectedKPlusCountVariances =
      listOf(
        4.3279113138E+03,
        4.8087903486E+03,
        5.2896693835E+03,
        5.7705484184E+03,
        6.2514274532E+03,
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(relativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            relativeVariances.getValue(frequency),
            expectedRelativeVariances[frequency - 1],
          )
        )
        .of(expectedRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusRelativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusRelativeVariances.getValue(frequency),
            expectedKPlusRelativeVariances[frequency - 1],
          )
        )
        .of(expectedKPlusRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(countVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            countVariances.getValue(frequency),
            expectedCountVariances[frequency - 1],
          )
        )
        .of(expectedCountVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusCountVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusCountVariances.getValue(frequency),
            expectedKPlusCountVariances[frequency - 1],
          )
        )
        .of(expectedKPlusCountVariances[frequency - 1])
    }
  }

  @Test
  fun `computeMeasurementVariance returns for HonestMajorityShareShuffle reach-frequency when maximum frequency is 1`() {
    val frequencyVectorSize = 10_000_000L
    val vidSamplingIntervalWidth = 0.1
    val totalReach = 50_000_000L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
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
        frequencyMeasurementParams,
      )

    val (relativeVariances, kPlusRelativeVariances, countVariances, kPlusCountVariances) =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRelativeVariances = 0.0
    val expectedKPlusRelativeVariances = 0.0
    val expectedCountVariances = 2.2543279338E+08
    val expectedKPlusCountVariances = 2.2543279338E+08

    assertThat(relativeVariances.getValue(1))
      .isWithin(computeErrorTolerance(relativeVariances.getValue(1), expectedRelativeVariances))
      .of(expectedRelativeVariances)
    assertThat(kPlusRelativeVariances.getValue(1))
      .isWithin(
        computeErrorTolerance(kPlusRelativeVariances.getValue(1), expectedKPlusRelativeVariances)
      )
      .of(expectedKPlusRelativeVariances)
    assertThat(countVariances.getValue(1))
      .isWithin(computeErrorTolerance(countVariances.getValue(1), expectedCountVariances))
      .of(expectedCountVariances)
    assertThat(kPlusCountVariances.getValue(1))
      .isWithin(computeErrorTolerance(kPlusCountVariances.getValue(1), expectedKPlusCountVariances))
      .of(expectedKPlusCountVariances)
  }

  @Test
  fun `computeMeasurementVariance returns for HonestMajorityShareShuffle reach-frequency when reach is too small`() {
    val frequencyVectorSize = 10_000_000L
    val vidSamplingIntervalWidth = 1.0
    val totalReach = 10L
    val reachDpParams = DpParams(0.1, 1e-9)
    val reachMeasurementParams =
      ReachMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        reachDpParams,
        NoiseMechanism.GAUSSIAN,
      )
    val reachMeasurementVarianceParams =
      ReachMeasurementVarianceParams(totalReach, reachMeasurementParams)
    val reachMeasurementVariance =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        reachMeasurementVarianceParams,
      )

    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { frequency -> frequency / 15.0 }
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
        frequencyMeasurementParams,
      )

    val (relativeVariances, kPlusRelativeVariances, countVariances, kPlusCountVariances) =
      VariancesImpl.computeMeasurementVariance(
        HonestMajorityShareShuffleMethodology(frequencyVectorSize),
        frequencyMeasurementVarianceParams,
      )

    val expectedRelativeVariances =
      listOf(
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedKPlusRelativeVariances =
      listOf(
        0.0,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
        0.08333333333333336,
      )
    val expectedCountVariances =
      listOf(
        4.8087903486E+02,
        4.8087903486E+02,
        4.8087903486E+02,
        4.8087903486E+02,
        6.2514274532E+03,
      )
    val expectedKPlusCountVariances =
      listOf(
        4.3279113138E+03,
        4.8087903486E+03,
        5.2896693835E+03,
        5.7705484184E+03,
        6.2514274532E+03,
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(relativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            relativeVariances.getValue(frequency),
            expectedRelativeVariances[frequency - 1],
          )
        )
        .of(expectedRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusRelativeVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusRelativeVariances.getValue(frequency),
            expectedKPlusRelativeVariances[frequency - 1],
          )
        )
        .of(expectedKPlusRelativeVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(countVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            countVariances.getValue(frequency),
            expectedCountVariances[frequency - 1],
          )
        )
        .of(expectedCountVariances[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(kPlusCountVariances.getValue(frequency))
        .isWithin(
          computeErrorTolerance(
            kPlusCountVariances.getValue(frequency),
            expectedKPlusCountVariances[frequency - 1],
          )
        )
        .of(expectedKPlusCountVariances[frequency - 1])
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(0.001, 1e6.toLong(), 1e8.toLong()),
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = VariancesImpl.computeMetricVariance(params)

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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(0.001, 1e6.toLong(), 1e8.toLong()),
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
                vidSamplingInterval = VidSamplingInterval(0.3, 0.7),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = VariancesImpl.computeMetricVariance(params)

    val expected = 22459.91623264358
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(0.001, 1e6.toLong(), 1e8.toLong()),
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
                vidSamplingInterval = VidSamplingInterval(0.5, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = VariancesImpl.computeMetricVariance(params)

    val expected = 27400.78312697515
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(0.001, 1e6.toLong(), 1e8.toLong()),
      )

    val varianceSingleMeasurement =
      VariancesImpl.computeMetricVariance(
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
                vidSamplingInterval = VidSamplingInterval(0.5, 0.5),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = CustomDirectScalarMethodology(varianceOtherSingleMeasurement),
      )

    val params =
      ReachMetricVarianceParams(
        listOf(weightedReachMeasurementVarianceParams, otherWeightedReachMeasurementVarianceParams)
      )

    val variance = VariancesImpl.computeMetricVariance(params)

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
                vidSamplingInterval = VidSamplingInterval(0.1, 0.9),
                dpParams = DpParams(0.1, 1e-9),
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(0.001, 1e6.toLong(), 1e8.toLong()),
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = DeterministicMethodology,
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    val params =
      ReachMetricVarianceParams(
        listOf(
          unionWeightedReachMeasurementVarianceParams,
          weightedReachMeasurementVarianceParams,
          otherWeightedReachMeasurementVarianceParams,
        )
      )

    val variance = VariancesImpl.computeMetricVariance(params)

    val expected = 11568.523587919317
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance for reach throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(ReachMetricVarianceParams(listOf()))
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
                noiseMechanism = NoiseMechanism.GAUSSIAN,
              ),
          ),
        methodology = CustomDirectFrequencyMethodology(mapOf(), mapOf()),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
        ReachMetricVarianceParams(listOf(weightedReachMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for reach-frequency throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(FrequencyMetricVarianceParams(listOf()))
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
                maximumFrequency = 10,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(
        FrequencyMetricVarianceParams(
          listOf(
            weightedFrequencyMeasurementVarianceParams,
            weightedFrequencyMeasurementVarianceParams,
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
                maximumFrequency = 10,
              ),
          ),
        methodology = CustomDirectScalarMethodology(0.0),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
        FrequencyMetricVarianceParams(listOf(weightedFrequencyMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance returns for impression`() {
    val impressions = 3e8.toLong()
    val vidSamplingIntervalWidth = 1e-2
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumFrequencyPerUser = 200
    val impressionMeasurementParams =
      ImpressionMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumFrequencyPerUser,
        NoiseMechanism.GAUSSIAN,
      )
    val impressionMeasurementVariancesParams =
      ImpressionMeasurementVarianceParams(impressions, impressionMeasurementParams)

    val weight = 2
    val coefficient = weight * weight
    val weightedImpressionMeasurementVarianceParams =
      WeightedImpressionMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = weight,
        measurementVarianceParams = impressionMeasurementVariancesParams,
        methodology = DeterministicMethodology,
      )

    val variance =
      VariancesImpl.computeMetricVariance(
        ImpressionMetricVarianceParams(listOf(weightedImpressionMeasurementVarianceParams))
      )
    val expected = 90027432806400.0 * coefficient
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance for impression throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(ImpressionMetricVarianceParams(listOf()))
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
                maximumFrequencyPerUser = 10,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(
        ImpressionMetricVarianceParams(
          listOf(
            weightedImpressionMeasurementVarianceParams,
            weightedImpressionMeasurementVarianceParams,
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
                maximumFrequencyPerUser = 10,
              ),
          ),
        methodology = CustomDirectFrequencyMethodology(mapOf(), mapOf()),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
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
                maximumFrequencyPerUser = 10,
              ),
          ),
        methodology = LiquidLegionsSketchMethodology(1.0, 1L),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
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
                maximumFrequencyPerUser = 10,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(1.0, 1L, 1L),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
        ImpressionMetricVarianceParams(listOf(weightedImpressionMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for impression throws UnsupportedMethodologyException when using HonestMajorityShareShuffle methodology`() {
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
                maximumFrequencyPerUser = 10,
              ),
          ),
        methodology = HonestMajorityShareShuffleMethodology(1000_000L),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
        ImpressionMetricVarianceParams(listOf(weightedImpressionMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance returns for watch duration`() {
    val watchDuration = 1.0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1e-2, 1e-9)
    val maximumDurationPerUser = 1.0
    val watchDurationMeasurementParams =
      WatchDurationMeasurementParams(
        VidSamplingInterval(0.0, vidSamplingIntervalWidth),
        dpParams,
        maximumDurationPerUser,
        NoiseMechanism.GAUSSIAN,
      )
    val watchDurationMeasurementVarianceParams =
      WatchDurationMeasurementVarianceParams(watchDuration, watchDurationMeasurementParams)

    val weight = 2
    val coefficient = weight * weight
    val weightedWatchDurationMeasurementVarianceParams =
      WeightedWatchDurationMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = weight,
        measurementVarianceParams = watchDurationMeasurementVarianceParams,
        methodology = DeterministicMethodology,
      )

    val variance =
      VariancesImpl.computeMetricVariance(
        WatchDurationMetricVarianceParams(listOf(weightedWatchDurationMeasurementVarianceParams))
      )
    val expected = 210218.58201600003 * coefficient
    val tolerance = computeErrorTolerance(variance, expected)
    assertThat(variance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMetricVariance for watch duration throws IllegalArgumentException when no measurement params`() {
    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(WatchDurationMetricVarianceParams(listOf()))
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
                maximumDurationPerUser = 10.0,
              ),
          ),
        methodology = DeterministicMethodology,
      )

    assertFailsWith<IllegalArgumentException> {
      VariancesImpl.computeMetricVariance(
        WatchDurationMetricVarianceParams(
          listOf(
            weightedWatchDurationMeasurementVarianceParams,
            weightedWatchDurationMeasurementVarianceParams,
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
                maximumDurationPerUser = 10.0,
              ),
          ),
        methodology = CustomDirectFrequencyMethodology(mapOf(), mapOf()),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
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
                maximumDurationPerUser = 10.0,
              ),
          ),
        methodology = LiquidLegionsSketchMethodology(1.0, 1L),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
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
                maximumDurationPerUser = 10.0,
              ),
          ),
        methodology = LiquidLegionsV2Methodology(1.0, 1L, 1L),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
        WatchDurationMetricVarianceParams(listOf(weightedWatchDurationMeasurementVarianceParams))
      )
    }
  }

  @Test
  fun `computeMetricVariance for watch duration throws UnsupportedMethodologyException when using HonestMajorityShareShuffle methodology`() {
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
                maximumDurationPerUser = 10.0,
              ),
          ),
        methodology = HonestMajorityShareShuffleMethodology(1000_000L),
      )

    assertFailsWith<UnsupportedMethodologyUsageException> {
      VariancesImpl.computeMetricVariance(
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
