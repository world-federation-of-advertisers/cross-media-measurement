// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.computation

import com.google.common.truth.Truth.assertThat
import kotlin.math.ln
import kotlin.math.min
import kotlin.math.sqrt
import org.junit.Test

class ImpressionComputationsTest {

  @Test
  fun `raw impression count calculation without noise`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = NoNoise,
        resultMinimumThresholds = null,
      )
    assertThat(result).isEqualTo(57L)
  }

  @Test
  fun `counts a histogram already capped at maximum frequency`() {
    // HistogramComputations.buildHistogram folds frequencies above the cap into the top bucket, so
    // a histogram capped at 4 has four buckets and the last holds every user seen 4 or more times.
    val histogram = longArrayOf(0L, 5L, 0L, 10L) // 2*5 + 4*10
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = NoNoise,
        resultMinimumThresholds = null,
      )
    assertThat(result).isEqualTo(50L)
  }

  @Test
  fun `impression count is scaled by vidSamplingIntervalWidth`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val scale = 0.5
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = scale,
        noiser = NoNoise,
        resultMinimumThresholds = null,
      )
    assertThat(result).isEqualTo((57L / scale).toLong())
  }

  @Test
  fun `impression count with DP noise is within expected tolerance`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val maxFrequency = 4L
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = GaussianResultNoiser(DP_PARAMS, DP_PARAMS, maxFrequency.toInt()),
        resultMinimumThresholds = null,
      )
    val rawImpressionCount =
      1 * 2 +
        min(maxFrequency, 2) * 4 +
        min(maxFrequency, 4) * 8 +
        min(maxFrequency, 5) * 10 +
        min(maxFrequency, 7) * 2
    val tolerance = calculateNoiseTolerance(DP_PARAMS, 1, maxFrequency.toDouble())
    check(rawImpressionCount > tolerance) {
      "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
    }
    assertThat(result).isAtLeast((rawImpressionCount - tolerance).coerceAtLeast(0))
    assertThat(result).isAtMost((rawImpressionCount + tolerance))
  }

  @Test
  fun `impression count with DP noise is within expected tolerance with smaller interval`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val maxFrequency = 4L
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 0.5,
        noiser = GaussianResultNoiser(DP_PARAMS, DP_PARAMS, maxFrequency.toInt()),
        resultMinimumThresholds = null,
      )
    val rawImpressionCount =
      1 * 2 +
        min(maxFrequency, 2) * 4 +
        min(maxFrequency, 4) * 8 +
        min(maxFrequency, 5) * 10 +
        min(maxFrequency, 7) * 2
    val tolerance = calculateNoiseTolerance(DP_PARAMS, 1, maxFrequency.toDouble())
    check(rawImpressionCount * 2 > tolerance) {
      "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
    }
    assertThat(result).isAtLeast((rawImpressionCount * 2 - tolerance).coerceAtLeast(0))
    assertThat(result).isAtMost((rawImpressionCount * 2 + tolerance))
  }

  @Test
  fun `impression count with K Anonymity is zero for too few unique users`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val resultMinimumThresholds = ResultMinimumThresholds(minUsers = 28, minImpressions = 50)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = NoNoise,
        resultMinimumThresholds = resultMinimumThresholds,
      )
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `impression count with K Anonymity is zero for too few impressions`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val resultMinimumThresholds = ResultMinimumThresholds(minUsers = 28, minImpressions = 100)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = NoNoise,
        resultMinimumThresholds = resultMinimumThresholds,
      )
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `impression count with K Anonymity not changed for sufficient impressions + users`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val resultMinimumThresholds = ResultMinimumThresholds(minUsers = 24, minImpressions = 50)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = NoNoise,
        resultMinimumThresholds = resultMinimumThresholds,
      )
    assertThat(result).isEqualTo(130)
  }

  @Test
  fun `scaled impression count with K Anonymity not changed`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val resultMinimumThresholds = ResultMinimumThresholds(minUsers = 48, minImpressions = 100)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 0.5,
        noiser = NoNoise,
        resultMinimumThresholds = resultMinimumThresholds,
      )
    assertThat(result).isEqualTo(260)
  }

  @Test
  fun `scaled impression count with K Anonymity is zero for too few unique users`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val resultMinimumThresholds = ResultMinimumThresholds(minUsers = 56, minImpressions = 100)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 0.5,
        noiser = NoNoise,
        resultMinimumThresholds = resultMinimumThresholds,
      )
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `impression count with deterministic noise stays within the truncation bound`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = deterministicNoiser(),
        resultMinimumThresholds = null,
      )
    // Capped at MAX_FREQUENCY: 1*2 + 2*4 + 4*8 + 4*10 + 4*2
    val rawImpressionCount = 90L

    assertThat(result).isAtLeast(rawImpressionCount - TRUNCATION_BOUND)
    assertThat(result).isAtMost(rawImpressionCount + TRUNCATION_BOUND)
  }

  @Test
  fun `impression count with deterministic noise is reproducible`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L)

    fun compute() =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = deterministicNoiser(),
        resultMinimumThresholds = null,
      )

    assertThat(compute()).isEqualTo(compute())
  }

  @Test
  fun `impression count with deterministic noise differs for a different seed vector`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L)

    fun compute(seedVector: IntArray) =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser =
          DeterministicTruncatedLaplaceResultNoiser(
            combinedFrequencyVector = seedVector,
            contributionCount = 1,
            maxFrequencyPerUser = MAX_FREQUENCY,
          ),
        resultMinimumThresholds = null,
      )

    assertThat(compute(IntArray(50) { 1 })).isNotEqualTo(compute(SEED_VECTOR))
  }

  @Test
  fun `impression count with deterministic noise applies K Anonymity`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0,
        noiser = deterministicNoiser(),
        // 26 users in the histogram, so the user threshold cannot be met.
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 1000, minImpressions = 1),
      )

    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `dynamically clipped count sums the bars below the clip`() {
    // Users at frequency {1: 100, 2: 50, 3: 20, 5: 5}, so the cumulative histogram counts users at
    // frequency at least k+1: 175, 75, 25, 5, 5. Summing all of it is the uncapped total, 285.
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 5,
        barNoiseVariance = 0.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(result.value).isEqualTo(285L)
  }

  @Test
  fun `clip truncates the sum to the clipped total`() {
    // Clipping at 3 is sum(min(freq, 3)) = 100 + 100 + 60 + 15 = 275, which is the first three
    // bars: 175 + 75 + 25.
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 3,
        barNoiseVariance = 0.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(result.value).isEqualTo(275L)
  }

  @Test
  fun `dynamically clipped count is scaled by vidSamplingIntervalWidth`() {
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 5,
        barNoiseVariance = 0.0,
        vidSamplingIntervalWidth = 0.5,
        resultMinimumThresholds = null,
      )

    assertThat(result.value).isEqualTo(570L)
  }

  @Test
  fun `a negative noised total is clamped to zero`() {
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = listOf(-40.0, -30.0),
        clip = 2,
        barNoiseVariance = 1.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(result.value).isEqualTo(0L)
  }

  @Test
  fun `a result below the minimum impressions is suppressed`() {
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 5,
        barNoiseVariance = 0.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = ResultMinimumThresholds(minImpressions = 300, minUsers = 1),
      )

    assertThat(result.value).isEqualTo(0L)
  }

  @Test
  fun `the first bar gates the minimum users`() {
    // Bar 0 is the noised user count, 175, so it fails a threshold above that without any further
    // draw being taken.
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 5,
        barNoiseVariance = 0.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = ResultMinimumThresholds(minImpressions = 1, minUsers = 200),
      )

    assertThat(result.value).isEqualTo(0L)
  }

  @Test
  fun `noise variance grows with the number of bars summed`() {
    // The count sums `clip` independent bar draws, so the noise term is clip * barNoiseVariance
    // rather than one draw calibrated to the clip. With no VID sampling term at width 1, the
    // variance is exactly that.
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 4,
        barNoiseVariance = 9.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(result.variance).isWithin(1.0e-9).of(36.0)
  }

  @Test
  fun `variance scales by the clip even when it overshoots the histogram`() {
    // The histogram is only as long as the highest frequency in the data, so scaling by the bars
    // actually summed would put that raw value into a released quantity. The clip comes out of the
    // noised search, so the variance uses it and overstates rather than leaks.
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = CUMULATIVE_HISTOGRAM,
        clip = 100,
        barNoiseVariance = 9.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(result.variance).isWithin(1.0e-9).of(900.0)
  }

  @Test
  fun `variance does not depend on the histogram length`() {
    // Two histograms of different length, same clip: a variance that read the length would differ.
    val short =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = listOf(10.0),
        clip = 50,
        barNoiseVariance = 4.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )
    val long =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        noisedCumulativeHistogram = listOf(10.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        clip = 50,
        barNoiseVariance = 4.0,
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(short.variance).isEqualTo(long.variance)
  }

  private fun deterministicNoiser(seedVector: IntArray = SEED_VECTOR) =
    DeterministicTruncatedLaplaceResultNoiser(
      combinedFrequencyVector = seedVector,
      contributionCount = 1,
      maxFrequencyPerUser = MAX_FREQUENCY,
    )

  companion object {
    /**
     * The cumulative histogram for users at frequency {1: 100, 2: 50, 3: 20, 5: 5}: entry `k` is
     * the number of users with frequency at least `k + 1`. Noise-free, so the assertions are exact.
     */
    private val CUMULATIVE_HISTOGRAM = listOf(175.0, 75.0, 25.0, 5.0, 5.0)
    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 2.0, delta = 1e-5)
    /** ceil of the compiled bound at sensitivity MAX_FREQUENCY: 4 * 6.7571 = 27.03. */
    private const val TRUNCATION_BOUND = 28L
    private const val MAX_FREQUENCY = 4
    private val SEED_VECTOR = IntArray(100) { if (it < 90) 1 else 2 }

    private fun getL2Sensitivity(l0Sensitivity: Int, lInfSensitivity: Double): Double {
      return sqrt(l0Sensitivity.toDouble()) * lInfSensitivity
    }

    /**
     * Returns an interval (tolerance) of ±6 standard deviations for the DP noise added. This
     * follows the convention to allow for expected fluctuation in noisy outputs for tests.
     */
    fun calculateNoiseTolerance(
      differentialPrivacyParams: DifferentialPrivacyParams,
      l0Sensitivity: Int = 1,
      lInfSensitivity: Double,
    ): Int {
      // Based on DP with Gaussian noise,
      // stddev = sqrt(2 * ln(1.25/delta)) * l2Sensitivity / epsilon
      // Per Google.privacy.differentialprivacy.GaussianNoise docs
      val stddev =
        sqrt(2.0 * ln(1.25 / differentialPrivacyParams.delta)) *
          getL2Sensitivity(l0Sensitivity, lInfSensitivity) / differentialPrivacyParams.epsilon
      return (6 * stddev).toInt() + 1 // ±6 sigma and round-up
    }
  }
}
