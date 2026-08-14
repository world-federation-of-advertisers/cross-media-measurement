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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClippingNoiseSource

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
  fun `dynamically clipped count includes every impression when the clip exceeds the tail`() {
    // A large charge makes the search run to the end of the tail, landing past frequency 5, so
    // nobody is clipped and the count is the uncapped total.
    val result = dynamicallyClipped(PRECISE_RHO)

    assertThat(result.clip).isGreaterThan(DYNAMIC_MAX_FREQUENCY)
    assertThat(result.value).isEqualTo(285L)
  }

  @Test
  fun `dynamically clipped count clips each user at the derived clip`() {
    val result = dynamicallyClipped(COARSE_RHO)

    // sum(min(frequency, 4)) = 100*1 + 50*2 + 20*3 + 5*4.
    assertThat(result.clip).isEqualTo(4)
    assertThat(result.value).isEqualTo(280L)
  }

  @Test
  fun `dynamically clipped count is scaled by vidSamplingIntervalWidth`() {
    val result = dynamicallyClipped(COARSE_RHO, vidSamplingIntervalWidth = 0.5)

    assertThat(result.value).isEqualTo(560L)
  }

  @Test
  fun `an all-zero frequency vector is still noised`() {
    // The empty case has no distribution to search, but releasing an exact zero beside a noised
    // value for a vector holding one impression would leave the two distinguishable.
    val result =
      ImpressionComputations.computeDynamicallyClippedImpressionCount(
        frequencyVector = IntArray(100),
        queryRho = COARSE_RHO,
        noiseSource = { _, _ -> 1.0 },
        vidSamplingIntervalWidth = 1.0,
        resultMinimumThresholds = null,
      )

    assertThat(result.value).isGreaterThan(0L)
  }

  @Test
  fun `dynamically clipped variance does not depend on the highest frequency in the data`() {
    // Both vectors agree on every bar the clip covers, so they yield the same clip and the same
    // count, and differ only in how long the histogram is. A variance that read the histogram
    // length would publish that raw value.
    val short = dynamicallyClipped(COARSE_RHO)
    val long = dynamicallyClipped(COARSE_RHO, frequencyVector = LONG_TAILED_FREQUENCY_VECTOR)

    assertThat(long.clip).isEqualTo(short.clip)
    assertThat(long.value).isEqualTo(short.value)
    assertThat(long.variance).isEqualTo(short.variance)
  }

  @Test
  fun `dynamically clipped variance falls as the charge rises`() {
    val noisy = dynamicallyClipped(COARSE_RHO)
    val precise = dynamicallyClipped(PRECISE_RHO)

    assertThat(noisy.variance).isGreaterThan(0.0)
    assertThat(precise.variance).isLessThan(noisy.variance)
  }

  @Test
  fun `dynamically clipped count below the minimum impressions is suppressed`() {
    val result =
      dynamicallyClipped(
        COARSE_RHO,
        thresholds = ResultMinimumThresholds(minUsers = 1, minImpressions = 500),
      )

    assertThat(result.value).isEqualTo(0L)
  }

  @Test
  fun `dynamically clipped count with too few users is suppressed`() {
    // Bar 0 is the noised user count, 175 here, so it fails a threshold above that.
    val result =
      dynamicallyClipped(
        COARSE_RHO,
        thresholds = ResultMinimumThresholds(minUsers = 200, minImpressions = 1),
      )

    assertThat(result.value).isEqualTo(0L)
  }

  @Test
  fun `dynamically clipped count meeting both thresholds passes through`() {
    val result =
      dynamicallyClipped(
        COARSE_RHO,
        thresholds = ResultMinimumThresholds(minUsers = 100, minImpressions = 100),
      )

    assertThat(result.value).isEqualTo(280L)
  }

  @Test
  fun `a non-positive vidSamplingIntervalWidth is rejected for dynamic clipping`() {
    assertFailsWith<IllegalArgumentException> {
      dynamicallyClipped(COARSE_RHO, vidSamplingIntervalWidth = 0.0)
    }
  }

  private fun dynamicallyClipped(
    queryRho: Double,
    frequencyVector: IntArray = DYNAMIC_FREQUENCY_VECTOR,
    vidSamplingIntervalWidth: Double = 1.0,
    thresholds: ResultMinimumThresholds? = null,
  ) =
    ImpressionComputations.computeDynamicallyClippedImpressionCount(
      frequencyVector = frequencyVector,
      queryRho = queryRho,
      noiseSource = NO_NOISE,
      vidSamplingIntervalWidth = vidSamplingIntervalWidth,
      resultMinimumThresholds = thresholds,
    )

  private fun deterministicNoiser(seedVector: IntArray = SEED_VECTOR) =
    DeterministicTruncatedLaplaceResultNoiser(
      combinedFrequencyVector = seedVector,
      contributionCount = 1,
      maxFrequencyPerUser = MAX_FREQUENCY,
    )

  companion object {

    /** Draws nothing, so the bars are exact and the assertions can be too. */
    private val NO_NOISE = DynamicClippingNoiseSource { _, _ -> 0.0 }

    private const val DYNAMIC_MAX_FREQUENCY = 5

    /**
     * 175 users at frequencies {1: 100, 2: 50, 3: 20, 5: 5}, so the cumulative histogram is
     * [175, 75, 25, 5, 5] and the uncapped total is 285.
     */
    private val DYNAMIC_FREQUENCY_VECTOR: IntArray =
      (List(100) { 1 } + List(50) { 2 } + List(20) { 3 } + List(5) { DYNAMIC_MAX_FREQUENCY })
        .toIntArray()

    /** The same distribution with its tail stretched to frequency 9, doubling the histogram. */
    private val LONG_TAILED_FREQUENCY_VECTOR: IntArray =
      (List(100) { 1 } + List(50) { 2 } + List(20) { 3 } + List(5) { 9 }).toIntArray()

    /** Large enough that the search runs the tail out to its end. */
    private const val PRECISE_RHO = 1e10

    /** Sized so the stopping rule cuts at frequency 4, inside the histogram. */
    private const val COARSE_RHO = 2e-4

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
