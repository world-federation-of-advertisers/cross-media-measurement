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

package org.wfanet.measurement.duchy.mill.trustee.crypto

import com.google.common.truth.Truth.assertThat
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult

private const val MAX_FREQUENCY = 5
private const val FLOAT_COMPARISON_TOLERANCE = 1e-9

private val DEFAULT_DP_PARAMS = differentialPrivacyParams {
  epsilon = 1.0
  delta = 0.99
}

private val REACH_ONLY_SPEC = measurementSpec { reach = MeasurementSpec.Reach.getDefaultInstance() }

private val REACH_AND_FREQUENCY_SPEC = measurementSpec {
  reachAndFrequency = MeasurementSpecKt.reachAndFrequency { maximumFrequency = MAX_FREQUENCY }
}

/** Helper to build a [MeasurementSpec] with the specified privacy parameters. */
private fun MeasurementSpec.withDpParams(
  reachDpParams: DifferentialPrivacyParams? = DEFAULT_DP_PARAMS,
  frequencyDpParams: DifferentialPrivacyParams? = DEFAULT_DP_PARAMS,
): MeasurementSpec {
  return when (checkNotNull(measurementTypeCase)) {
    MeasurementSpec.MeasurementTypeCase.REACH ->
      copy {
        if (reachDpParams != null) {
          reach = MeasurementSpecKt.reach { privacyParams = reachDpParams }
        }
      }
    MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
      copy {
        reachAndFrequency =
          reachAndFrequency.copy {
            if (reachDpParams != null) {
              reachPrivacyParams = reachDpParams
            }
            if (frequencyDpParams != null) {
              frequencyPrivacyParams = frequencyDpParams
            }
          }
      }
    else -> error("Unsupported spec type for this helper.")
  }
}

/**
 * Calculates a test tolerance for a noised value.
 *
 * The standard deviation of the Gaussian noise is `sqrt(2 * ln(1.25 / delta)) / epsilon`. We return
 * a tolerance of 6 standard deviations, which means a correct implementation should pass this check
 * with near-certainty.
 *
 * @param l2Sensitivity The L2 sensitivity of the query (1 for a simple count).
 */
private fun getNoiseTolerance(
  dpParams: DifferentialPrivacyParams,
  l2Sensitivity: Double = 1.0,
): Long {
  val stddev = sqrt(2 * ln(1.25 / dpParams.delta)) * l2Sensitivity / dpParams.epsilon
  return (6 * stddev).toLong()
}

@RunWith(JUnit4::class)
class TrusTeeCryptorImplTest {
  @Test
  fun `constructor throws for unsupported measurement type`() {
    val impressionSpec = measurementSpec {
      impression = MeasurementSpec.Impression.getDefaultInstance()
    }
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeCryptorImpl(impressionSpec) }
    assertThat(exception.message).contains("TrusTEE only supports Reach and Reach & Frequency")
  }

  @Test
  fun `constructor throws for invalid maxFrequency of zero`() {
    val specWithZeroMaxFrequency = measurementSpec {
      reachAndFrequency = MeasurementSpecKt.reachAndFrequency { maximumFrequency = 0 }
    }
    val exception =
      assertFailsWith<IllegalArgumentException> { TrusTeeCryptorImpl(specWithZeroMaxFrequency) }
    assertThat(exception.message).contains("Invalid max frequency")
  }

  @Test
  fun `constructor throws for invalid negative maxFrequency`() {
    val specWithNegativeMaxFrequency = measurementSpec {
      reachAndFrequency = MeasurementSpecKt.reachAndFrequency { maximumFrequency = -1 }
    }
    val exception =
      assertFailsWith<IllegalArgumentException> { TrusTeeCryptorImpl(specWithNegativeMaxFrequency) }
    assertThat(exception.message).contains("Invalid max frequency")
  }

  @Test
  fun `addFrequencyVector initializes and aggregates a single vector`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    val vector = intArrayOf(1, 1, 0, 2)

    cryptor.addFrequencyVector(vector)
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isEqualTo(3)
    val expectedDistribution =
      mapOf(0L to 0.25, 1L to 0.5, 2L to 0.25, 3L to 0.0, 4L to 0.0, 5L to 0.0)
    assertThat(result.frequency).isEqualTo(expectedDistribution)
  }

  @Test
  fun `addFrequencyVector throws for empty vector`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    val exception =
      assertFailsWith<IllegalArgumentException> { cryptor.addFrequencyVector(intArrayOf()) }
    assertThat(exception.message).contains("Input frequency vector cannot be empty")
  }

  @Test
  fun `addFrequencyVector correctly sums multiple vectors`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    val vector1 = intArrayOf(1, 0, 1, 3)
    val vector2 = intArrayOf(0, 1, 1, 1)
    val vector3 = intArrayOf(1, 1, 0, 0)

    cryptor.addFrequencyVector(vector1)
    cryptor.addFrequencyVector(vector2)
    cryptor.addFrequencyVector(vector3)
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 2, 2, 4].
    assertThat(result.reach).isEqualTo(4)
    val expectedDistribution =
      mapOf(0L to 0.0, 1L to 0.0, 2L to 0.75, 3L to 0.0, 4L to 0.25, 5L to 0.0)
    assertThat(result.frequency).isEqualTo(expectedDistribution)
  }

  @Test
  fun `addFrequencyVector caps frequencies at the specified maxFrequency`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    val vector1 = intArrayOf(3, 4, 5)
    val vector2 = intArrayOf(3, 0, 1)

    cryptor.addFrequencyVector(vector1)
    cryptor.addFrequencyVector(vector2)
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [5, 4, 5].
    assertThat(result.reach).isEqualTo(3)
    val expectedDistribution =
      mapOf(0L to 0.0, 1L to 0.0, 2L to 0.0, 3L to 0.0, 4L to (1.0 / 3.0), 5L to (2.0 / 3.0))

    assertThat(result.frequency.keys).isEqualTo(expectedDistribution.keys)
    for ((freq, expectedValue) in expectedDistribution) {
      assertThat(result.frequency[freq]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expectedValue)
    }
  }

  @Test
  fun `addFrequencyVector throws for mismatched vector sizes`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    cryptor.addFrequencyVector(intArrayOf(1, 2, 3))

    val exception =
      assertFailsWith<IllegalArgumentException> { cryptor.addFrequencyVector(intArrayOf(1, 2)) }
    assertThat(exception.message).contains("size")
  }

  @Test
  fun `computeResult throws IllegalStateException if no vectors are added`() {
    val cryptor = TrusTeeCryptorImpl(REACH_ONLY_SPEC)
    assertFailsWith<IllegalStateException> { cryptor.computeResult() }
  }

  @Test
  fun `computeResult with all-zero frequency vectors returns zero results`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    cryptor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    cryptor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    cryptor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isEqualTo(0)
    val expectedDistribution = (0L..MAX_FREQUENCY).associateWith { 0.0 }.toMutableMap()
    expectedDistribution[0L] = 1.0
    assertThat(result.frequency).isEqualTo(expectedDistribution)
  }

  @Test
  fun `computeResult for Reach computes correct raw reach without DP`() {
    val cryptor = TrusTeeCryptorImpl(REACH_ONLY_SPEC)
    val vector1 = intArrayOf(1, 0, 1, 0, 1)
    val vector2 = intArrayOf(0, 1, 1, 0, 0)
    val vector3 = intArrayOf(1, 0, 0, 0, 1)

    cryptor.addFrequencyVector(vector1)
    cryptor.addFrequencyVector(vector2)
    cryptor.addFrequencyVector(vector3)

    // Aggregated frequencies: [1, 1, 1, 0, 1]
    val result = cryptor.computeResult() as ReachResult
    assertThat(result.reach).isEqualTo(4)
  }

  @Test
  fun `computeResult for Reach computes noised reach with DP`() {
    val spec = REACH_ONLY_SPEC.withDpParams(reachDpParams = DEFAULT_DP_PARAMS)
    val cryptor = TrusTeeCryptorImpl(spec)
    val vector1 = intArrayOf(1, 0, 1, 0, 1)
    val vector2 = intArrayOf(0, 1, 1, 0, 0)
    val vector3 = intArrayOf(1, 0, 0, 0, 1)

    cryptor.addFrequencyVector(vector1)
    cryptor.addFrequencyVector(vector2)
    cryptor.addFrequencyVector(vector3)
    val result = cryptor.computeResult() as ReachResult

    // Aggregated frequencies: [1, 1, 1, 0, 1]
    val rawReach = 4L
    val tolerance = getNoiseTolerance(DEFAULT_DP_PARAMS)
    assertThat(result.reach).isAtMost(rawReach + tolerance)
    assertThat(result.reach).isAtLeast(max(0L, rawReach - tolerance))
  }

  // region Test Cases for Reach and Frequency Computations
  @Test
  fun `computeResult for R&F computes correct raw results without DP`() {
    val cryptor = TrusTeeCryptorImpl(REACH_AND_FREQUENCY_SPEC)
    val vector1 = intArrayOf(1, 2, 0, 1, 3)
    val vector2 = intArrayOf(0, 1, 1, 0, 1)
    val vector3 = intArrayOf(1, 0, 0, 1, 1)

    cryptor.addFrequencyVector(vector1)
    cryptor.addFrequencyVector(vector2)
    cryptor.addFrequencyVector(vector3)
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 3, 1, 2, 5].
    // Histogram: [0, 1, 2, 1, 0, 1].
    assertThat(result.reach).isEqualTo(5)
    val expected =
      mapOf(
        0L to 0.0,
        1L to 1.0 / 5.0,
        2L to 2.0 / 5.0,
        3L to 1.0 / 5.0,
        4L to 0.0,
        5L to 1.0 / 5.0,
      )
    assertThat(result.frequency.keys).isEqualTo(expected.keys)
    for ((k, v) in result.frequency) {
      assertThat(v).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expected[k]!!)
    }
  }

  @Test
  fun `computeResult for R&F computes results with DP params`() {
    val spec =
      REACH_AND_FREQUENCY_SPEC.withDpParams(
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
      )
    val cryptor = TrusTeeCryptorImpl(spec)
    val vector1 = intArrayOf(1, 2, 0, 1, 3)
    val vector2 = intArrayOf(0, 1, 1, 0, 1)
    val vector3 = intArrayOf(1, 0, 0, 1, 1)

    cryptor.addFrequencyVector(vector1)
    cryptor.addFrequencyVector(vector2)
    cryptor.addFrequencyVector(vector3)
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 3, 1, 2, 5].
    // Histogram: [0, 1, 2, 1, 0, 1].
    val rawReach = 5L
    val rawHistogram = longArrayOf(0, 1, 2, 1, 0, 1)
    val totalUsers = rawHistogram.sum()

    val reachTolerance = getNoiseTolerance(DEFAULT_DP_PARAMS)
    assertThat(result.reach).isAtMost(rawReach + reachTolerance)
    assertThat(result.reach).isAtLeast(max(0L, rawReach - reachTolerance))

    assertThat(result.frequency.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
    assertThat(result.frequency.keys).containsExactlyElementsIn(0L..MAX_FREQUENCY)

    // Check that each frequency probability is within a plausible range.
    val binCountTolerance = getNoiseTolerance(DEFAULT_DP_PARAMS)
    val numBins = (MAX_FREQUENCY + 1).toDouble()
    val totalCountTolerance = getNoiseTolerance(DEFAULT_DP_PARAMS, l2Sensitivity = sqrt(numBins))

    val minTotalNoisedCount = max(1.0, (totalUsers - totalCountTolerance).toDouble())
    val maxTotalNoisedCount = (totalUsers + totalCountTolerance).toDouble()

    for (i in 0..MAX_FREQUENCY) {
      val rawCount = rawHistogram[i]
      val minBinNoisedCount = max(0.0, (rawCount - binCountTolerance).toDouble())
      val maxBinNoisedCount = (rawCount + binCountTolerance).toDouble()

      val minProbability = minBinNoisedCount / maxTotalNoisedCount
      val maxProbability = maxBinNoisedCount / minTotalNoisedCount

      assertThat(result.frequency[i.toLong()]).isAtLeast(minProbability)
      assertThat(result.frequency[i.toLong()]).isAtMost(maxProbability)
    }
  }

  @Test
  fun `computeResult with all-zero frequency vectors returns zero results with dp params`() {
    val spec =
      REACH_AND_FREQUENCY_SPEC.withDpParams(
        reachDpParams =
          differentialPrivacyParams {
            epsilon = 100.0
            delta = 0.99
          },
        frequencyDpParams =
          differentialPrivacyParams {
            epsilon = 100.0
            delta = 0.99
          },
      )
    val cryptor = TrusTeeCryptorImpl(spec)
    cryptor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    cryptor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    cryptor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    val result = cryptor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isEqualTo(0)
    assertThat(result.frequency[0L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }
}
