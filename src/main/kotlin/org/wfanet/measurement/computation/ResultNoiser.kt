// Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.privacy.differentialprivacy.GaussianNoise
import java.nio.ByteBuffer
import java.security.MessageDigest
import kotlin.math.min

/**
 * Applies noise to the quantities a reach/frequency computation releases.
 *
 * [ReachAndFrequencyComputations] owns the scaling, capping and thresholding; the mechanism owns
 * the draws. Each released quantity has its own method so a mechanism can calibrate it to that
 * quantity's sensitivity.
 */
interface ResultNoiser {
  /** Returns the in-sample reach with noise applied. Callers clamp and scale the result. */
  fun noiseReach(reachInSample: Long): Long

  /**
   * Returns the noised impression count that gates `min_impressions`. The mechanism derives the
   * count itself, since the per-user cap it applies sets the sensitivity it noises against.
   */
  fun noiseImpressionsFromFrequencyHistogram(frequencyHistogram: LongArray): Long

  /** Returns the count for the frequency bucket at [index] (frequency `index + 1`) with noise. */
  fun noiseFrequencyBucket(index: Int, count: Long): Long
}

/** A [ResultNoiser] that releases the raw values. */
object NoNoise : ResultNoiser {
  override fun noiseReach(reachInSample: Long): Long = reachInSample

  override fun noiseImpressionsFromFrequencyHistogram(frequencyHistogram: LongArray): Long =
    frequencyHistogram.weightedSum(cap = null)

  override fun noiseFrequencyBucket(index: Int, count: Long): Long = count
}

/**
 * A [ResultNoiser] drawing continuous Gaussian noise.
 *
 * Reach and the impression threshold draw from [reachDpParams]; frequency buckets draw from
 * [frequencyDpParams]. A reach-only measurement has a single set of params and never draws a
 * bucket, so it may pass the same value for both.
 */
class GaussianResultNoiser(
  private val reachDpParams: DifferentialPrivacyParams,
  private val frequencyDpParams: DifferentialPrivacyParams,
  private val maxFrequencyPerUser: Int = 1,
) : ResultNoiser {
  private val noise = GaussianNoise()

  override fun noiseReach(reachInSample: Long): Long =
    noise.addNoise(
      reachInSample,
      L0_SENSITIVITY,
      L_INFINITE_SENSITIVITY,
      reachDpParams.epsilon,
      reachDpParams.delta,
    )

  override fun noiseImpressionsFromFrequencyHistogram(frequencyHistogram: LongArray): Long =
    noise.addNoise(
      frequencyHistogram.weightedSum(cap = maxFrequencyPerUser),
      L0_SENSITIVITY,
      maxFrequencyPerUser.toLong(),
      reachDpParams.epsilon,
      reachDpParams.delta,
    )

  override fun noiseFrequencyBucket(index: Int, count: Long): Long =
    noise
      .addNoise(
        count,
        L0_SENSITIVITY,
        L_INFINITE_SENSITIVITY,
        frequencyDpParams.epsilon,
        frequencyDpParams.delta,
      )
      .coerceAtLeast(0L)
}

/**
 * A [ResultNoiser] drawing deterministic truncated-Laplace noise.
 *
 * Each draw is a pure function of the seed and an output label. Reach uses [REACH_LABEL], the
 * impression threshold uses [IMPRESSION_LABEL], and frequency bucket `f` uses `f`.
 */
class DeterministicTruncatedLaplaceResultNoiser(
  combinedFrequencyVector: IntArray,
  contributionCount: Int,
  private val maxFrequencyPerUser: Int = 1,
) : ResultNoiser {
  private val fingerprint: ByteArray = fingerprint(combinedFrequencyVector, contributionCount)

  // One sampler per released quantity, each calibrated to that quantity's L1 sensitivity: reach and
  // each frequency bucket move by 1 per VID, the capped impression count by maxFrequencyPerUser.
  private val reachSampler by lazy { sampler(UNIT_SENSITIVITY) }
  private val frequencySampler by lazy { sampler(UNIT_SENSITIVITY) }
  private val impressionSampler by lazy { sampler(maxFrequencyPerUser.toDouble()) }

  private fun sampler(sensitivity: Double) =
    DeterministicTruncatedLaplaceNoiseSampler(
      TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(
        DeterministicTruncatedLaplaceParams.EPSILON,
        DeterministicTruncatedLaplaceParams.DELTA,
        sensitivity,
      )
    )

  override fun noiseReach(reachInSample: Long): Long =
    reachInSample + reachSampler.sampleRounded(fingerprint, label(REACH_LABEL))

  override fun noiseImpressionsFromFrequencyHistogram(frequencyHistogram: LongArray): Long =
    // One draw calibrated to the capped count's sensitivity, mirroring the Gaussian mechanism.
    // Deriving this from the bucket draws instead would weight each by its frequency, giving the
    // threshold a noise magnitude that is not calibrated to any sensitivity.
    frequencyHistogram.weightedSum(cap = maxFrequencyPerUser) +
      impressionSampler.sampleRounded(fingerprint, label(IMPRESSION_LABEL))

  override fun noiseFrequencyBucket(index: Int, count: Long): Long =
    (count + frequencySampler.sampleRounded(fingerprint, label(index + 1))).coerceAtLeast(0L)

  private fun label(value: Int): ByteArray =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(value).array()

  companion object {
    private const val REACH_LABEL = 0
    private const val IMPRESSION_LABEL = -1
    private const val UNIT_SENSITIVITY = 1.0

    /**
     * The noise seed: SHA-256 over the combined frequency vector and [contributionCount], which is
     * the count after input suppression.
     */
    fun fingerprint(combinedFrequencyVector: IntArray, contributionCount: Int): ByteArray {
      val buffer = ByteBuffer.allocate((combinedFrequencyVector.size + 1) * Int.SIZE_BYTES)
      buffer.putInt(contributionCount)
      buffer.asIntBuffer().put(combinedFrequencyVector)
      return MessageDigest.getInstance("SHA-256").digest(buffer.array())
    }
  }
}

private const val L0_SENSITIVITY = 1
private const val L_INFINITE_SENSITIVITY = 1L

/**
 * Returns `sum(frequency * count)` over the histogram, with each contribution capped at [cap] when
 * it is non-null.
 */
private fun LongArray.weightedSum(cap: Int?): Long =
  withIndex().sumOf { (index, count) ->
    val frequency = if (cap == null) index + 1L else min(cap, index + 1).toLong()
    frequency * count
  }
