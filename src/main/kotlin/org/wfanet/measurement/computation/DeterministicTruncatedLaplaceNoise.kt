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

import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * Deterministic truncated-Laplace noising of a [SampledReachAndFrequency].
 *
 * The noise is keyless and reproducible: each output's draw is seeded from a [fingerprint] of the
 * combined frequency vector and an output label ([DeterministicTruncatedLaplaceNoiseSampler]), so
 * the same measurement always yields the same noise and it cannot be averaged away across repeated
 * queries.
 *
 * Reach is noised as a single scalar draw (label 0), not the sum of the noised frequency buckets
 * (which would scale reach noise with `maxFrequency`).
 */
object DeterministicTruncatedLaplaceNoise {
  /**
   * Fingerprint of the combined frequency vector, used as the noise seed.
   *
   * SHA-256 binds the seed to the vector's contents, so the noise cannot change unless the data
   * changes.
   */
  fun fingerprint(combinedFrequencyVector: IntArray): ByteArray {
    val buffer = ByteBuffer.allocate(combinedFrequencyVector.size * Int.SIZE_BYTES)
    buffer.asIntBuffer().put(combinedFrequencyVector)
    return MessageDigest.getInstance("SHA-256").digest(buffer.array())
  }

  /**
   * Returns a noised copy of [sampled].
   *
   * The frequency histogram is noised bucket-by-bucket with [frequencyEpsilon]; reach is noised as
   * a scalar with [reachEpsilon], mirroring how the Gaussian path splits reach and frequency
   * privacy parameters. Each output receives a single draw seeded from [fingerprint]. Counts are
   * clamped to be non-negative.
   *
   * @param sampled the in-sample reach and frequency to noise.
   * @param fingerprint the [fingerprint] of the combined frequency vector.
   */
  fun noise(
    sampled: SampledReachAndFrequency,
    fingerprint: ByteArray,
    reachEpsilon: Double,
    frequencyEpsilon: Double,
    sensitivity: Double,
    truncationBound: Int,
  ): SampledReachAndFrequency {
    val rawHistogram = sampled.frequencyHistogram
    val maxFrequency = rawHistogram.size

    val reachSampler =
      DeterministicTruncatedLaplaceNoiseSampler(reachEpsilon, sensitivity, truncationBound)
    val frequencySampler =
      DeterministicTruncatedLaplaceNoiseSampler(frequencyEpsilon, sensitivity, truncationBound)

    val noisedReach =
      (sampled.sampledReach + reachSampler.sampleRounded(fingerprint, label(0))).coerceAtLeast(0L)

    val noisedHistogram = LongArray(maxFrequency)
    for (frequency in 1..maxFrequency) {
      noisedHistogram[frequency - 1] =
        (rawHistogram[frequency - 1] +
            frequencySampler.sampleRounded(fingerprint, label(frequency)))
          .coerceAtLeast(0L)
    }

    return SampledReachAndFrequency(noisedReach, noisedHistogram)
  }

  /** The output label that makes each bucket's draw independent (bucket 0 is reach). */
  private fun label(frequency: Int): ByteArray =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(frequency).array()
}
