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

package org.wfanet.measurement.duchy.mill.trustee.processor

import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams

/** Parameters required for a TrusTEE computation. */
sealed interface TrusTeeParams

/** TrusTEE parameters for a Reach-only measurement. */
data class TrusTeeReachParams(
  val vidSamplingIntervalWidth: Float,
  val dpParams: DifferentialPrivacyParams,
) : TrusTeeParams

/** TrusTEE parameters for a Reach-and-Frequency measurement. */
data class TrusTeeReachAndFrequencyParams(
  val maximumFrequency: Int,
  val vidSamplingIntervalWidth: Float,
  val reachDpParams: DifferentialPrivacyParams,
  val frequencyDpParams: DifferentialPrivacyParams,
) : TrusTeeParams

/**
 * A processor for the TrusTEE protocol that aggregates frequency vectors from multiple sources to
 * compute a reach and frequency distribution.
 *
 * This component is stateful. It accumulates data from each call to [addFrequencyVector] and uses
 * the final aggregated data to perform the final computation in [computeResult]. A single instance
 * should be used for a single computation.
 *
 * The processor is not thread-safe.
 */
interface TrusTeeProcessor {
  /** The [TrusTeeParams] for the computation. */
  val trusTeeParams: TrusTeeParams

  /**
   * Adds a frequency vector, represented as a byte array, to the internal state of the processor.
   *
   * A frequency vector is an array where each index represents a unique user ID (or a hash
   * thereof), and the value at that index is the frequency with which that user was observed by a
   * single data provider. The frequencies are represented as 8-bit signed integers, which must be
   * non-negative.
   *
   * This method should be called for each frequency vector from each data provider.
   *
   * @param vector The frequency vector from a single data provider, where each byte is a
   *   non-negative 8-bit signed integer representing a frequency.
   */
  fun addFrequencyVector(vector: ByteArray)

  /**
   * Computes the final reach and frequency result from all previously added vectors.
   *
   * This method should only be called after all frequency vectors for the computation have been
   * added via [addFrequencyVector].
   *
   * The output is a [ComputationResult] protobuf message. It contains either a reach result or a
   * reach-and-frequency result based on the [TrusTeeParams].
   *
   * @return A [ComputationResult] containing the final computation result.
   */
  fun computeResult(): ComputationResult

  /** Factory for creating [TrusTeeProcessor] instances. */
  interface Factory {
    fun create(trusTeeParams: TrusTeeParams): TrusTeeProcessor
  }
}
