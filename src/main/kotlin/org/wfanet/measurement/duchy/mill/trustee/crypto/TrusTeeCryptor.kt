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

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.utils.ComputationResult

/**
 * A cryptor for the TrusTEE protocol that aggregates frequency vectors from multiple sources to
 * compute a reach and frequency distribution.
 *
 * This component is stateful. It accumulates data from each call to [addFrequencyVector] and uses
 * the final aggregated data to perform the final computation in [computeResult]. A single instance
 * should be used for a single computation.
 */
interface TrusTeeCryptor {
  /**
   * The [MeasurementSpec] for the computation. This determines the calculation type (e.g.,
   * reach-only or reach-and-frequency), the maximum frequency cap, and privacy parameters.
   */
  val measurementSpec: MeasurementSpec

  /**
   * Adds a frequency vector to the internal state of the cryptor.
   *
   * A frequency vector is an array where each index represents a unique user ID (or a hash
   * thereof), and the value at that index is the frequency with which that user was observed by a
   * single data provider.
   *
   * This method should be called for each frequency vector from each data provider.
   *
   * @param vector The frequency vector from a single data provider.
   * @throws IllegalArgumentException if the provided vector has a different size than previously
   *   added vectors.
   */
  fun addFrequencyVector(vector: IntArray)

  /**
   * Computes the final reach and frequency result from all previously added vectors.
   *
   * This method should only be called after all frequency vectors for the computation have been
   * added via [addFrequencyVector].
   *
   * The output is a [ComputationResult] protobuf message. It contains either a reach result or
   * reach_and_frequency result based on the measurement spec.
   *
   * @return A [ComputationResult] containing the final computation result.
   */
  fun computeResult(): ComputationResult

  /** Factory for creating [TrusTeeCryptor] instances. */
  interface Factory {
    fun create(measurementSpec: MeasurementSpec): TrusTeeCryptor
  }
}
