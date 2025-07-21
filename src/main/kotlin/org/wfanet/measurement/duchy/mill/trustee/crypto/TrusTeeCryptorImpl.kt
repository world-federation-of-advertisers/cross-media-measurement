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

import kotlin.math.min
import kotlin.properties.Delegates
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.measurementconsumer.stats.TrusTeeMethodology

/** A concrete, stateful implementation of [TrusTeeCryptor]. */
class TrusTeeCryptorImpl(override val measurementSpec: MeasurementSpec) : TrusTeeCryptor {
  /**
   * Holds the aggregated frequency vector.
   *
   * This is initialized with a copy of the first vector passed to [addFrequencyVector]. Subsequent
   * calls add to this vector. It is null until the first vector is added.
   */
  private var aggregatedFrequencyVector: IntArray? = null

  private var maxFrequency by Delegates.notNull<Int>()

  init {
    require(
      measurementSpec.hasReachAndFrequency() || measurementSpec.hasReach()
    ) { "TrusTEE only supports Reach and Reach & Frequency measurements" }

    maxFrequency =
      if (measurementSpec.hasReachAndFrequency()) measurementSpec.reachAndFrequency.maximumFrequency
      else 1
  }

  override fun addFrequencyVector(vector: IntArray) {
    if (aggregatedFrequencyVector == null) {
      aggregatedFrequencyVector = IntArray(vector.size)
    }

    val currentVector = requireNotNull(aggregatedFrequencyVector)
    require(vector.size == currentVector.size) {
      "Input vector size ${vector.size} does not match expected size ${currentVector.size}"
    }

    for (i in currentVector.indices) {
      currentVector[i] = min(currentVector[i] + vector[i], maxFrequency)
    }
  }

  override fun computeResult(): ComputationResult {
    val frequencyVector =
      aggregatedFrequencyVector
        ?: throw IllegalStateException("addFrequencyVector must be called before computeResult.")

    return when {
      measurementSpec.hasReachAndFrequency() -> computeReachAndFrequency(frequencyVector)
      measurementSpec.hasReach() -> computeReach(frequencyVector)
      else -> error("Unsupported measurement spec type")
    }
  }

  /** Computes a reach-and-frequency result from the aggregated data. */
  private fun computeReachAndFrequency(frequencyVector: IntArray): ComputationResult {
    val frequencyCounts = LongArray(maxFrequency + 1)
    var reach = 0L

    for (userFrequency in frequencyVector) {
      if (userFrequency == 0) {
        continue
      }
      reach++
      // The userFrequency is already capped, so no min() is needed here.
      frequencyCounts[userFrequency]++
    }

    if (reach == 0L) {
      error("Empty input data")
    }

    val frequencyDistribution =
      frequencyCounts
        .mapIndexed { frequency, count ->
          // The value is now the ratio of (count for this frequency) / (total reach).
          frequency.toLong() to count.toDouble() / reach
        }
        // We only include frequencies that were actually observed.
        .filter { (_, ratio) -> ratio > 0.0 }
        .toMap()

    return ReachAndFrequencyResult(
      reach = reach,
      frequency = frequencyDistribution,
      methodology = TrusTeeMethodology(frequencyVector.size.toLong()),
    )
  }

  /** Computes a reach-only result from the aggregated data. */
  private fun computeReach(frequencyVector: IntArray): ComputationResult {
    // Since addFrequencyVector caps values at 1 for Reach measurements, the final vector
    // will only contain 0s and 1s. The count of non-zero elements is the reach.
    val reach = frequencyVector.count { it > 0 }.toLong()
    return ReachResult(
      reach = reach,
      methodology = TrusTeeMethodology(frequencyVector.size.toLong()),
    )
  }

  companion object Factory : TrusTeeCryptor.Factory {
    override fun create(measurementSpec: MeasurementSpec): TrusTeeCryptor {
      return TrusTeeCryptorImpl(measurementSpec)
    }
  }
}
