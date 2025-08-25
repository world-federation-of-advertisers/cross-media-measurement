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

import kotlin.math.min

object HistogramComputations {
  /**
   * Builds a histogram from a frequency vector, counting only non-zero frequencies.
   *
   * Frequencies greater than [maxFrequency] are treated as [maxFrequency].
   *
   * @param frequencyVector An array where each element is a frequency.
   * @param maxFrequency The maximum possible frequency value. The histogram will have
   *   `maxFrequency` buckets.
   * @return A [LongArray] representing the histogram, where index `k-1` is the count with frequency
   *   `k`.
   */
  fun buildHistogram(frequencyVector: IntArray, maxFrequency: Int): LongArray {
    val histogram = LongArray(maxFrequency)
    for (frequency in frequencyVector) {
      if (frequency > 0) {
        // Cap the frequency at maxFrequency.
        val cappedFrequency = min(frequency, maxFrequency)
        histogram[cappedFrequency - 1]++
      }
    }
    return histogram
  }
}
