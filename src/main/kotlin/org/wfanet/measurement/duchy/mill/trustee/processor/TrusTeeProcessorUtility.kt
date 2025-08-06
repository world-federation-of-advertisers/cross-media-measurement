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

/**
 * Converts a ByteArray of signed 8-bit integers to an IntArray, with validation.
 *
 * @throws IllegalArgumentException if any byte represents a value that is negative or >= 127.
 */
fun ByteArray.toIntArray(): IntArray {
  return this.map { byte ->
      val frequency = byte.toInt()
      require(frequency >= 0 && frequency < 127) {
        "Invalid frequency value in byte array: $frequency. Must be non-negative and less than 127."
      }
      frequency
    }
    .toIntArray()
}

/**
 * Builds a histogram from a frequency vector.
 *
 * @param frequencyVector An array where each element is the frequency for a given user.
 * @param maxFrequency The maximum possible frequency value. The histogram will have `maxFrequency +
 *   1` bins.
 * @return A [LongArray] representing the histogram, where the index is the frequency and the value
 *   is the count of users with that frequency.
 */
fun buildHistogram(frequencyVector: IntArray, maxFrequency: Int): LongArray {
  val histogram = LongArray(maxFrequency + 1)
  for (userFrequency in frequencyVector) {
    histogram[userFrequency]++
  }
  return histogram
}
