/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import java.security.SecureRandom

/**
 * Interface for frequency vector data that supports various measurement computations.
 * 
 * This abstraction allows fulfillers to compute different metrics from the underlying
 * frequency data without being tied to a specific implementation.
 */
interface FrequencyVector {
  /** Returns the frequency count for a specific VID, or 0 if not present. */
  fun getFrequency(vid: Long): Int
  
  /** Returns the total reach (number of unique VIDs with frequency > 0). */
  fun getReach(): Long
  
  /** Returns a map of frequency values to their counts (frequency distribution). */
  fun getFrequencyDistribution(): Map<Int, Long>
  
  /** Returns the sum of all frequency values. */
  fun getTotalFrequency(): Long
  
  /** Returns the maximum frequency value observed. */
  fun getMaxFrequency(): Int
  
  /** Returns the set of all VIDs with frequency > 0. */
  fun getVids(): Set<Long>
  
  /** Merges this frequency vector with another, returning a new vector. */
  fun merge(other: FrequencyVector): FrequencyVector
  
  /** Samples this frequency vector at the given rate, returning a new vector. */
  fun sample(rate: Double, random: SecureRandom): FrequencyVector
}