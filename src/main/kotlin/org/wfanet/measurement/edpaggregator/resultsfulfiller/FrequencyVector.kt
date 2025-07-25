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
 * Interface for frequency vectors used in reach and frequency measurements.
 * 
 * Provides operations for managing and analyzing frequency data, including
 * getting frequency counts, computing reach metrics, and handling sampling.
 */
interface FrequencyVector {
  
  /**
   * Gets the frequency for a specific VID.
   * 
   * @param vid The virtual ID to query
   * @return The frequency count for the VID, or 0 if not present
   */
  fun getFrequency(vid: Long): Int
  
  /**
   * Gets the total reach (unique VID count).
   * 
   * @return The number of unique VIDs in this frequency vector
   */
  fun getReach(): Long
  
  /**
   * Gets the frequency distribution.
   * 
   * @return A map from frequency values to counts of VIDs with that frequency
   */
  fun getFrequencyDistribution(): Map<Int, Long>
  
  /**
   * Gets the total frequency across all VIDs.
   * 
   * @return The sum of all frequencies
   */
  fun getTotalFrequency(): Long
  
  /**
   * Gets the maximum frequency in this vector.
   * 
   * @return The highest frequency value, or 0 if empty
   */
  fun getMaxFrequency(): Int
  
  /**
   * Gets all VIDs in this frequency vector.
   * 
   * @return A set of all VIDs that have non-zero frequency
   */
  fun getVids(): Set<Long>
  
  /**
   * Merges this frequency vector with another.
   * 
   * @param other The frequency vector to merge with
   * @return A new frequency vector containing the combined data
   */
  fun merge(other: FrequencyVector): FrequencyVector
  
  /**
   * Samples this frequency vector at the given rate.
   * 
   * @param rate The sampling rate (0.0 to 1.0)
   * @param random The random number generator to use
   * @return A new frequency vector containing the sampled data
   */
  fun sample(rate: Double, random: SecureRandom): FrequencyVector
}