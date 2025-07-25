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

/**
 * Builder for creating FrequencyVector instances.
 * 
 * Provides a unified interface for constructing different types of frequency vectors
 * based on the data size and performance requirements.
 */
class FrequencyVectorBuilder {
  
  private val vids = mutableListOf<Long>()
  
  /**
   * Adds a VID to the frequency vector being built.
   * 
   * @param vid The virtual ID to add
   */
  fun addVid(vid: Long): FrequencyVectorBuilder {
    vids.add(vid)
    return this
  }
  
  /**
   * Adds multiple VIDs to the frequency vector being built.
   * 
   * @param vids The virtual IDs to add
   */
  fun addVids(vids: Collection<Long>): FrequencyVectorBuilder {
    this.vids.addAll(vids)
    return this
  }
  
  
  /**
   * Builds a StripedByteFrequencyVector from the accumulated VIDs.
   * 
   * This method is more memory-efficient for large datasets.
   * 
   * @return A new StripedByteFrequencyVector instance
   */
  fun buildStriped(): FrequencyVector {
    return StripedByteFrequencyVector(vids.toList())
  }
  
  /**
   * Builds a FrequencyVector from the accumulated VIDs.
   * 
   * Uses StripedByteFrequencyVector for optimal performance.
   * 
   * @return A new StripedByteFrequencyVector instance
   */
  fun build(): FrequencyVector {
    return buildStriped()
  }
  
  /**
   * Clears all accumulated VIDs.
   */
  fun clear(): FrequencyVectorBuilder {
    vids.clear()
    return this
  }
  
  /**
   * Gets the current number of VIDs in the builder.
   * 
   * @return The number of VIDs that have been added
   */
  fun size(): Int = vids.size
}