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
 * Interface for frequency vectors used in reach and frequency measurements.
 *
 * Provides operations for getting reach, average frequency, and total count metrics.
 */
interface FrequencyVector {

  /**
   * Increments the frequency count for a given index.
   *
   * @param index The index to increment
   */
  fun incrementByIndex(index: Int)

  /**
   * Get a byte array representing the vector.
   */
  fun getByteArray(): ByteArray
}
