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

import com.google.protobuf.DynamicMessage
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

/**
 * Frequency vector sink that receives filtered events and builds frequency vectors.
 *
 * Each sink corresponds to a specific filter specification and maintains its own frequency vector.
 * Thread-safe for concurrent access.
 */
class FrequencyVectorSink(
  private val filterProcessor: FilterProcessor,
  private val frequencyVector: FrequencyVector,
  private val vidIndexMap: VidIndexMap
) {

  /**
   * Processes a batch of events and updates frequency vector for matched events.
   *
   * @param batch
   */
  suspend fun processBatch(batch: EventBatch) {
    filterProcessor.processBatch(batch).events.forEach { event ->
      val index = vidIndexMap[event.vid]
      frequencyVector.incrementByIndex(index)
    }
  }

  /**
   * Returns the filter spec
   */
  fun getFilterSpec(): FilterSpec {
    return filterProcessor.filterSpec
  }

  /**
   * Returns the frequency vector.
   */
  fun getFrequencyVector(): FrequencyVector {
    return frequencyVector
  }
}
