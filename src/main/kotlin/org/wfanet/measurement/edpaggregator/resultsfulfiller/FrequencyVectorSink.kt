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

import com.google.protobuf.Message
import java.util.logging.Logger
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

/**
 * Frequency vector sink that receives filtered events and builds frequency vectors.
 *
 * Each sink corresponds to a specific filter specification and maintains its own frequency vector.
 * Thread-safe for concurrent access.
 */
class FrequencyVectorSink<T : Message>(
  private val filterProcessor: FilterProcessor<T>,
  private val frequencyVector: StripedByteFrequencyVector,
  private val vidIndexMap: VidIndexMap,
) {

  /**
   * Processes a batch of events and updates frequency vector for matched events.
   *
   * @param batch
   */
  fun processBatch(batch: EventBatch<T>) {
    logger.fine(
      "Processing batch with ${batch.events.size} events for filter: " +
      "CEL='${filterProcessor.filterSpec.celExpression}', " +
      "eventGroups=${filterProcessor.filterSpec.eventGroupReferenceIds}"
    )
    
    val filteredBatch = filterProcessor.processBatch(batch)
    val matchedEvents = filteredBatch.events
    
    logger.info(
      "Filter matched ${matchedEvents.size}/${batch.events.size} events for sink with " +
      "eventGroups=${filterProcessor.filterSpec.eventGroupReferenceIds}"
    )
    
    var incrementCount = 0
    val vidCounts = mutableMapOf<Int, Int>()
    matchedEvents.forEach { event ->
      val index = vidIndexMap[event.vid]
      if (index >= 0) {
        val beforeValue = frequencyVector.getByteArray()[index].toInt() and 0xFF
        frequencyVector.increment(index)
        val afterValue = frequencyVector.getByteArray()[index].toInt() and 0xFF
        incrementCount++
        vidCounts[index] = vidCounts.getOrDefault(index, 0) + 1
        logger.finest("Incremented frequency vector at index $index for VID '${event.vid}': $beforeValue -> $afterValue")
      } else {
        logger.warning("VID '${event.vid}' not found in VidIndexMap, skipping")
      }
    }
    
    logger.info(
      "Updated frequency vector with $incrementCount increments " +
      "(${matchedEvents.size - incrementCount} VIDs not in index map). " +
      "Unique VIDs processed: ${vidCounts.size}"
    )
    
    if (vidCounts.isNotEmpty()) {
      val maxCount = vidCounts.values.maxOrNull() ?: 0
      val duplicateVids = vidCounts.filter { it.value > 1 }
      if (duplicateVids.isNotEmpty()) {
        logger.warning("Duplicate VIDs in batch - same VID processed multiple times: $duplicateVids")
      }
      logger.fine("VID frequency distribution in batch - max events per VID: $maxCount, unique VIDs: ${vidCounts.size}")
    }
  }

  /** Returns the filter spec */
  fun getFilterSpec(): FilterSpec {
    return filterProcessor.filterSpec
  }

  /** Returns the frequency vector. */
  fun getFrequencyVector(): StripedByteFrequencyVector {
    return frequencyVector
  }
  
  companion object {
    private val logger = Logger.getLogger(FrequencyVectorSink::class.java.name)
  }
}
