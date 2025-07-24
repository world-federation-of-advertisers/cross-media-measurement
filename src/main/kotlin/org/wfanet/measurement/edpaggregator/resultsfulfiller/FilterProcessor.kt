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

import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
import java.time.Instant
import java.util.logging.Logger
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

/**
 * Filter processor for event filtering with CEL expressions and time ranges.
 *
 * This processor compiles a CEL expression once and reuses it for batch processing.
 * It processes events sequentially through the CEL filter and identifies matching events.
 */
class FilterProcessor(
  val filterId: String,
  val celExpression: String,
  private val eventMessageDescriptor: Descriptors.Descriptor,
  private val typeRegistry: TypeRegistry,
  private val collectionInterval: Interval? = null
) {
  
  companion object {
    private val logger = Logger.getLogger(FilterProcessor::class.java.name)
  }
  
  private val program: Program = if (celExpression.isEmpty()) {
    Program { Program.newEvalResult(BoolT.True, null) }
  } else {
    EventFilters.compileProgram(eventMessageDescriptor, celExpression)
  }
  
  private val cachedStartInstant: Instant? = collectionInterval?.let { 
    Instant.ofEpochSecond(it.startTime.seconds, it.startTime.nanos.toLong())
  }
  
  private val cachedEndInstant: Instant? = collectionInterval?.let {
    Instant.ofEpochSecond(it.endTime.seconds, it.endTime.nanos.toLong())
  }
  
  /**
   * Processes a batch of events and returns the matching events.
   * Applies both CEL filtering and time range filtering based on collection interval.
   */
  suspend fun processBatch(batch: EventBatch): List<LabeledEvent<DynamicMessage>> {
    return batch.events.filter { event ->
      // First apply time range filter if collection interval is specified
      val timeMatches = if (collectionInterval != null) {
        isEventInTimeRange(event, collectionInterval)
      } else {
        true
      }
      
      if (!timeMatches) {
        return@filter false
      }
      
      // Then apply CEL filter - event.message is already a DynamicMessage
      try {
        EventFilters.matches(event.message, program)
      } catch (e: Exception) {
        logger.warning("CEL filter evaluation failed for event with VID ${event.vid} (filter: $filterId): ${e.message}")
        false
      }
    }
  }
  
  /**
   * Checks if an event's timestamp falls within the collection interval.
   */
  private fun isEventInTimeRange(event: LabeledEvent<DynamicMessage>, interval: Interval): Boolean {
    try {
      val eventTime = event.timestamp
      val startTime = cachedStartInstant ?: return false
      val endTime = cachedEndInstant ?: return false
      
      return !eventTime.isBefore(startTime) && eventTime.isBefore(endTime)
    } catch (e: Exception) {
      logger.warning("Time range evaluation failed for event with VID ${event.vid} (filter: $filterId): ${e.message}")
      return false
    }
  }
}