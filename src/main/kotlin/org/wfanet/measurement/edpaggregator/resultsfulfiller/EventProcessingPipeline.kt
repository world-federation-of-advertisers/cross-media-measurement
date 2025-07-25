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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.collect

/**
 * Main event processing pipeline that orchestrates the flow from events to frequency vectors.
 * 
 * This pipeline coordinates the reading of events from sources, applying filters,
 * and collecting results into frequency vectors for measurement calculations.
 */
class EventProcessingPipeline(
  private val configuration: PipelineConfiguration
) {
  
  /**
   * Processes events from the given source through the complete pipeline.
   * 
   * @param eventSource The source of events to process
   * @return The resulting frequency vector
   */
  suspend fun process(eventSource: EventSource): FrequencyVector {
    val filterProcessor = configuration.filterConfiguration.createProcessor()
    val sink = FrequencyVectorSink(configuration.filterConfiguration)
    
    val events = eventSource.getEvents()
    
    // Apply filtering if configured
    val filteredEvents = if (configuration.filterConfiguration.isFilteringEnabled()) {
      events.filter { event -> filterProcessor.matches(event) }
    } else {
      events
    }
    
    // Process through sink to build frequency vector
    return sink.process(filteredEvents)
  }
  
  /**
   * Processes events in batches for improved efficiency.
   * 
   * @param eventSource The source of events to process
   * @return The resulting frequency vector
   */
  suspend fun processBatched(eventSource: EventSource): FrequencyVector {
    val filterProcessor = configuration.filterConfiguration.createProcessor()
    val builder = FrequencyVectorBuilder()
    
    val events = eventSource.getEvents()
    val eventsList = mutableListOf<LabeledEvent<DynamicMessage>>()
    
    // Collect events and process in batches
    events.collect { event ->
      eventsList.add(event)
      if (eventsList.size >= configuration.batchSize) {
        val eventBatch = EventBatch(
          events = eventsList.toList(),
          batchId = System.currentTimeMillis(),
          timestamp = System.currentTimeMillis()
        )
        processBatch(eventBatch, filterProcessor, builder)
        eventsList.clear()
      }
    }
    
    // Process remaining events
    if (eventsList.isNotEmpty()) {
      val eventBatch = EventBatch(
        events = eventsList.toList(),
        batchId = System.currentTimeMillis(),
        timestamp = System.currentTimeMillis()
      )
      processBatch(eventBatch, filterProcessor, builder)
    }
    
    return builder.buildOptimal()
  }
  
  private fun processBatch(
    batch: EventBatch,
    filterProcessor: FilterProcessor,
    builder: FrequencyVectorBuilder
  ) {
    for (event in batch.events) {
      if (!configuration.filterConfiguration.isFilteringEnabled() || 
          filterProcessor.matches(event)) {
        builder.addVid(event.vid)
      }
    }
  }
}