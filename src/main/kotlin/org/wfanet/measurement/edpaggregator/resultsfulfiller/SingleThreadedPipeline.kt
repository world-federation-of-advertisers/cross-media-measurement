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
import com.google.protobuf.TypeRegistry
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap

/**
 * Single-threaded implementation of the event processing pipeline.
 * 
 * This implementation processes events sequentially through all filters
 * without batching. It's simpler but may have lower throughput than
 * the parallel implementation.
 */
class SingleThreadedPipeline(
  private val dispatcher: CoroutineDispatcher
) : EventProcessingPipeline {
  
  companion object {
    private val logger = Logger.getLogger(SingleThreadedPipeline::class.java.name)
  }
  
  override val pipelineType: String = "Single-Threaded"
  
  override suspend fun processEventBatches(
    eventBatchFlow: Flow<List<LabeledEvent<DynamicMessage>>>,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>,
    typeRegistry: TypeRegistry
  ): Map<String, SinkStatistics> = coroutineScope {
    
    logger.info("Starting single-threaded pipeline with ${filters.size} filters")
    
    // We'll need to get the descriptor from the first event
    var eventTemplateDescriptor: com.google.protobuf.Descriptors.Descriptor? = null
    val processors = mutableListOf<FilterProcessor>()
    var processorsInitialized = false
    
    val sinks = filters.associate { config ->
      config.filterId to FrequencyVectorSink(
        sinkId = config.filterId,
        description = config.description,
        vidIndexMap = vidIndexMap
      )
    }
    
    val totalEvents = AtomicLong(0)
    val startTime = System.currentTimeMillis()
    
    // Process event batches
    eventBatchFlow
      .onStart { logger.info("Single pipeline started") }
      .flowOn(dispatcher)
      .collect { eventList ->
        // Initialize processors on first batch
        if (!processorsInitialized && eventList.isNotEmpty()) {
          val firstEvent = eventList.first()
          val eventMessage = firstEvent.message
          eventTemplateDescriptor = eventMessage.descriptorForType
          
          // Now create the processors with the actual descriptor
          processors.clear()
          filters.forEach { config ->
            processors.add(
              FilterProcessor(
                filterId = config.filterId,
                celExpression = config.celExpression,
                eventMessageDescriptor = eventTemplateDescriptor!!,
                typeRegistry = typeRegistry,
                collectionInterval = config.timeInterval
              )
            )
          }
          processorsInitialized = true
          logger.info("Initialized processors with event type: ${eventTemplateDescriptor!!.fullName}")
        }
        
        val batchSize = eventList.size
        totalEvents.addAndGet(batchSize.toLong())
        
        // Process batch through all filters
        processors.forEach { processor ->
          val batch = EventBatch(eventList, totalEvents.get())
          val matchedEvents = processor.processBatch(batch)
          sinks[processor.filterId]?.processMatchedEvents(matchedEvents, batchSize)
        }
        
        if (totalEvents.get() % 100000 == 0L) {
          logProgress(totalEvents.get(), startTime)
        }
      }
    
    val endTime = System.currentTimeMillis()
    logCompletion(totalEvents.get(), startTime, endTime)
    
    // Return statistics
    sinks.mapValues { (_, sink) -> sink.getStatistics() }
  }
  
  private fun logProgress(eventCount: Long, startTime: Long) {
    val elapsed = System.currentTimeMillis() - startTime
    val rate = if (elapsed > 0) eventCount * 1000 / elapsed else 0
    logger.info("Processed $eventCount events ($rate events/sec)")
  }
  
  private fun logCompletion(totalEvents: Long, startTime: Long, endTime: Long) {
    val totalTime = endTime - startTime
    val throughput = if (totalTime > 0) totalEvents * 1000 / totalTime else 0
    
    logger.info("Single pipeline completed:")
    logger.info("  Total events: $totalEvents")
    logger.info("  Processing time: $totalTime ms")
    logger.info("  Throughput: $throughput events/sec")
  }
}