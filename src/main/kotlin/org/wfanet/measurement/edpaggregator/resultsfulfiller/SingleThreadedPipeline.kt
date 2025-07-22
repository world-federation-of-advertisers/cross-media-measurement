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

import com.google.protobuf.TypeRegistry
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

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
  
  override suspend fun processEvents(
    eventFlow: Flow<LabeledEvent<TestEvent>>,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>
  ): Map<String, SinkStatistics> = coroutineScope {
    
    logger.info("Starting single-threaded pipeline with ${filters.size} filters")
    
    val typeRegistry = TypeRegistry.newBuilder()
      .add(TestEvent.getDescriptor())
      .build()
    
    // Create processors and sinks
    val processors = filters.map { config ->
      FilterProcessor(
        filterId = config.filterId,
        celExpression = config.celExpression,
        eventMessageDescriptor = TestEvent.getDescriptor(),
        typeRegistry = typeRegistry,
        collectionInterval = config.timeInterval
      )
    }
    
    val sinks = filters.associate { config ->
      config.filterId to FrequencyVectorSink(
        sinkId = config.filterId,
        description = config.description,
        vidIndexMap = vidIndexMap
      )
    }
    
    val totalEvents = AtomicLong(0)
    val startTime = System.currentTimeMillis()
    
    // Process events
    eventFlow
      .onStart { logger.info("Single pipeline started") }
      .flowOn(dispatcher)
      .collect { event ->
        totalEvents.incrementAndGet()
        
        // Process event through all filters
        processors.forEach { processor ->
          val batch = EventBatch(listOf(event), totalEvents.get())
          val matchedEvents = processor.processBatch(batch)
          sinks[processor.filterId]?.processMatchedEvents(matchedEvents, 1)
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