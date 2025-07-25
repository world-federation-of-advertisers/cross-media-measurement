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

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.supervisorScope
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

/**
 * High-performance parallel pipeline for processing large volumes of events.
 * 
 * This pipeline processes events in parallel batches to maximize throughput
 * for large-scale measurements with millions of events.
 */
class ParallelBatchedPipeline(
  private val configuration: PipelineConfiguration
) {
  
  private val processedBatches = AtomicLong(0)
  private val totalEvents = AtomicLong(0)
  
  /**
   * Processes events using parallel batching for maximum throughput.
   * 
   * @param eventSource The source of events to process
   * @return The resulting frequency vector
   */
  suspend fun process(eventSource: EventSource): FrequencyVector = supervisorScope {
    val filterProcessor = configuration.filterConfiguration.createProcessor()
    val builders = ConcurrentLinkedQueue<FrequencyVectorBuilder>()
    val statisticsAggregator = PipelineStatisticsAggregator()
    
    val events = eventSource.getEvents()
      .buffer(configuration.channelCapacity)
    
    // Collect events and process in parallel batches
    val allEvents = events.toList()
    val batches = allEvents.chunked(configuration.parallelBatchSize)
    
    val batchResults = batches.chunked(configuration.parallelWorkers).map { batchChunk ->
      batchChunk.map { batch ->
        async {
          processParallelBatch(batch, filterProcessor, statisticsAggregator)
        }
      }.awaitAll()
    }.flatten()
    
    // Merge all frequency vectors
    val finalBuilder = FrequencyVectorBuilder()
    batchResults.forEach { batchVids ->
      finalBuilder.addVids(batchVids)
    }
    
    logProcessingStats(statisticsAggregator)
    
    return@supervisorScope finalBuilder.buildOptimal()
  }
  
  private suspend fun processParallelBatch(
    batch: List<LabeledEvent<com.google.protobuf.DynamicMessage>>,
    filterProcessor: FilterProcessor,
    statisticsAggregator: PipelineStatisticsAggregator
  ): List<Long> = coroutineScope {
    val startTime = System.currentTimeMillis()
    val batchVids = mutableListOf<Long>()
    var eventsFiltered = 0L
    
    for (event in batch) {
      if (!configuration.filterConfiguration.isFilteringEnabled() || 
          filterProcessor.matches(event)) {
        batchVids.add(event.vid)
      } else {
        eventsFiltered++
      }
    }
    
    val endTime = System.currentTimeMillis()
    val batchStats = SinkStatistics(
      eventsProcessed = batchVids.size.toLong(),
      eventsFiltered = eventsFiltered,
      processingTimeMs = endTime - startTime
    )
    
    statisticsAggregator.addBatchStatistics(batchStats)
    
    val batchNumber = processedBatches.incrementAndGet()
    totalEvents.addAndGet(batch.size.toLong())
    
    if (!configuration.disableLogging && batchNumber % 100 == 0L) {
      println("Processed batch $batchNumber, total events: ${totalEvents.get()}")
    }
    
    return@coroutineScope batchVids
  }
  
  private fun logProcessingStats(statisticsAggregator: PipelineStatisticsAggregator) {
    if (!configuration.disableLogging) {
      val aggregatedStats = statisticsAggregator.getAggregatedStatistics()
      println("=== Pipeline Processing Complete ===")
      println("Total batches processed: ${processedBatches.get()}")
      println("Total events processed: ${aggregatedStats.eventsProcessed}")
      println("Total events filtered: ${aggregatedStats.eventsFiltered}")
      println("Total processing time: ${aggregatedStats.processingTimeMs}ms")
      println("Events per second: ${aggregatedStats.eventsProcessed * 1000 / aggregatedStats.processingTimeMs}")
    }
  }
}