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
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import com.google.protobuf.DynamicMessage
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap

/**
 * Parallel batched implementation of the event processing pipeline.
 *
 * ## Overview
 *
 * This pipeline implements a multi-stage parallel processing architecture:
 *
 * ### Stage 1: Batch Collection and Distribution
 * - Receives pre-batched events from upstream flow
 * - Distributes batches to worker coroutines via round-robin assignment
 * - Tracks total events and batches for monitoring
 *
 * ### Stage 2: Parallel Processing Workers
 * - Multiple worker coroutines process assigned batches sequentially
 * - Each worker processes its assigned batch through ALL filters in parallel
 * - Uses structured concurrency to ensure batch completion
 *
 * ### Key Components:
 * - **FilterProcessor**: Evaluates CEL expressions and time range against events
 * - **FrequencyVectorSink**: Aggregates matched events into frequency vectors
 * - **Channels**: Enable round-robin work distribution across workers
 *
 * ### Processing Flow:
 * ```
 * EventBatchFlow -> RoundRobin -> Worker[1] -> FilterProcessor[1..M] -> Sink
 *                              -> Worker[2] -> FilterProcessor[1..M] -> Sink
 *                              -> Worker[N] -> FilterProcessor[1..M] -> Sink
 * ```
 *
 * ### Performance Characteristics:
 * - Batching reduces processing overhead
 * - Parallel workers maximize CPU utilization
 * - Round-robin assignment eliminates batch duplication
 * - Concurrent filter evaluation per batch
 */
class ParallelBatchedPipeline(
  private val batchSize: Int,
  private val workers: Int,
  private val dispatcher: CoroutineDispatcher,
  private val disableLogging: Boolean = false
) : EventProcessingPipeline {

  companion object {
    private val logger = Logger.getLogger(ParallelBatchedPipeline::class.java.name)
  }

  override val pipelineType: String = "Parallel-Batched"

  override suspend fun processEventBatches(
    eventSource: EventSource,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>,
    typeRegistry: TypeRegistry
  ): Map<FilterSpec, FrequencyVector> = coroutineScope {

    logger.info("Starting parallel pipeline with:")
    logger.info("  Batch size: $batchSize")
    logger.info("  Workers: $workers")
    logger.info("  Filters: ${filters.size}")

    // We'll need to get the descriptor from the first event
    var eventTemplateDescriptor: com.google.protobuf.Descriptors.Descriptor? = null
    val processors = mutableListOf<FilterProcessor>()
    
    // This will be populated once we receive the first event
    var processorsInitialized = false

    val sinks = filters.associate { config ->
      config.filterSpec to FrequencyVectorSink(
        filterSpec = config.filterSpec,
        frequencyVector = StripedByteFrequencyVector(vidIndexMap.size.toInt(), config.maxFrequency),
        vidIndexMap = vidIndexMap
      )
    }
    
    // Create a lookup map for easier access by unique filter key
    val sinksByFilter = mutableMapOf<String, FrequencyVectorSink>()
    filters.forEach { config ->
      // Use FilterSpec hashCode to create unique key since FilterSpec is a data class with all fields
      val filterKey = config.filterSpec.hashCode().toString()
      sinksByFilter[filterKey] = sinks[config.filterSpec]!!
    }

    // Create channels for round-robin distribution to workers
    val workerChannels = (1..workers).map { Channel<EventBatch>(Channel.UNLIMITED) }

    val totalBatches = AtomicLong(0)
    val totalEvents = AtomicLong(0)
    val processedBatches = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    // Stage 1: Round-robin batch distribution coroutine
    val batchingJob = launch(dispatcher) {
      logger.info("Round-robin batch distribution started")
      var batchId = 0L
      var workerIndex = 0

      eventSource.generateEventBatches(dispatcher).collect { eventList ->
        // Initialize processors on first batch
        if (!processorsInitialized && eventList.isNotEmpty()) {
          val firstEvent = eventList.first()
          val eventMessage = firstEvent.message
          eventTemplateDescriptor = eventMessage.descriptorForType
          
          logger.info("Initialized processors with event type: ${eventTemplateDescriptor!!.fullName}")
          
          // Now create the processors with the actual descriptor
          processors.clear()
          filters.forEach { config ->
            processors.add(
              FilterProcessor(
                filterId = config.filterSpec.hashCode().toString(), // Use hashCode for unique ID
                celExpression = config.filterSpec.celExpression,
                eventMessageDescriptor = eventTemplateDescriptor!!,
                typeRegistry = typeRegistry,
                collectionInterval = config.filterSpec.collectionInterval,
                eventGroupReferenceId = config.filterSpec.eventGroupReferenceId
              )
            )
          }
          processorsInitialized = true
        }
        
        val batch = EventBatch(eventList.map { it as LabeledEvent<DynamicMessage> }, batchId++)
        workerChannels[workerIndex % workers].send(batch)
        workerIndex++
        
        totalBatches.incrementAndGet()
        totalEvents.addAndGet(eventList.size.toLong())

        if (!disableLogging && batchId % 10 == 0L) {
          logBatchingProgress(totalEvents.get(), totalBatches.get(), startTime)
        }
      }
      
      // Close all worker channels to signal completion
      workerChannels.forEach { it.close() }
      logger.info("Batch distribution completed: ${totalEvents.get()} events in ${totalBatches.get()} batches")
    }

    // Stage 2: Parallel processing workers using individual channels
    val processingJobs = workerChannels.mapIndexed { index, channel ->
      val workerId = index + 1
      launch(dispatcher) {
        logger.fine("Worker $workerId started")

        for (batch in channel) {
          // Process batch through all filters concurrently
          val filterJobs = processors.map { processor ->
            launch(dispatcher) {
              val matchedEvents = processor.processBatch(batch)
              // Use processor filterId (which is FilterSpec hashCode) for lookup
              val matchingSink = sinksByFilter[processor.filterId]
              
              if (matchingSink != null) {
                logger.fine("Processing ${matchedEvents.size} matched events for processor ${processor.filterId}")
                matchingSink.processMatchedEvents(matchedEvents, batch.size)
              } else {
                logger.warning("No matching sink found for processor ${processor.filterId}")
              }
            }
          }

          // Wait for all filter processing to complete
          filterJobs.forEach { it.join() }

          val processed = processedBatches.incrementAndGet()
          if (!disableLogging && processed % 100 == 0L) {
            logProcessingProgress(processed * batchSize, startTime)
          }
        }

        logger.fine("Worker $workerId completed")
      }
    }

    // Wait for batching to complete
    batchingJob.join()

    // Wait for all processing jobs to complete
    processingJobs.forEach { it.join() }

    val endTime = System.currentTimeMillis()
    logCompletion(totalEvents.get(), totalBatches.get(), startTime, endTime)

    // Return frequency vectors
    sinks.mapValues { (_, sink) -> sink.getFrequencyVector() }
  }

  private fun logBatchingProgress(events: Long, batches: Long, startTime: Long) {
    val elapsed = System.currentTimeMillis() - startTime
    val rate = if (elapsed > 0) events * 1000 / elapsed else 0
    logger.info("Batched $events events in $batches batches ($rate events/sec)")
  }

  private fun logProcessingProgress(processed: Long, startTime: Long) {
    val elapsed = System.currentTimeMillis() - startTime
    val rate = if (elapsed > 0) processed * 1000 / elapsed else 0
    logger.info("Processed $processed events ($rate events/sec)")
  }

  private fun logCompletion(totalEvents: Long, totalBatches: Long, startTime: Long, endTime: Long) {
    val totalTime = endTime - startTime
    val throughput = if (totalTime > 0) totalEvents * 1000 / totalTime else 0

    logger.info("Parallel pipeline completed:")
    logger.info("  Total events: $totalEvents")
    logger.info("  Total batches: $totalBatches")
    logger.info("  Processing time: $totalTime ms")
    logger.info("  Throughput: $throughput events/sec")
  }
}