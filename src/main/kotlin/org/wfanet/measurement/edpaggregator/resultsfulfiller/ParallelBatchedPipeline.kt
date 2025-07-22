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
import kotlinx.coroutines.flow.*
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

/**
 * Parallel batched implementation of the event processing pipeline.
 *
 * This implementation:
 * - Batches events for efficient processing
 * - Uses multiple worker coroutines for parallel processing
 * - Processes batches through all filters concurrently
 * - Provides high throughput for large-scale event processing
 */
class ParallelBatchedPipeline(
  private val batchSize: Int,
  private val workers: Int,
  private val dispatcher: CoroutineDispatcher
) : EventProcessingPipeline {

  companion object {
    private val logger = Logger.getLogger(ParallelBatchedPipeline::class.java.name)
  }

  override val pipelineType: String = "Parallel-Batched"

  override suspend fun processEvents(
    eventFlow: Flow<LabeledEvent<TestEvent>>,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>
  ): Map<String, SinkStatistics> = coroutineScope {

    logger.info("Starting parallel pipeline with:")
    logger.info("  Batch size: $batchSize")
    logger.info("  Workers: $workers")
    logger.info("  Filters: ${filters.size}")

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

    // Create SharedFlow for fan-out to multiple workers
    val batchSharedFlow = MutableSharedFlow<EventBatch>(
      extraBufferCapacity = Int.MAX_VALUE
    )

    val totalBatches = AtomicLong(0)
    val totalEvents = AtomicLong(0)
    val processedBatches = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    // Stage 1: Batching coroutine
    val batchingJob = launch(dispatcher) {
      logger.info("Parallel batching stage started")
      var batchId = 0L

      eventFlow
        .chunked(batchSize)
        .collect { eventList ->
          batchSharedFlow.emit(EventBatch(eventList, batchId++))
          totalBatches.incrementAndGet()
          totalEvents.addAndGet(eventList.size.toLong())

          if (batchId % 10 == 0L) {
            logBatchingProgress(totalEvents.get(), totalBatches.get(), startTime)
          }
        }
      logger.info("Batching completed: ${totalEvents.get()} events in ${totalBatches.get()} batches")
    }

    // Stage 2: Parallel processing workers using SharedFlow
    val processingJobs = (1..workers).map { workerId ->
      launch(dispatcher) {
        logger.fine("Worker $workerId started")

        batchSharedFlow.collect { batch ->
          // Process batch through all filters concurrently
          val filterJobs = processors.map { processor ->
            launch(dispatcher) {
              val matchedEvents = processor.processBatch(batch)
              sinks[processor.filterId]?.processMatchedEvents(matchedEvents, batch.size)
            }
          }
          
          // Wait for all filter processing to complete
          filterJobs.forEach { it.join() }

          val processed = processedBatches.incrementAndGet()
          if (processed % 100 == 0L) {
            logProcessingProgress(processed * batchSize, startTime)
          }
        }

        logger.fine("Worker $workerId completed")
      }
    }

    // Wait for batching to complete
    batchingJob.join()
    
    // Wait until all batches have been processed
    while (processedBatches.get() < totalBatches.get()) {
      delay(10)
    }
    
    // Cancel processing jobs since all batches are done
    processingJobs.forEach { it.cancel() }

    val endTime = System.currentTimeMillis()
    logCompletion(totalEvents.get(), totalBatches.get(), startTime, endTime)

    // Return statistics
    sinks.mapValues { (_, sink) -> sink.getStatistics() }
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
