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

import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ParallelBatchedPipeline.Companion.DEFAULT_WORKER_CHANNEL_CAPACITY

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
 *
 * ### Stage 2: Parallel Processing Workers
 * - Multiple worker coroutines process assigned batches in parallel
 * - Each worker processes its assigned batch through ALL sinks sequentially
 *
 * ### Key Components:
 * - **FrequencyVectorSink**: Filter and aggregate events into frequency vectors
 * - **Channels**: Enable round-robin work distribution across workers
 *
 * ### Processing Flow:
 * ```
 * EventSource -> RoundRobin -> Worker[1] -> Sinks[1..M]
 *                           -> Worker[2] -> Sinks[1..M]
 *                           -> Worker[N] -> Sinks[1..M]
 * ```
 *
 * ### Performance Characteristics:
 * - Batching reduces processing overhead
 * - Parallel workers maximize CPU utilization
 */
class ParallelBatchedPipeline(
  private val batchSize: Int,
  private val workers: Int,
  private val dispatcher: CoroutineDispatcher,
  /**
   * Per-worker channel capacity, in batches.
   *
   * Rationale: A bounded capacity provides backpressure so that if processing slows down, upstream
   * batch production does not grow without bound.
   *
   * Memory model (rule of thumb): assume ~1 KiB per event and batches of 256 events. That is ~256
   * KiB per batch. With the default capacity of [DEFAULT_WORKER_CHANNEL_CAPACITY] (128), a single
   * worker can buffer ~32 MiB of batch payload (128 × 256 KiB). Across `N` workers the pipeline
   * would buffer roughly `N × 32 MiB` of batch payload, plus object overhead. This balances
   * smoothing producer/consumer jitter with bounding memory.
   */
  private val workerChannelCapacity: Int = DEFAULT_WORKER_CHANNEL_CAPACITY,
) : EventProcessingPipeline {

  companion object {
    private val logger = Logger.getLogger(ParallelBatchedPipeline::class.java.name)

    /**
     * Default per-worker channel capacity (in batches).
     * - Provides backpressure to avoid unbounded memory growth
     * - Large enough to absorb transient skew between batch production and processing
     * - Small enough to keep the total in-flight memory bounded across workers
     */
    const val DEFAULT_WORKER_CHANNEL_CAPACITY: Int = 128
  }

  override suspend fun processEventBatches(
    eventSource: EventSource,
    sinks: List<FrequencyVectorSink>,
  ): Unit = coroutineScope {
    logger.info("Starting parallel pipeline with:")
    logger.info("  Batch size: $batchSize")
    logger.info("  Workers: $workers")
    logger.info("  Sinks: ${sinks.size}")
    logger.info("  Worker channel capacity (batches): $workerChannelCapacity")
    logger.fine { "Dispatcher: $dispatcher" }
    
    sinks.forEachIndexed { index, sink ->
      logger.fine { "Sink[$index]: $sink" }
    }

    // Create channels for round-robin distribution to workers
    val workerChannels = (1..workers).map { Channel<EventBatch>(workerChannelCapacity) }

    val totalBatches = AtomicLong(0)
    val totalEvents = AtomicLong(0)
    val processedBatches = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    // Stage 1: Round-robin batch distribution
    val batchingJob =
      launchBatchDistribution(
        eventSource = eventSource,
        workerChannels = workerChannels,
        totalBatches = totalBatches,
        totalEvents = totalEvents,
      )

    // Stage 2: Parallel processing workers
    val processingJobs =
      launchProcessingWorkers(
        workerChannels = workerChannels,
        sinks = sinks,
        processedBatches = processedBatches,
        startTime = startTime,
      )

    batchingJob.join()
    processingJobs.joinAll()

    val endTime = System.currentTimeMillis()
    logCompletion(totalEvents.get(), totalBatches.get(), startTime, endTime)
  }

  /**
   * Stage 1: Batch Collection and Distribution
   *
   * Receives pre-batched events from upstream flow and distributes them to worker coroutines via
   * round-robin assignment.
   */
  private suspend fun launchBatchDistribution(
    eventSource: EventSource,
    workerChannels: List<Channel<EventBatch>>,
    totalBatches: AtomicLong,
    totalEvents: AtomicLong,
  ) = coroutineScope {
    launch(dispatcher) {
      logger.info("Round-robin batch distribution started")
      var workerIndex = 0

      eventSource.generateEventBatches(dispatcher).collect { batch ->
        val targetWorker = workerIndex % workers
        logger.fine { "Distributing batch (${batch.events.size} events) to worker $targetWorker" }
        
        workerChannels[targetWorker].send(batch)
        workerIndex++

        val batchCount = totalBatches.incrementAndGet()
        val eventCount = totalEvents.addAndGet(batch.events.size.toLong())
        
        if (batchCount % 10 == 0L) {
          logger.fine { "Distribution progress: $batchCount batches, $eventCount events" }
        }
      }

      // Close all worker channels to signal completion
      logger.fine { "Closing all ${workerChannels.size} worker channels" }
      workerChannels.forEach { it.close() }
      logger.info(
        "Batch distribution completed: ${totalEvents.get()} events in ${totalBatches.get()} batches"
      )
    }
  }

  /**
   * Stage 2: Parallel Processing Workers
   *
   * Multiple worker coroutines process assigned batches in parallel. Each worker processes its
   * assigned batch through ALL sinks sequentially.
   */
  private suspend fun launchProcessingWorkers(
    workerChannels: List<Channel<EventBatch>>,
    sinks: List<FrequencyVectorSink>,
    processedBatches: AtomicLong,
    startTime: Long,
  ) = coroutineScope {
    workerChannels.mapIndexed { index, channel ->
      val workerId = index + 1
      launch(dispatcher) {
        processWorkerBatches(
          workerId = workerId,
          channel = channel,
          sinks = sinks,
          processedBatches = processedBatches,
          startTime = startTime,
        )
      }
    }
  }

  /**
   * Process batches for a single worker.
   *
   * Processes each batch through all filters sequentially for better cache locality.
   */
  private suspend fun processWorkerBatches(
    workerId: Int,
    channel: Channel<EventBatch>,
    sinks: List<FrequencyVectorSink>,
    processedBatches: AtomicLong,
    startTime: Long,
  ) {
    logger.fine { "Worker $workerId started with ${sinks.size} sinks" }
    var localBatchCount = 0
    var localEventCount = 0L

    for (batch in channel) {
      localBatchCount++
      localEventCount += batch.events.size
      
      logger.finest { "Worker $workerId processing batch #$localBatchCount (${batch.events.size} events)" }
      
      sinks.forEachIndexed { index, sink -> 
        logger.finest { "Worker $workerId applying sink $index to batch" }
        sink.processBatch(batch) 
      }

      val processed = processedBatches.incrementAndGet()
      if (processed % 100 == 0L) {
        logProcessingProgress(processed * batchSize, startTime)
      }
      
      if (localBatchCount % 50 == 0) {
        logger.fine { "Worker $workerId progress: processed $localBatchCount batches, $localEventCount events" }
      }
    }

    logger.fine { "Worker $workerId completed: processed $localBatchCount batches, $localEventCount total events" }
  }

  private fun logProcessingProgress(processed: Long, startTime: Long) {
    val elapsed = System.currentTimeMillis() - startTime
    val rate = if (elapsed > 0) processed * 1000 / elapsed else 0
    logger.info("Processed $processed events ($rate events/sec)")
  }

  private fun logCompletion(totalEvents: Long, totalBatches: Long, startTime: Long, endTime: Long) {
    val totalTime = endTime - startTime
    val throughput = if (totalTime > 0) totalEvents * 1000 / totalTime else 0
    val avgBatchSize = if (totalBatches > 0) totalEvents / totalBatches else 0
    val avgBatchTime = if (totalBatches > 0) totalTime.toDouble() / totalBatches else 0.0

    logger.info("Parallel pipeline completed:")
    logger.info("  Total events: $totalEvents")
    logger.info("  Total batches: $totalBatches")
    logger.info("  Processing time: $totalTime ms")
    logger.info("  Throughput: $throughput events/sec")
    
    logger.fine { "Pipeline statistics:" }
    logger.fine { "  Average batch size: $avgBatchSize events" }
    logger.fine { "  Average batch processing time: %.2f ms".format(avgBatchTime) }
    logger.fine { "  Workers used: $workers" }
    logger.fine { "  Configured batch size: $batchSize" }
  }
}
