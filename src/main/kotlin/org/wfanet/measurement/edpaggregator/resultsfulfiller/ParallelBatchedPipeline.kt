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
) : EventProcessingPipeline {

  companion object {
    private val logger = Logger.getLogger(ParallelBatchedPipeline::class.java.name)
  }

  override suspend fun processEventBatches(
    eventSource: EventSource,
    sinks: List<FrequencyVectorSink>
  ): Unit = coroutineScope {

    logger.info("Starting parallel pipeline with:")
    logger.info("  Batch size: $batchSize")
    logger.info("  Workers: $workers")
    logger.info("  Sinks: ${sinks.size}")

    // Create channels for round-robin distribution to workers
    val workerChannels = (1..workers).map { Channel<EventBatch>(Channel.UNLIMITED) }

    val totalBatches = AtomicLong(0)
    val totalEvents = AtomicLong(0)
    val processedBatches = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    // Stage 1: Round-robin batch distribution
    val batchingJob = launchBatchDistribution(
      eventSource = eventSource,
      workerChannels = workerChannels,
      totalBatches = totalBatches,
      totalEvents = totalEvents
    )

    // Stage 2: Parallel processing workers
    val processingJobs = launchProcessingWorkers(
      workerChannels = workerChannels,
      sinks = sinks,
      processedBatches = processedBatches,
      startTime = startTime
    )

    batchingJob.join()
    processingJobs.joinAll()

    val endTime = System.currentTimeMillis()
    logCompletion(totalEvents.get(), totalBatches.get(), startTime, endTime)
  }

  /**
   * Stage 1: Batch Collection and Distribution
   *
   * Receives pre-batched events from upstream flow and distributes them
   * to worker coroutines via round-robin assignment.
   */
  private suspend fun launchBatchDistribution(
    eventSource: EventSource,
    workerChannels: List<Channel<EventBatch>>,
    totalBatches: AtomicLong,
    totalEvents: AtomicLong
  ) = coroutineScope {
    launch(dispatcher) {
      logger.info("Round-robin batch distribution started")
      var workerIndex = 0

      eventSource.generateEventBatches(dispatcher).collect { batch ->
        workerChannels[workerIndex % workers].send(batch)
        workerIndex++

        totalBatches.incrementAndGet()
        totalEvents.addAndGet(batch.events.size.toLong())
      }

      // Close all worker channels to signal completion
      workerChannels.forEach { it.close() }
      logger.info("Batch distribution completed: ${totalEvents.get()} events in ${totalBatches.get()} batches")
    }
  }

  /**
   * Stage 2: Parallel Processing Workers
   *
   * Multiple worker coroutines process assigned batches in parallel.
   * Each worker processes its assigned batch through ALL sinks sequentially.
   */
  private suspend fun launchProcessingWorkers(
    workerChannels: List<Channel<EventBatch>>,
    sinks: List<FrequencyVectorSink>,
    processedBatches: AtomicLong,
    startTime: Long
  ) = coroutineScope {
    workerChannels.mapIndexed { index, channel ->
      val workerId = index + 1
      launch(dispatcher) {
        processWorkerBatches(
          workerId = workerId,
          channel = channel,
          sinks = sinks,
          processedBatches = processedBatches,
          startTime = startTime
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
    startTime: Long
  ) {
    logger.fine("Worker $workerId started")

    for (batch in channel) {
      sinks.forEach { sink ->
        sink.processBatch(batch)
      }

      val processed = processedBatches.incrementAndGet()
      if (processed % 100 == 0L) {
        logProcessingProgress(processed * batchSize, startTime)
      }
    }

    logger.fine("Worker $workerId completed")
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
