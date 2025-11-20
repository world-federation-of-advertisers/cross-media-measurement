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
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.launch

/**
 * Parallel, batched implementation of the event processing pipeline with **structured concurrency**
 * and **bounded backpressure**.
 *
 * ## Overview
 *
 * This pipeline uses a two-stage parallel architecture that overlaps production and consumption.
 *
 * ### Stage 1: Batch Collection and Distribution
 * - Collects pre-batched events from the upstream flow.
 * - Sends batches to per-worker channels in **strict round-robin** order.
 *     - `send` **suspends** (does not block the thread) when a worker’s channel is full, providing
 *       backpressure.
 *     - **Always closes all worker channels in a `finally` block** to guarantee worker termination
 *       on upstream completion, failure, or cancellation.
 *
 * ### Stage 2: Parallel Processing Workers
 * - Worker coroutines run in parallel and **consume until their channel is closed and drained**.
 * - Each worker processes its batch through **all** sinks sequentially (better cache locality).
 * - If a worker fails, the enclosing scope cancels siblings (structured concurrency).
 *
 * ### Key Components:
 * - **FrequencyVectorSink**: Filters/aggregates events into frequency vectors.
 * - **Channels**: Per-worker, bounded queues enabling round-robin distribution and backpressure.
 *
 * ### Processing Flow:
 * ```
 * EventSource -> RoundRobin -> Worker[1] -> Sinks[1..M]
 *                           -> Worker[2] -> Sinks[1..M]
 *                           -> Worker[N] -> Sinks[1..M]
 * ```
 *
 * ### Performance Characteristics:
 * - Batching reduces per-item overhead.
 * - Parallel workers maximize CPU utilization; producer/consumer run concurrently.
 * - Note: strict round-robin can stall on a slow worker if its channel fills (fairness trade-off).
 */
class ParallelBatchedPipeline<T : Message>(
  private val batchSize: Int,
  private val workers: Int,
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
) : EventProcessingPipeline<T> {

  override suspend fun processEventBatches(
    eventSource: EventSource<T>,
    sinks: List<FrequencyVectorSink<T>>,
  ): Unit = coroutineScope {
    logger.info(
      """
      |Starting parallel pipeline with:
      |  Batch size: $batchSize
      |  Workers: $workers
      |  Sinks: ${sinks.size}
      |  Worker channel capacity (batches): $workerChannelCapacity
      """
        .trimMargin()
    )

    // Create channels for round-robin distribution to workers
    val workerChannels = (1..workers).map { Channel<EventBatch<T>>(workerChannelCapacity) }
    val processedBatches = AtomicLong(0)
    val startMark: TimeMark = TimeSource.Monotonic.markNow()

    // Start workers concurrently
    val totalEvents = coroutineScope {
      launch {
        runProcessingWorkers(
          workerChannels = workerChannels,
          sinks = sinks,
          processedBatches = processedBatches,
          startTime = startMark,
        )
      }

      // Produce and send batches
      // If workers fail, this is cancelled by structured concurrency.
      distributeBatches(eventSource, workerChannels)
    }

    logCompletion(totalEvents, startMark)
  }

  /**
   * Stage 1: Batch Collection and Distribution
   *
   * Collects batches from [EventSource.generateEventBatches] and sends them to worker channels in
   * **strict round-robin** order.
   *
   * Channels are **closed in a `finally` block** so workers terminate promptly whether the upstream
   * flow completes normally, throws, or this scope is cancelled.
   *
   * @return the total number of events distributed
   */
  private suspend fun distributeBatches(
    eventSource: EventSource<T>,
    workerChannels: List<Channel<EventBatch<T>>>,
  ): Long {
    logger.info("Round-robin batch distribution started")

    try {
      val totalEvents =
        eventSource.generateEventBatches().withIndex().fold(0) { acc: Long, (i, batch) ->
          workerChannels[i % workers].send(batch) // suspends on backpressure
          acc + batch.size.toLong()
        }
      logger.info("Batch distribution finished (channels closed): ${totalEvents} events")
      return totalEvents
    } finally {
      // Always close channels so workers can exit, even on failure/cancellation.
      workerChannels.forEach { it.close() }
    }
  }

  /**
   * **Stage 2 – Processing Workers.**
   *
   * Launches one worker coroutine per channel. Runs inside a `coroutineScope`, so this function
   * **suspends until all workers complete** (each exits after its channel is closed and drained).
   *
   * **Failure semantics**: An exception in any worker cancels the `coroutineScope`, which cancels
   * sibling workers and propagates to the caller.
   */
  private suspend fun runProcessingWorkers(
    workerChannels: List<Channel<EventBatch<T>>>,
    sinks: List<FrequencyVectorSink<T>>,
    processedBatches: AtomicLong,
    startTime: TimeMark,
  ) = coroutineScope {
    workerChannels.forEachIndexed { index, channel ->
      val workerId = index + 1
      launch { processWorkerBatches(workerId, channel, sinks, processedBatches, startTime) }
    }
  }

  /**
   * Processes all batches for a **single worker**.
   * - Consumes `for (batch in channel)` until the channel is **closed** and **drained**.
   * - Applies **all sinks sequentially** per batch for better data/cache locality.
   * - Every [PROGRESS_LOG_INTERVAL] batches, logs cumulative throughput (events/sec).
   *
   * **Error propagation**: Any exception thrown by a sink fails this worker and cancels siblings
   * via the enclosing `coroutineScope`.
   */
  private suspend fun processWorkerBatches(
    workerId: Int,
    channel: Channel<EventBatch<T>>,
    sinks: List<FrequencyVectorSink<T>>,
    processedBatches: AtomicLong,
    startTime: TimeMark,
  ) {
    logger.fine("Worker $workerId started")

    for (batch in channel) {
      sinks.forEach { sink -> sink.processBatch(batch) }

      val processed = processedBatches.incrementAndGet()
      if (processed % PROGRESS_LOG_INTERVAL == 0L) {
        logProcessingProgress(processed * batchSize, startTime)
      }
    }

    logger.fine("Worker $workerId completed")
  }

  private fun logProcessingProgress(processed: Long, startTime: TimeMark) {
    val elapsedMs = startTime.elapsedNow().inWholeMilliseconds
    val rate = if (elapsedMs > 0) processed * 1000 / elapsedMs else 0
    logger.info("Processed $processed events ($rate events/sec)")
  }

  private fun logCompletion(totalEvents: Long, startMark: TimeMark) {
    val elapsedMs = startMark.elapsedNow().inWholeMilliseconds
    val throughput = if (elapsedMs > 0) totalEvents * 1000 / elapsedMs else 0
    logger.info(
      """
      |Parallel pipeline completed:
      |  Total events: $totalEvents
      |  Processing time: $elapsedMs ms
      |  Throughput: $throughput events/sec
      """
        .trimMargin()
    )
  }

  companion object {
    private val logger = Logger.getLogger(ParallelBatchedPipeline::class.java.name)

    /**
     * Default per-worker channel capacity (in batches).
     * - Provides backpressure to avoid unbounded memory growth
     * - Large enough to absorb transient skew between batch production and processing
     * - Small enough to keep the total in-flight memory bounded across workers
     */
    const val DEFAULT_WORKER_CHANNEL_CAPACITY: Int = 128

    /** Number of batches between progress log messages. */
    const val PROGRESS_LOG_INTERVAL: Int = 30000
  }
}
