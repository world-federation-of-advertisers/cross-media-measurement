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

import com.google.protobuf.Any
import java.time.Instant
import java.time.ZoneId
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration

/**
 * Generates synthetic events using SyntheticDataGeneration for testing and load evaluation.
 *
 * This generator creates events using population and event group specifications:
 * - Uses SyntheticPopulationSpec for demographic distributions
 * - Uses SyntheticEventGroupSpec for event patterns and frequencies
 * - Supports both individual events and batched output
 * - Deterministic generation based on specifications
 */
class SyntheticEventGenerator(
  private val populationSpec: SyntheticPopulationSpec,
  private val eventGroupSpec: SyntheticEventGroupSpec,
  private val timeRange: OpenEndTimeRange = OpenEndTimeRange(Instant.MIN, Instant.MAX),
  private val zoneId: ZoneId = ZoneId.of("UTC"),
  private val batchSize: Int = DEFAULT_BATCH_SIZE
) : EventSource {

  companion object {
    private val logger = Logger.getLogger(SyntheticEventGenerator::class.java.name)
    const val DEFAULT_BATCH_SIZE = 10000
  }

  /**
   * Generates a flow of synthetic event batches using SyntheticDataGeneration.
   * 
   * Events are generated deterministically based on population and event group specs.
   * Each batch contains up to `batchSize` events.
   * Date shards are processed in parallel using the provided dispatcher.
   * Multiple days are processed concurrently for better throughput.
   */
  override suspend fun generateEventBatches(dispatcher: CoroutineContext): Flow<List<LabeledEvent<Any>>> {
    logger.info("Starting parallel synthetic event generation with batching")
    
    return channelFlow {
      // Process date shards in parallel
      val processingJobs = mutableListOf<kotlinx.coroutines.Job>()
      var shardCount = 0
      
      SyntheticDataGeneration.generateEvents(
        TestEvent.getDefaultInstance(),
        populationSpec,
        eventGroupSpec,
        timeRange,
        zoneId
      ).collect { dateShardedImpression ->
        shardCount++
        val shardId = shardCount
        
        // Launch parallel job for each date shard
        val job = launch(dispatcher) {
          logger.fine("Processing date shard $shardId for date ${dateShardedImpression.localDate}")
          
          val events = dateShardedImpression.impressions.toList()
          val shardBatches = events.chunked(batchSize)
          
          logger.fine("Date shard $shardId generated ${events.size} events in ${shardBatches.size} batches")
          
          // Send batches directly through the channel, converting TestEvent to Any
          shardBatches.forEach { batch ->
            val anyBatch = batch.map { event ->
              LabeledEvent(
                timestamp = event.timestamp,
                vid = event.vid,
                message = Any.pack(event.message)
              )
            }
            send(anyBatch)
          }
        }
        
        processingJobs.add(job)
      }
      
      logger.info("Launched $shardCount date shard processing jobs")
      
      // Wait for all processing to complete
      processingJobs.forEach { it.join() }
    }
  }

  /**
   * Generates a flow of individual synthetic events using SyntheticDataGeneration.
   *
   * Events are generated deterministically based on population and event group specs.
   */
  override suspend fun generateEvents(): Flow<LabeledEvent<Any>> {
    logger.info("Starting synthetic event generation")
    
    return kotlinx.coroutines.flow.flow {
      SyntheticDataGeneration.generateEvents(
        TestEvent.getDefaultInstance(),
        populationSpec,
        eventGroupSpec,
        timeRange,
        zoneId
      ).collect { dateShardedImpression ->
        dateShardedImpression.impressions.collect { event ->
          // Convert TestEvent to Any
          val anyEvent = LabeledEvent(
            timestamp = event.timestamp,
            vid = event.vid,
            message = Any.pack(event.message)
          )
          emit(anyEvent)
        }
      }
    }
  }

}
