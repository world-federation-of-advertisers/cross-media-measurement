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

import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap

/**
 * Orchestrates the entire event processing pipeline.
 * 
 * This class coordinates:
 * - Thread pool management
 * - Event generation
 * - Pipeline execution
 * - Statistics aggregation and display
 */
class EventProcessingOrchestrator {
  
  companion object {
    private val logger = Logger.getLogger(EventProcessingOrchestrator::class.java.name)
    
    private const val THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 10L
  }
  
  private val statisticsAggregator = PipelineStatisticsAggregator()
  
  /**
   * Runs the event processing pipeline with the given configuration.
   */
  suspend fun run(config: PipelineConfiguration) {
    config.validate()
    
    displayConfiguration(config)
    
    val threadPool = createThreadPool(config.threadPoolSize)
    val dispatcher = threadPool.asCoroutineDispatcher()
    
    try {
      val pipelineStartTime = System.currentTimeMillis()
      
      // Set up pipeline components
      val timeRange = OpenEndTimeRange.fromClosedDateRange(config.startDate..config.endDate)
      val populationSpec = createPopulationSpec(config.maxVidRange)
      val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)
      
      logger.info("VID index map created with ${vidIndexMap.size} entries")
      
      // Generate filter configurations
      val filters = generateFilterConfigurations(config)
      
      // Create event generator
      val eventGenerator = SyntheticEventGenerator(
        timeRange = timeRange,
        totalEvents = config.totalEvents,
        uniqueVids = config.syntheticUniqueVids,
        dispatcher = dispatcher
      )
      
      // Create and run pipeline
      val pipeline = createPipeline(config, dispatcher)
      val eventFlow = eventGenerator.generateEvents()
      
      val statistics = pipeline.processEvents(
        eventFlow = eventFlow,
        vidIndexMap = vidIndexMap,
        filters = filters
      )
      
      val pipelineEndTime = System.currentTimeMillis()
      val totalDuration = pipelineEndTime - pipelineStartTime
      
      // Display results
      statisticsAggregator.displayExecutionSummary(
        totalDuration = totalDuration,
        totalEvents = config.totalEvents,
        pipelineType = pipeline.pipelineType
      )
      
      statisticsAggregator.displayFilterStatistics(statistics)
      
      val weeklyIntervals = generateProgressiveWeeklyIntervals(config.startDate, config.endDate)
      statisticsAggregator.displayAggregatedMetrics(statistics, weeklyIntervals.size)
      
    } finally {
      shutdownThreadPool(threadPool)
    }
  }
  
  private fun displayConfiguration(config: PipelineConfiguration) {
    println("Configuration:")
    println("  Date range: ${config.startDate} to ${config.endDate}")
    println("  Max VID range: ${config.maxVidRange}")
    println("  Batch size: ${config.effectiveBatchSize}")
    println("  Channel capacity: ${config.channelCapacity}")
    println()
    println("Synthetic Data Configuration:")
    println("  Total events: ${config.totalEvents}")
    println("  Unique VIDs: ${config.syntheticUniqueVids}")
    println()
    println("Pipeline Configuration:")
    println("  Use parallel pipeline: ${config.useParallelPipeline}")
    if (config.useParallelPipeline) {
      println("  Parallel batch size: ${config.parallelBatchSize}")
      println("  Parallel workers: ${config.parallelWorkers}")
    }
    println()
  }
  
  private fun createThreadPool(size: Int): ForkJoinPool {
    logger.info("Creating shared work-stealing thread pool with max size: $size")
    
    return ForkJoinPool(
      size,
      { pool ->
        val thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        thread.name = "SharedPool-${thread.poolIndex}"
        thread.isDaemon = false
        thread
      },
      { t, e ->
        logger.severe("Uncaught exception in thread pool: ${t.name} - ${e.message}")
        e.printStackTrace()
      },
      true // async mode for better throughput
    )
  }
  
  private fun shutdownThreadPool(threadPool: ForkJoinPool) {
    logger.info("Shutting down shared thread pool...")
    
    threadPool.shutdown()
    
    if (!threadPool.awaitTermination(THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      logger.warning("Thread pool did not terminate gracefully, forcing shutdown")
      threadPool.shutdownNow()
    }
    
    logger.info("Thread pool shut down successfully")
  }
  
  private fun createPopulationSpec(maxVid: Long): PopulationSpec {
    return populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = maxVid
        }
      }
    }
  }
  
  private fun createPipeline(
    config: PipelineConfiguration,
    dispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): EventProcessingPipeline {
    return if (config.useParallelPipeline) {
      ParallelBatchedPipeline(
        batchSize = config.parallelBatchSize,
        workers = config.parallelWorkers,
        dispatcher = dispatcher
      )
    } else {
      SingleThreadedPipeline(dispatcher)
    }
  }
  
  private fun generateFilterConfigurations(config: PipelineConfiguration): List<FilterConfiguration> {
    val baseCelFilters = mapOf(
      "male_18_34" to "person.gender == 1 && person.age_group == 1",
      "male_35_54" to "person.gender == 1 && person.age_group == 2",
      "male_55_plus" to "person.gender == 1 && person.age_group == 3",
      "female_18_34" to "person.gender == 2 && person.age_group == 1",
      "female_35_54" to "person.gender == 2 && person.age_group == 2",
      "female_55_plus" to "person.gender == 2 && person.age_group == 3"
    )
    
    val weeklyIntervals = generateProgressiveWeeklyIntervals(config.startDate, config.endDate)
    
    val filters = mutableListOf<FilterConfiguration>()
    
    for ((demographicId, celExpression) in baseCelFilters) {
      weeklyIntervals.forEachIndexed { weekIndex, interval ->
        val filterId = "${demographicId}_week${weekIndex + 1}"
        filters.add(
          FilterConfiguration(
            filterId = filterId,
            celExpression = celExpression,
            timeInterval = interval
          )
        )
      }
    }
    
    logger.info("Generated ${filters.size} filter combinations")
    
    return filters
  }
  
  private fun generateProgressiveWeeklyIntervals(
    startDate: LocalDate,
    endDate: LocalDate
  ): List<Interval> {
    val startInstant = startDate.atStartOfDay(ZoneId.systemDefault()).toInstant()
    val endInstant = endDate.atStartOfDay(ZoneId.systemDefault()).toInstant()
    val totalDays = java.time.Duration.between(startInstant, endInstant).toDays()
    val totalWeeks = ((totalDays + 6) / 7).toInt() // Round up to include partial weeks
    
    val intervals = mutableListOf<Interval>()
    
    for (weekIndex in 1..totalWeeks) {
      val weekEndInstant = startInstant.plus(java.time.Duration.ofDays(weekIndex * 7L))
      val actualEndInstant = minOf(weekEndInstant, endInstant)
      
      val interval = Interval.newBuilder()
        .setStartTime(
          Timestamp.newBuilder()
            .setSeconds(startInstant.epochSecond)
            .setNanos(startInstant.nano)
        )
        .setEndTime(
          Timestamp.newBuilder()
            .setSeconds(actualEndInstant.epochSecond)
            .setNanos(actualEndInstant.nano)
        )
        .build()
      
      intervals.add(interval)
    }
    
    return intervals
  }
}