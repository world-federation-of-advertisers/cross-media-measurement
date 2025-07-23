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
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap

/**
 * Orchestrates the entire event processing pipeline.
 * Demo implementation!!!
 *
 * ## Overview
 *
 * The EventProcessingOrchestrator serves as the central coordinator for the
 * event processing during requisition fulfillment,
 * managing the lifecycle and execution of all components.
 *
 * ### Major Components:
 *
 * 1. **Configuration Management**
 *    - Validates and processes pipeline configuration
 *    - Creates population and event group specifications
 *    - Generates filter configurations (demographic × time combinations)
 *
 * 2. **Resource Management**
 *    - Creates and manages ForkJoinPool for work-stealing parallelism
 *    - Provides coroutine dispatcher for async operations
 *    - Ensures graceful shutdown and resource cleanup
 *
 * 3. **Event Generation**
 *    - SyntheticEventGenerator produces test events based on specs
 *    - Events are pre-batched for efficient processing
 *    - Supports configurable population demographics and time ranges
 *
 * 4. **Pipeline Selection**
 *    - SingleThreadedPipeline: Sequential processing for debugging/testing
 *    - ParallelBatchedPipeline: High-throughput parallel processing
 *    - Dynamic selection based on configuration
 *
 * 5. **Filter Generation**
 *    - Creates demographic filters (gender × age groups)
 *    - Generates progressive weekly time intervals
 *    - Produces cartesian product of demographics × time periods
 *
 * 6. **Statistics Aggregation**
 *    - Collects metrics from all pipeline sinks
 *    - Displays execution summary and throughput
 *    - Shows per-filter and aggregated statistics
 *
 * ### Execution Flow:
 * 1. Validate configuration and display settings
 * 2. Create thread pool and coroutine dispatcher
 * 3. Build VID index map for population mapping
 * 4. Generate filter configurations
 * 5. Create event generator with specifications
 * 6. Select and create appropriate pipeline
 * 7. Process events through pipeline
 * 8. Collect and display statistics
 * 9. Cleanup resources
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

      // Create population spec from synthetic population spec for VID indexing
      val totalVidRange = config.populationSpec.vidRange.endExclusive - config.populationSpec.vidRange.start
      val populationSpec = createPopulationSpec(totalVidRange)
      
      logger.info("Building VID index map using parallel processing with ${config.threadPoolSize} workers...")
      val vidIndexMapStartTime = System.currentTimeMillis()
      
      val vidIndexMap = InMemoryVidIndexMap.buildParallel(
        populationSpec, 
        dispatcher = dispatcher,
        parallelism = config.threadPoolSize
      )
      
      val vidIndexMapEndTime = System.currentTimeMillis()
      val vidIndexMapDuration = vidIndexMapEndTime - vidIndexMapStartTime
      
      logger.info("VID index map created with ${vidIndexMap.size} entries in ${vidIndexMapDuration}ms")

      // Generate filter configurations
      val filters = generateFilterConfigurations(config)

      // Create event generator using specs from configuration
      val eventGenerator = SyntheticEventGenerator(
        populationSpec = config.populationSpec,
        eventGroupSpec = config.eventGroupSpec,
        timeRange = timeRange,
        zoneId = config.zoneId,
        batchSize = config.effectiveBatchSize
      )

      // Create and run pipeline
      val pipeline = createPipeline(config, dispatcher)
      val eventBatchFlow = eventGenerator.generateEventBatches(dispatcher)

      val statistics = pipeline.processEventBatches(
        eventBatchFlow = eventBatchFlow,
        vidIndexMap = vidIndexMap,
        filters = filters
      )

      val pipelineEndTime = System.currentTimeMillis()
      val totalDuration = pipelineEndTime - pipelineStartTime

      // Display results
      // Calculate total expected events from population spec VID ranges
      val totalExpectedEvents = config.populationSpec.subPopulationsList.sumOf { subPop ->
        subPop.vidSubRange.endExclusive - subPop.vidSubRange.start
      }

      statisticsAggregator.displayExecutionSummary(
        totalDuration = totalDuration,
        totalEvents = totalExpectedEvents,
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
    println("  Zone ID: ${config.zoneId}")
    println("  Batch size: ${config.effectiveBatchSize}")
    println("  Channel capacity: ${config.channelCapacity}")
    println()
    println("Population Spec Configuration:")
    println("  VID range: ${config.populationSpec.vidRange.start} to ${config.populationSpec.vidRange.endExclusive - 1}")
    println("  Sub-populations: ${config.populationSpec.subPopulationsCount}")
    println()
    println("Event Group Spec Configuration:")
    println("  Description: ${config.eventGroupSpec.description}")
    println("  Date specs: ${config.eventGroupSpec.dateSpecsCount}")
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
        dispatcher = dispatcher,
        disableLogging = config.disableLogging
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

  private fun createDummyPopulationSpec(uniqueVids: Int): SyntheticPopulationSpec {
    return SyntheticPopulationSpec.newBuilder().apply {
      vidRangeBuilder.apply {
        start = 1L
        endExclusive = uniqueVids.toLong() + 1L
      }
      eventMessageTypeUrl = TestEvent.getDefaultInstance().descriptorForType.fullName
      addPopulationFields("person.gender")
      addPopulationFields("person.age_group")

      // Create a simple sub-population covering all VIDs
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = 1L
          endExclusive = uniqueVids.toLong() + 1L
        }
        putPopulationFieldsValues("person.gender",
          org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
            .setEnumValue(1) // MALE
            .build()
        )
        putPopulationFieldsValues("person.age_group",
          org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
            .setEnumValue(1) // YEARS_18_TO_34
            .build()
        )
      }.build())
    }.build()
  }

  private fun createDummyEventGroupSpec(totalEvents: Long): SyntheticEventGroupSpec {
    return SyntheticEventGroupSpec.newBuilder().apply {
      description = "Dummy event group spec for pipeline testing"
      samplingNonce = 12345L

      addDateSpecs(SyntheticEventGroupSpec.DateSpec.newBuilder().apply {
        dateRangeBuilder.apply {
          startBuilder.apply {
            year = 2024
            month = 1
            day = 1
          }
          endExclusiveBuilder.apply {
            year = 2024
            month = 2
            day = 1
          }
        }

        addFrequencySpecs(SyntheticEventGroupSpec.FrequencySpec.newBuilder().apply {
          frequency = 1L // 1 event per VID
          addVidRangeSpecs(SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.newBuilder().apply {
            vidRangeBuilder.apply {
              start = 1L
              endExclusive = totalEvents + 1L
            }
            samplingRate = 1.0 // Include all VIDs
          }.build())
        }.build())
      }.build())
    }.build()
  }
}
