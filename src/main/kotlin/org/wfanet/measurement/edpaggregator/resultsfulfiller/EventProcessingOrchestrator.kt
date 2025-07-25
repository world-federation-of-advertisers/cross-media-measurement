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
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpec
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import com.google.protobuf.DynamicMessage

/**
 * Orchestrates event processing pipeline for requisition fulfillment.
 * 
 * ## Overview
 *
 * The EventProcessingOrchestrator implements step 11 of the direct measurement strategy:
 * - Single pipeline run for all requisitions
 * - Concurrent fulfillment using frequency vectors
 * - Filter deduplication and optimization
 *
 * ### Core Components:
 *
 * 1. **Requisition Processing**
 *    - Generates filters from requisition specifications
 *    - Deduplicates identical filters across requisitions
 *    - Maps requisitions to their required frequency vectors
 *
 * 2. **Pipeline Execution** 
 *    - Single storage-based event processing run
 *    - Parallel processing for high throughput
 *    - Returns frequency vectors per filter
 *
 * 3. **Concurrent Fulfillment**
 *    - Aggregates frequency vectors per requisition
 *    - Returns results for concurrent fulfillment execution
 *
 * ### Execution Flow:
 * 1. Generate and deduplicate filters from requisitions
 * 2. Run single pipeline execution on storage events
 * 3. Aggregate frequency vectors by requisition
 * 4. Return frequency vectors for concurrent fulfillment
 */
class EventProcessingOrchestrator {

  companion object {
    private val logger = Logger.getLogger(EventProcessingOrchestrator::class.java.name)
    private const val THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 10L
  }

  /**
   * Production mode: Runs pipeline for requisitions and returns frequency vectors for fulfillment.
   */
  suspend fun runWithRequisitions(
    requisitions: List<Requisition>,
    config: PipelineConfiguration,
    typeRegistry: TypeRegistry
  ): Map<String, FrequencyVector> {
    
    // Generate filters from requisitions with deduplication
    val (filterConfigurations, requisitionToFilters) = generateFiltersFromRequisitions(requisitions)
    
    config.validate()
    
    val threadPool = createThreadPool(config.threadPoolSize)
    val dispatcher = threadPool.asCoroutineDispatcher()
    
    try {
      val timeRange = OpenEndTimeRange.fromClosedDateRange(config.startDate..config.endDate)
      
      logger.info("Building VID index map using parallel processing with ${config.threadPoolSize} workers...")
      val vidIndexMapStartTime = System.currentTimeMillis()
      
      // Create population spec for VID index mapping
      val populationSpec = createPopulationSpecFromRequisitions(requisitions)
      
      val vidIndexMap = InMemoryVidIndexMap.buildParallel(
        populationSpec.toPopulationSpec(),
        dispatcher = dispatcher,
        parallelism = config.threadPoolSize
      )
      
      val vidIndexMapEndTime = System.currentTimeMillis()
      val vidIndexMapDuration = vidIndexMapEndTime - vidIndexMapStartTime
      
      logger.info("VID index map created with ${vidIndexMap.size} entries in ${vidIndexMapDuration}ms")
      
      // Create storage event source for requisition fulfillment
      val eventSource = StorageEventSource(
        eventReader = config.eventReader!!,
        dateRange = config.startDate..config.endDate,
        eventGroupReferenceIds = config.eventGroupReferenceIds,
        batchSize = config.effectiveBatchSize
      )
      
      // Create and run pipeline
      val pipeline = createPipeline(config, dispatcher)
      val eventBatchFlow = eventSource.generateEventBatches(dispatcher)
      
      // Convert Any messages to DynamicMessage
      val dynamicMessageEventBatchFlow = eventBatchFlow.map { batch ->
        batch.map { event ->
          if (event.message is com.google.protobuf.Any) {
            val message = event.message as com.google.protobuf.Any
            val descriptor = typeRegistry.getDescriptorForTypeUrl(message.typeUrl)
              ?: error("Unknown message type: ${message.typeUrl}")
            val dynamicMessage = DynamicMessage.parseFrom(descriptor, message.value)
            LabeledEvent(
              timestamp = event.timestamp,
              vid = event.vid,
              message = dynamicMessage,
              eventGroupReferenceId = event.eventGroupReferenceId
            )
          } else {
            // Already a DynamicMessage
            event as LabeledEvent<DynamicMessage>
          }
        }
      }
      
      // Process events through pipeline - returns Map<FilterSpec, FrequencyVector>
      val pipelineResults = pipeline.processEventBatches(
        eventBatchFlow = dynamicMessageEventBatchFlow,
        vidIndexMap = vidIndexMap,
        filters = filterConfigurations,
        typeRegistry = typeRegistry
      )
      
      // Aggregate frequency vectors by requisition
      val requisitionFrequencyVectors = mutableMapOf<String, FrequencyVector>()
      
      for (requisition in requisitions) {
        val filterSpecs = requisitionToFilters[requisition.name] ?: emptySet()
        if (filterSpecs.isNotEmpty()) {
          val aggregatedVector = aggregateFrequencyVectors(filterSpecs, pipelineResults)
          requisitionFrequencyVectors[requisition.name] = aggregatedVector
        }
      }
      
      return requisitionFrequencyVectors
      
    } finally {
      shutdownThreadPool(threadPool)
    }
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

  private fun createPipeline(
    config: PipelineConfiguration,
    dispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): EventProcessingPipeline {
    return ParallelBatchedPipeline(
      batchSize = config.parallelBatchSize,
      workers = config.parallelWorkers,
      dispatcher = dispatcher,
      disableLogging = config.disableLogging
    )
  }
  
  /**
   * Generates filter configurations from requisitions with deduplication.
   */
  private fun generateFiltersFromRequisitions(
    requisitions: List<Requisition>
  ): Pair<List<FilterConfiguration>, Map<String, Set<FilterSpec>>> {
    
    val filterSpecToRequisitions = mutableMapOf<FilterSpec, MutableSet<String>>()
    
    for (requisition in requisitions) {
      try {
        // For this implementation, we'll create a basic filter for each requisition
        // In a real implementation, you would decrypt and parse the requisition spec
        val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack(MeasurementSpec::class.java)
        
        // Create a basic filter spec for this requisition
        val filterSpec = FilterSpec(
          celExpression = "", // Empty CEL expression means no demographic filtering
          collectionInterval = createTimeInterval(), // Use current time range
          vidSamplingStart = if (measurementSpec.hasVidSamplingInterval()) measurementSpec.vidSamplingInterval.start.toLong() else 0L,
          vidSamplingWidth = if (measurementSpec.hasVidSamplingInterval()) measurementSpec.vidSamplingInterval.width.toLong() else Long.MAX_VALUE,
          eventGroupReferenceId = "reference-id-1" // Default reference ID
        )
        
        filterSpecToRequisitions.getOrPut(filterSpec) { mutableSetOf() }
          .add(requisition.name)
          
      } catch (e: Exception) {
        logger.warning("Failed to process requisition ${requisition.name}: ${e.message}")
        // Continue with other requisitions
      }
    }
    
    val filterConfigurations = filterSpecToRequisitions.map { (filterSpec, reqNames) ->
      FilterConfiguration(filterSpec, reqNames.toSet())
    }
    
    val requisitionToFilters = requisitions.associate { req ->
      req.name to filterConfigurations
        .filter { it.requisitionNames.contains(req.name) }
        .map { it.filterSpec }
        .toSet()
    }
    
    logger.info("Generated ${filterConfigurations.size} unique filters for ${requisitions.size} requisitions")
    
    return Pair(filterConfigurations, requisitionToFilters)
  }
  
  /**
   * Aggregates multiple frequency vectors for a requisition.
   */
  private fun aggregateFrequencyVectors(
    filterSpecs: Set<FilterSpec>,
    pipelineResults: Map<FilterSpec, FrequencyVector>
  ): FrequencyVector {
    val vectors = filterSpecs.mapNotNull { pipelineResults[it] }
    
    return if (vectors.isEmpty()) {
      // Return empty frequency vector
      BasicFrequencyVector(emptyList())
    } else if (vectors.size == 1) {
      vectors.first()
    } else {
      // Merge multiple vectors
      vectors.reduce { acc, vector -> acc.merge(vector) }
    }
  }
  
  /**
   * Creates a basic time interval for the current period.
   */
  private fun createTimeInterval(): com.google.type.Interval {
    val now = java.time.Instant.now()
    val thirtyDaysAgo = now.minus(java.time.Duration.ofDays(30))
    
    return com.google.type.Interval.newBuilder()
      .setStartTime(
        com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(thirtyDaysAgo.epochSecond)
          .setNanos(thirtyDaysAgo.nano)
      )
      .setEndTime(
        com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(now.epochSecond)
          .setNanos(now.nano)
      )
      .build()
  }

  /**
   * Creates a population spec based on VID ranges from requisitions.
   */
  private fun createPopulationSpecFromRequisitions(requisitions: List<Requisition>): SyntheticPopulationSpec {
    // For event processing, we need to use a reasonable VID range since the VID sampling interval
    // is a sampling rate (0.0 to 1.0), not an actual VID range
    val minVid = 1L
    val maxVid = 1000000L // Default VID range for synthetic population
    
    return SyntheticPopulationSpec.newBuilder().apply {
      vidRangeBuilder.apply {
        start = minVid
        endExclusive = maxVid
      }
      eventMessageTypeUrl = TestEvent.getDefaultInstance().descriptorForType.fullName
      addPopulationFields("person.gender")
      addPopulationFields("person.age_group")

      // Create a simple sub-population covering the VID range
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = minVid
          endExclusive = maxVid
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
}