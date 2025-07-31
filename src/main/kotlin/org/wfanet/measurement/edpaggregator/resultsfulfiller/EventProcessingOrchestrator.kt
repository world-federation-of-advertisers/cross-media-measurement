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
import com.google.protobuf.TypeRegistry
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import java.io.File
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpecWithAttributes

/**
 * Orchestrates event processing pipeline for requisition fulfillment.
 *
e* ## Overview
 *
 * The EventProcessingOrchestrator implements:
 * - Single pipeline run for all requisitions
 * - Concurrent fulfillment using frequency vectors
 * - Filter deduplication
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
class EventProcessingOrchestrator(
  private val privateEncryptionKey: PrivateKeyHandle
) {

  companion object {
    private val logger = Logger.getLogger(EventProcessingOrchestrator::class.java.name)
    private const val THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 10L
  }

  /**
   * Runs pipeline for requisitions and returns frequency vectors, keyed by requisition name.
   */
  suspend fun runWithRequisitions(
    requisitions: List<Requisition>,
    config: PipelineConfiguration,
    typeRegistry: TypeRegistry
  ): Map<String, FrequencyVector> {
    require(config.populationSpecPath.isNotEmpty()) { "Population spec path must be provided" }

    // Generate filters from requisitions with deduplication
    val (filterConfigurations, requisitionToFilters) = generateFiltersFromRequisitions(requisitions, config)

    config.validate()

    val threadPool = createThreadPool(config.threadPoolSize)
    val dispatcher = threadPool.asCoroutineDispatcher()

    try {
      val timeRange = OpenEndTimeRange.fromClosedDateRange(config.startDate..config.endDate)
      val vidIndexMapStartTime = System.currentTimeMillis()

      // Create or load population spec for VID index mapping
      val populationSpec = loadPopulationSpecFromFile(config.populationSpecPath, config.isSyntheticPopulationSpec)

      val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

      val vidIndexMapEndTime = System.currentTimeMillis()
      val vidIndexMapDuration = vidIndexMapEndTime - vidIndexMapStartTime

      logger.info("VID index map created with ${vidIndexMap.size} entries in ${vidIndexMapDuration}ms")

      // Log sample VIDs for debugging
      if (vidIndexMap.size > 0) {
        val sampleVids = mutableListOf<Long>()
        val iterator = vidIndexMap.iterator()
        var count = 0
        while (iterator.hasNext() && count < 10) {
          sampleVids.add(iterator.next().key)
          count++
        }
        logger.info("Sample VIDs in index map: $sampleVids")
      }

      // Create storage event source for requisition fulfillment
      val eventSource = StorageEventSource(
        eventReader = config.eventReader,
        dateRange = config.startDate..config.endDate,
        eventGroupReferenceIds = config.eventGroupReferenceIds,
        batchSize = config.batchSize
      )

      // Create and run pipeline
      val pipeline = createPipeline(config, dispatcher)
      val eventBatchFlow = eventSource.generateEventBatches(dispatcher)

      // Process events through pipeline - returns Map<FilterSpec, FrequencyVector>
      val pipelineResults = pipeline.processEventBatches(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        filters = filterConfigurations,
        typeRegistry = typeRegistry
      )

      // Aggregate frequency vectors by requisition
      val requisitionFrequencyVectors = mutableMapOf<String, FrequencyVector>()

      for (requisition in requisitions) {
        val filterSpecs = requisitionToFilters[requisition.name] ?: emptySet()
        if (filterSpecs.isNotEmpty()) {
          logger.info("Requisition ${requisition.name} has ${filterSpecs.size} filter specs")
          logger.info("Pipeline results has ${pipelineResults.size} results")
          filterSpecs.forEach { spec ->
            val hasResult = pipelineResults.containsKey(spec)
            logger.info("FilterSpec for ${spec.eventGroupReferenceId} (time: ${spec.collectionInterval.startTime.seconds} to ${spec.collectionInterval.endTime.seconds}): hasResult=$hasResult")
          }

          val aggregatedVector = aggregateFrequencyVectors(filterSpecs, pipelineResults)
          requisitionFrequencyVectors[requisition.name] = aggregatedVector

          // Log aggregation results for debugging
          logger.info("Requisition ${requisition.name}: filters=${filterSpecs.map { it.eventGroupReferenceId }} -> " +
              "reach=${aggregatedVector.getReach()}, totalCount=${aggregatedVector.getTotalCount()}")
        } else {
          logger.warning("No filter specs found for requisition ${requisition.name}")
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
      batchSize = config.batchSize,
      workers = config.workers,
      dispatcher = dispatcher,
      disableLogging = config.disableLogging
    )
  }

  /**
   * Generates filter configurations from requisitions with deduplication.
   */
  private fun generateFiltersFromRequisitions(
    requisitions: List<Requisition>,
    config: PipelineConfiguration
  ): Pair<List<FilterConfiguration>, Map<String, Set<FilterSpec>>> {

    val filterSpecToRequisitions = mutableMapOf<FilterSpec, MutableSet<String>>()

    for (requisition in requisitions) {
      // Extract event groups from the requisition spec
      val signedRequisitionSpec: SignedMessage = decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

      // Create filter specs for each event group in the requisition
      for (eventGroup in requisitionSpec.events.eventGroupsList) {
        val filterSpec = FilterSpec(
          celExpression = eventGroup.value.filter.expression,
          collectionInterval = eventGroup.value.collectionInterval,
          vidSamplingStart = 0.0f,
          vidSamplingWidth = 1.0f,
          eventGroupReferenceId = eventGroup.key // Use event group key as reference ID
        )

        filterSpecToRequisitions.getOrPut(filterSpec) { mutableSetOf() }
          .add(requisition.name)
      }
    }

    val filterConfigurations = filterSpecToRequisitions.map { (filterSpec, reqNames) ->
      FilterConfiguration(filterSpec, reqNames.toSet(), config.maxFrequency)
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
   * Extracts event group reference IDs from the requisition spec.
   */
  private fun getEventGroupsFromRequisitionSpec(
    requisition: Requisition
  ): List<String> {
    val signedRequisitionSpec: SignedMessage = decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
    return requisitionSpec.events.eventGroupsList.map { eventGroup ->
      eventGroup.key
    }
  }

  /**
   * Aggregates multiple frequency vectors for a requisition.
   */
  private fun aggregateFrequencyVectors(
    filterSpecs: Set<FilterSpec>,
    pipelineResults: Map<FilterSpec, FrequencyVector>
  ): FrequencyVector {
    require(filterSpecs.isNotEmpty()) {
      "Filter specs cannot be empty for frequency vector aggregation"
    }

    val vectors = filterSpecs.mapNotNull { pipelineResults[it] }

    require(vectors.isNotEmpty()) {
      "No frequency vectors found for aggregation. All pipelines failed or returned null."
    }

    return if (vectors.size == 1) {
      vectors.first()
    } else {
      // Merge multiple vectors using the merge method
      val stripedVectors = vectors.filterIsInstance<StripedByteFrequencyVector>()

      if (stripedVectors.isEmpty()) {
        // Fallback if no StripedByteFrequencyVector instances found
        vectors.first()
      } else if (stripedVectors.size == 1) {
        stripedVectors.first()
      } else {
        // Merge all striped vectors
        stripedVectors.reduce { acc, vector -> acc.merge(vector) }
      }
    }
  }


  /**
   * Loads a population spec from a textproto file.
   *
   * @param path The file path to load from
   * @param isSynthetic If true, loads as SyntheticPopulationSpec and converts to PopulationSpec
   * @return PopulationSpec loaded from the file
   * TODO: Switch to fetch population spec from the API instead of loading from file
   */
  private fun loadPopulationSpecFromFile(path: String, isSynthetic: Boolean): PopulationSpec {
    val file = File(path)
    require(file.exists()) { "Population spec file not found: ${file.absolutePath}" }

    logger.info("Loading population spec from: ${file.absolutePath} (synthetic=$isSynthetic)")

    return if (isSynthetic) {
      val syntheticSpec = parseTextProto(
        file,
        SyntheticPopulationSpec.getDefaultInstance()
      )
      logger.info("Converting synthetic population spec to regular population spec")
      syntheticSpec.toPopulationSpecWithAttributes()
    } else {
      parseTextProto(
        file,
        PopulationSpec.getDefaultInstance()
      )
    }
  }
}
