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
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import java.io.File
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.size
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpecWithAttributes

/**
 * Orchestrates event processing pipeline for requisition fulfillment.
 *
 * ### Core Components:
 *
 * 1. **Requisition Processing**
 *    - Generates filters from requisition specifications
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
    val filterConfigurations = generateFiltersFromRequisitions(requisitions, config)

    config.validate()

    val threadPool = createThreadPool(config.threadPoolSize)
    val dispatcher = threadPool.asCoroutineDispatcher()

    try {
      // Create or load population spec for VID index mapping
      val populationSpec = loadPopulationSpecFromFile(config.populationSpecPath, config.isSyntheticPopulationSpec)
      val vidIndexMapStartTime = System.currentTimeMillis()
      val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)
      val vidIndexMapDuration = System.currentTimeMillis() - vidIndexMapStartTime

      logger.info("VID index map created with ${vidIndexMap.size} entries in ${vidIndexMapDuration}ms")

      val pipeline = createPipeline(config, dispatcher)
      val sinks = pipeline.processEventBatches(
        eventSource = config.eventSource,
        vidIndexMap = vidIndexMap,
        filters = filterConfigurations.values.flatten(),
        typeRegistry = typeRegistry,
        maxFrequency = config.maxFrequency,
        populationSize = populationSpec.size
      )

      // Aggregate frequency vectors by requisition
      val requisitionFrequencyVectors = mutableMapOf<String, FrequencyVector>()

      for (requisition in requisitions) {
        val filters = filterConfigurations.getOrDefault(requisition.name, emptyList())
        require(filters.isNotEmpty()) {
          "Filters are empty"
        }
        requisitionFrequencyVectors[requisition.name] = aggregateFrequencyVectors(filters, sinks)
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
    config: PipelineConfiguration,
  ): Map<String, List<FilterConfiguration>> {
    var result = mutableMapOf<String, MutableList<FilterConfiguration>>()
    for (requisition in requisitions) {
      // Extract event groups from the requisition spec
      val signedRequisitionSpec: SignedMessage = decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

      // Extract measurement spec to get VID sampling interval
      val signedMeasurementSpec: SignedMessage = requisition.measurementSpec
      val measurementSpec: MeasurementSpec = signedMeasurementSpec.unpack()
      val vidSamplingInterval = measurementSpec.vidSamplingInterval

      // Create filter specs for each event group in the requisition
      for (eventGroup in requisitionSpec.events.eventGroupsList) {
        val filterSpec = FilterSpec(
          celExpression = eventGroup.value.filter.expression,
          collectionInterval = eventGroup.value.collectionInterval,
          vidSamplingStart = vidSamplingInterval.start.toDouble(),
          vidSamplingWidth = vidSamplingInterval.width.toDouble(),
          eventGroupReferenceId = eventGroup.key
        )
        val filterConfig = FilterConfiguration(filterSpec, config.maxFrequency)
        result.getOrPut(requisition.name, ::mutableListOf).add(filterConfig)
      }
    }
    return result
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
    filters: List<FilterConfiguration>,
    sinks: List<FrequencyVectorSink>
  ): FrequencyVector {
    require(filters.isNotEmpty()) {
      "Filter specs cannot be empty for frequency vector aggregation"
    }
    val specs = filters.map { it.filterSpec }
    val vectors = sinks.filter { specs.contains(it.getFilterSpec()) }.map { it.getFrequencyVector() }

    vectors.forEachIndexed { index, vector ->
      val array = vector.getArray()
      logger.info("Vector $index: size=${array.size}, sum=${array.sum()}, non-zero=${array.count { it > 0 }}")
    }
    require(vectors.isNotEmpty()) {
      "No frequency vectors found for aggregation."
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
        // Merge all striped vectors using thread-unsafe merge for performance
        // This is safe here as the pipeline has finished
        stripedVectors.reduce { acc, vector -> acc.mergeUnsafe(vector) }
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
