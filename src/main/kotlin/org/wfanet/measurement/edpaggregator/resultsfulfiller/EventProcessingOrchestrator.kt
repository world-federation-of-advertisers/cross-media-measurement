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

import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import java.util.concurrent.ExecutorService
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.collections.toList
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.size

/**
 * Index containing mappings between requisitions and their canonical [FilterSpec]s.
 *
 * This data structure enables efficient deduplication of requisitions that share the same filtering
 * criteria, allowing a single pipeline pass to fulfill multiple requisitions.
 *
 * @property filterSpecToRequisitionNames Maps each unique [FilterSpec] to the list of requisition
 *   names that use it.
 * @property requisitionNameToFilterSpec Maps each requisition name to its canonical [FilterSpec].
 */
data class FilterSpecIndex(
  val filterSpecToRequisitionNames: Map<FilterSpec, List<String>>,
  val requisitionNameToFilterSpec: Map<String, FilterSpec>,
) {

  companion object {

    /**
     * Builds canonical [FilterSpec]s from requisitions and indexes them.
     *
     * Canonicalization details:
     * - Event group reference IDs are sorted to ensure deterministic equality semantics.
     * - All event groups within a requisition must share the same CEL expression and interval.
     *
     * Returns two structures:
     * - A map from each unique [FilterSpec] to the list of requisition names using it.
     * - A reverse lookup from requisition name to its canonical [FilterSpec].
     *
     * @param requisitions Requisitions to analyze.
     * @param eventGroupReferenceIdMap Mapping from event group resource name to reference id
     * @return [FilterSpecIndex] containing the bidirectional mapping.
     * @throws IllegalArgumentException if a requisition has no event groups or groups are
     *   inconsistent in expression or interval.
     */
    fun fromRequisitions(
      requisitions: List<Requisition>,
      eventGroupReferenceIdMap: Map<String, String>,
      privateEncryptionKey: PrivateKeyHandle,
    ): FilterSpecIndex {
      val filterSpecToReqNames = mutableMapOf<FilterSpec, MutableList<String>>()
      val reqNameToFilterSpec = mutableMapOf<String, FilterSpec>()

      for (requisition in requisitions) {
        val signedRequisitionSpec: SignedMessage =
          decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

        require(!requisitionSpec.events.eventGroupsList.isEmpty()) {
          "event groups list is empty for requisition"
        }

        val eventGroups = requisitionSpec.events.eventGroupsList
        val firstEventGroup = eventGroups.first()

        // Validate consistent filter and interval across groups
        require(
          eventGroups.all { it.value.filter.expression == firstEventGroup.value.filter.expression }
        ) {
          "All event groups must have the same CEL expression"
        }
        require(
          eventGroups.all {
            it.value.collectionInterval == firstEventGroup.value.collectionInterval
          }
        ) {
          "All event groups must have the same collection interval"
        }

        // Canonicalize eventGroupReferenceIds for deduplication
        val eventGroupReferenceIds =
          eventGroups.map { eventGroupReferenceIdMap.getValue(it.key) }.sorted()

        val filterSpec =
          FilterSpec(
            celExpression = firstEventGroup.value.filter.expression,
            collectionInterval = firstEventGroup.value.collectionInterval,
            eventGroupReferenceIds = eventGroupReferenceIds,
          )

        filterSpecToReqNames.getOrPut(filterSpec) { mutableListOf() }.add(requisition.name)
        reqNameToFilterSpec[requisition.name] = filterSpec
      }

      return FilterSpecIndex(filterSpecToReqNames, reqNameToFilterSpec)
    }
  }
}

/**
 * Event Processing Orchestrator
 *
 * Provides a high-level coordinator for storage-backed event processing to fulfill requisitions.
 * The orchestrator resolves filters, deduplicates them into unique [FilterSpec]s, constructs one
 * [FrequencyVectorSink] per unique filter, and drives a parallel, batched pipeline that fans event
 * batches out to all sinks.
 *
 * Key responsibilities:
 * - Parse and validate encrypted requisition specs.
 * - Canonicalize and deduplicate filter definitions.
 * - Manage lifecycle of the shared work‑stealing thread pool used by the pipeline.
 * - Execute the pipeline once per invocation and map results back to callers.
 *
 * Invariants:
 * - All event groups in a requisition share the same CEL expression and collection interval.
 * - Filter deduplication uses sorted event group IDs to ensure consistent equality semantics.
 *
 * Error handling:
 * - Pipeline errors fail fast; all in‑flight readers cancel cooperatively.
 *
 * Thread safety:
 * - The orchestrator is not thread‑safe; treat instances as single‑use per invocation context.
 * - Sinks encapsulate their own concurrency control where applicable.
 *
 * Performance characteristics:
 * - Throughput is primarily bounded by storage and CEL evaluation; batch size and worker count
 *   should be tuned accordingly. Deduplicating sinks significantly reduces redundant work when
 *   multiple requisitions specify identical filters.
 *
 * @param privateEncryptionKey Private key used to decrypt requisition specs.
 */
class EventProcessingOrchestrator<T : Message>(private val privateEncryptionKey: PrivateKeyHandle) {

  /**
   * Runs the event processing pipeline for a list of requisitions.
   *
   * Steps:
   * 1. Parse and validate requisition specs; canonicalize each into a [FilterSpec].
   * 2. Deduplicate to one sink per unique [FilterSpec].
   * 3. Execute a single pipeline pass, distributing event batches to all sinks.
   * 4. Return a map from requisition name to its frequency vector (via the sink for its filter).
   *
   * Failure behavior:
   * - Propagates storage and pipeline exceptions; no partial results are returned.
   *
   * @param eventSource Source of events to process.
   * @param vidIndexMap Mapping from VID to population index for frequency vectors.
   * @param populationSpec Population spec defining index space for frequency vectors.
   * @param config Pipeline configuration (batch size, workers, etc.).
   * @param requisitions Requisitions to fulfill
   * @param eventGroupReferenceIdMap Mapping from event group resource name to reference id.
   * @param eventDescriptor Descriptor of the packed event messages (for CEL evaluation).
   * @return Map from requisition name to computed [StripedByteFrequencyVector].
   * @throws ImpressionReadException on impression read failures
   * @throws IllegalArgumentException if a requisition has no event groups or groups are
   *   inconsistent in expression or interval.
   */
  suspend fun run(
    eventSource: EventSource<T>,
    vidIndexMap: VidIndexMap,
    populationSpec: PopulationSpec,
    requisitions: List<Requisition>,
    eventGroupReferenceIdMap: Map<String, String>,
    config: PipelineConfiguration,
    eventDescriptor: Descriptors.Descriptor,
  ): Map<String, StripedByteFrequencyVector> {
    logger.info("[ORCHESTRATOR] === STARTING EVENT PROCESSING ORCHESTRATOR ===")
    logger.info("[ORCHESTRATOR] Processing ${requisitions.size} requisitions")
    logger.info("[ORCHESTRATOR] Event group reference ID map: ${eventGroupReferenceIdMap.size} entries")
    logger.info("[ORCHESTRATOR] Population spec size: ${populationSpec.size}")
    logger.info("[ORCHESTRATOR] VID index map size: ${vidIndexMap.size}")
    logger.info("[ORCHESTRATOR] Event descriptor: ${eventDescriptor.name}")
    logger.info("[ORCHESTRATOR] Pipeline config: batchSize=${config.batchSize}, workers=${config.workers}, threadPoolSize=${config.threadPoolSize}")

    logger.info("[ORCHESTRATOR] Validating pipeline configuration...")
    config.validate()
    logger.info("[ORCHESTRATOR] Configuration validated successfully")

    logger.info("[ORCHESTRATOR] Creating executor service with ${config.threadPoolSize} threads...")
    val executorService: ExecutorService = createExecutorService(config.threadPoolSize)
    logger.info("[ORCHESTRATOR] Executor service created, creating coroutine dispatcher...")
    val dispatcher = executorService.asCoroutineDispatcher()
    logger.info("[ORCHESTRATOR] Coroutine dispatcher created")

    try {
      // Build a mapping from requisitions to canonical FilterSpecs
      logger.info("[ORCHESTRATOR] Building FilterSpecIndex from ${requisitions.size} requisitions...")
      val filterSpecIndex = try {
        FilterSpecIndex.fromRequisitions(
          requisitions,
          eventGroupReferenceIdMap,
          privateEncryptionKey,
        )
      } catch (e: Exception) {
        logger.severe("[ORCHESTRATOR] ERROR building FilterSpecIndex: ${e.message}")
        throw e
      }
      logger.info("[ORCHESTRATOR] FilterSpecIndex built with ${filterSpecIndex.filterSpecToRequisitionNames.size} unique FilterSpecs")
      logger.info("[ORCHESTRATOR] FilterSpec details:")
      filterSpecIndex.filterSpecToRequisitionNames.forEach { (spec, reqNames) ->
        logger.info("[ORCHESTRATOR]   - FilterSpec(expr='${spec.celExpression}', eventGroups=${spec.eventGroupReferenceIds.size}) -> ${reqNames.size} requisitions")
      }

      // Create one sink per unique FilterSpec
      logger.info("[ORCHESTRATOR] Creating sinks for ${filterSpecIndex.filterSpecToRequisitionNames.size} unique FilterSpecs...")
      val sinkByFilterSpec = try {
        createSinksFromFilterSpecs(
          filterSpecs = filterSpecIndex.filterSpecToRequisitionNames.keys,
          vidIndexMap = vidIndexMap,
          populationSpec = populationSpec,
          eventDescriptor = eventDescriptor,
        )
      } catch (e: Exception) {
        logger.severe("[ORCHESTRATOR] ERROR creating sinks: ${e.message}")
        throw e
      }
      logger.info("[ORCHESTRATOR] Created ${sinkByFilterSpec.size} sinks successfully")

      logger.info("[ORCHESTRATOR] Creating pipeline...")
      val pipeline = createPipeline(config)
      logger.info("[ORCHESTRATOR] Pipeline created: ${pipeline.javaClass.simpleName}")

      logger.info("[ORCHESTRATOR] === STARTING PIPELINE EXECUTION ===")
      logger.info("[ORCHESTRATOR] Processing with ${sinkByFilterSpec.size} sinks on custom dispatcher")
      withContext(dispatcher) {
        logger.info("[ORCHESTRATOR] Inside custom dispatcher context, calling processEventBatches()...")
        try {
          pipeline.processEventBatches(
            eventSource = eventSource,
            sinks = sinkByFilterSpec.values.toList(),
          )
          logger.info("[ORCHESTRATOR] processEventBatches() completed successfully")
        } catch (e: Exception) {
          logger.severe("[ORCHESTRATOR] ERROR in processEventBatches(): ${e.message}")
          logger.severe("[ORCHESTRATOR] Exception type: ${e.javaClass.name}")
          e.printStackTrace()
          throw e
        }
      }
      logger.info("[ORCHESTRATOR] === PIPELINE EXECUTION COMPLETED ===")

      // Map results back to requisition names using their FilterSpec
      logger.info("[ORCHESTRATOR] Mapping results back to requisition names...")
      val results = try {
        requisitions.associate { req ->
          logger.info("[ORCHESTRATOR]   - Mapping result for requisition: ${req.name}")
          val spec = filterSpecIndex.requisitionNameToFilterSpec.getValue(req.name)
          val frequencyVector = sinkByFilterSpec.getValue(spec).getFrequencyVector()
          val vectorSize = frequencyVector.getByteArray().size
          logger.info("[ORCHESTRATOR]   - Got frequency vector of size $vectorSize for ${req.name}")
          req.name to frequencyVector
        }
      } catch (e: Exception) {
        logger.severe("[ORCHESTRATOR] ERROR mapping results: ${e.message}")
        throw e
      }

      logger.info("[ORCHESTRATOR] === ORCHESTRATOR COMPLETED SUCCESSFULLY ===")
      logger.info("[ORCHESTRATOR] Returning ${results.size} results")
      return results
    } catch (e: Exception) {
      logger.severe("[ORCHESTRATOR] FATAL ERROR in orchestrator: ${e.message}")
      logger.severe("[ORCHESTRATOR] Exception type: ${e.javaClass.name}")
      e.printStackTrace()
      throw e
    } finally {
      logger.info("[ORCHESTRATOR] Shutting down executor service...")
      shutdownExecutorService(executorService)
      logger.info("[ORCHESTRATOR] Executor service shutdown completed")
    }
  }

  /**
   * Create a bounded, work‑stealing thread pool for pipeline workers.
   *
   * The pool uses async mode to reduce contention under high fan‑out and assigns stable names to
   * worker threads for easier diagnostics.
   *
   * @param size Maximum number of worker threads.
   * @return Configured [ExecutorService] suitable for coroutine dispatching.
   */
  private fun createExecutorService(size: Int): ExecutorService {
    logger.info("[ORCHESTRATOR] Creating shared work-stealing thread pool with max size: $size")

    return ForkJoinPool(
      size,
      { pool ->
        val thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        thread.name = "SharedPool-${thread.poolIndex}"
        thread.isDaemon = false
        thread
      },
      { t, e -> logger.log(Level.SEVERE, "Uncaught exception in thread pool: ${t.name}", e) },
      true, /* asyncMode */
    )
  }

  /**
   * Shut down the shared worker pool gracefully.
   *
   * Waits up to [THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS] for in‑flight work, then forces shutdown if
   * needed. Logs outcomes for operational visibility.
   *
   * @param executorService the ExecutorService to shut down
   */
  private fun shutdownExecutorService(executorService: ExecutorService) {
    logger.info("[ORCHESTRATOR] Shutting down shared thread pool...")

    executorService.shutdown()

    try {
      // Wait a while for existing tasks to terminate
      if (
        !executorService.awaitTermination(THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      ) {
        logger.warning("[ORCHESTRATOR] Thread pool did not terminate gracefully, forcing shutdown")
        executorService.shutdownNow() // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (
          !executorService.awaitTermination(THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        ) {
          logger.warning("[ORCHESTRATOR] Thread pool did not terminate after forceful shutdown")
        }
      }
    } catch (e: InterruptedException) {
      // (Re-)Cancel if current thread also interrupted
      executorService.shutdownNow()
      Thread.currentThread().interrupt()
    }

    logger.info("[ORCHESTRATOR] Thread pool shut down successfully")
  }

  /**
   * Create the event processing pipeline implementation.
   *
   * Currently returns a [ParallelBatchedPipeline] tuned with the provided batch size and worker
   * count.
   *
   * @param config Pipeline configuration values.
   * @return The configured [EventProcessingPipeline].
   */
  private fun createPipeline(config: PipelineConfiguration): EventProcessingPipeline<T> {
    logger.info("[ORCHESTRATOR] Creating ParallelBatchedPipeline with batchSize=${config.batchSize}, workers=${config.workers}")
    val pipeline = ParallelBatchedPipeline<T>(batchSize = config.batchSize, workers = config.workers)
    logger.info("[ORCHESTRATOR] Pipeline created successfully")
    return pipeline
  }

  /**
   * Creates one sink per unique [FilterSpec].
   *
   * Sinks encapsulate:
   * - Parsed/compiled CEL program
   * - Collection interval checks
   * - Event group membership checks
   * - Backing [StripedByteFrequencyVector]
   *
   * @param filterSpecs Unique filter specifications (deduplicated and canonicalized).
   * @param vidIndexMap Mapping from VID to population index for frequency vectors.
   * @param populationSpec Population describing the vector size.
   * @param eventDescriptor Descriptor of the packed event messages.
   * @return Map from [FilterSpec] to a configured [StripedByteFrequencyVectorSink].
   */
  fun createSinksFromFilterSpecs(
    filterSpecs: Collection<FilterSpec>,
    vidIndexMap: VidIndexMap,
    populationSpec: PopulationSpec,
    eventDescriptor: Descriptors.Descriptor,
  ): Map<FilterSpec, FrequencyVectorSink<T>> {
    logger.info("[ORCHESTRATOR] Creating sinks for ${filterSpecs.size} unique FilterSpecs")
    logger.info("[ORCHESTRATOR] Population size: ${populationSpec.size}")
    logger.info("[ORCHESTRATOR] VID index map size: ${vidIndexMap.size}")
    logger.info("[ORCHESTRATOR] Event descriptor: ${eventDescriptor.name}")

    return filterSpecs.associateWith { spec ->
      val index = filterSpecs.indexOf(spec) + 1
      logger.info("[ORCHESTRATOR]   - Creating sink $index/${filterSpecs.size} for FilterSpec: ${spec.celExpression}")
      logger.info("[ORCHESTRATOR]     * Event groups: ${spec.eventGroupReferenceIds.size}")
      logger.info("[ORCHESTRATOR]     * Collection interval: ${spec.collectionInterval}")
      
      val filterProcessor = FilterProcessor<T>(spec, eventDescriptor)
      logger.info("[ORCHESTRATOR]     * FilterProcessor created")
      
      val frequencyVector = StripedByteFrequencyVector(populationSpec.size.toInt())
      logger.info("[ORCHESTRATOR]     * FrequencyVector created with size ${populationSpec.size}")
      
      val sink = FrequencyVectorSink<T>(
        filterProcessor = filterProcessor,
        frequencyVector = frequencyVector,
        vidIndexMap = vidIndexMap,
      )
      logger.info("[ORCHESTRATOR]     * Sink created successfully")
      
      sink
    }
  }

  companion object {
    private val logger = Logger.getLogger(EventProcessingOrchestrator::class.java.name)
    private const val THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 10L
  }
}
