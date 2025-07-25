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
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventProcessingOrchestrator.Companion.THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.size

/**
 * Event Processing Orchestrator
 *
 * Provides a high-level coordinator for storage-backed event processing to fulfill requisitions.
 * The orchestrator resolves filters, deduplicates them into unique [FilterSpec]s, constructs one
 * [FrequencyVectorSink] per unique filter, and drives a parallel, batched pipeline that fans event
 * batches out to all sinks. Results can be returned either per requisition (legacy shape) or per
 * filter (advanced shape) without re-reading storage.
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
 * - Storage errors surface as [ImpressionReadException].
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
class EventProcessingOrchestrator(
  private val privateEncryptionKey: PrivateKeyHandle
) {

  companion object {
    private val logger = Logger.getLogger(EventProcessingOrchestrator::class.java.name)
    private const val THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS = 10L
  }

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
   * - Throws on invalid requisition specs (e.g., empty event groups, inconsistent filters).
   * - Propagates storage and pipeline exceptions; no partial results are returned.
   *
   * @param eventSource Source of events to process.
   * @param vidIndexMap Mapping from VID to population index for frequency vectors.
   * @param populationSpec Population spec defining index space for frequency vectors.
   * @param requisitions Requisitions to fulfill.
   * @param config Pipeline configuration (batch size, workers, etc.).
   * @param eventDescriptor Descriptor of the packed event messages (for CEL evaluation).
   * @return Map from requisition name to computed [FrequencyVector].
   */
  suspend fun runWithRequisitions(
    eventSource: EventSource,
    vidIndexMap: VidIndexMap,
    populationSpec: PopulationSpec,
    requisitions: List<Requisition>,
    config: PipelineConfiguration,
    eventDescriptor: Descriptors.Descriptor,
  ): Map<String, FrequencyVector> {
    logger.info("Starting EventProcessingOrchestrator.runWithRequisitions with ${requisitions.size} requisitions")
    requisitions.forEach { req ->
      logger.info("Requisition: ${req.name}, state: ${req.state}, measurement: ${req.measurement}")
    }

    config.validate()

    val threadPool = createThreadPool(config.threadPoolSize)
    val dispatcher = threadPool.asCoroutineDispatcher()

    try {
      // Build a mapping from requisitions to canonical FilterSpecs
      val (filterSpecToReqs, requisitionToFilterSpec) =
        buildFilterSpecIndexFromRequisitions(requisitions)

      // Create one sink per unique FilterSpec
      val sinkByFilterSpec = createSinksFromFilterSpecs(
        filterSpecs = filterSpecToReqs.keys,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        eventDescriptor = eventDescriptor,
      )

      logger.info(
        "Created ${sinkByFilterSpec.size} unique sinks for ${requisitions.size} requisitions"
      )

      val pipeline = createPipeline(config, dispatcher)
      logger.info("Starting pipeline processing with ${sinkByFilterSpec.size} sinks")

      // Exceptions thrown by any pipeline coroutine are propagated by structured concurrency.
      // Log context and rethrow; the finally block guarantees pool shutdown.
      try {
        pipeline.processEventBatches(
          eventSource = eventSource,
          sinks = sinkByFilterSpec.values.toList()
        )
      } catch (t: Throwable) {
        logger.log(Level.SEVERE, "Pipeline failed", t)
        throw t
      }

      // Map results back to requisition names using their FilterSpec
      val results = requisitions.associate { req ->
        val spec = checkNotNull(requisitionToFilterSpec[req.name])
        req.name to checkNotNull(sinkByFilterSpec[spec]).getFrequencyVector()
      }

      logger.info("EventProcessingOrchestrator completed successfully, returning ${results.size} results")
      return results

    } finally {
      shutdownThreadPool(threadPool)
    }
  }

  /**
   * Create a bounded, work‑stealing thread pool for pipeline workers.
   *
   * The pool uses async mode to reduce contention under high fan‑out and assigns stable names to
   * worker threads for easier diagnostics.
   *
   * @param size Maximum number of worker threads.
   * @return Configured [ForkJoinPool] suitable for coroutine dispatching.
   */
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
      { t, e -> logger.log(Level.SEVERE, "Uncaught exception in thread pool: ${t.name}", e) },
      true /* asyncMode */
    )
  }

  /**
   * Shut down the shared worker pool gracefully.
   *
   * Waits up to [THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS] for in‑flight work, then forces shutdown if
   * needed. Logs outcomes for operational visibility.
   *
   * @param threadPool The pool to shut down.
   */
  private fun shutdownThreadPool(threadPool: ForkJoinPool) {
    logger.info("Shutting down shared thread pool...")

    threadPool.shutdown()

    if (!threadPool.awaitTermination(THREAD_POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      logger.warning("Thread pool did not terminate gracefully, forcing shutdown")
      threadPool.shutdownNow()
    }

    logger.info("Thread pool shut down successfully")
  }

  /**
   * Create the event processing pipeline implementation.
   *
   * Currently returns a [ParallelBatchedPipeline] tuned with the provided batch size and worker
   * count. The dispatcher controls concurrency across I/O and CPU steps.
   *
   * @param config Pipeline configuration values.
   * @param dispatcher Coroutine dispatcher backed by the shared worker pool.
   * @return The configured [EventProcessingPipeline].
   */
  private fun createPipeline(
    config: PipelineConfiguration,
    dispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): EventProcessingPipeline {
    return ParallelBatchedPipeline(
      batchSize = config.batchSize,
      workers = config.workers,
      dispatcher = dispatcher,
    )
  }

  /**
   * Creates one sink per unique [FilterSpec].
   *
   * Sinks encapsulate:
   * - Parsed/compiled CEL program
   * - Collection interval checks
   * - Event group membership checks
   * - Backing [FrequencyVector]
   *
   * Callers typically invoke this prior to running the pipeline, then pass the resulting sinks to
   * batch processing.
   *
   * @param filterSpecs Unique filter specifications (deduplicated and canonicalized).
   * @param vidIndexMap Mapping from VID to population index for frequency vectors.
   * @param populationSpec Population describing the vector size.
   * @param eventDescriptor Descriptor of the packed event messages.
   * @return Map from [FilterSpec] to a configured [FrequencyVectorSink].
   */
  fun createSinksFromFilterSpecs(
    filterSpecs: Collection<FilterSpec>,
    vidIndexMap: VidIndexMap,
    populationSpec: PopulationSpec,
    eventDescriptor: Descriptors.Descriptor,
  ): Map<FilterSpec, FrequencyVectorSink> {
    logger.info("Creating sinks for ${filterSpecs.size} unique FilterSpecs")
    return filterSpecs.associateWith { spec ->
      FrequencyVectorSink(
        filterProcessor = FilterProcessor(filterSpec = spec, eventDescriptor = eventDescriptor),
        frequencyVector = StripedByteFrequencyVector(populationSpec.size.toInt()),
        vidIndexMap = vidIndexMap,
      )
    }
  }

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
   * @return Pair of (FilterSpec -> requisition names, requisition name -> FilterSpec).
   * @throws IllegalArgumentException if a requisition has no event groups or groups are
   *   inconsistent in expression or interval.
   */
  fun buildFilterSpecIndexFromRequisitions(
    requisitions: List<Requisition>,
  ): Pair<Map<FilterSpec, List<String>>, Map<String, FilterSpec>> {
    val filterSpecToReqNames = mutableMapOf<FilterSpec, MutableList<String>>()
    val reqNameToFilterSpec = mutableMapOf<String, FilterSpec>()

    for (requisition in requisitions) {
      logger.info("Processing requisition for FilterSpec index: ${requisition.name}")

      val signedRequisitionSpec: SignedMessage =
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

      require(!requisitionSpec.events.eventGroupsList.isEmpty()) {
        "event groups list is empty for requisition"
      }

      val eventGroups = requisitionSpec.events.eventGroupsList
      val firstEventGroup = eventGroups.first()

      // Validate consistent filter and interval across groups
      require(eventGroups.all { it.value.filter.expression == firstEventGroup.value.filter.expression }) {
        "All event groups must have the same CEL expression"
      }
      require(eventGroups.all { it.value.collectionInterval == firstEventGroup.value.collectionInterval }) {
        "All event groups must have the same collection interval"
      }

      // Canonicalize eventGroupReferenceIds for deduplication
      val eventGroupIds = eventGroups.map { it.key }.sorted()
      val filterSpec =
        FilterSpec(
          celExpression = firstEventGroup.value.filter.expression,
          collectionInterval = firstEventGroup.value.collectionInterval,
          eventGroupReferenceIds = eventGroupIds,
        )

      filterSpecToReqNames.getOrPut(filterSpec) { mutableListOf() }.add(requisition.name)
      reqNameToFilterSpec[requisition.name] = filterSpec
    }

    return Pair(filterSpecToReqNames, reqNameToFilterSpec)
  }
}
