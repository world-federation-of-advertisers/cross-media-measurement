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
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import java.io.File
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.computation.KAnonymityParams
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
 * ### Execution Flow:
 * 1. Generate and deduplicate filters from requisitions
 * 2. Run single pipeline execution on storage events
 * 3. Aggregate frequency vectors by requisition
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
    eventSource: EventSource,
    vidIndexMap: VidIndexMap,
    populationSpec: PopulationSpec,
    requisitions: List<Requisition>,
    config: PipelineConfiguration,
    eventDescriptor: Descriptors.Descriptor,
    kAnonymityParams: KAnonymityParams
  ): Map<String, FrequencyVector> {
    logger.info("Starting EventProcessingOrchestrator.runWithRequisitions with ${requisitions.size} requisitions")
    requisitions.forEach { req ->
      logger.info("Requisition: ${req.name}, state: ${req.state}, measurement: ${req.measurement}")
    }

    config.validate()

    val threadPool = createThreadPool(config.threadPoolSize)
    val dispatcher = threadPool.asCoroutineDispatcher()

    try {
      // Create frequency vector sinks, one per requisition
      val requisitionSinks = createSinksFromRequisitions(
        requisitions = requisitions,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        eventDescriptor = eventDescriptor,
        kAnonymityParams = kAnonymityParams
      )

      logger.info("Created ${requisitionSinks.size} sinks for requisitions")

      val pipeline = createPipeline(config, dispatcher)
      logger.info("Starting pipeline processing with ${requisitionSinks.size} sinks")

      pipeline.processEventBatches(
        eventSource = eventSource,
        sinks = requisitionSinks.values.toList()
      )

      // Return frequency vectors by requisition
      val results = requisitionSinks.mapValues { (name, sink) ->
        val frequencyVector = sink.getFrequencyVector()
        logger.info("Requisition $name: FrequencyVector with ${frequencyVector.getArray().size} elements, sum=${frequencyVector.getArray().sum()}")
        frequencyVector
      }

      logger.info("EventProcessingOrchestrator completed successfully, returning ${results.size} results")
      return results

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
    )
  }

  /**
   * Creates frequency vector sinks from requisitions, one per requisition.
   */
  private fun createSinksFromRequisitions(
    requisitions: List<Requisition>,
    vidIndexMap: VidIndexMap,
    populationSpec: PopulationSpec,
    eventDescriptor: Descriptors.Descriptor,
    kAnonymityParams: KAnonymityParams
  ): Map<String, FrequencyVectorSink> {
    logger.info("Creating sinks from ${requisitions.size} requisitions")
    val requisitionSinks = mutableMapOf<String, FrequencyVectorSink>()

    for (requisition in requisitions) {
      logger.info("Processing requisition: ${requisition.name}")

      // Extract event groups from the requisition spec
      val signedRequisitionSpec: SignedMessage = decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

      // Extract measurement spec
      val signedMeasurementSpec: SignedMessage = requisition.measurementSpec
      val measurementSpec: MeasurementSpec = signedMeasurementSpec.unpack()

      logger.info("Requisition ${requisition.name}: MeasurementSpec VID sampling start=${measurementSpec.vidSamplingInterval.start}, width=${measurementSpec.vidSamplingInterval.width}")

      require(!requisitionSpec.events.eventGroupsList.isEmpty()) {
        "event groups list is empty for requisition"
      }

      requisitionSpec.events.eventGroupsList.let { eventGroups ->
          val firstEventGroup = eventGroups.first()

          logger.info("Requisition ${requisition.name}: ${eventGroups.size} event groups")
          eventGroups.forEach { eg ->
            logger.info("  Event group: ${eg.key}, filter: '${eg.value.filter.expression}', interval: ${eg.value.collectionInterval}")
          }

          // Validate all event groups have consistent filter and interval
          require(eventGroups.all { it.value.filter.expression == firstEventGroup.value.filter.expression }) {
            "All event groups must have the same CEL expression"
          }
          require(eventGroups.all { it.value.collectionInterval == firstEventGroup.value.collectionInterval }) {
            "All event groups must have the same collection interval"
          }

          val filterSpec = FilterSpec(
            celExpression = firstEventGroup.value.filter.expression,
            collectionInterval = firstEventGroup.value.collectionInterval,
            eventGroupReferenceIds = eventGroups.map { it.key }
          )

          logger.info("Requisition ${requisition.name}: Creating FilterProcessor with CEL expression='${filterSpec.celExpression}', eventGroupReferenceIds=${filterSpec.eventGroupReferenceIds}")

          requisitionSinks[requisition.name] = FrequencyVectorSink(
            filterProcessor = FilterProcessor(
              filterSpec = filterSpec,
              eventDescriptor = eventDescriptor
            ),
            frequencyVector = StripedByteFrequencyVector(
              populationSpec = populationSpec,
              measurementSpec = measurementSpec,
              kAnonymityParams = kAnonymityParams
            ),
            vidIndexMap = vidIndexMap
          )

          logger.info("Requisition ${requisition.name}: Sink created successfully")
        }
    }

    logger.info("Created ${requisitionSinks.size} frequency vector sinks for ${requisitions.size} requisitions")
    return requisitionSinks
  }

  /**
   * Loads a population spec from a textproto file.
   *
   * @param path The file path to load from
   * @param isSynthetic If true, loads as SyntheticPopulationSpec and converts to PopulationSpec
   * @return PopulationSpec loaded from the file
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
