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

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Message
import com.google.protobuf.kotlin.unpack
import java.security.GeneralSecurityException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions

/**
 * Fulfills event-level measurement requisitions using protocol-specific fulfillers.
 *
 * This orchestrates the lifecycle for a batch of requisitions: it processes grouped requisitions,
 * calculates frequency vectors via the event processing pipeline, and dispatches fulfillment using
 * a FulfillerSelector to choose the appropriate protocol implementation.
 *
 * Concurrency: supports concurrent fulfillment per batch via Kotlin coroutines. Long-running or
 * blocking operations (storage access, crypto, and RPC) execute on the IO dispatcher as
 * appropriate.
 *
 * @param privateEncryptionKey Private key used to decrypt `RequisitionSpec`s.
 * @param groupedRequisitions The grouped requisitions to fulfill.
 * @param modelLineInfoMap Map of model line to [ModelLineInfo] providing descriptors and indexes.
 * @param pipelineConfiguration Configuration for the event processing pipeline.
 * @param impressionMetadataService Service to resolve impression metadata and sources.
 * @param kmsClient KMS client for accessing encrypted resources in storage.
 * @param impressionsStorageConfig Storage configuration for impression/event ingestion.
 * @param fulfillerSelector Selector for choosing the appropriate fulfiller based on protocol.
 */
class ResultsFulfiller(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val groupedRequisitions: GroupedRequisitions,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val pipelineConfiguration: PipelineConfiguration,
  private val impressionMetadataService: ImpressionMetadataService,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val fulfillerSelector: FulfillerSelector,
) {

  private val totalRequisitions = AtomicInteger(0)

  private val buildTime = AtomicLong(0)
  private val sendTime = AtomicLong(0)
  private val fulfillmentTime = AtomicLong(0)
  private val frequencyVectorTime = AtomicLong(0)

  private val orchestrator: EventProcessingOrchestrator<Message> by lazy {
    EventProcessingOrchestrator<Message>(privateEncryptionKey)
  }

  /**
   * Processes and fulfills all requisitions in the grouped requisitions.
   *
   * Steps:
   * - Builds frequency vectors via the event-processing pipeline.
   * - Selects a protocol-specific fulfiller for each requisition and submits results.
   *
   * @param parallelism Maximum number of requisitions to fulfill concurrently.
   * @throws IllegalArgumentException If a requisition specifies an unsupported protocol.
   * @throws Exception If decryption or RPC fulfillment fails.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  suspend fun fulfillRequisitions(parallelism: Int = DEFAULT_FULFILLMENT_PARALLELISM) {
    logger.info("[FULFILLER] Starting fulfillRequisitions with parallelism=$parallelism")
    logger.info("[FULFILLER] Unpacking ${groupedRequisitions.requisitionsCount} requisitions...")
    val requisitions =
      groupedRequisitions.requisitionsList.mapIndexed { index, entry ->
        if (index % 100 == 0 || index < 10) {
          logger.info(
            "[FULFILLER]   - Unpacking requisition ${index + 1}/${groupedRequisitions.requisitionsCount}"
          )
        }
        entry.requisition.unpack(Requisition::class.java)
      }
    logger.info("[FULFILLER] Building event group reference ID map...")
    val eventGroupReferenceIdMap =
      groupedRequisitions.eventGroupMapList.associate {
        it.eventGroup to it.details.eventGroupReferenceId
      }
    logger.info("[FULFILLER] Event group map built with ${eventGroupReferenceIdMap.size} entries")

    logger.info("[FULFILLER] Processing ${requisitions.size} Requisitions")
    totalRequisitions.addAndGet(requisitions.size)

    val modelLine = groupedRequisitions.modelLine
    logger.info("[FULFILLER] Model line: $modelLine")
    logger.info("[FULFILLER] Getting model info from map...")
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val eventDescriptor = modelInfo.eventDescriptor
    logger.info("[FULFILLER] Event descriptor: ${eventDescriptor.name}")

    val populationSpec = modelInfo.populationSpec
    logger.info(
      "[FULFILLER] Population spec has ${populationSpec.subpopulationsCount} subpopulations"
    )
    val vidIndexMap = modelInfo.vidIndexMap
    logger.info("[FULFILLER] VID index map ready")

    logger.info("[FULFILLER] Creating StorageEventSource...")
    logger.info(
      "[FULFILLER]   - Event group details count: ${groupedRequisitions.eventGroupMapCount}"
    )
    logger.info("[FULFILLER]   - Model line: $modelLine")
    logger.info("[FULFILLER]   - Batch size: ${pipelineConfiguration.batchSize}")
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionMetadataService,
        eventGroupDetailsList = groupedRequisitions.eventGroupMapList.map { it.details },
        modelLine = modelLine,
        kmsClient = kmsClient,
        impressionsStorageConfig = impressionsStorageConfig,
        descriptor = eventDescriptor,
        batchSize = pipelineConfiguration.batchSize,
      )
    logger.info("[FULFILLER] StorageEventSource created successfully")

    logger.info("[FULFILLER] === STARTING FREQUENCY VECTOR CALCULATION ===")
    logger.info("[FULFILLER] Calling orchestrator.run() with:")
    logger.info("[FULFILLER]   - ${requisitions.size} requisitions")
    logger.info(
      "[FULFILLER]   - Pipeline config: batchSize=${pipelineConfiguration.batchSize}, workers=${pipelineConfiguration.workers}"
    )
    logger.info("[FULFILLER]   - Thread pool size: ${pipelineConfiguration.threadPoolSize}")
    logger.info("[FULFILLER]   - Channel capacity: ${pipelineConfiguration.channelCapacity}")

    val frequencyVectorStart = TimeSource.Monotonic.markNow()
    val frequencyVectorMap =
      try {
        logger.info("[FULFILLER] Entering orchestrator.run()...")
        orchestrator.run(
          eventSource = eventSource,
          vidIndexMap = vidIndexMap,
          populationSpec = populationSpec,
          requisitions = requisitions,
          eventGroupReferenceIdMap = eventGroupReferenceIdMap,
          config = pipelineConfiguration,
          eventDescriptor = eventDescriptor,
        )
      } catch (e: Exception) {
        logger.severe("[FULFILLER] ERROR in orchestrator.run(): ${e.message}")
        logger.severe("[FULFILLER] Stack trace:")
        e.printStackTrace()
        throw e
      }
    val elapsedMs = frequencyVectorStart.elapsedNow().inWholeMilliseconds
    frequencyVectorTime.addAndGet(frequencyVectorStart.elapsedNow().inWholeNanoseconds)
    logger.info("[FULFILLER] === FREQUENCY VECTOR CALCULATION COMPLETED in ${elapsedMs}ms ===")
    logger.info("[FULFILLER] Frequency vector map has ${frequencyVectorMap.size} entries")

    logger.info("[FULFILLER] === STARTING INDIVIDUAL REQUISITION FULFILLMENT ===")
    logger.info(
      "[FULFILLER] Processing ${requisitions.size} requisitions with parallelism=$parallelism"
    )

    var processedCount = 0
    requisitions
      .asFlow()
      .map { req: Requisition ->
        logger.info("[FULFILLER] Mapping requisition ${req.name} to frequency vector...")
        req to frequencyVectorMap.getValue(req.name)
      }
      .flatMapMerge(concurrency = parallelism) {
        (req: Requisition, frequencyVector: StripedByteFrequencyVector) ->
        flow {
          val start = TimeSource.Monotonic.markNow()
          try {
            logger.info("[FULFILLER] Starting fulfillment for requisition ${req.name}")
            fulfillSingleRequisition(req, frequencyVector, populationSpec)
            val elapsedMs = start.elapsedNow().inWholeMilliseconds
            fulfillmentTime.addAndGet(start.elapsedNow().inWholeNanoseconds)
            processedCount++
            logger.info(
              "[FULFILLER] Completed requisition ${req.name} in ${elapsedMs}ms ($processedCount/${requisitions.size} done)"
            )
            emit(Unit)
          } catch (t: Throwable) {
            logger.severe("[FULFILLER] ERROR: Failed fulfilling ${req.name}: ${t.message}")
            logger.severe("[FULFILLER] Exception type: ${t.javaClass.name}")
            t.printStackTrace()
            throw t
          }
        }
      }
      .collect()

    logger.info("[FULFILLER] === ALL REQUISITIONS COMPLETED ===")
    logger.info("[FULFILLER] Total requisitions processed: $processedCount")

    logger.info("[FULFILLER] Calling logFulfillmentStats()...")
    logFulfillmentStats()
    logger.info("[FULFILLER] fulfillRequisitions() completed successfully")
  }

  /**
   * Decrypts inputs, selects a protocol implementation, and fulfills a single requisition.
   *
   * @param requisition The `Requisition` to fulfill.
   * @param frequencyVector Pre-computed per-VID frequency vector for this requisition.
   * @param populationSpec Population specification associated with the model line.
   * @throws Exception If the requisition spec cannot be decrypted or fulfillment fails.
   */
  private suspend fun fulfillSingleRequisition(
    requisition: Requisition,
    frequencyVector: StripedByteFrequencyVector,
    populationSpec: PopulationSpec,
  ) {
    logger.info("[SINGLE] Processing requisition: ${requisition.name}")
    logger.info("[SINGLE] Unpacking measurement spec...")
    val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
    logger.info("[SINGLE] Measurement type: ${measurementSpec.measurementTypeCase}")

    logger.info("[SINGLE] Converting frequency vector to int array...")
    val freqBytes = frequencyVector.getByteArray()
    val frequencyData: IntArray = freqBytes.map { it.toInt() and 0xFF }.toIntArray()
    logger.info("[SINGLE] Frequency data size: ${frequencyData.size}")

    logger.info("[SINGLE] Decrypting requisition spec...")
    val signedRequisitionSpec: SignedMessage =
      try {
        withContext(Dispatchers.IO) {
          logger.info("[SINGLE] Calling decryptRequisitionSpec on IO dispatcher...")
          val result =
            decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
          logger.info("[SINGLE] Decryption successful")
          result
        }
      } catch (e: GeneralSecurityException) {
        logger.severe("[SINGLE] Decryption failed: ${e.message}")
        throw Exception("RequisitionSpec decryption failed", e)
      }
    logger.info("[SINGLE] Unpacking requisition spec...")
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
    logger.info("[SINGLE] Requisition spec unpacked successfully")

    logger.info("[SINGLE] Selecting fulfiller via fulfillerSelector...")
    val buildStart = TimeSource.Monotonic.markNow()
    val fulfiller =
      try {
        fulfillerSelector.selectFulfiller(
          requisition,
          measurementSpec,
          requisitionSpec,
          frequencyData,
          populationSpec,
        )
      } catch (e: Exception) {
        logger.severe("[SINGLE] Fulfiller selection failed: ${e.message}")
        throw e
      }
    val buildElapsedMs = buildStart.elapsedNow().inWholeMilliseconds
    buildTime.addAndGet(buildStart.elapsedNow().inWholeNanoseconds)
    logger.info("[SINGLE] Fulfiller selected in ${buildElapsedMs}ms")

    logger.info("[SINGLE] Ready to fulfill requisition: ${requisition.name}")
    logger.info("[SINGLE] Measurement spec type: ${measurementSpec.measurementTypeCase}")
    logger.info(
      "[SINGLE] Protocol configs: ${requisition.protocolConfig.protocolsList.map { it.protocolCase }}"
    )

    logger.info("[SINGLE] Calling fulfiller.fulfillRequisition() on IO dispatcher...")
    val sendStart = TimeSource.Monotonic.markNow()
    withContext(Dispatchers.IO) {
      logger.info("[SINGLE] Inside IO dispatcher, calling fulfillRequisition()...")
      fulfiller.fulfillRequisition()
      logger.info("[SINGLE] fulfillRequisition() completed")
    }
    val sendElapsedMs = sendStart.elapsedNow().inWholeMilliseconds
    sendTime.addAndGet(sendStart.elapsedNow().inWholeNanoseconds)
    logger.info("[SINGLE] Fulfillment sent in ${sendElapsedMs}ms")
  }

  /**
   * Logs aggregate counters and timings for the current process lifetime.
   *
   * Includes totals for requisitions processed, frequency vector construction, builder creation,
   * send time, and end-to-end fulfillment time.
   */
  fun logFulfillmentStats() {
    val stats =
      """
      |=== FULFILLMENT STATISTICS ===
      |  Total requisitions: ${totalRequisitions.get()}
      |  Frequency vector total ms: ${frequencyVectorTime.get() / 1_000_000}
      |  Build total ms: ${buildTime.get() / 1_000_000}
      |  Send total ms: ${sendTime.get() / 1_000_000}
      |  Fulfillment total ms: ${fulfillmentTime.get() / 1_000_000}
      |  Average per requisition:
      |    - Frequency vector: ${if (totalRequisitions.get() > 0) (frequencyVectorTime.get() / 1_000_000) / totalRequisitions.get() else 0}ms
      |    - Build: ${if (totalRequisitions.get() > 0) (buildTime.get() / 1_000_000) / totalRequisitions.get() else 0}ms
      |    - Send: ${if (totalRequisitions.get() > 0) (sendTime.get() / 1_000_000) / totalRequisitions.get() else 0}ms
      |    - Total: ${if (totalRequisitions.get() > 0) (fulfillmentTime.get() / 1_000_000) / totalRequisitions.get() else 0}ms
      |==============================
      """
        .trimMargin()
    logger.info(stats)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Utilize all cpu cores but keep one free for GC and system work. */
    private val DEFAULT_FULFILLMENT_PARALLELISM: Int =
      (Runtime.getRuntime().availableProcessors()).coerceAtLeast(2) - 1
  }
}
