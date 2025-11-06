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
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.fulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.startProcessingRequisitionMetadataRequest

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
 * @param dataProvider [DataProvider] resource name.
 * @param requisitionMetadataStub used to sync [Requisition]s with the RequisitionMetadataStorage
 * @param privateEncryptionKey Private key used to decrypt `RequisitionSpec`s.
 * @param groupedRequisitions The grouped requisitions to fulfill.
 * @param modelLineInfoMap Map of model line to [ModelLineInfo] providing descriptors and indexes.
 * @param pipelineConfiguration Configuration for the event processing pipeline.
 * @param impressionDataSourceProvider Service to resolve impression metadata and sources.
 * @param kmsClient KMS client for accessing encrypted resources in storage.
 * @param impressionsStorageConfig Storage configuration for impression/event ingestion.
 * @param fulfillerSelector Selector for choosing the appropriate fulfiller based on protocol.
 * @param responsePageSize
 */
class ResultsFulfiller(
  private val dataProvider: String,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val privateEncryptionKey: PrivateKeyHandle,
  private val groupedRequisitions: GroupedRequisitions,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val pipelineConfiguration: PipelineConfiguration,
  private val impressionDataSourceProvider: ImpressionDataSourceProvider,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val fulfillerSelector: FulfillerSelector,
  private val responsePageSize: Int? = null,
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
    val requisitions =
      groupedRequisitions.requisitionsList.mapIndexed { index, entry ->
        entry.requisition.unpack(Requisition::class.java)
      }

    val eventGroupReferenceIdMap =
      groupedRequisitions.eventGroupMapList.associate {
        it.eventGroup to it.details.eventGroupReferenceId
      }

    // Filter requisitions that are not in the requisitions metadata storage or have been either
    // fulfilled or refused already
    val requisitionMetadataByName: Map<String, RequisitionMetadata> =
      listRequisitionMetadata().associateBy { it.cmmsRequisition }

    val filteredRequisitions =
      requisitions.filter { it.shouldBeProcessed(requisitionMetadataByName) }

    val updatedRequisitionMetadata: List<RequisitionMetadata> =
      requisitionMetadataByName.values.map { metadata ->
        signalRequisitionStartProcessing(metadata)
      }

    totalRequisitions.addAndGet(filteredRequisitions.size)
    logger.info {
      "Starting fulfillment for ${filteredRequisitions.size} requisitions (${requisitions.size - filteredRequisitions.size} filtered out)"
    }

    val modelLine = groupedRequisitions.modelLine
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val eventDescriptor = modelInfo.eventDescriptor

    val populationSpec = modelInfo.populationSpec
    val vidIndexMap = modelInfo.vidIndexMap

    logger.info {
      "Creating event source for model line: $modelLine with ${groupedRequisitions.eventGroupMapList.size} event groups"
    }
    val eventSource =
      StorageEventSource(
        impressionDataSourceProvider = impressionDataSourceProvider,
        eventGroupDetailsList = groupedRequisitions.eventGroupMapList.map { it.details },
        modelLine = modelLine,
        kmsClient = kmsClient,
        impressionsStorageConfig = impressionsStorageConfig,
        descriptor = eventDescriptor,
        batchSize = pipelineConfiguration.batchSize,
      )

    logger.info {
      "Starting event processing pipeline to compute frequency vectors for ${filteredRequisitions.size} requisitions"
    }
    val frequencyVectorStart = TimeSource.Monotonic.markNow()
    val frequencyVectorMap =
      try {
        orchestrator.run(
          eventSource = eventSource,
          vidIndexMap = vidIndexMap,
          populationSpec = populationSpec,
          requisitions = filteredRequisitions,
          eventGroupReferenceIdMap = eventGroupReferenceIdMap,
          config = pipelineConfiguration,
          eventDescriptor = eventDescriptor,
        )
      } catch (e: Exception) {
        e.printStackTrace()
        throw e
      }
    frequencyVectorTime.addAndGet(frequencyVectorStart.elapsedNow().inWholeNanoseconds)
    logger.info {
      "Frequency vector computation completed in ${frequencyVectorStart.elapsedNow().inWholeMilliseconds}ms. Starting individual requisition fulfillment with parallelism=$parallelism"
    }

    var processedCount = 0
    withContext(Dispatchers.Default) {
      filteredRequisitions
        .asFlow()
        .map { req: Requisition -> req to frequencyVectorMap.getValue(req.name) }
        .flatMapMerge(concurrency = parallelism) {
          (req: Requisition, frequencyVector: StripedByteFrequencyVector) ->
          flow {
            val start = TimeSource.Monotonic.markNow()
            try {
              logger.info { "Starting fulfillment for requisition: ${req.name}" }

              fulfillSingleRequisition(
                req,
                frequencyVector,
                populationSpec,
                updatedRequisitionMetadata,
              )

              fulfillmentTime.addAndGet(start.elapsedNow().inWholeNanoseconds)
              processedCount++
              logger.info {
                "Completed fulfillment for requisition: ${req.name} in ${start.elapsedNow().inWholeMilliseconds}ms (${processedCount}/${filteredRequisitions.size})"
              }
              emit(Unit)
            } catch (t: Throwable) {
              logger.severe { "Failed to fulfill requisition: ${req.name} - ${t.message}" }
              t.printStackTrace()
              throw t
            }
          }
        }
        .collect()
    }

    logger.info { "All ${filteredRequisitions.size} requisitions fulfilled successfully" }

    logFulfillmentStats()
  }

  private fun Requisition.shouldBeProcessed(
    metadataByName: Map<String, RequisitionMetadata>
  ): Boolean {
    val metadata = metadataByName[name]

    requireNotNull(metadata) { "Requisition metadata not found for requisition: $name" }

    return metadata.state != RequisitionMetadata.State.FULFILLED &&
      metadata.state != RequisitionMetadata.State.REFUSED
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
    requisitionsMetadata: List<RequisitionMetadata>,
  ) {
    logger.info { "Processing requisition ${requisition.name}: Start - ${getMemoryStats()}" }
    logger.info { "Processing requisition ${requisition.name}: Unpacking measurement spec" }
    val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
    logger.info { "Processing requisition ${requisition.name}: After unpacking - ${getMemoryStats()}" }
    val frequencyDataBytes = frequencyVector.getByteArray()
    logger.info {
      "Processing requisition ${requisition.name}: After getByteArray (frequency data size: ${frequencyDataBytes.size} bytes) - ${getMemoryStats()}"
    }
    logger.info {
      "Processing requisition ${requisition.name}: Decrypting requisition spec"
    }
    val signedRequisitionSpec: SignedMessage =
      try {
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      } catch (e: GeneralSecurityException) {
        throw Exception("RequisitionSpec decryption failed", e)
      }
    logger.info { "Processing requisition ${requisition.name}: After decryption - ${getMemoryStats()}" }
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack() // TODO: Issue #2914
    logger.info { "Processing requisition ${requisition.name}: After requisition spec unpack - ${getMemoryStats()}" }
    logger.info { "Processing requisition ${requisition.name}: Selecting fulfiller" }
    val buildStart = TimeSource.Monotonic.markNow()
    val fulfiller =
      fulfillerSelector.selectFulfiller(
        requisition,
        measurementSpec,
        requisitionSpec,
        frequencyDataBytes,
        populationSpec,
      )
    logger.info { "Processing requisition ${requisition.name}: After selectFulfiller - ${getMemoryStats()}" }
    buildTime.addAndGet(buildStart.elapsedNow().inWholeNanoseconds)
    logger.info {
      "Processing requisition ${requisition.name}: Sending fulfillment request (build took ${buildStart.elapsedNow().inWholeMilliseconds}ms)"
    }
    val sendStart = TimeSource.Monotonic.markNow()
    fulfiller.fulfillRequisition()
    logger.info {
      "Processing requisition ${requisition.name}: Fulfillment request sent in ${sendStart.elapsedNow().inWholeMilliseconds}ms"
    }

    val requisitionMetadata = requisitionsMetadata.find { it.cmmsRequisition == requisition.name }

    require(requisitionMetadata != null) {
      "Requisition metadata not found for requisition: ${requisition.name}"
    }

    logger.info { "Processing requisition ${requisition.name}: Signaling fulfillment to metadata service" }
    signalRequisitionFulfilled(requisitionMetadata)
    logger.info { "Processing requisition ${requisition.name}: Fulfillment signaled successfully" }

    sendTime.addAndGet(sendStart.elapsedNow().inWholeNanoseconds)
  }

  /**
   * Returns memory statistics in a formatted string.
   */
  private fun getMemoryStats(): String {
    val runtime = Runtime.getRuntime()
    val usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
    val totalMemoryMB = runtime.totalMemory() / (1024 * 1024)
    val maxMemoryMB = runtime.maxMemory() / (1024 * 1024)
    return "Memory: ${usedMemoryMB}MB used / ${totalMemoryMB}MB total / ${maxMemoryMB}MB max"
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
      |  ${getMemoryStats()}
      |==============================
      """
        .trimMargin()
    logger.info(stats)
  }

  // List requisitions metadata for the goup id being processed.
  private suspend fun listRequisitionMetadata(): List<RequisitionMetadata> {
    val requisitionsMetadata: Flow<RequisitionMetadata> =
      requisitionMetadataStub
        .listResources { pageToken: String ->
          val request = listRequisitionMetadataRequest {
            parent = dataProvider
            filter =
              ListRequisitionMetadataRequestKt.filter { groupId = groupedRequisitions.groupId }
            if (responsePageSize != null) {
              pageSize = responsePageSize
            }
            this.pageToken = pageToken
          }
          val response: ListRequisitionMetadataResponse =
            try {
              requisitionMetadataStub.listRequisitionMetadata(request)
            } catch (e: StatusException) {
              throw Exception(
                "Error listing requisition metadata for group id: ${groupedRequisitions.groupId}",
                e,
              )
            }
          ResourceList(response.requisitionMetadataList, response.nextPageToken)
        }
        .flattenConcat()
    return requisitionsMetadata.toList()
  }

  private suspend fun signalRequisitionStartProcessing(
    requisitionMetadata: RequisitionMetadata
  ): RequisitionMetadata {
    val startProcessingRequisitionMetadataRequest = startProcessingRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
    }
    return requisitionMetadataStub.startProcessingRequisitionMetadata(
      startProcessingRequisitionMetadataRequest
    )
  }

  private suspend fun signalRequisitionFulfilled(
    requisitionMetadata: RequisitionMetadata
  ): RequisitionMetadata {
    val fulfillRequisitionMetadataRequest = fulfillRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
    }
    return requisitionMetadataStub.fulfillRequisitionMetadata(fulfillRequisitionMetadataRequest)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    // Memory-based parallelism limit to prevent OOM with large frequency vectors
    // With 360M byte arrays converted to IntArray (4x memory), each requisition uses ~2GB
    // For 80GB heap with conservative overhead accounting, limit to 15 concurrent requisitions
    // This allows ~32GB for requisition processing, leaving ~48GB for JVM overhead and GC
    private const val MAX_FULFILLMENT_PARALLELISM: Int = 15

    /** Utilize all cpu cores but keep one free for GC and system work, capped by memory limit. */
    private val DEFAULT_FULFILLMENT_PARALLELISM: Int =
      ((Runtime.getRuntime().availableProcessors()).coerceAtLeast(2) - 1)
        .coerceAtMost(MAX_FULFILLMENT_PARALLELISM)
  }
}
