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
import com.google.protobuf.Any
import com.google.protobuf.ByteString
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
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.DirectMeasurementResultFactory
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.DirectMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.HMShuffleMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.MeasurementFulfiller
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.size
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Fulfills event-level measurement requisitions using protocol-specific fulfillers.
 *
 * This orchestrates the lifecycle for a batch of requisitions: it loads grouped requisitions from
 * blob storage, calculates frequency vectors via the event processing pipeline, and dispatches
 * fulfillment using either the Direct or Honest-Majority Shuffle protocol implementation.
 *
 * Concurrency: supports concurrent fulfillment per batch via Kotlin coroutines. Long-running or
 * blocking operations (storage access, crypto, and RPC) execute on the IO dispatcher as
 * appropriate.
 *
 * @param privateEncryptionKey Private key used to decrypt `RequisitionSpec`s.
 * @param requisitionsStub gRPC stub for the Requisitions service.
 * @param requisitionFulfillmentStubMap Map from EDP hostname to fulfillment stubs.
 * @param dataProviderCertificateKey Certificate/key pair identifying the data provider.
 * @param dataProviderSigningKeyHandle Signing key used to authenticate fulfillment.
 * @param requisitionsBlobUri Blob URI where grouped requisitions are stored.
 * @param requisitionsStorageConfig Storage configuration for reading grouped requisitions.
 * @param noiserSelector Selector used to choose the noise mechanism for Direct measurements.
 * @param modelLineInfoMap Map of model line to [ModelLineInfo] providing descriptors and indexes.
 * @param kAnonymityParams Optional k-anonymity parameters to apply, when supported.
 * @param pipelineConfiguration Configuration for the event processing pipeline.
 * @param impressionMetadataService Service to resolve impression metadata and sources.
 * @param kmsClient KMS client for accessing encrypted resources in storage.
 * @param impressionsStorageConfig Storage configuration for impression/event ingestion.
 *
 * TODO(2347): Support additional differential privacy and k-anonymization strategies.
 */
class ResultsFulfiller(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val requisitionFulfillmentStubMap:
    Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val requisitionsBlobUri: String,
  private val requisitionsStorageConfig: StorageConfig,
  private val noiserSelector: NoiserSelector,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val kAnonymityParams: KAnonymityParams?,
  private val pipelineConfiguration: PipelineConfiguration,
  private val impressionMetadataService: ImpressionMetadataService,
  private val kmsClient: KmsClient,
  private val impressionsStorageConfig: StorageConfig,
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
   * Loads, processes, and fulfills all requisitions in the configured blob.
   *
   * Steps:
   * - Reads [GroupedRequisitions] from `requisitionsBlobUri` using `requisitionsStorageConfig`.
   * - Builds frequency vectors via the event-processing pipeline.
   * - Selects a protocol-specific fulfiller for each requisition and submits results.
   *
   * @param parallelism Maximum number of requisitions to fulfill concurrently.
   * @throws IllegalArgumentException If a requisition specifies an unsupported protocol.
   * @throws Exception If decryption, storage access, or RPC fulfillment fails.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  suspend fun fulfillRequisitions(parallelism: Int = DEFAULT_FULFILLMENT_PARALLELISM) {
    logger.info("ResultsFulfiller.fulfillRequisitions() started")
    val groupedRequisitions = getRequisitions()
    logger.info("Retrieved grouped requisitions with ${groupedRequisitions.requisitionsList.size} items")
    val requisitions =
      groupedRequisitions.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
    val eventGroupReferenceIdMap =
      groupedRequisitions.eventGroupMapList.associate {
        it.eventGroup to it.details.eventGroupReferenceId
      }

    logger.info("Processing ${requisitions.size} Requisitions")
    logger.info("Event group reference ID map: ${eventGroupReferenceIdMap.keys}")
    totalRequisitions.addAndGet(requisitions.size)

    val modelLine = groupedRequisitions.modelLine
    logger.info("Model line: $modelLine")
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val eventDescriptor = modelInfo.eventDescriptor

    val populationSpec = modelInfo.populationSpec
    val vidIndexMap = modelInfo.vidIndexMap
    logger.info("Population spec size: ${populationSpec.size}, VID index map size: ${vidIndexMap.size}")

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
    logger.info("Created StorageEventSource with ${groupedRequisitions.eventGroupMapList.size} event group details")

    val frequencyVectorStart = TimeSource.Monotonic.markNow()
    logger.info("Starting event processing orchestrator...")
    val frequencyVectorMap =
      orchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = eventGroupReferenceIdMap,
        config = pipelineConfiguration,
        eventDescriptor = eventDescriptor,
      )
    frequencyVectorTime.addAndGet(frequencyVectorStart.elapsedNow().inWholeNanoseconds)

    logger.info("Frequency vector calculation completed, got ${frequencyVectorMap.size} results")
    logger.info("Processing individual requisitions with parallelism: $parallelism")

    requisitions
      .asFlow()
      .map { req: Requisition -> req to frequencyVectorMap.getValue(req.name) }
      .flatMapMerge(concurrency = parallelism) {
        (req: Requisition, frequencyVector: StripedByteFrequencyVector) ->
        flow {
          val start = TimeSource.Monotonic.markNow()
          try {
            fulfillSingleRequisition(req, frequencyVector, populationSpec)
            fulfillmentTime.addAndGet(start.elapsedNow().inWholeNanoseconds)
            emit(Unit)
          } catch (t: Throwable) {
            logger.severe("Failed fulfilling ${req.name}: ${t.message}")
            throw t
          }
        }
      }
      .collect()

    logger.info("All requisitions fulfilled successfully")
    logFulfillmentStats()
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
    logger.info("Starting fulfillSingleRequisition for: ${requisition.name}")
    val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
    val freqBytes = frequencyVector.getByteArray()
    val frequencyData: IntArray = freqBytes.map { it.toInt() and 0xFF }.toIntArray()
    val nonZeroEntries = frequencyData.count { it > 0 }
    val totalCount = frequencyData.sum()
    val maxFrequency = frequencyData.maxOrNull() ?: 0
    logger.info("Frequency data size: ${frequencyData.size}, non-zero entries: $nonZeroEntries, total count: $totalCount, max frequency: $maxFrequency")
    
    // Log first few non-zero entries for debugging
    val nonZeroSample = frequencyData.withIndex().filter { it.value > 0 }.take(10)
    logger.info("Sample non-zero entries: ${nonZeroSample.map { "(idx=${it.index}, freq=${it.value})" }}")
    
    // Calculate reach expectation from frequency data
    val reachFromFreqData = frequencyData.count { it > 0 }
    logger.info("Expected reach from frequency data: $reachFromFreqData")

    val signedRequisitionSpec: SignedMessage =
      try {
        withContext(Dispatchers.IO) {
          decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
        }
      } catch (e: GeneralSecurityException) {
        throw Exception("RequisitionSpec decryption failed", e)
      }
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

    val buildStart = TimeSource.Monotonic.markNow()
    val fulfiller =
      selectFulfiller(
        requisition = requisition,
        measurementSpec = measurementSpec,
        requisitionSpec = requisitionSpec,
        frequencyData = frequencyData,
        populationSpec = populationSpec,
      )
    buildTime.addAndGet(buildStart.elapsedNow().inWholeNanoseconds)

    logger.info("Fulfilling requisition: ${requisition.name}")
    logger.info("Measurement spec type: ${measurementSpec.measurementTypeCase}")
    logger.info(
      "Protocol configs: ${requisition.protocolConfig.protocolsList.map { it.protocolCase }}"
    )

    val sendStart = TimeSource.Monotonic.markNow()
    logger.info("Sending fulfillment for requisition: ${requisition.name}")
    withContext(Dispatchers.IO) { fulfiller.fulfillRequisition() }
    sendTime.addAndGet(sendStart.elapsedNow().inWholeNanoseconds)
    logger.info("Successfully fulfilled requisition: ${requisition.name}")
  }

  /**
   * Chooses the appropriate `MeasurementFulfiller` based on the requisition protocol.
   * - Direct: builds a [DirectMeasurementFulfiller].
   * - Honest-Majority Share Shuffle: builds an [HMShuffleMeasurementFulfiller], optionally applying
   *   k-anonymity.
   *
   * @throws IllegalArgumentException If the protocol is unsupported.
   */
  private suspend fun selectFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    frequencyData: IntArray,
    populationSpec: PopulationSpec,
  ): MeasurementFulfiller {
    return if (requisition.protocolConfig.protocolsList.any { it.hasDirect() }) {
      // TODO: Calculate the maximum population for a given cel filter
      buildDirectMeasurementFulfiller(
        requisition = requisition,
        measurementSpec = measurementSpec,
        requisitionSpec = requisitionSpec,
        maxPopulation = null,
        frequencyData = frequencyData,
        kAnonymityParams = kAnonymityParams,
      )
    } else if (
      requisition.protocolConfig.protocolsList.any { it.hasHonestMajorityShareShuffle() }
    ) {
      if (kAnonymityParams == null) {
        HMShuffleMeasurementFulfiller(
          requisition,
          requisitionSpec.nonce,
          createFrequencyVectorBuilderFromArray(measurementSpec, populationSpec, frequencyData)
            .build(),
          dataProviderSigningKeyHandle,
          dataProviderCertificateKey,
          requisitionFulfillmentStubMap,
        )
      } else {
        HMShuffleMeasurementFulfiller.buildKAnonymized(
          requisition,
          requisitionSpec.nonce,
          measurementSpec,
          populationSpec,
          createFrequencyVectorBuilderFromArray(measurementSpec, populationSpec, frequencyData),
          dataProviderSigningKeyHandle,
          dataProviderCertificateKey,
          requisitionFulfillmentStubMap,
          kAnonymityParams,
          maxPopulation = null,
        )
      }
    } else {
      throw IllegalArgumentException("Protocol not supported for ${requisition.name}")
    }
  }

  /**
   * Creates a [FrequencyVectorBuilder] and populates it from a raw frequency array.
   *
   * @param measurementSpec The measurement specification describing the sketch layout.
   * @param populationSpec The population specification used when building the sketch.
   * @param frequencyData Raw frequency counts per index (byte-expanded to `Int`).
   * @return A builder pre-populated with the provided frequencies.
   */
  private fun createFrequencyVectorBuilderFromArray(
    measurementSpec: MeasurementSpec,
    populationSpec: PopulationSpec,
    frequencyData: IntArray,
  ): FrequencyVectorBuilder {
    val builder = FrequencyVectorBuilder(populationSpec, measurementSpec, strict = false)

    // Populate the builder with the frequency data
    for (index in frequencyData.indices) {
      repeat(frequencyData[index]) { builder.increment(index) }
    }

    return builder
  }

  /**
   * Loads [GroupedRequisitions] from blob storage using the configured URI.
   *
   * Validates that the blob exists and is in the expected serialized `Any` format.
   *
   * @return The parsed [GroupedRequisitions] payload.
   * @throws ImpressionReadException If the blob is missing or has an invalid format.
   */
  private suspend fun getRequisitions(): GroupedRequisitions {
    val storageClientUri = SelectedStorageClient.parseBlobUri(requisitionsBlobUri)
    val requisitionsStorageClient =
      SelectedStorageClient(
        storageClientUri,
        requisitionsStorageConfig.rootDirectory,
        requisitionsStorageConfig.projectId,
      )

    val requisitionBytes: ByteString =
      requisitionsStorageClient.getBlob(storageClientUri.key)?.read()?.flatten()
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )

    return try {
      Any.parseFrom(requisitionBytes).unpack(GroupedRequisitions::class.java)
    } catch (e: Exception) {
      throw ImpressionReadException(
        storageClientUri.key,
        ImpressionReadException.Code.INVALID_FORMAT,
      )
    }
  }

  /**
   * Builds a [DirectMeasurementFulfiller] and an encrypted measurement result for Direct protocol.
   *
   * Uses the configured [NoiserSelector] to choose a noise mechanism and
   * [DirectMeasurementResultFactory] to construct the encrypted result.
   *
   * @param requisition The requisition being fulfilled.
   * @param measurementSpec The measurement spec containing the public encryption key.
   * @param requisitionSpec The decrypted requisition spec, including the nonce.
   * @param maxPopulation Optional upper bound on eligible population (when available).
   * @param frequencyData Raw frequency counts used to compute the result.
   * @param kAnonymityParams Optional k-anonymity parameters to apply.
   * @return A fully configured [DirectMeasurementFulfiller].
   */
  private suspend fun buildDirectMeasurementFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    maxPopulation: Int?,
    frequencyData: IntArray,
    kAnonymityParams: KAnonymityParams?,
  ): DirectMeasurementFulfiller {
    val measurementEncryptionPublicKey: EncryptionPublicKey =
      measurementSpec.measurementPublicKey.unpack()
    val directProtocolConfig =
      requisition.protocolConfig.protocolsList.first { it.hasDirect() }.direct
    val noiseMechanism =
      noiserSelector.selectNoiseMechanism(directProtocolConfig.noiseMechanismsList)
    val result =
      DirectMeasurementResultFactory.buildMeasurementResult(
        directProtocolConfig,
        noiseMechanism,
        measurementSpec,
        frequencyData,
        maxPopulation,
        kAnonymityParams = kAnonymityParams,
      )
    return DirectMeasurementFulfiller(
      requisition.name,
      requisition.dataProviderCertificate,
      result,
      requisitionSpec.nonce,
      measurementEncryptionPublicKey,
      directProtocolConfig,
      noiseMechanism,
      dataProviderSigningKeyHandle,
      dataProviderCertificateKey,
      requisitionsStub,
    )
  }

  /**
   * Logs aggregate counters and timings for the current process lifetime.
   *
   * Includes totals for requisitions processed, frequency vector construction, builder creation,
   * send time, and end-to-end fulfillment time.
   */
  fun logFulfillmentStats() {
    logger.info(
      """
      |[ResultsFulfiller] Fulfillment Statistics:
      |  Total requisitions: ${totalRequisitions.get()}
      |  Frequency vector total ms: ${frequencyVectorTime.get() / 1_000_000}
      |  Build total ms: ${buildTime.get() / 1_000_000}
      |  Send total ms: ${sendTime.get() / 1_000_000}
      |  Fulfillment total ms: ${fulfillmentTime.get() / 1_000_000}
      """
        .trimMargin()
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Utilize all cpu cores but keep one free for GC and system work. */
    private val DEFAULT_FULFILLMENT_PARALLELISM: Int =
      (Runtime.getRuntime().availableProcessors()).coerceAtLeast(2) - 1
  }
}
