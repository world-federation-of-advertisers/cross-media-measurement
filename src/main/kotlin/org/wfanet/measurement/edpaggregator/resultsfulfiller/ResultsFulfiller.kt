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
import org.wfanet.measurement.storage.SelectedStorageClient

private val totalRequisitions = AtomicInteger(0)

private val buildTime = AtomicLong(0)
private val sendTime = AtomicLong(0)
private val fulfillmentTime = AtomicLong(0)
private val frequencyVectorTime = AtomicLong(0)

private val DEFAULT_PARALLELISM: Int =
  (Runtime.getRuntime().availableProcessors()).coerceAtLeast(2)

/**
 * A class responsible for fulfilling results.
 *
 * @param privateEncryptionKey Handle to the private encryption key.
 * @param requisitionsStub Stub for requisitions gRPC coroutine.
 * @param dataProviderCertificateKey Data provider certificate key.
 * @param dataProviderSigningKeyHandle Handle to the data provider signing key.
 * @param requisitionFulfillmentStubMap Map of fulfillment stubs.
 * @param requisitionsBlobUri URI for requisitions blob storage.
 * @param requisitionsStorageConfig Configuration for requisitions storage.
 * @param noiserSelector Selector for noise addition.
 * @param pipelineConfiguration configuration for the event processing pipeline
 * @param eventDescriptor descriptor for events processing
 * @param modelLineInfoMap map of model line to [ModelLineInfo]
 * @param impressionMetadataService service for managing impression data sources
 * @param kAnonymityParams [KAnonymityParams] for this measurement
 *
 * TODO(2347) - Support additional differential privacy and k-anonymization.
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

  private val orchestrator: EventProcessingOrchestrator by lazy {
    EventProcessingOrchestrator(
      privateEncryptionKey
    )
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  suspend fun fulfillRequisitions(
    parallelism: Int = DEFAULT_PARALLELISM,
  ) {
    val groupedRequisitions = getRequisitions()
    val requisitions =
      groupedRequisitions.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
    logger.info("Processing ${requisitions.size} Requisitions")
    totalRequisitions.addAndGet(requisitions.size)

    val modelLine = groupedRequisitions.modelLine
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val eventDescriptor = modelInfo.eventDescriptor

    val populationSpec = modelInfo.populationSpec
    val vidIndexMap = modelInfo.vidIndexMap

    val eventSource = StorageEventSource(
      impressionMetadataService = impressionMetadataService,
      eventGroupDetailsList = groupedRequisitions.eventGroupMapList.map { it.details },
      modelLine = modelLine,
      kmsClient = kmsClient,
      impressionsStorageConfig = impressionsStorageConfig,
      descriptor = eventDescriptor,
      batchSize = pipelineConfiguration.batchSize,
    )

    val frequencyVectorStart = TimeSource.Monotonic.markNow()
    val frequencyVectorMap = orchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = pipelineConfiguration,
      eventDescriptor = eventDescriptor,
    )
    frequencyVectorTime.addAndGet(frequencyVectorStart.elapsedNow().inWholeNanoseconds)

    requisitions
      .asFlow()
      .map { req -> req to frequencyVectorMap.getValue(req.name) }
      .flatMapMerge(concurrency = parallelism) { (req, fv) ->
        flow {
          val start = TimeSource.Monotonic.markNow()
          try {
            fulfillSingleRequisition(
              requisition = req,
              frequencyVector = fv,
              populationSpec = populationSpec,
            )
            fulfillmentTime.addAndGet(start.elapsedNow().inWholeNanoseconds)
            emit(Unit)
          } catch (t: Throwable) {
            logger.severe("Failed fulfilling ${req.name}: ${t.message}")
            throw t
          }
        }
      }
      .collect()
  }

  private suspend fun fulfillSingleRequisition(
    requisition: Requisition,
    frequencyVector: StripedByteFrequencyVector,
    populationSpec: PopulationSpec,
  ) {
    val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
    val freqBytes = frequencyVector.getByteArray()
    val frequencyData: IntArray = freqBytes.map { it.toInt() and 0xFF }.toIntArray()

    val signedRequisitionSpec: SignedMessage = try {
      withContext(Dispatchers.IO) {
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      }
    } catch (e: GeneralSecurityException) {
      throw Exception("RequisitionSpec decryption failed", e)
    }
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

    val buildStart = TimeSource.Monotonic.markNow()
    val fulfiller = selectFulfiller(
      requisition = requisition,
      measurementSpec = measurementSpec,
      requisitionSpec = requisitionSpec,
      frequencyData = frequencyData,
      populationSpec = populationSpec,
    )
    buildTime.addAndGet(buildStart.elapsedNow().inWholeNanoseconds)

    logger.info("Fulfill requisition: ${requisition.name}")

    val sendStart = TimeSource.Monotonic.markNow()
    withContext(Dispatchers.IO) {
      fulfiller.fulfillRequisition()
    }
    sendTime.addAndGet(sendStart.elapsedNow().inWholeNanoseconds)
  }

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
    } else if (requisition.protocolConfig.protocolsList.any { it.hasHonestMajorityShareShuffle() }) {
      if (kAnonymityParams == null) {
        HMShuffleMeasurementFulfiller(
          requisition,
          requisitionSpec.nonce,
          createFrequencyVectorBuilderFromArray(
            measurementSpec,
            populationSpec,
            frequencyData
          ).build(),
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
   * Creates a FrequencyVectorBuilder from frequency data array.
   */
  private fun createFrequencyVectorBuilderFromArray(
    measurementSpec: MeasurementSpec,
    populationSpec: PopulationSpec,
    frequencyData: IntArray
  ): FrequencyVectorBuilder {
    val builder = FrequencyVectorBuilder(
      measurementSpec = measurementSpec,
      populationSpec = populationSpec,
      strict = false,
    )

    // Populate the builder with the frequency data
    for (index in frequencyData.indices) {
      repeat(frequencyData[index]) {
        builder.increment(index)
      }
    }

    return builder
  }

  /**
   * Retrieves a list of requisitions from the configured blob storage.
   *
   * @return A [GroupredRequisitions] retrieved from blob storage
   * @throws ImpressionReadException If the requisition blob cannot be found at the specified URI
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

  /** Builds a [DirectMeasurementFulfiller]. */
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

  fun logFulfillmentStats() {
    logger.info(
      """
      |Fulfillment Statistics:
      |  Total requisitions: ${totalRequisitions.get()}
      |  Frequency vector total ms: ${frequencyVectorTime.get() / 1_000_000}
      |  Build total ms: ${buildTime.get() / 1_000_000}
      |  Send total ms: ${sendTime.get() / 1_000_000}
      |  Fulfillment total ms: ${fulfillmentTime.get() / 1_000_000}
      """.trimMargin()
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
