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
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import java.security.GeneralSecurityException
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.DirectMeasurementResultFactory
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.DirectMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.HMShuffleMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.storage.SelectedStorageClient
import com.google.protobuf.Descriptors
import org.wfanet.measurement.api.v2alpha.PopulationSpec

/**
 * A class responsible for fulfilling results.
 *
 * @param privateEncryptionKey Handle to the private encryption key.
 * @param requisitionsStub Stub for requisitions gRPC coroutine.
 * @param requisitionFulfillmentStub Stub for requisitionFulfillment gRPC coroutine.
 * @param dataProviderCertificateKey Data provider certificate key.
 * @param dataProviderSigningKeyHandle Handle to the data provider signing key.
 * @param typeRegistry Type registry instance.
 * @param requisitionsBlobUri URI for requisitions blob storage.
 * @param requisitionsStorageConfig Configuration for requisitions storage.
 * @param zoneId Zone ID instance.
 * @param noiserSelector Selector for noise addition.
 * @param eventReader the [EventReader] to read in impressions data
  * @param pipelineConfiguration configuration for the event processing pipeline
 * @param eventDescriptor descriptor for events processing
 * @param modelLineInfoMap map of model line to [ModelLineInfo]
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
  private val eventDescriptor: Descriptors.Descriptor,
  private val kmsClient: KmsClient,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionDekStorageConfig: StorageConfig,
  private val impressionsBucketUri: String,
  private val impressionsDekBucketUri: String,
  private val typeRegistry: TypeRegistry,
  private val zoneId: ZoneId,
) {

  private val orchestrator: EventProcessingOrchestrator by lazy { EventProcessingOrchestrator(privateEncryptionKey) }
  private lateinit var populationSpec: PopulationSpec
  private lateinit var vidIndexMap: VidIndexMap
  suspend fun fulfillRequisitions() {
    val groupedRequisitions = getRequisitions()
    val requisitions =
      groupedRequisitions.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
    logger.info("Processing ${requisitions.size} Requisitions")
    val modelLine = groupedRequisitions.modelLine
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val eventDescriptor = modelInfo.eventDescriptor

    // Set population spec from first requisition (assuming all have same model line)
    populationSpec = modelInfo.populationSpec
    vidIndexMap = modelInfo.vidIndexMap

    // Create a simple event source adapter from the existing event reader
    val eventSource = createEventSourceFrom(groupedRequisitions.eventGroupMapList.map { it.details })

    // Use orchestrator to get frequency vectors for all requisitions
    val frequencyVectorMap = orchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = pipelineConfiguration,
      eventDescriptor = eventDescriptor,
      kAnonymityParams = kAnonymityParams ?: KAnonymityParams(minUsers = 1, minImpressions = 1)
    )

    for (requisition in requisitions) {
      logger.info("Fulfill requisition: ${requisition.name}")
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
      val frequencyVector = frequencyVectorMap[requisition.name]
        ?: throw IllegalStateException("No frequency vector found for requisition ${requisition.name}")
      val frequencyData: IntArray = frequencyVector.getArray()
      val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList
      // Decrypt requisition spec for nonce and other details
      val signedRequisitionSpec: SignedMessage =
        try {
          decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
        } catch (e: GeneralSecurityException) {
          throw Exception("RequisitionSpec decryption failed", e)
        }
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      val fulfiller =
        if (protocols.any { it.hasDirect() }) {
          buildDirectMeasurementFulfiller(
            requisition,
            measurementSpec,
            requisitionSpec,
            maxPopulation = null,
            frequencyData,
            kAnonymityParams = kAnonymityParams,
          )
        } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
          if (kAnonymityParams == null) {
            HMShuffleMeasurementFulfiller(
              requisition,
              requisitionSpec.nonce,
              createFrequencyVectorBuilderFromArray(measurementSpec, frequencyData).build(),
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
              createFrequencyVectorBuilderFromArray(measurementSpec, frequencyData),
              dataProviderSigningKeyHandle,
              dataProviderCertificateKey,
              requisitionFulfillmentStubMap,
              kAnonymityParams,
              maxPopulation = null,
            )
          }
        } else {
          throw Exception("Protocol not supported")
        }
      fulfiller.fulfillRequisition()
    }
  }

  /**
   * Creates an event source for a list of event groups.
   */
  private fun createEventSourceFrom(eventGroupDetailsList: List<GroupedRequisitions.EventGroupDetails>): EventSource {
    return StorageEventSource(
      kmsClient = kmsClient,
      impressionsStorageConfig = impressionsStorageConfig,
      impressionDekStorageConfig = impressionDekStorageConfig,
      impressionsDekBucketUri = impressionsDekBucketUri,
      eventGroupDetailsList = eventGroupDetailsList,
      descriptor = eventDescriptor,
      batchSize = pipelineConfiguration.batchSize,
      zoneId = zoneId,
    )
  }

  /**
   * Creates a FrequencyVectorBuilder from frequency data array.
   */
  private fun createFrequencyVectorBuilderFromArray(
    measurementSpec: MeasurementSpec,
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
