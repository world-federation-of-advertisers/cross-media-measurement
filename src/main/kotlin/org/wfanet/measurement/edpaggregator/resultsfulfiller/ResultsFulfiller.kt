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

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import java.security.GeneralSecurityException
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
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
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.size
import org.wfanet.measurement.storage.SelectedStorageClient

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
 * @param populationSpecMap map of model line to population spec
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
  private val zoneId: ZoneId,
  private val noiserSelector: NoiserSelector,
  private val eventReader: LegacyEventReader,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val kAnonymityParams: KAnonymityParams?,
) {

  suspend fun fulfillRequisitions() {
    val groupedRequisitions = getRequisitions()
    val requisitions =
      groupedRequisitions.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
    logger.info("Processing ${requisitions.size} Requisitions")
    val eventGroupMap =
      groupedRequisitions.eventGroupMapList
        .map { Pair(it.eventGroup, it.details.eventGroupReferenceId) }
        .toMap()
    val modelLine = groupedRequisitions.modelLine
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val typeRegistry = TypeRegistry.newBuilder().add(modelInfo.eventDescriptor).build()
    
    // Log population spec details
    logger.info("=== POPULATION SPEC DETAILS ===")
    logger.info("Model line: $modelLine")
    logger.info("Population spec size: ${modelInfo.populationSpec.size}")
    logger.info("VidIndexMap size: ${modelInfo.vidIndexMap.size}")
    logger.info("Event descriptor: ${modelInfo.eventDescriptor.name}")
    
    for (requisition in requisitions) {
      logger.info("=== PROCESSING REQUISITION: ${requisition.name} ===")
      logger.info("Measurement: ${requisition.measurement}")
      logger.info("Protocol configs: ${requisition.protocolConfig.protocolsList.map { it.protocolCase }}")
      
      val signedRequisitionSpec: SignedMessage =
        try {
          decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
        } catch (e: GeneralSecurityException) {
          throw Exception("RequisitionSpec decryption failed", e)
        }
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
      
      // Log measurement spec details
      logger.info("=== MEASUREMENT SPEC DETAILS ===")
      logger.info("Measurement type: ${measurementSpec.measurementTypeCase}")
      logger.info("VID sampling interval start: ${measurementSpec.vidSamplingInterval.start}")
      logger.info("VID sampling interval width: ${measurementSpec.vidSamplingInterval.width}")
      
      // Calculate expected frequency vector size based on sampling
      val populationSize = modelInfo.populationSpec.size.toInt()
      val samplingStart = measurementSpec.vidSamplingInterval.start
      val samplingWidth = measurementSpec.vidSamplingInterval.width
      val globalStartIndex = (populationSize * samplingStart).toInt()
      val globalEndIndex = (populationSize * (samplingStart + samplingWidth)).toInt() - 1
      val primaryRangeCount = (globalStartIndex..minOf(globalEndIndex, populationSize - 1)).count()
      val wrappedRangeCount = if (globalEndIndex >= populationSize) {
        (0..(globalEndIndex - populationSize)).count()
      } else 0
      val expectedFreqVectorSize = primaryRangeCount + wrappedRangeCount
      
      logger.info("=== VID SAMPLING CALCULATIONS ===")
      logger.info("Population size: $populationSize")
      logger.info("Global start index: $globalStartIndex")
      logger.info("Global end index: $globalEndIndex")
      logger.info("Primary range count: $primaryRangeCount")
      logger.info("Wrapped range count: $wrappedRangeCount")
      logger.info("Expected frequency vector size: $expectedFreqVectorSize")
      
      val sampledVids: Flow<Long> =
        RequisitionSpecs.getSampledVids(
          requisitionSpec,
          eventGroupMap,
          typeRegistry,
          eventReader,
          zoneId,
        )
      val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList
      val frequencyVectorBuilder =
        FrequencyVectorBuilder(
          measurementSpec = measurementSpec,
          populationSpec = modelInfo.populationSpec,
          strict = false,
        )
      
      // Count VIDs as we process them
      var vidCount = 0
      val vidSample = mutableListOf<Long>()
      sampledVids.collect { vid ->
        vidCount++
        if (vidSample.size < 10) {
          vidSample.add(vid)
        }
        val globalIndex = modelInfo.vidIndexMap[vid]
        frequencyVectorBuilder.increment(globalIndex)
      }
      
      logger.info("=== VID PROCESSING RESULTS ===")
      logger.info("Total VIDs processed: $vidCount")
      logger.info("Sample VIDs (first 10): $vidSample")
      logger.info("Sample VID indices: ${vidSample.map { modelInfo.vidIndexMap[it] }}")
      
      val frequencyData: IntArray = frequencyVectorBuilder.frequencyDataArray
      
      // Log frequency data details
      logger.info("=== FREQUENCY DATA DETAILS ===")
      logger.info("Frequency data array size: ${frequencyData.size}")
      logger.info("Frequency vector builder size: ${frequencyVectorBuilder.size}")
      val nonZeroCount = frequencyData.count { it > 0 }
      val totalFrequency = frequencyData.sum()
      val maxFrequency = frequencyData.maxOrNull() ?: 0
      logger.info("Non-zero entries: $nonZeroCount")
      logger.info("Total frequency (sum): $totalFrequency")
      logger.info("Max frequency value: $maxFrequency")
      
      // Sample of non-zero entries
      val nonZeroSample = frequencyData.withIndex()
        .filter { it.value > 0 }
        .take(10)
        .map { "(idx=${it.index}, freq=${it.value})" }
      logger.info("Sample non-zero entries (first 10): $nonZeroSample")
      
      // Check if frequency data size matches expected
      if (frequencyData.size != expectedFreqVectorSize) {
        logger.warning("WARNING: Frequency data size (${frequencyData.size}) != expected size ($expectedFreqVectorSize)")
      } else {
        logger.info("Frequency data size matches expected size: ${frequencyData.size}")
      }
      val fulfiller =
        if (protocols.any { it.hasDirect() }) {
          logger.info("=== DIRECT PROTOCOL HANDLING ===")
          logger.info("Using frequency data array directly")
          logger.info("Frequency data will be used as-is for Direct protocol")
          // TODO: Calculate the maximum population for a given cel filter
          buildDirectMeasurementFulfiller(
            requisition,
            measurementSpec,
            requisitionSpec,
            maxPopulation = null,
            frequencyData,
            kAnonymityParams = kAnonymityParams,
          )
        } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
          logger.info("=== HMSS PROTOCOL HANDLING ===")
          logger.info("K-anonymity params: ${if (kAnonymityParams == null) "null" else "present"}")
          
          val frequencyVector = frequencyVectorBuilder.build()
          logger.info("Built frequency vector - data count: ${frequencyVector.dataCount}")
          logger.info("Frequency vector data size: ${frequencyVector.dataList.size}")
          logger.info("First 10 frequency values: ${frequencyVector.dataList.take(10)}")
          val nonZeroInVector = frequencyVector.dataList.count { it > 0 }
          logger.info("Non-zero entries in built frequency vector: $nonZeroInVector")
          
          if (kAnonymityParams == null) {
            logger.info("Creating HMShuffleMeasurementFulfiller without k-anonymity")
            HMShuffleMeasurementFulfiller(
              requisition,
              requisitionSpec.nonce,
              frequencyVector,
              dataProviderSigningKeyHandle,
              dataProviderCertificateKey,
              requisitionFulfillmentStubMap,
            )
          } else {
            logger.info("Creating HMShuffleMeasurementFulfiller with k-anonymity")
            HMShuffleMeasurementFulfiller.buildKAnonymized(
              requisition,
              requisitionSpec.nonce,
              measurementSpec,
              modelInfo.populationSpec,
              frequencyVectorBuilder,
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
      
      logger.info("=== FULFILLING REQUISITION ===")
      logger.info("Fulfiller type: ${fulfiller.javaClass.simpleName}")
      fulfiller.fulfillRequisition()
      logger.info("=== REQUISITION FULFILLED SUCCESSFULLY ===\n")
    }
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
