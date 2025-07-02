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
import java.security.SecureRandom
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.DirectMeasurementResultFactory
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.DirectMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * A class responsible for fulfilling results.
 *
 * @param privateEncryptionKey Handle to the private encryption key.
 * @param requisitionsStub Stub for requisitions gRPC coroutine.
 * @param dataProviderCertificateKey Data provider certificate key.
 * @param dataProviderSigningKeyHandle Handle to the data provider signing key.
 * @param typeRegistry Type registry instance.
 * @param requisitionsBlobUri URI for requisitions blob storage.
 * @param requisitionsStorageConfig Configuration for requisitions storage.
 * @param random Secure random number generator. Defaults to a new instance of [SecureRandom].
 * @param zoneId Zone ID instance.
 * @param noiserSelector Selector for noise addition.
 * @param eventReader the [EventReader] to read in impressions data
 *
 * TODO(2347) - Support additional differential privacy and k-anonymization.
 */
class ResultsFulfiller(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val typeRegistry: TypeRegistry,
  private val requisitionsBlobUri: String,
  private val requisitionsStorageConfig: StorageConfig,
  private val random: SecureRandom = SecureRandom(),
  private val zoneId: ZoneId,
  private val noiserSelector: NoiserSelector,
  private val eventReader: EventReader,
) {
  suspend fun fulfillRequisitions() {
    val groupedRequisitions = getRequisitions()
    val requisitions =
      groupedRequisitions.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
    val eventGroupMap =
      groupedRequisitions.eventGroupMapList
        .map { Pair(it.eventGroup, it.details.eventGroupReferenceId) }
        .toMap()
    for (requisition in requisitions) {
      logger.info("Processing requisition: ${requisition.name}")
      val signedRequisitionSpec: SignedMessage =
        try {
          decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
        } catch (e: GeneralSecurityException) {
          throw Exception("RequisitionSpec decryption failed", e)
        }
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()

      val sampledVids: Flow<Long> =
        RequisitionSpecs.getSampledVids(
          requisitionSpec,
          eventGroupMap,
          measurementSpec.vidSamplingInterval,
          typeRegistry,
          eventReader,
          zoneId,
        )

      val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList

      val fulfiller =
        if (protocols.any { it.hasDirect() }) {
          buildDirectMeasurementFulfiller(
            requisition,
            measurementSpec,
            requisitionSpec,
            random,
            sampledVids,
          )
        } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
          TODO("Not yet implemented")
        } else {
          throw Exception("Protocol not supported")
        }
      fulfiller.fulfillRequisition()
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
    random: SecureRandom,
    sampledVids: Flow<Long>,
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
        sampledVids,
        random,
      )
    return DirectMeasurementFulfiller(
      requisition.name,
      requisition.dataProviderCertificate,
      result,
      requisitionSpec.nonce,
      measurementEncryptionPublicKey,
      sampledVids,
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
