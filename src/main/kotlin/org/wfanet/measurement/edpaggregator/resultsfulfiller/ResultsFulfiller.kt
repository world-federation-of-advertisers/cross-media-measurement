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
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import java.security.GeneralSecurityException
import java.security.SecureRandom
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.storage.SelectedStorageClient

class ResultsFulfiller(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val typeRegistry: TypeRegistry,
  private val requisitionsBlobUri: String,
  private val labeledImpressionMetadataPrefix: String,
  private val kmsClient: KmsClient,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionMetadataStorageConfig: StorageConfig,
  private val requisitionsStorageConfig: StorageConfig,
  private val random: SecureRandom = SecureRandom(),
) {
  suspend fun fulfillRequisitions() {
    val requisitions = getRequisitions()
    requisitions.collect { requisition ->
      val signedRequisitionSpec: SignedMessage =
        try {
          decryptRequisitionSpec(
            requisition.encryptedRequisitionSpec,
            privateEncryptionKey,
          )
        } catch (e: GeneralSecurityException) {
          throw Exception("RequisitionSpec decryption failed", e)
        }
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()

      val sampledVids = VidUtils.getSampledVids(
        requisitionSpec,
        measurementSpec.vidSamplingInterval,
        typeRegistry,
        kmsClient,
        impressionsStorageConfig,
        impressionMetadataStorageConfig,
        labeledImpressionMetadataPrefix,
      )

      val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList

      val measurementHelper = MeasurementHelper(
        requisitionsStub,
        dataProviderSigningKeyHandle,
        random,
        dataProviderCertificateKey
      )

      if (protocols.any { it.hasDirect() }) {
        val directProtocolConfig =
          requisition.protocolConfig.protocolsList.first { it.hasDirect() }.direct
        val directNoiseMechanismOptions =
          directProtocolConfig.noiseMechanismsList
            .mapNotNull { protocolConfigNoiseMechanism ->
              protocolConfigNoiseMechanism.toDirectNoiseMechanism()
            }
            .toSet()
        if (measurementSpec.hasReach() || measurementSpec.hasReachAndFrequency()) {
          measurementHelper.fulfillDirectReachAndFrequencyMeasurement(
            requisition,
            measurementSpec,
            sampledVids,
            directProtocolConfig,
            selectReachAndFrequencyNoiseMechanism(directNoiseMechanismOptions),
            requisitionSpec.nonce,
          )
        } else if (measurementSpec.hasDuration()) {
          TODO("Not yet implemented")
        } else if (measurementSpec.hasImpression()) {
          TODO("Not yet implemented")
        } else {
          throw MeasurementHelper.RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Measurement type not supported for direct fulfillment.",
          )
        }
      } else if (protocols.any { it.hasLiquidLegionsV2() }) {
        TODO("Not yet implemented")
      } else if (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
        TODO("Not yet implemented")
      } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
        TODO("Not yet implemented")
      } else {
        throw Exception("Protocol not supported")
      }
    }
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for reach and frequency measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   */
  private fun selectReachAndFrequencyNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preference = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN
    return if (options.contains(preference)) {
      preference
    } else {
      throw Exception("No valid noise mechanism option for reach or frequency measurements.")
    }
  }

  /**
   * Converts a [NoiseMechanism] to a nullable [DirectNoiseMechanism].
   *
   * @return [DirectNoiseMechanism] when there is a matched, otherwise null.
   */
  private fun NoiseMechanism.toDirectNoiseMechanism(): DirectNoiseMechanism? {
    return when (this) {
      NoiseMechanism.NONE -> DirectNoiseMechanism.NONE
      NoiseMechanism.CONTINUOUS_LAPLACE -> DirectNoiseMechanism.CONTINUOUS_LAPLACE
      NoiseMechanism.CONTINUOUS_GAUSSIAN -> DirectNoiseMechanism.CONTINUOUS_GAUSSIAN
      NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
      NoiseMechanism.GEOMETRIC,
      NoiseMechanism.DISCRETE_GAUSSIAN,
      NoiseMechanism.UNRECOGNIZED -> {
        null
      }
    }
  }

  /**
   * Retrieves a list of requisitions from the configured blob storage.
   *
   * This method performs the following operations:
   * 1. Parses the requisitions blob URI to create a storage client
   * 2. Fetches the requisition blob from storage
   * 3. Reads and concatenates all data from the blob
   * 4. Parses the UTF-8 encoded string data into a Requisition object using TextFormat
   *
   * @return A Flow containing the single requisition retrieved from blob storage
   * @throws NullPointerException If the requisition blob cannot be found at the specified URI
   */
  private suspend fun getRequisitions(): Flow<Requisition> {
    // Create storage client based on blob URI
    val storageClientUri = SelectedStorageClient.parseBlobUri(requisitionsBlobUri)
    val requisitionsStorageClient = VidUtils.createStorageClient(storageClientUri, requisitionsStorageConfig)

    // TODO(@jojijac0b): Refactor once grouped requisitions are supported
    val requisition = Requisition.parseFrom(requisitionsStorageClient.getBlob(storageClientUri.key)!!.read().flatten())

    return listOf(requisition).asFlow()
  }
}
