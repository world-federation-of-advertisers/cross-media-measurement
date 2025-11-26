// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.shareshuffle

import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.SecretShare
import org.wfanet.frequencycount.SecretShareGeneratorAdapter
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.frequencycount.secretShareGeneratorRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.HonestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.randomSeed
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.NativeLibraryLoader
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.encryptRandomSeed
import org.wfanet.measurement.consent.client.dataprovider.signRandomSeed

/**
 * Builds a Sequence of FulfillRequisitionRequests
 *
 * This class assumes that the client has verified the identities of all Duchies.
 *
 * This depends on a native library for secret share generation. See [loadNativeLibrary].
 *
 * @param requisition The requisition being fulfilled
 * @param frequencyVector The payload for the fulfillment
 * @param dataProviderCertificateKey The certificate key of the data provider fulfilling the
 *   requisition
 * @param signingKeyHandle The handle of the EDP's key that is used to sign the random seed
 * @throws IllegalArgumentException if the requisition is malformed or the frequency vector is empty
 */
class FulfillRequisitionRequestBuilder(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val frequencyVector: FrequencyVector,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val signingKeyHandle: SigningKeyHandle,
  private val etag: String,
  private val generateSecretShares: (ByteArray) -> (ByteArray) =
    SecretShareGeneratorAdapter::generateSecretShares,
) {
  private val protocolConfig: HonestMajorityShareShuffle

  init {
    val hmssProtocolList =
      requisition.protocolConfig.protocolsList
        .filter { it.hasHonestMajorityShareShuffle() }
        .map { it.honestMajorityShareShuffle }

    protocolConfig =
      hmssProtocolList.singleOrNull()
        ?: throw IllegalArgumentException(
          "Expected to find exactly one config for HonestMajorityShareShuffle. Found: ${hmssProtocolList.size}"
        )

    require(protocolConfig.ringModulus > 1) {
      "HMSS ring modulus must be greater than one. Found: ${protocolConfig.ringModulus}"
    }
  }

  private val shareSeedEncryptionKey: EncryptionPublicKey

  init {
    if (requisition.duchiesList.size != 2) {
      throw IllegalArgumentException(
        "Two duchy entries are expected. Found: ${requisition.duchiesList.size}."
      )
    }

    val publicKeyList =
      requisition.duchiesList
        .filter { it.value.honestMajorityShareShuffle.hasPublicKey() }
        .map { it.value.honestMajorityShareShuffle.publicKey }

    val publicKeyBlob =
      publicKeyList.singleOrNull()
        ?: throw IllegalArgumentException(
          "Exactly one duchy entry is expected to have the encryption public key. Found: ${publicKeyList.size}"
        )
    shareSeedEncryptionKey = publicKeyBlob.unpack()
  }

  private val shareVector: FrequencyVector
  private val encryptedSignedShareSeed: EncryptedMessage

  init {
    require(frequencyVector.dataCount > 0) { "FrequencyVector must have size > 0" }

    val secretShareGeneratorRequest = secretShareGeneratorRequest {
      data += frequencyVector.dataList
      ringModulus = protocolConfig.ringModulus
    }

    val secretShare =
      SecretShare.parseFrom(generateSecretShares(secretShareGeneratorRequest.toByteArray()))

    shareVector = frequencyVector { data += secretShare.shareVectorList }

    val shareSeed = randomSeed { data = secretShare.shareSeed.key.concat(secretShare.shareSeed.iv) }
    val signedShareSeed =
      signRandomSeed(shareSeed, signingKeyHandle, signingKeyHandle.defaultAlgorithm)
    encryptedSignedShareSeed = encryptRandomSeed(signedShareSeed, shareSeedEncryptionKey)
  }

  /** Builds the Sequence of requests. */
  fun build(): Sequence<FulfillRequisitionRequest> = sequence {
    yield(
      fulfillRequisitionRequest {
        header = header {
          name = requisition.name
          requisitionFingerprint = computeRequisitionFingerprint(requisition)
          this.nonce = requisitionNonce
          protocolConfig = requisition.protocolConfig
          this.honestMajorityShareShuffle = honestMajorityShareShuffle {
            secretSeed = encryptedSignedShareSeed
            registerCount = shareVector.dataList.size.toLong()
            dataProviderCertificate = dataProviderCertificateKey.toName()
          }
          this.etag = etag
        }
      }
    )

    val shareVectorBytes = shareVector.toByteString()
    for (begin in 0 until shareVectorBytes.size() step RPC_CHUNK_SIZE_BYTES) {
      yield(
        fulfillRequisitionRequest {
          bodyChunk = bodyChunk {
            data =
              shareVectorBytes.substring(
                begin,
                minOf(shareVectorBytes.size(), begin + RPC_CHUNK_SIZE_BYTES),
              )
          }
        }
      )
    }
  }

  companion object {
    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB
    private const val NATIVE_LIB_NAME = "secret_share_generator_adapter"
    private const val NATIVE_LOADER_OBJECT_NAME =
      "org.wfanet.measurement.eventdataprovider.requisition.Native"

    init {
      // This is required to create secret shares out of the frequency vector
      loadNativeLibrary()
    }

    /**
     * Loads the secret share generator native library.
     *
     * If [NATIVE_LOADER_OBJECT_NAME] can be found at runtime, it will use that to load the library.
     * Otherwise, it will load [NATIVE_LIB_NAME] from the library path.
     */
    private fun loadNativeLibrary() {
      val nativeLoader: NativeLibraryLoader? =
        NativeLibraryLoader.getLoaderByName(NATIVE_LOADER_OBJECT_NAME)
      if (nativeLoader == null) {
        System.loadLibrary(NATIVE_LIB_NAME)
      } else {
        nativeLoader.loadLibrary()
      }
    }

    /** A convenience function for building the Sequence of Requests. */
    fun build(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
      dataProviderCertificateKey: DataProviderCertificateKey,
      signingKeyHandle: SigningKeyHandle,
      etag: String,
      generateSecretShares: (ByteArray) -> (ByteArray) =
        SecretShareGeneratorAdapter::generateSecretShares,
    ): Sequence<FulfillRequisitionRequest> =
      FulfillRequisitionRequestBuilder(
          requisition,
          requisitionNonce,
          frequencyVector,
          dataProviderCertificateKey,
          signingKeyHandle,
          etag,
          generateSecretShares,
        )
        .build()
  }
}
