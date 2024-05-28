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

/*

package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

// import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.SecretShare
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.frequencycount.secretShare
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.signRandomSeed

// DO NOT SUBMIT - get feedback on interfaces and implement them

data class ShareVectorAndShareSeed(val shareVector: FrequencyVector, val shareSeed: RandomSeed)

fun FrequencyVector.toSecretShare(protocolConfig: ProtocolConfig) : SecretShare {
  return secretShare {}
  /*
    val secretShareGeneratorRequest = secretShareGeneratorRequest {
      data += frequencyVector.toList()
      ringModulus = protocolConfig.sketchParams.ringModulus
    }

    val secretShare =
      SecretShare.parseFrom(
        SecretShareGeneratorAdapter.generateSecretShares(secretShareGeneratorRequest.toByteArray())
      )
   */
}

fun FrequencyVector.toShareVectorAndShareSeed(protocolConfig: ProtocolConfig) : ShareVectorAndShareSeed {
  val secretShare = this.toSecretShare(protocolConfig)
  return ShareVectorAndShareSeed(
    frequencyVector { data += secretShare.shareVectorList },
    randomSeed { data = secretShare.shareSeed.key.concat(secretShare.shareSeed.iv) })
}


val privateEncryptionKey: PrivateKeyHandle,

fun FrequencyVector.toShareVectorAndEncryptedSignedShareSeed(
  protocolConfig: ProtocolConfig,
  signingKeyHandle: SigningKeyHandle) : ShareVectorAndShareSeed {
  val shareVectorAndShareSeed = this.toShareVectorAndShareSeed(protocolConfig)
  val signedShareSeed =
    signRandomSeed(shareVectorAndShareSeed.shareSeed, signingKeyHandle, signingKeyHandle.defaultAlgorithm)
  val publicKey =
    EncryptionPublicKey.parseFrom(getEncryptionKeyForShareSeed(requisition).message.value)
  val shareSeedCiphertext = encryptRandomSeed(signedShareSeed, publicKey)
}

// TODO: create a single top level fulfill requisition function that
// accepts the requisition, the FV, the "edp data", and the stubs
val requisitionFulfillmentStubsByDuchyName = mapOf<String, >()

class RequisitionFulfiller(
  val Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub> duchyNameToStubMap) {

  init {
 }

  fun fulfillRequisition(requisition: Requisition, frequencyVector: FrequencyVector, ) {
    // See EdpSimulator.kt:872
    verifyProtocolConfig()
    verifyDuchyEntries() // See EDPSimulator
  }
}

class FulfillRequisitionRequestBuilder() {
  // EdpSimulator.kt:1355

}


fun SecretShare.toShareVectorAndShareSeed()



    fulfillRequisition(requisition, requisitionFingerprint, nonce, shareSeedCiphertext, shareVector)


private suspend fun fulfillRequisition(
    requisition: Requisition,
    requisitionFingerprint: ByteString,
    nonce: Long,
    encryptedSignedSeed: EncryptedMessage,
    shareVector: FrequencyVector,
  ) {
    logger.info("Fulfilling requisition ${requisition.name}...")
    val requests: Flow<FulfillRequisitionRequest> = flow {
      logger.info { "Emitting FulfillRequisitionRequests..." }
      emit(
        fulfillRequisitionRequest {
          header = header {
            name = requisition.name
            this.requisitionFingerprint = requisitionFingerprint
            this.nonce = nonce
            this.honestMajorityShareShuffle = honestMajorityShareShuffle {
              secretSeed = encryptedSignedSeed
              registerCount = shareVector.dataList.size.toLong()
              dataProviderCertificate = edpData.certificateKey.toName()
            }
          }
        }
      )
      emitAll(
        shareVector.toByteString().asBufferedFlow(RPC_CHUNK_SIZE_BYTES).map {
          fulfillRequisitionRequest { bodyChunk = bodyChunk { this.data = it } }
        }
      )
    }
    try {
      val requisitionFulfillmentStub =
        requisitionFulfillmentStubsByDuchyName[getDuchyWithoutPublicKey(requisition)]
          ?: throw Exception("Requisition fulfillment stub not found.")
      requisitionFulfillmentStub.fulfillRequisition(requests)
    } catch (e: StatusException) {
      throw Exception("Error fulfilling requisition ${requisition.name}", e)
    }
  }

data class EdpData(
  /** The EDP's public API resource name. */
  val name: String,
  /** The EDP's display name. */
  val displayName: String,
  /** The EDP's decryption key. */
  val privateEncryptionKey: PrivateKeyHandle,
  /** The EDP's consent signaling signing key. */
  val signingKeyHandle: SigningKeyHandle,
  /** The CertificateKey to use for result signing. */
  val certificateKey: DataProviderCertificateKey,
)

*/
