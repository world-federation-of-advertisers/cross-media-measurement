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

package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.SecretShare
import org.wfanet.frequencycount.SecretShareGeneratorAdapter
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.frequencycount.secretShareGeneratorRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.HonestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.randomSeed
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.common.asBufferedFlow

/**
 * A exception with a justification for why a requisition cannot be fulfilled.
 */
class RequisitionRefusalException(
  val justification: Requisition.Refusal.Justification,
  message: String,
) : Exception(message)

/**
 * Build a Flow of FulfillRequisitionRequests to be used with the Kotlin gRPC client stub.
 *
 * @param requisition
 * @param frequencyVector
 * @param dataProviderCertificateKey
 * @throws RequisitionRefusalException with an appropriate justification if the Request cannot be built
 */
class FulfillRequisitionRequestBuilder(
  val requisition: Requisition, val frequencyVector: FrequencyVector, val dataProviderCertificateKey: DataProviderCertificateKey
) {

  val protocolConfig : HonestMajorityShareShuffle
  init {
    var tempProtocolConfig: HonestMajorityShareShuffle? = null
    for (protocol in requisition.protocolConfig.protocolsList) {
      if (protocol.hasHonestMajorityShareShuffle()) {
        if (tempProtocolConfig == null) {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Found multiple configs for HonestMajorityShareShuffle")
        }
        tempProtocolConfig = protocol.honestMajorityShareShuffle
        break
      }
    }
    if (tempProtocolConfig == null) {
      throw RequisitionRefusalException(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "Expected the protocol config to allow HonestMajorityShareShuffle")
    }
    protocolConfig = tempProtocolConfig
    //verifyProtocolConfig()
    //verifyDuchyEntries()
  }
  val requisitionFingerprint: ByteString = computeRequisitionFingerprint(requisition)

  val shareVector: FrequencyVector
  val encryptedSignedSeed: EncryptedMessage
  init {
    val secretShareGeneratorRequest = secretShareGeneratorRequest {
      data += frequencyVector.dataList
      ringModulus = protocolConfig.sketchParams.ringModulus
    }

    val secretShare =
      SecretShare.parseFrom(
        SecretShareGeneratorAdapter.generateSecretShares(secretShareGeneratorRequest.toByteArray())
      )

    val shareSeed = randomSeed { data = secretShare.shareSeed.key.concat(secretShare.shareSeed.iv) }
     shareVector = frequencyVector{}
    encryptedSignedSeed = encryptedMessage {}
  }

  fun build() : Flow<FulfillRequisitionRequest> {
    return flow {
      emit(
        fulfillRequisitionRequest {
          header = header {
            name = requisition.name
            this.requisitionFingerprint = requisitionFingerprint
            this.nonce = nonce
            this.honestMajorityShareShuffle = honestMajorityShareShuffle {
              secretSeed = encryptedSignedSeed
              registerCount = shareVector.dataList.size.toLong()
              dataProviderCertificate = dataProviderCertificateKey.toName()
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
  }

  companion object {
      private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB
    }

  }
