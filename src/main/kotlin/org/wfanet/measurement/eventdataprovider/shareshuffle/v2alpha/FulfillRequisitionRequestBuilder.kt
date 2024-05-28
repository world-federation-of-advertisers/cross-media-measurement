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
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.common.asBufferedFlow

/**
 * Builds a Flow of FulfillRequisitionRequests to be used with the Kotlin gRPC client stub.
 */
class FulfillRequisitionRequestBuilder(val requisition: Requisition, val frequencyVector: FrequencyVector) {

  init {
    var requisitionAllowsHmss = false
    for (protocol in requisition.protocolConfig.protocolsList) {
      if (protocol.hasHonestMajorityShareShuffle()) {
        requisitionAllowsHmss = true
      }
    }
    require(requisitionAllowsHmss)
    {"Honest Majority Share Shuffle is not a valid Protocol for this requisition."}



    //verifyProtocolConfig()
    //verifyDuchyEntries()

  }
  val requisitionFingerprint: ByteString = computeRequisitionFingerprint(requisition)

  val shareVector: FrequencyVector
  val encryptedSignedSeed: EncryptedMessage
  init {
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
  }

  companion object {
      private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB
    }

  }

}
