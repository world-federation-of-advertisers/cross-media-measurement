// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.trustee.v2alpha

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.ByteArrayOutputStream
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.EncryptionKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.trusTee
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.TrusTee
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.encryptionKey
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint

class FulfillRequisitionRequestBuilder(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val frequencyVector: FrequencyVector,
  private val kmsClient: KmsClient,
  private val kmsKekUriVal: String,
  private val workloadIdentityProviderVal: String,
  private val impersonatedServiceAccountVal: String,
) {
  private val protocolConfig: TrusTee

  init {
    val trusTeeProtocolList =
      requisition.protocolConfig.protocolsList.filter { it.hasTrusTee() }.map { it.trusTee }

    protocolConfig =
      trusTeeProtocolList.singleOrNull()
        ?: throw IllegalArgumentException(
          "Expected to find exactly one config for TrusTee. Found: ${trusTeeProtocolList.size}"
        )
  }

  private val dekHandle: KeysetHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
  private val encryptedFrequencyVectorBytes: ByteString

  init {
    require(frequencyVector.dataCount > 0) { "FrequencyVector must have size > 0" }

    val dekAead = dekHandle.getPrimitive(Aead::class.java)
    val encryptedData = dekAead.encrypt(frequencyVector.toByteArray(), null)
    encryptedFrequencyVectorBytes = encryptedData.toByteString()
  }

  private val kekAead: Aead = kmsClient.getAead(kmsKekUriVal)
  private val encryptedDekBytes: ByteString

  init {
    val outputStream = ByteArrayOutputStream()
    dekHandle.write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
    encryptedDekBytes = outputStream.toByteArray().toByteString()
  }

  /** Builds the Sequence of requests. */
  fun build(): Sequence<FulfillRequisitionRequest> = sequence {
    yield(
      fulfillRequisitionRequest {
        header = header {
          name = requisition.name
          requisitionFingerprint = computeRequisitionFingerprint(requisition)
          nonce = requisitionNonce
          protocolConfig = requisition.protocolConfig
          trusTee = trusTee {
            dataFormat =
              FulfillRequisitionRequest.Header.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR
            envelopeEncryption =
              FulfillRequisitionRequestKt.HeaderKt.TrusTeeKt.envelopeEncryption {
                encryptedDek = encryptionKey {
                  format = EncryptionKey.Format.TINK_ENCRYPTED_KEYSET
                  data = encryptedDekBytes
                }
                kmsKekUri = kmsKekUriVal
                workloadIdentityProvider = workloadIdentityProviderVal
                impersonatedServiceAccount = impersonatedServiceAccountVal
              }
            // TODO: populationSpecFingerprint
          }
        }
      }
    )

    for (begin in 0 until encryptedFrequencyVectorBytes.size() step RPC_CHUNK_SIZE_BYTES) {
      yield(
        fulfillRequisitionRequest {
          bodyChunk = bodyChunk {
            data =
              encryptedFrequencyVectorBytes.substring(
                begin,
                minOf(encryptedFrequencyVectorBytes.size(), begin + RPC_CHUNK_SIZE_BYTES),
              )
          }
        }
      )
    }
  }

  companion object {
    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB

    /** A convenience function */
    fun build(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
      kmsClient: KmsClient,
      kmsKekUriVal: String,
      workloadIdentityProviderVal: String,
      impersonatedServiceAccountVal: String,
    ): Sequence<FulfillRequisitionRequest> =
      FulfillRequisitionRequestBuilder(
          requisition,
          requisitionNonce,
          frequencyVector,
          kmsClient,
          kmsKekUriVal,
          workloadIdentityProviderVal,
          impersonatedServiceAccountVal,
        )
        .build()
  }
}
