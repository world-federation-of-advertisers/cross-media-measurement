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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
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

/**
 * Builds a Sequence of FulfillRequisitionRequests
 *
 * @param requisition The requisition being fulfilled
 * @param requisitionNonce The nonce value from the encrypted_requisition_spec
 * @param frequencyVector The payload for the fulfillment
 * @param kmsClient The key management system client
 * @param kmsKekUri The key encryption key uri
 * @param workloadIdentityProvider The resource name of the workload identity provider
 * @param impersonatedServiceAccount The name of the service account to impersonate
 * @throws IllegalArgumentException if the requisition is malformed or the frequency vector is empty
 * @throws RuntimeException if a cryptographic error happens
 */
class FulfillRequisitionRequestBuilder(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val frequencyVector: FrequencyVector,
  private val kmsClient: KmsClient,
  private val kmsKekUri: String,
  private val workloadIdentityProvider: String,
  private val impersonatedServiceAccount: String,
) {
  private val protocolConfig: TrusTee
  private val encryptedFrequencyVector: ByteString
  private val encryptedDek: ByteString

  init {
    val trusTeeProtocolList =
      requisition.protocolConfig.protocolsList.filter { it.hasTrusTee() }.map { it.trusTee }

    protocolConfig =
      trusTeeProtocolList.singleOrNull()
        ?: throw IllegalArgumentException(
          "Expected to find exactly one config for TrusTee. Found: ${trusTeeProtocolList.size}"
        )

    require(frequencyVector.dataCount > 0) { "FrequencyVector must have size > 0" }

    val dekHandle = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM"))
    val dekAead = dekHandle.getPrimitive(Aead::class.java)

    val frequencyVectorData = integersToBytes(frequencyVector.dataList)
    val encryptedData = dekAead.encrypt(frequencyVectorData, null)
    encryptedFrequencyVector = encryptedData.toByteString()

    val kekAead = kmsClient.getAead(kmsKekUri)
    val outputStream = ByteArrayOutputStream()
    dekHandle.write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
    encryptedDek = outputStream.toByteArray().toByteString()
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
                  data = this@FulfillRequisitionRequestBuilder.encryptedDek
                }
                kmsKekUri = this@FulfillRequisitionRequestBuilder.kmsKekUri
                workloadIdentityProvider =
                  this@FulfillRequisitionRequestBuilder.workloadIdentityProvider
                impersonatedServiceAccount =
                  this@FulfillRequisitionRequestBuilder.impersonatedServiceAccount
              }
            // TODO(world-federation-of-advertisers/cross-media-measurement#2624): generate
            // populationSpec fingerprint
          }
        }
      }
    )

    for (begin in 0 until encryptedFrequencyVector.size() step RPC_CHUNK_SIZE_BYTES) {
      yield(
        fulfillRequisitionRequest {
          bodyChunk = bodyChunk {
            data =
              encryptedFrequencyVector.substring(
                begin,
                minOf(encryptedFrequencyVector.size(), begin + RPC_CHUNK_SIZE_BYTES),
              )
          }
        }
      )
    }
  }

  companion object {
    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB

    init {
      AeadConfig.register()
    }

    private fun integersToBytes(integers: List<Int>): ByteArray {
      val bytes = ByteArray(integers.size)
      for ((index, value) in integers.withIndex()) {
        require(value in 0..255) { "FrequencyVector value must be between 0 and 255" }
        bytes[index] = value.toByte()
      }
      return bytes
    }

    /** A convenience function */
    fun build(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
      kmsClient: KmsClient,
      kmsKekUri: String,
      workloadIdentityProvider: String,
      impersonatedServiceAccount: String,
    ): Sequence<FulfillRequisitionRequest> =
      FulfillRequisitionRequestBuilder(
          requisition,
          requisitionNonce,
          frequencyVector,
          kmsClient,
          kmsKekUri,
          workloadIdentityProvider,
          impersonatedServiceAccount,
        )
        .build()
  }
}
