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

import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
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
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.encryptionKey
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.common.crypto.tink.StreamingEncryption
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint

/**
 * Builds a Sequence of FulfillRequisitionRequests
 *
 * @param requisition The requisition being fulfilled
 * @param requisitionNonce The nonce value from the encrypted_requisition_spec
 * @param frequencyVector The payload for the fulfillment
 * @param encryptionParams The parameters for encryption. If null, the payload will not be
 *   encrypted.
 * @throws IllegalArgumentException if the requisition is malformed or the frequency vector is empty
 * @throws GeneralSecurityException if a cryptographic error happens
 */
class FulfillRequisitionRequestBuilder(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  // TODO(world-federation-of-advertisers/cross-media-measurement#2705): make it ByteArray
  private val frequencyVector: FrequencyVector,
  private val encryptionParams: EncryptionParams?,
) {
  /**
   * Parameters for optional encryption
   *
   * @param kmsClient The key management system client
   * @param kmsKekUri The key encryption key uri
   * @param workloadIdentityProvider The resource name of the workload identity provider. For
   *   example:
   *   `//iam.googleapis.com/projects/<project-id>/locations/global/workloadIdentityPools/wip/providers/provider-1`
   * @param impersonatedServiceAccount The name of the service account to impersonate. For example:
   *   `tee-workload@halo-cmm-dev.iam.gserrviceaccount.com`
   */
  data class EncryptionParams(
    val kmsClient: KmsClient,
    val kmsKekUri: String,
    val workloadIdentityProvider: String,
    val impersonatedServiceAccount: String,
  )

  private val frequencyVectorBytes: ByteArray

  init {
    val trusTeeProtocolList =
      requisition.protocolConfig.protocolsList.filter { it.hasTrusTee() }.map { it.trusTee }

    require(trusTeeProtocolList.size == 1) {
      "Expected to find exactly one config for TrusTee. Found: ${trusTeeProtocolList.size}"
    }

    require(frequencyVector.dataCount > 0) { "FrequencyVector must have size > 0" }

    frequencyVectorBytes =
      ByteArray(frequencyVector.dataList.size) { index ->
        require(frequencyVector.dataList[index] in 0..255) {
          "FrequencyVector value must be between 0 and 255"
        }
        frequencyVector.dataList[index].toByte()
      }
  }

  /** Builds the Sequence of requests with payload. */
  fun build(): Sequence<FulfillRequisitionRequest> = sequence {
    if (encryptionParams == null) {
      val headerRequest = buildUnencryptedHeader()
      yield(headerRequest)
      val payload = frequencyVectorBytes.toByteString()
      for (begin in 0 until payload.size() step RPC_CHUNK_SIZE_BYTES) {
        yield(
          fulfillRequisitionRequest {
            bodyChunk = bodyChunk {
              val end = minOf(payload.size(), begin + RPC_CHUNK_SIZE_BYTES)
              data = payload.substring(begin, end)
            }
          }
        )
      }
    } else {
      val dekHandle = KeysetHandle.generateNew(KeyTemplates.get(KEY_TEMPLATE))
      val encryptedDek = encryptDek(dekHandle)
      val headerRequest = buildEncryptedHeader(encryptedDek)
      yield(headerRequest)

      val bodyChunkRequests: Sequence<FulfillRequisitionRequest> =
        StreamingEncryption.encryptChunked(
            dekHandle,
            frequencyVectorBytes.toByteString(),
            null,
            RPC_CHUNK_SIZE_BYTES,
          )
          .map { fulfillRequisitionRequest { bodyChunk = bodyChunk { data = it } } }

      yieldAll(bodyChunkRequests)
    }
  }

  private fun encryptDek(dekHandle: KeysetHandle): ByteString {
    val kekAead = encryptionParams!!.kmsClient.getAead(encryptionParams.kmsKekUri)
    val outputStream = ByteArrayOutputStream()
    dekHandle.write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
    return outputStream.toByteArray().toByteString()
  }

  private fun buildUnencryptedHeader(): FulfillRequisitionRequest {
    return fulfillRequisitionRequest {
      header = header {
        name = requisition.name
        requisitionFingerprint = computeRequisitionFingerprint(requisition)
        nonce = requisitionNonce
        protocolConfig = requisition.protocolConfig
        trusTee = trusTee {
          dataFormat = FulfillRequisitionRequest.Header.TrusTee.DataFormat.FREQUENCY_VECTOR
        }
      }
    }
  }

  private fun buildEncryptedHeader(encryptedDek: ByteString): FulfillRequisitionRequest {
    return fulfillRequisitionRequest {
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
              this.encryptedDek = encryptionKey {
                format = EncryptionKey.Format.TINK_ENCRYPTED_KEYSET
                data = encryptedDek
              }
              kmsKekUri = encryptionParams!!.kmsKekUri
              workloadIdentityProvider = encryptionParams.workloadIdentityProvider
              impersonatedServiceAccount = encryptionParams.impersonatedServiceAccount
            }
          // TODO(world-federation-of-advertisers/cross-media-measurement#2624): generate
          // populationSpec fingerprint
        }
      }
    }
  }

  companion object {
    private const val KEY_TEMPLATE = "AES256_GCM_HKDF_1MB"
    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    /** Builds a sequence of requests with an encrypted payload. */
    fun buildEncrypted(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
      encryptionParams: EncryptionParams,
    ): Sequence<FulfillRequisitionRequest> {
      return FulfillRequisitionRequestBuilder(
          requisition,
          requisitionNonce,
          frequencyVector,
          encryptionParams,
        )
        .build()
    }

    /** Builds a sequence of requests with an unencrypted payload. */
    fun buildUnencrypted(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
    ): Sequence<FulfillRequisitionRequest> =
      FulfillRequisitionRequestBuilder(requisition, requisitionNonce, frequencyVector, null).build()
  }
}
