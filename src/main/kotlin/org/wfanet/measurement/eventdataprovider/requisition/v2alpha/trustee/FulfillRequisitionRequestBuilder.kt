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
 * @param encryptionParams The parameters for encryption. If null, the payload will not be
 *   encrypted.
 * @throws IllegalArgumentException if the requisition is malformed or the frequency vector is empty
 * @throws RuntimeException if a cryptographic error happens
 */
class FulfillRequisitionRequestBuilder(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
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

  private val protocolConfig: TrusTee
  private val frequencyVectorBytes: ByteArray
  private var encryptedFrequencyVector: ByteString? = null
  private var encryptedDek: ByteString? = null

  init {
    val trusTeeProtocolList =
      requisition.protocolConfig.protocolsList.filter { it.hasTrusTee() }.map { it.trusTee }

    protocolConfig =
      trusTeeProtocolList.singleOrNull()
        ?: throw IllegalArgumentException(
          "Expected to find exactly one config for TrusTee. Found: ${trusTeeProtocolList.size}"
        )

    require(frequencyVector.dataCount > 0) { "FrequencyVector must have size > 0" }

    frequencyVectorBytes =
      ByteArray(frequencyVector.dataList.size) { index ->
        require(frequencyVector.dataList[index] in 0..255) {
          "FrequencyVector value must be between 0 and 255"
        }
        frequencyVector.dataList[index].toByte()
      }

    encryptionParams?.let { params ->
      val dekHandle = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM"))
      val dekAead = dekHandle.getPrimitive(Aead::class.java)

      val encryptedData = dekAead.encrypt(frequencyVectorBytes, null)
      encryptedFrequencyVector = encryptedData.toByteString()

      val kekAead = params.kmsClient.getAead(params.kmsKekUri)
      val outputStream = ByteArrayOutputStream()
      dekHandle.write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
      encryptedDek = outputStream.toByteArray().toByteString()
    }
  }

  /** Builds the Sequence of requests with encrypted payload. */
  fun buildEncrypted(): Sequence<FulfillRequisitionRequest> = sequence {
    requireNotNull(encryptionParams) { "Encryption parameters are required to build." }
    requireNotNull(encryptedFrequencyVector) { "Encrypted frequency vector is null." }
    requireNotNull(encryptedDek) { "Encrypted DEK is null." }

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
                  data = this@FulfillRequisitionRequestBuilder.encryptedDek!!
                }
                kmsKekUri = encryptionParams.kmsKekUri
                workloadIdentityProvider = encryptionParams.workloadIdentityProvider
                impersonatedServiceAccount = encryptionParams.impersonatedServiceAccount
              }
            // TODO(world-federation-of-advertisers/cross-media-measurement#2624): generate
            // populationSpec fingerprint
          }
        }
      }
    )

    for (begin in 0 until encryptedFrequencyVector!!.size() step RPC_CHUNK_SIZE_BYTES) {
      yield(
        fulfillRequisitionRequest {
          bodyChunk = bodyChunk {
            data =
              encryptedFrequencyVector!!.substring(
                begin,
                minOf(encryptedFrequencyVector!!.size(), begin + RPC_CHUNK_SIZE_BYTES),
              )
          }
        }
      )
    }
  }

  /** Builds the Sequence of requests with an unencrypted payload. */
  fun buildUnencrypted(): Sequence<FulfillRequisitionRequest> = sequence {
    check(encryptionParams == null) {
      "Cannot build unencrypted request when encryption params are present."
    }

    yield(
      fulfillRequisitionRequest {
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
    )

    for (begin in 0 until frequencyVectorBytes.size step RPC_CHUNK_SIZE_BYTES) {
      yield(
        fulfillRequisitionRequest {
          bodyChunk = bodyChunk {
            val chunkSize = minOf(frequencyVectorBytes.size - begin, RPC_CHUNK_SIZE_BYTES)
            data = ByteString.copyFrom(frequencyVectorBytes, begin, chunkSize)
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

    /** Builds a sequence of requests with an encrypted payload. */
    fun buildEncrypted(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
      kmsClient: KmsClient,
      kmsKekUri: String,
      workloadIdentityProvider: String,
      impersonatedServiceAccount: String,
    ): Sequence<FulfillRequisitionRequest> {
      val encryptionParams =
        EncryptionParams(kmsClient, kmsKekUri, workloadIdentityProvider, impersonatedServiceAccount)
      return FulfillRequisitionRequestBuilder(
          requisition,
          requisitionNonce,
          frequencyVector,
          encryptionParams,
        )
        .buildEncrypted()
    }

    /** Builds a sequence of requests with an unencrypted payload. */
    fun buildUnencrypted(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
    ): Sequence<FulfillRequisitionRequest> =
      FulfillRequisitionRequestBuilder(requisition, requisitionNonce, frequencyVector, null)
        .buildUnencrypted()
  }
}
