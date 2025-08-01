/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import java.security.GeneralSecurityException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint

@RunWith(JUnit4::class)
class FulfillRequisitionRequestBuilderTest {
  @Test
  fun `buildEncrypted fails when requisition has no TrusTee config`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          requisition { protocolConfig = protocolConfig {} },
          NONCE,
          FREQUENCY_VECTOR,
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }
    assertThat(exception.message).contains("Expected to find exactly one config for TrusTee")
    assertThat(exception.message).contains("Found: 0")
  }

  @Test
  fun `buildEncrypted fails when requisition has multiple TrusTee configs`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION.copy {
            protocolConfig =
              protocolConfig.copy {
                protocols += ProtocolConfigKt.protocol { trusTee = ProtocolConfigKt.trusTee {} }
              }
          },
          NONCE,
          FREQUENCY_VECTOR,
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }
    assertThat(exception.message).contains("Expected to find exactly one config for TrusTee")
    assertThat(exception.message).contains("Found: 2")
  }

  @Test
  fun `buildEncrypted fails when frequency vector is empty`() {
    val exception =
      assertFailsWith<IllegalArgumentException>("expected exception") {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          FrequencyVector.getDefaultInstance(),
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }
    assertThat(exception.message).contains("must have size")
  }

  @Test
  fun `buildEncrypted fails when frequency vector has negative value`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          frequencyVector { data += -1 },
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }
    assertThat(exception.message).contains("FrequencyVector value")
  }

  @Test
  fun `buildEncrypted fails when frequency vector has value over 255`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          frequencyVector { data += 256 },
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }
    assertThat(exception.message).contains("FrequencyVector value")
  }

  @Test
  fun `buildEncrypted fails for invalid kek uri`() {
    val invalidKekUri = "gcp-kms://unregistered/key/uri"

    val exception =
      assertFailsWith<GeneralSecurityException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          FREQUENCY_VECTOR,
          KMS_CLIENT,
          invalidKekUri,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }

    assertThat(exception.message).contains("URI")
  }

  @Test
  fun `buildEncrypted fails for kms client getAead failure`() {
    val kmsClientMock: KmsClient = mock()
    val errorMessage = "Mocked get AEAD failure"
    whenever(kmsClientMock.getAead(any())).thenThrow(GeneralSecurityException(errorMessage))

    val exception =
      assertFailsWith<GeneralSecurityException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          FREQUENCY_VECTOR,
          kmsClientMock,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }

    assertThat(exception.message).isEqualTo(errorMessage)
  }

  @Test
  fun `buildEncrypted fails for dek encryption failure`() {
    val aeadMock: Aead = mock()
    KMS_CLIENT.setAead(KEK_URI, aeadMock)
    val errorMessage = "Mocked KEK AEAD encryption failure"
    whenever(aeadMock.encrypt(any(), any())).thenThrow(GeneralSecurityException(errorMessage))

    val exception =
      assertFailsWith<GeneralSecurityException> {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          FREQUENCY_VECTOR,
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
      }

    assertThat(exception.message).isEqualTo(errorMessage)
  }

  @Test
  fun `buildUnencrypted fails when encryption params are provided`() {
    val encryptionParams =
      FulfillRequisitionRequestBuilder.EncryptionParams(
        KMS_CLIENT,
        KEK_URI,
        WORKLOAD_ID_PROVIDER,
        IMPERSONATED_SERVICE_ACCOUNT,
      )
    val builder =
      FulfillRequisitionRequestBuilder(REQUISITION, NONCE, FREQUENCY_VECTOR, encryptionParams)
    val exception = assertFailsWith<IllegalStateException> { builder.buildUnencrypted().toList() }
    assertThat(exception).hasMessageThat().contains("Cannot build unencrypted request")
  }

  @Test
  fun `buildEncrypted fails when encryption params are not provided`() {
    val builder = FulfillRequisitionRequestBuilder(REQUISITION, NONCE, FREQUENCY_VECTOR, null)
    val exception = assertFailsWith<IllegalArgumentException> { builder.buildEncrypted().toList() }
    assertThat(exception).hasMessageThat().contains("Encryption parameters are required")
  }

  @Test
  fun `buildEncrypted returns requests with correctly encrypted payload`() {
    val inputFrequencyVector = frequencyVector { data += listOf(1, 8, 27) }

    val requests =
      FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          inputFrequencyVector,
          KMS_CLIENT,
          KEK_URI,
          WORKLOAD_ID_PROVIDER,
          IMPERSONATED_SERVICE_ACCOUNT,
        )
        .toList()

    assertThat(requests).hasSize(2)
    val headerRequest = requests[0]
    val bodyRequest = requests[1]

    assertThat(headerRequest.hasHeader()).isTrue()
    assertThat(bodyRequest.hasBodyChunk()).isTrue()

    val header = headerRequest.header
    assertThat(header.name).isEqualTo(REQUISITION.name)
    assertThat(header.requisitionFingerprint).isEqualTo(computeRequisitionFingerprint(REQUISITION))
    assertThat(header.nonce).isEqualTo(NONCE)

    val trusteeHeader = header.trusTee
    assertThat(trusteeHeader.dataFormat)
      .isEqualTo(FulfillRequisitionRequest.Header.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR)
    val envelope = trusteeHeader.envelopeEncryption
    assertThat(envelope.kmsKekUri).isEqualTo(KEK_URI)
    assertThat(envelope.workloadIdentityProvider).isEqualTo(WORKLOAD_ID_PROVIDER)
    assertThat(envelope.impersonatedServiceAccount).isEqualTo(IMPERSONATED_SERVICE_ACCOUNT)
    assertThat(envelope.hasEncryptedDek()).isTrue()

    val kekAead = KMS_CLIENT.getAead(KEK_URI)
    val encryptedDek: ByteString = header.trusTee.envelopeEncryption.encryptedDek.data
    val dekKeysetHandle =
      KeysetHandle.read(BinaryKeysetReader.withInputStream(encryptedDek.newInput()), kekAead)
    val dekAead = dekKeysetHandle.getPrimitive(Aead::class.java)

    val decryptedFrequencyVectorData =
      dekAead.decrypt(bodyRequest.bodyChunk.data.toByteArray(), null)
    val decryptedIntegers = bytesToIntegers(decryptedFrequencyVectorData)
    assertThat(decryptedIntegers).isEqualTo(inputFrequencyVector.dataList)
  }

  @Test
  fun `buildUnencrypted returns requests with unencrypted payload`() {
    val inputFrequencyVector = frequencyVector { data += listOf(1, 8, 27) }
    val requests =
      FulfillRequisitionRequestBuilder.buildUnencrypted(REQUISITION, NONCE, inputFrequencyVector)
        .toList()

    assertThat(requests).hasSize(2)
    val headerRequest = requests[0]
    val bodyRequest = requests[1]

    assertThat(headerRequest.hasHeader()).isTrue()
    assertThat(bodyRequest.hasBodyChunk()).isTrue()

    val header = headerRequest.header
    assertThat(header.name).isEqualTo(REQUISITION.name)
    assertThat(header.requisitionFingerprint).isEqualTo(computeRequisitionFingerprint(REQUISITION))
    assertThat(header.nonce).isEqualTo(NONCE)

    val trusteeHeader = header.trusTee
    assertThat(trusteeHeader.dataFormat)
      .isEqualTo(FulfillRequisitionRequest.Header.TrusTee.DataFormat.FREQUENCY_VECTOR)
    assertThat(trusteeHeader.hasEnvelopeEncryption()).isFalse()

    val payload = bodyRequest.bodyChunk.data
    val payloadIntegers = bytesToIntegers(payload.toByteArray())
    assertThat(payloadIntegers).isEqualTo(inputFrequencyVector.dataList)
  }

  companion object {
    private val KMS_CLIENT = FakeKmsClient()
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"

    init {
      AeadConfig.register()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM"))
      KMS_CLIENT.setAead(KEK_URI, kmsKeyHandle.getPrimitive(Aead::class.java))
    }

    private const val NONCE = 12345L
    private const val WORKLOAD_ID_PROVIDER = "workload-id-provider"
    private const val IMPERSONATED_SERVICE_ACCOUNT = "impersonated-sa"
    private val FREQUENCY_VECTOR = frequencyVector { data += 1 }
    private val REQUISITION = requisition {
      name = "requisitions/test"
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol { trusTee = ProtocolConfigKt.trusTee {} }
      }
    }

    private fun bytesToIntegers(bytes: ByteArray): List<Int> {
      return bytes.map { it.toInt() and 0xFF }
    }
  }
}
