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
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
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
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt
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
          ENCRYPTED_PARAMS,
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
          ENCRYPTED_PARAMS,
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
          ENCRYPTED_PARAMS,
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
          ENCRYPTED_PARAMS,
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
          ENCRYPTED_PARAMS,
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
            ENCRYPTED_PARAMS.copy(kmsKekUri = invalidKekUri),
          )
          .toList()
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
            ENCRYPTED_PARAMS.copy(kmsClient = kmsClientMock),
          )
          .toList()
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
            ENCRYPTED_PARAMS,
          )
          .toList()
      }

    assertThat(exception.message).isEqualTo(errorMessage)
  }

  @Test
  fun `buildEncrypted returns requests with correctly encrypted payload`() {
    val inputFrequencyVector = frequencyVector { data += listOf(1, 8, 27) }

    val requests =
      FulfillRequisitionRequestBuilder.buildEncrypted(
          REQUISITION,
          NONCE,
          inputFrequencyVector,
          ENCRYPTED_PARAMS,
        )
        .toList()

    assertThat(requests).hasSize(2)
    val headerRequest = requests.first { it.hasHeader() }
    val bodyChunks = requests.filter { it.hasBodyChunk() }.map { it.bodyChunk.data }
    assertThat(bodyChunks).isNotEmpty()
    val encryptedPayload = bodyChunks.reduce { a, b -> a.concat(b) }

    assertThat(headerRequest.hasHeader()).isTrue()

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
    val dekStreamingAead = dekKeysetHandle.getPrimitive(StreamingAead::class.java)

    val decryptedFrequencyVectorData =
      dekStreamingAead.newDecryptingStream(encryptedPayload.newInput(), byteArrayOf()).use {
        it.readAllBytes()
      }

    val decryptedIntegers = bytesToIntegers(decryptedFrequencyVectorData)
    assertThat(decryptedIntegers).isEqualTo(inputFrequencyVector.dataList)
  }

  @Test
  fun `EncryptionParams without awsKmsParams creates GCP params`() {
    val params =
      FulfillRequisitionRequestBuilder.EncryptionParams(
        kmsClient = KMS_CLIENT,
        kmsKekUri = KEK_URI,
        workloadIdentityProvider = WORKLOAD_ID_PROVIDER,
        impersonatedServiceAccount = IMPERSONATED_SERVICE_ACCOUNT,
        awsKmsParams = null,
      )

    assertThat(params.awsKmsParams).isNull()
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

  @Test
  fun `buildUnencrypted sets TrusTeeV2 header for TrusTeeV2 Requisition`() {
    val requests =
      FulfillRequisitionRequestBuilder.buildUnencrypted(V2_REQUISITION, NONCE, FREQUENCY_VECTOR)
        .toList()

    val header = requests.first { it.hasHeader() }.header
    assertThat(header.hasTrusTeeV2()).isTrue()
    assertThat(header.hasTrusTee()).isFalse()
    assertThat(header.trusTeeV2.trusTee.dataFormat)
      .isEqualTo(FulfillRequisitionRequest.Header.TrusTee.DataFormat.FREQUENCY_VECTOR)
    assertThat(header.trusTeeV2.hasFulfillmentDetails()).isFalse()
  }

  @Test
  fun `buildUnencrypted produces same payload for TrusTee and TrusTeeV2`() {
    val inputFrequencyVector = frequencyVector { data += listOf(1, 8, 27) }

    val v1Requests =
      FulfillRequisitionRequestBuilder.buildUnencrypted(REQUISITION, NONCE, inputFrequencyVector)
        .toList()
    val v2Requests =
      FulfillRequisitionRequestBuilder.buildUnencrypted(V2_REQUISITION, NONCE, inputFrequencyVector)
        .toList()

    assertThat(v2Requests.filter { it.hasBodyChunk() })
      .containsExactlyElementsIn(v1Requests.filter { it.hasBodyChunk() })
      .inOrder()
  }

  @Test
  fun `buildUnencrypted sets plaintext FulfillmentDetails`() {
    val requests =
      FulfillRequisitionRequestBuilder.buildUnencrypted(
          V2_REQUISITION,
          NONCE,
          FREQUENCY_VECTOR,
          FULFILLMENT_DETAILS,
        )
        .toList()

    val header = requests.first { it.hasHeader() }.header
    assertThat(header.trusTeeV2.fulfillmentDetails).isEqualTo(FULFILLMENT_DETAILS)
    assertThat(header.trusTeeV2.hasEncryptedFulfillmentDetails()).isFalse()
  }

  @Test
  fun `buildEncrypted encrypts FulfillmentDetails with the payload DEK`() {
    val requests =
      FulfillRequisitionRequestBuilder.buildEncrypted(
          V2_REQUISITION,
          NONCE,
          FREQUENCY_VECTOR,
          ENCRYPTED_PARAMS,
          FULFILLMENT_DETAILS,
        )
        .toList()

    val header = requests.first { it.hasHeader() }.header
    assertThat(header.trusTeeV2.hasFulfillmentDetails()).isFalse()
    val encryptedDetails = header.trusTeeV2.encryptedFulfillmentDetails
    assertThat(encryptedDetails.typeUrl).endsWith("FulfillmentDetails")

    val kekAead = KMS_CLIENT.getAead(KEK_URI)
    val encryptedDek: ByteString = header.trusTeeV2.trusTee.envelopeEncryption.encryptedDek.data
    val dekKeysetHandle =
      KeysetHandle.read(BinaryKeysetReader.withInputStream(encryptedDek.newInput()), kekAead)
    val dekStreamingAead = dekKeysetHandle.getPrimitive(StreamingAead::class.java)
    val plaintext =
      dekStreamingAead
        .newDecryptingStream(encryptedDetails.ciphertext.newInput(), byteArrayOf())
        .use { it.readAllBytes() }

    assertThat(FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails.parseFrom(plaintext))
      .isEqualTo(FULFILLMENT_DETAILS)
  }

  @Test
  fun `build fails when FulfillmentDetails set for a TrusTee Requisition`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.buildUnencrypted(
          REQUISITION,
          NONCE,
          FREQUENCY_VECTOR,
          FULFILLMENT_DETAILS,
        )
      }

    assertThat(exception.message).contains("fulfillmentDetails")
  }

  @Test
  fun `build fails when Requisition has both TrusTee and TrusTeeV2 configs`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.buildUnencrypted(
          REQUISITION.copy {
            protocolConfig =
              protocolConfig.copy {
                protocols += ProtocolConfigKt.protocol { trusTeeV2 = ProtocolConfigKt.trusTeeV2 {} }
              }
          },
          NONCE,
          FREQUENCY_VECTOR,
        )
      }

    assertThat(exception.message).contains("Found: 2")
  }

  companion object {
    private val KMS_CLIENT = FakeKmsClient()
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
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
    private val V2_REQUISITION = requisition {
      name = "requisitions/test"
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol { trusTeeV2 = ProtocolConfigKt.trusTeeV2 {} }
      }
    }
    private val FULFILLMENT_DETAILS =
      FulfillRequisitionRequestKt.HeaderKt.TrusTeeV2Kt.fulfillmentDetails {
        impression = MeasurementKt.ResultKt.impression { value = 1234L }
      }
    private val ENCRYPTED_PARAMS =
      FulfillRequisitionRequestBuilder.EncryptionParams(
        kmsClient = KMS_CLIENT,
        kmsKekUri = KEK_URI,
        workloadIdentityProvider = WORKLOAD_ID_PROVIDER,
        impersonatedServiceAccount = IMPERSONATED_SERVICE_ACCOUNT,
        awsKmsParams = null,
      )

    private fun bytesToIntegers(bytes: ByteArray): List<Int> {
      return bytes.map { it.toInt() and 0xFF }
    }
  }
}
