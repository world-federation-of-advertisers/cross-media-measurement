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

package org.wfanet.measurement.securecomputation.controlplane.client.v1alpha.kms

import org.mockito.ArgumentCaptor
import com.google.protobuf.ByteString
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import com.google.cloud.kms.v1.CryptoKeyName
import com.google.cloud.kms.v1.DecryptRequest
import com.google.cloud.kms.v1.DecryptResponse
import com.google.cloud.kms.v1.KeyManagementServiceClient
import java.util.Base64
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.securecomputation.controlplane.client.v1alpha.kms.GcpKmsClient
import org.wfanet.measurement.securecomputation.controlplane.client.v1alpha.kms.GcpKmsDecryptionParams
@RunWith(JUnit4::class)
class GcpKmsClientTest {

  private lateinit var kmsClientMock: KeyManagementServiceClient
  private lateinit var gcpKmsClient: GcpKmsClient

  @Before
  fun setUp() {
    // Mock the KMS client
    kmsClientMock = mock(KeyManagementServiceClient::class.java)
    gcpKmsClient = GcpKmsClient(kmsClientMock)
  }

  @Test
  fun `test decryptKey with valid encrypted key`() {
    val projectId = "test-project"
    val locationId = "global"
    val keyRingId = "test-key-ring"
    val keyId = "test-key"
    val encryptedKeyBase64 = Base64.getEncoder().encodeToString("encrypted-dek".toByteArray())

    val params = GcpKmsDecryptionParams(
      projectId,
      locationId,
      keyRingId,
      keyId,
      encryptedKeyBase64
    )

    val decryptedData = "decrypted-symmetric-key".toByteArray()
    val decryptResponseMock = DecryptResponse.newBuilder()
      .setPlaintext(ByteString.copyFrom(decryptedData))
      .build()

    `when`(kmsClientMock.decrypt(any(DecryptRequest::class.java)))
      .thenReturn(decryptResponseMock)
    val decryptedKey = gcpKmsClient.decryptKey(params)
    assertEquals(decryptedData.size, decryptedKey.size)
    assertEquals(String(decryptedData), String(decryptedKey))
  }

  @Test
  fun `test decryptKey handles exception from KMS client`() {
    val projectId = "test-project"
    val locationId = "global"
    val keyRingId = "test-key-ring"
    val keyId = "test-key"
    val encryptedKeyBase64 = Base64.getEncoder().encodeToString("encrypted-dek".toByteArray())

    val params = GcpKmsDecryptionParams(
      projectId,
      locationId,
      keyRingId,
      keyId,
      encryptedKeyBase64
    )

    `when`(kmsClientMock.decrypt(any(DecryptRequest::class.java)))
      .thenThrow(RuntimeException("KMS decryption failed"))
    assertFailsWith<RuntimeException> {
      gcpKmsClient.decryptKey(params)
    }
  }

  @Test
  fun `test decryptKey passes correct request to KMS`() {
    val projectId = "test-project"
    val locationId = "global"
    val keyRingId = "test-key-ring"
    val keyId = "test-key"
    val encryptedKeyBase64 = Base64.getEncoder().encodeToString("encrypted-dek".toByteArray())

    val params = GcpKmsDecryptionParams(
      projectId,
      locationId,
      keyRingId,
      keyId,
      encryptedKeyBase64
    )

    val decryptedData = "decrypted-symmetric-key".toByteArray()
    val decryptResponseMock = DecryptResponse.newBuilder()
      .setPlaintext(ByteString.copyFrom(decryptedData))
      .build()

    `when`(kmsClientMock.decrypt(any(DecryptRequest::class.java)))
      .thenReturn(decryptResponseMock)
    val requestCaptor = ArgumentCaptor.forClass(DecryptRequest::class.java)
    gcpKmsClient.decryptKey(params)
    verify(kmsClientMock).decrypt(requestCaptor.capture())
    val capturedRequest = requestCaptor.value
    assertEquals(
      CryptoKeyName.of(projectId, locationId, keyRingId, keyId).toString(),
      capturedRequest.name
    )
    assertEquals(encryptedKeyBase64, Base64.getEncoder().encodeToString(capturedRequest.ciphertext.toByteArray()))
  }
}

