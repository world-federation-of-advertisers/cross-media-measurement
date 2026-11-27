// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.ConfidentialSpaceToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients.PermanentErrorException
import org.wfanet.measurement.internal.duchy.RequisitionDetails.RequisitionProtocol.TrusTee.DataFormat
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.trusTee

@RunWith(JUnit4::class)
class TrusTeeRequisitionDecryptorTest {
  private val kmsClient = FakeKmsClient().also { it.setAead(KEK_URI, KEK_AEAD) }
  private val decryptor =
    TrusTeeRequisitionDecryptor(
      gcloudKmsClientFactory =
        object : KmsClientFactory<GCloudWifCredentials> {
          override fun getKmsClient(config: GCloudWifCredentials): KmsClient = kmsClient
        },
      gcloudToAwsKmsClientFactory =
        object : KmsClientFactory<GCloudToAwsWifCredentials> {
          override fun getKmsClient(config: GCloudToAwsWifCredentials): KmsClient = kmsClient
        },
      confidentialSpaceToAwsKmsClientFactory =
        object : KmsClientFactory<ConfidentialSpaceToAwsWifCredentials> {
          override fun getKmsClient(config: ConfidentialSpaceToAwsWifCredentials): KmsClient =
            kmsClient
        },
      attestationTokenPath = Paths.get("/dev/null"),
    )

  @Test
  fun `readFrequencyVector returns an unencrypted vector as is`() {
    val protocol = trusTee { dataFormat = DataFormat.FREQUENCY_VECTOR }

    val vector = decryptor.readFrequencyVector(protocol, VECTOR.toByteString())

    assertThat(vector).isEqualTo(VECTOR)
  }

  @Test
  fun `readFrequencyVector decrypts an encrypted vector`() {
    val dek = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_4KB"))
    val protocol = trusTee {
      dataFormat = DataFormat.ENCRYPTED_FREQUENCY_VECTOR
      encryptedDekCiphertext = dek.encryptedWith(KEK_AEAD)
      kmsKekUri = KEK_URI
      workloadIdentityProvider = "workloadIdentityProvider"
      impersonatedServiceAccount = "serviceAccount"
    }

    val vector = decryptor.readFrequencyVector(protocol, encrypt(dek, VECTOR))

    assertThat(vector).isEqualTo(VECTOR)
  }

  @Test
  fun `readFrequencyVector throws PermanentErrorException for a vector under another key`() {
    val dek = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_4KB"))
    val otherDek = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_4KB"))
    val protocol = trusTee {
      dataFormat = DataFormat.ENCRYPTED_FREQUENCY_VECTOR
      encryptedDekCiphertext = dek.encryptedWith(KEK_AEAD)
      kmsKekUri = KEK_URI
    }

    assertFailsWith<PermanentErrorException> {
      decryptor.readFrequencyVector(protocol, encrypt(otherDek, VECTOR))
    }
  }

  @Test
  fun `readFrequencyVector throws for an unspecified data format`() {
    val protocol = trusTee { dataFormat = DataFormat.DATA_FORMAT_UNSPECIFIED }

    assertFailsWith<IllegalArgumentException> {
      decryptor.readFrequencyVector(protocol, VECTOR.toByteString())
    }
  }

  companion object {
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"
    private val VECTOR = byteArrayOf(0, 1, 2, 127, 200.toByte())

    private lateinit var KEK_AEAD: Aead

    @JvmStatic
    @BeforeClass
    fun initTink() {
      AeadConfig.register()
      StreamingAeadConfig.register()
      val kekHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      KEK_AEAD = kekHandle.getPrimitive(Aead::class.java)
    }

    private fun KeysetHandle.encryptedWith(kekAead: Aead): ByteString {
      val outputStream = ByteArrayOutputStream()
      write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
      return outputStream.toByteArray().toByteString()
    }

    private fun encrypt(dek: KeysetHandle, data: ByteArray): ByteString {
      val outputStream = ByteArrayOutputStream()
      val streamingAead = dek.getPrimitive(StreamingAead::class.java)
      streamingAead.newEncryptingStream(outputStream, byteArrayOf()).use { it.write(data) }
      return outputStream.toByteArray().toByteString()
    }
  }
}
