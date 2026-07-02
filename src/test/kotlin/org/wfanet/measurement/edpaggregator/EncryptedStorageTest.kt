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

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.Base64
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek.ProtobufFormat
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class EncryptedStorageTest {

  @Before
  fun setUp() {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Test
  fun `buildEncryptedMesosStorageClient round-trips records via BINARY Tink keyset DEK`() {
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "binary-key"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(
        kmsClient = kmsClient,
        kekUri = kekUri,
        tinkKeyTemplateType = "AES128_GCM_HKDF_1MB",
      )
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      typeUrl = TYPE_URL_TINK_KEYSET
      protobufFormat = ProtobufFormat.BINARY
      ciphertext = serializedEncryptionKey
    }

    val storageClient = InMemoryStorageClient()
    val mesosClient =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        storageClient = storageClient,
        kmsClient = kmsClient,
        kekUri = kekUri,
        encryptedDek = encryptedDek,
      )

    val records = listOf("alpha".toByteStringUtf8(), "beta".toByteStringUtf8())
    runBlocking { mesosClient.writeBlob("impressions", flowOf(*records.toTypedArray())) }

    val readBack = runBlocking { mesosClient.getBlob("impressions")!!.read().toList() }
    assertThat(readBack).containsExactlyElementsIn(records).inOrder()
  }

  @Test
  fun `buildEncryptedMesosStorageClient round-trips records via JSON EncryptionKey DEK`() {
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "json-key"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    val kmsAead = kmsClient.getAead(kekUri)

    // Build an EncryptionKey JSON payload that parseJsonEncryptedKey understands. The wire
    // shape mirrors what real EDP writers emit on staging: an aesGcmHkdfStreamingKey wrapped
    // in JSON, KMS-encrypted, base64 ciphertext referenced by the EncryptedDek.
    val streamingKeyBytes = ByteArray(16) { it.toByte() }
    val keyValueB64 = Base64.getEncoder().encodeToString(streamingKeyBytes)
    val encryptionKeyJson =
      """
      {
        "aesGcmHkdfStreamingKey": {
          "version": 0,
          "params": {
            "ciphertextSegmentSize": 1048576,
            "derivedKeySize": 16,
            "hkdfHashType": "SHA256"
          },
          "keyValue": "$keyValueB64"
        }
      }
      """
        .trimIndent()
    val ciphertext =
      ByteString.copyFrom(
        kmsAead.encrypt(encryptionKeyJson.toByteArray(Charsets.UTF_8), byteArrayOf())
      )
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      typeUrl = TYPE_URL_ENCRYPTION_KEY
      protobufFormat = ProtobufFormat.JSON
      this.ciphertext = ciphertext
    }

    val storageClient = InMemoryStorageClient()
    val mesosClient =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        storageClient = storageClient,
        kmsClient = kmsClient,
        kekUri = kekUri,
        encryptedDek = encryptedDek,
      )

    val records = listOf("first".toByteStringUtf8(), "second".toByteStringUtf8())
    runBlocking { mesosClient.writeBlob("impressions", flowOf(*records.toTypedArray())) }

    val readBack = runBlocking { mesosClient.getBlob("impressions")!!.read().toList() }
    assertThat(readBack).containsExactlyElementsIn(records).inOrder()
  }

  @Test
  fun `buildEncryptedMesosStorageClient rejects unsupported type_url and format combination`() {
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "mismatch-key"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    // BINARY format paired with the JSON EncryptionKey type_url is not a supported branch
    // — confirm the dispatch rejects it rather than silently falling through.
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      typeUrl = TYPE_URL_ENCRYPTION_KEY
      protobufFormat = ProtobufFormat.BINARY
      ciphertext = ByteString.EMPTY
    }

    val failure =
      assertFailsWith<IllegalArgumentException> {
        EncryptedStorage.buildEncryptedMesosStorageClient(
          storageClient = InMemoryStorageClient(),
          kmsClient = kmsClient,
          kekUri = kekUri,
          encryptedDek = encryptedDek,
        )
      }
    assertThat(failure)
      .hasMessageThat()
      .contains("Unsupported type_url=$TYPE_URL_ENCRYPTION_KEY with format=BINARY")
  }

  companion object {
    private const val TYPE_URL_TINK_KEYSET = "type.googleapis.com/google.crypto.tink.Keyset"
    private const val TYPE_URL_ENCRYPTION_KEY =
      "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey"
  }
}
