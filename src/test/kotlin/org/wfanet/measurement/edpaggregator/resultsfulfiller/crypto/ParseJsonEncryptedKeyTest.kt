/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller.crypto

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.aead.AeadKey
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadKey
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.Base64
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.AeadStorageClient
import org.wfanet.measurement.common.crypto.tink.StreamingAeadStorageClient
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class ParseJsonEncryptedKeyTest {

  @Before
  fun setUp() {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Test
  fun `parses AES key`() {
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    val kmsAead = kmsClient.getAead(kekUri)
    val keyBytes = ByteArray(32) { it.toByte() } // Example: 0,1,2,...,15
    val keyValue = Base64.getEncoder().encodeToString(keyBytes)
    val encryptionKeyJson =
      """
      {
          "aesGcmKey": {
            "version": 0,
            "keyValue": "$keyValue"
          }
       }
      """
        .trimIndent()
    val ciphertext = kmsAead.encrypt(encryptionKeyJson.toByteArray(Charsets.UTF_8), byteArrayOf())

    val keysetHandle = parseJsonEncryptedKey(ciphertext, kmsAead, null)
    assert(keysetHandle.primary.key is AeadKey)

    val wrappedStorageClient = InMemoryStorageClient()
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keysetHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
    assertThat(storageClient).isInstanceOf(AeadStorageClient::class.java)

    runBlocking { storageClient.writeBlob("some-blob-key", "some-content".toByteStringUtf8()) }
    val readData = runBlocking { storageClient.getBlob("some-blob-key")!!.read().flatten() }
    assertThat(readData).isEqualTo("some-content".toByteStringUtf8())
  }

  @Test
  fun `parses Streaming AES key`() {
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    val kmsAead = kmsClient.getAead(kekUri)
    val keyBytes = ByteArray(16) { it.toByte() }
    val keyValueB64 = Base64.getEncoder().encodeToString(keyBytes)

    // Example hardcoded JSON (client side)
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
        .trimIndent()
    val ciphertext = kmsAead.encrypt(encryptionKeyJson.toByteArray(Charsets.UTF_8), byteArrayOf())

    val keysetHandle = parseJsonEncryptedKey(ciphertext, kmsAead, null)
    assert(keysetHandle.primary.key is StreamingAeadKey)

    val wrappedStorageClient = InMemoryStorageClient()
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keysetHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)

    runBlocking { storageClient.writeBlob("some-blob-key", "some-content".toByteStringUtf8()) }
    val readData = runBlocking { storageClient.getBlob("some-blob-key")!!.read().flatten() }
    assertThat(readData).isEqualTo("some-content".toByteStringUtf8())
  }
}
