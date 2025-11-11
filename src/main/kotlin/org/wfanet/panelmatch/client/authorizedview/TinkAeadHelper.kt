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

package org.wfanet.panelmatch.client.authorizedview

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.proto.AesGcmKey
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.security.SecureRandom
import kotlin.math.absoluteValue

/**
 * Helper object for AES-GCM encryption operations using Tink AEAD primitives.
 *
 * This class creates Tink AEAD instances from raw key bytes for use in the authorized view
 * workflow. The encrypted data format matches standard AES-GCM: IV (12 bytes) || ciphertext ||
 * auth tag (16 bytes).
 *
 * Uses [OutputPrefixType.RAW] to ensure compatibility with standard AES-GCM format without Tink's
 * key ID prefix.
 * 
 * TODO(world-federation-of-advertisers/common-jvm#365): Move this utility to common-jvm.
 */
object TinkAeadHelper {
  private const val AES_KEY_SIZE = 32 // 256 bits for AES-256
  private const val AES_GCM_KEY_TYPE_URL = "type.googleapis.com/google.crypto.tink.AesGcmKey"
  private val NO_ASSOCIATED_DATA = byteArrayOf()
  private val secureRandom = SecureRandom()

  init {
    AeadConfig.register()
  }

  /**
   * Creates a Tink [Aead] primitive from raw key bytes.
   *
   * @param key The raw key bytes (must be at least 32 bytes; only first 32 bytes are used)
   * @return An [Aead] instance for encryption/decryption
   * @throws IllegalArgumentException if key has fewer than 32 bytes
   */
  fun createAead(key: ByteString): Aead {
    require(key.size() >= AES_KEY_SIZE) {
      "Key must be at least $AES_KEY_SIZE bytes, got ${key.size()}"
    }
    val truncatedKey = key.substring(0, AES_KEY_SIZE)

    val aesGcmKey =
      AesGcmKey.newBuilder().setVersion(0).setKeyValue(truncatedKey).build()

    val keyData =
      KeyData.newBuilder()
        .setTypeUrl(AES_GCM_KEY_TYPE_URL)
        .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
        .setValue(aesGcmKey.toByteString())
        .build()

    val keyId = secureRandom.nextInt().absoluteValue

    val keyset =
      Keyset.newBuilder()
        .setPrimaryKeyId(keyId)
        .addKey(
          Keyset.Key.newBuilder()
            .setKeyId(keyId)
            .setStatus(KeyStatusType.ENABLED)
            .setOutputPrefixType(OutputPrefixType.RAW)
            .setKeyData(keyData)
        )
        .build()

    val keysetHandle = CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(keyset.toByteArray()))
    return keysetHandle.getPrimitive(Aead::class.java)
  }

  /**
   * Encrypts data using AES-GCM via Tink AEAD.
   *
   * @param plaintext The data to encrypt
   * @param key The raw key bytes (must be at least 32 bytes)
   * @return Encrypted data in format: IV || ciphertext || auth tag
   * @throws IllegalArgumentException if key has fewer than 32 bytes
   */
  fun encrypt(plaintext: ByteString, key: ByteString): ByteString {
    val aead = createAead(key)
    return aead.encrypt(plaintext.toByteArray(), NO_ASSOCIATED_DATA).toByteString()
  }

  /**
   * Decrypts data that was encrypted using Tink AEAD.
   *
   * @param encryptedData The encrypted data (IV || ciphertext || auth tag)
   * @param key The raw key bytes (must be at least 32 bytes)
   * @return Decrypted plaintext
   * @throws IllegalArgumentException if key has fewer than 32 bytes
   * @throws java.security.GeneralSecurityException if decryption or authentication fails
   */
  fun decrypt(encryptedData: ByteString, key: ByteString): ByteString {
    val aead = createAead(key)
    return aead.decrypt(encryptedData.toByteArray(), NO_ASSOCIATED_DATA).toByteString()
  }
}
