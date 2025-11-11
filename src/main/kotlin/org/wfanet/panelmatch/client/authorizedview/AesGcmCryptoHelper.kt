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

import com.google.protobuf.ByteString
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Helper class for AES-GCM encryption operations used in authorized view workflow.
 *
 * This class provides simple AES-256-GCM encryption and decryption.
 *
 * The encrypted data format is: IV (12 bytes) || ciphertext || auth tag (16 bytes)
 */
object AesGcmCryptoHelper {
  // AES-GCM parameters
  private const val AES_KEY_SIZE = 256 / 8 // 32 bytes for AES-256
  private const val GCM_IV_SIZE = 12 // 96 bits (recommended for AES-GCM)
  private const val GCM_TAG_SIZE = 128 // 128 bits auth tag
  private const val CIPHER_ALGORITHM = "AES/GCM/NoPadding"
  private const val KEY_ALGORITHM = "AES"

  private val secureRandom = SecureRandom()

  /**
   * Encrypts data using AES-GCM. Truncates the key to the first 32 bytes for AES-256
   *
   * @param plaintext The data to encrypt
   * @param key The raw key bytes (must be at least 32 bytes)
   * @return Encrypted data in format: IV || ciphertext || auth tag
   * @throws IllegalArgumentException if key has fewer than 32 bytes
   */
  fun encrypt(plaintext: ByteString, key: ByteString): ByteString {
    require(key.size() >= AES_KEY_SIZE) {
      "Key  must be at least $AES_KEY_SIZE bytes, got ${key.size()}"
    }
    val aesKey = key.substring(0, AES_KEY_SIZE).toByteArray()

    // Generate random IV
    val iv = ByteArray(GCM_IV_SIZE)
    secureRandom.nextBytes(iv)

    // Initialize cipher
    val cipher = Cipher.getInstance(CIPHER_ALGORITHM)
    val keySpec = SecretKeySpec(aesKey, KEY_ALGORITHM)
    val gcmSpec = GCMParameterSpec(GCM_TAG_SIZE, iv)

    cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec)

    // Encrypt (produces ciphertext + auth tag)
    val ciphertext = cipher.doFinal(plaintext.toByteArray())

    // Combine IV + ciphertext (which includes auth tag)
    return ByteString.copyFrom(iv + ciphertext)
  }

  /**
   * Decrypts data that was encrypted using the encrypt method.
   *
   * @param encryptedData The encrypted data (IV || ciphertext || auth tag)
   * @param key The raw key bytes (must be at least 32 bytes)
   * @return Decrypted plaintext
   * @throws IllegalArgumentException if key has fewer than 32 bytes
   * @throws javax.crypto.AEADBadTagException if authentication fails
   * 
   * TODO(world-federation-of-advertisers/common-jvm#365): Move this utility to common-jvm.
   */
  fun decrypt(encryptedData: ByteString, key: ByteString): ByteString {
    require(key.size() >= AES_KEY_SIZE) {
      "Key  must be at least $AES_KEY_SIZE bytes, got ${key.size()}"
    }
    require(encryptedData.size() >= GCM_IV_SIZE + GCM_TAG_SIZE / 8) {
      "Encrypted data too short: ${encryptedData.size()} bytes (minimum ${GCM_IV_SIZE + GCM_TAG_SIZE / 8})"
    }

    val aesKey = key.substring(0, AES_KEY_SIZE).toByteArray()
    val encryptedBytes = encryptedData.toByteArray()

    // Extract IV and ciphertext (with auth tag)
    val iv = encryptedBytes.sliceArray(0 until GCM_IV_SIZE)
    val ciphertextWithTag = encryptedBytes.sliceArray(GCM_IV_SIZE until encryptedBytes.size)

    // Initialize cipher for decryption
    val cipher = Cipher.getInstance(CIPHER_ALGORITHM)
    val keySpec = SecretKeySpec(aesKey, KEY_ALGORITHM)
    val gcmSpec = GCMParameterSpec(GCM_TAG_SIZE, iv)

    cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

    // Decrypt and verify auth tag
    val plaintext = cipher.doFinal(ciphertextWithTag)

    return ByteString.copyFrom(plaintext)
  }
}
