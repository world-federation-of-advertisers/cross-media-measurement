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
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Helper class for AES-GCM encryption operations used in authorized view workflow.
 *
 * This class centralizes all cryptographic operations including:
 * - AES-256 key derivation from SHA-256 hash
 * - AES-GCM encryption with automatic IV generation
 * - AES-GCM decryption with IV extraction
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
   * Derives an AES-256 key from the given input bytes using SHA-256.
   *
   * This provides deterministic key derivation: the same input always produces the same key.
   * SHA-256 produces exactly 32 bytes, which is the required size for AES-256.
   *
   * @param keyMaterial The input bytes to derive the key from
   * @return 32-byte AES key as ByteString
   */
  fun deriveAesKey(keyMaterial: ByteArray): ByteString {
    val digest = MessageDigest.getInstance("SHA-256")
    // SHA-256 produces exactly 32 bytes, which is exactly what AES-256 needs
    return ByteString.copyFrom(digest.digest(keyMaterial))
  }

  /**
   * Encrypts data using AES-GCM with the provided key.
   *
   * @param plaintext The data to encrypt
   * @param aesKey The 32-byte AES key
   * @return Encrypted data in format: IV || ciphertext || auth tag
   * @throws IllegalArgumentException if the key size is incorrect
   */
  fun encrypt(plaintext: ByteString, aesKey: ByteString): ByteString {
    require(aesKey.size() == AES_KEY_SIZE) {
      "AES key must be exactly $AES_KEY_SIZE bytes, got ${aesKey.size()}"
    }

    // Generate random IV
    val iv = ByteArray(GCM_IV_SIZE)
    secureRandom.nextBytes(iv)

    // Initialize cipher
    val cipher = Cipher.getInstance(CIPHER_ALGORITHM)
    val keySpec = SecretKeySpec(aesKey.toByteArray(), KEY_ALGORITHM)
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
   * @param aesKey The 32-byte AES key
   * @return Decrypted plaintext
   * @throws IllegalArgumentException if the key size is incorrect or encrypted data is too short
   * @throws javax.crypto.AEADBadTagException if authentication fails
   */
  fun decrypt(encryptedData: ByteString, aesKey: ByteString): ByteString {
    require(aesKey.size() == AES_KEY_SIZE) {
      "AES key must be exactly $AES_KEY_SIZE bytes, got ${aesKey.size()}"
    }

    val encryptedBytes = encryptedData.toByteArray()

    require(encryptedBytes.size >= GCM_IV_SIZE + GCM_TAG_SIZE / 8) {
      "Encrypted data too short: ${encryptedBytes.size} bytes (minimum ${GCM_IV_SIZE + GCM_TAG_SIZE / 8})"
    }

    // Extract IV and ciphertext (with auth tag)
    val iv = encryptedBytes.sliceArray(0 until GCM_IV_SIZE)
    val ciphertextWithTag = encryptedBytes.sliceArray(GCM_IV_SIZE until encryptedBytes.size)

    // Initialize cipher for decryption
    val cipher = Cipher.getInstance(CIPHER_ALGORITHM)
    val keySpec = SecretKeySpec(aesKey.toByteArray(), KEY_ALGORITHM)
    val gcmSpec = GCMParameterSpec(GCM_TAG_SIZE, iv)

    cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

    // Decrypt and verify auth tag
    val plaintext = cipher.doFinal(ciphertextWithTag)

    return ByteString.copyFrom(plaintext)
  }

  /**
   * Convenience method that derives a key and then decrypts data.
   *
   * @param encryptedData The encrypted data (IV || ciphertext || auth tag)
   * @param keyMaterial The bytes to derive the AES key from
   * @return Decrypted plaintext
   */
  fun decryptWithDerivedKey(encryptedData: ByteString, keyMaterial: ByteArray): ByteString {
    val aesKey = deriveAesKey(keyMaterial)
    return decrypt(encryptedData, aesKey)
  }

  /**
   * Convenience method that derives a key and then encrypts data.
   *
   * @param plaintext The data to encrypt
   * @param keyMaterial The bytes to derive the AES key from
   * @return Encrypted data in format: IV || ciphertext || auth tag
   */
  fun encryptWithDerivedKey(plaintext: ByteString, keyMaterial: ByteArray): ByteString {
    val aesKey = deriveAesKey(keyMaterial)
    return encrypt(plaintext, aesKey)
  }
}
