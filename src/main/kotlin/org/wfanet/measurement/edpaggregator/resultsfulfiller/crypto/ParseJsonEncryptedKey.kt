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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.proto.AesGcmHkdfStreamingKey
import com.google.crypto.tink.proto.AesGcmHkdfStreamingParams
import com.google.crypto.tink.proto.HashType
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import com.google.protobuf.util.JsonFormat
import java.security.SecureRandom
import kotlin.math.absoluteValue
import org.wfanet.measurement.edpaggregator.v1alpha.AesGcmHkdfStreamingKey as EdpAggregatorAesGcmHkdfStreamingKey
import org.wfanet.measurement.edpaggregator.v1alpha.AesGcmKey as EdpAggregatorAesGcmKey
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptionKey
import org.wfanet.measurement.edpaggregator.v1alpha.HashType as EdpAggregatorHashType

fun parseJsonEncryptedKey(
  encryptedDek: ByteArray,
  kekAead: Aead,
  associatedData: ByteArray?,
): KeysetHandle {
  val decrypted = kekAead.decrypt(encryptedDek, associatedData ?: byteArrayOf())

  val builder = EncryptionKey.newBuilder()
  JsonFormat.parser().ignoringUnknownFields().merge(decrypted.toString(Charsets.UTF_8), builder)

  val encryptionKey = builder.build()

  val keyData =
    when (encryptionKey.keyCase) {
      EncryptionKey.KeyCase.AES_GCM_HKDF_STREAMING_KEY ->
        buildAesStreamingKeyData(encryptionKey.aesGcmHkdfStreamingKey)
      EncryptionKey.KeyCase.AES_GCM_KEY -> buildAesGcmKeyData(encryptionKey.aesGcmKey)
      EncryptionKey.KeyCase.KEY_NOT_SET ->
        throw IllegalArgumentException("EncryptionKey has no key_type set")
    }
  val keyId = SecureRandom().nextInt().absoluteValue
  val ks =
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

  return CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(ks.toByteArray()))
}

private fun buildAesStreamingKeyData(
  aesStreamingKey: EdpAggregatorAesGcmHkdfStreamingKey
): KeyData {
  val params =
    AesGcmHkdfStreamingParams.newBuilder()
      .setDerivedKeySize(aesStreamingKey.params.derivedKeySize)
      .setHkdfHashType(aesStreamingKey.params.hkdfHashType.mapHashTypeCloneToTink())
      .setCiphertextSegmentSize(aesStreamingKey.params.ciphertextSegmentSize)
      .build()

  val tinkKey =
    AesGcmHkdfStreamingKey.newBuilder()
      .setParams(params)
      .setKeyValue(aesStreamingKey.keyValue)
      .setVersion(aesStreamingKey.version)
      .build()

  val keyData =
    KeyData.newBuilder()
      .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
      .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
      .setValue(tinkKey.toByteString())
      .build()
  return keyData
}

private fun buildAesGcmKeyData(aesGcmKey: EdpAggregatorAesGcmKey): KeyData {
  val tinkKey =
    com.google.crypto.tink.proto.AesGcmKey.newBuilder()
      .setKeyValue(aesGcmKey.keyValue)
      .setVersion(aesGcmKey.version)
      .build()
  return KeyData.newBuilder()
    .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmKey")
    .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
    .setValue(tinkKey.toByteString())
    .build()
}

private fun EdpAggregatorHashType.mapHashTypeCloneToTink(): HashType =
  when (this) {
    EdpAggregatorHashType.SHA1 -> HashType.SHA1
    EdpAggregatorHashType.SHA256 -> HashType.SHA256
    EdpAggregatorHashType.SHA512 -> HashType.SHA512
    else -> throw IllegalArgumentException("Unsupported hkdf_hash_type: $this")
  }
