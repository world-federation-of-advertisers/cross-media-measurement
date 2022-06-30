// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.hybrid.HybridConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle

class TinkPublicKeyHandle internal constructor(internal val keysetHandle: KeysetHandle) :
  PublicKeyHandle {

  init {
    require(!keysetHandle.primaryKey().hasSecret())
  }

  constructor(serializedKeyset: ByteString) : this(parseKeyset(serializedKeyset))

  fun toByteString(): ByteString {
    return ByteString.newOutput().use {
      keysetHandle.writeNoSecret(BinaryKeysetWriter.withOutputStream(it))
      it.toByteString()
    }
  }

  override fun hybridEncrypt(plaintext: ByteString, contextInfo: ByteString?): ByteString {
    val hybridEncrypt: HybridEncrypt = keysetHandle.getPrimitive(HybridEncrypt::class.java)
    val ciphertext = hybridEncrypt.encrypt(plaintext.toByteArray(), contextInfo?.toByteArray())
    return ciphertext.toByteString()
  }

  companion object {
    init {
      HybridConfig.register()
    }

    private fun parseKeyset(serialized: ByteString): KeysetHandle {
      return serialized.newInput().use {
        KeysetHandle.readNoSecret(BinaryKeysetReader.withInputStream(it))
      }
    }
  }
}

class TinkPrivateKeyHandle internal constructor(internal val keysetHandle: KeysetHandle) :
  PrivateKeyHandle {

  init {
    require(keysetHandle.primaryKey().hasSecret())
  }

  override val publicKey = TinkPublicKeyHandle(keysetHandle.publicKeysetHandle)

  override fun hybridDecrypt(ciphertext: ByteString, contextInfo: ByteString?): ByteString {
    val hybridDecrypt = keysetHandle.getPrimitive(HybridDecrypt::class.java)
    val plaintext = hybridDecrypt.decrypt(ciphertext.toByteArray(), contextInfo?.toByteArray())
    return plaintext.toByteString()
  }

  companion object {
    init {
      HybridConfig.register()
    }

    private val ECIES_KEY_TEMPLATE = KeyTemplates.get("ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM")

    /** Generates a new ECIES key pair. */
    fun generateEcies(): TinkPrivateKeyHandle {
      return TinkPrivateKeyHandle(KeysetHandle.generateNew(ECIES_KEY_TEMPLATE))
    }
  }
}
