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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.hybrid.HybridConfig
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.StorageClient

private const val PRIVATE_KEY_LABEL = "private-key-handle"
private const val PUBLIC_KEY_LABEL = "public-key-handle"

/** Generates a hybrid encryption key pair (public/private key) */
class GenerateHybridEncryptionKeyPairTask : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    val privateKeysetHandle = KeysetHandle.generateNew(ECIES_KEY_TEMPLATE)
    // TODO: Require the storage layer to be a KMS
    val cleartextPrivateKeysetHandle =
      ByteString.newOutput().use {
        CleartextKeysetHandle.write(privateKeysetHandle, BinaryKeysetWriter.withOutputStream(it))
        it.toByteString()
      }
    val publicKeyHandle = privateKeysetHandle.publicKeysetHandle
    return mapOf(
      PUBLIC_KEY_LABEL to flowOf(publicKeyHandle.toByteString()),
      PRIVATE_KEY_LABEL to flowOf(cleartextPrivateKeysetHandle),
    )
  }

  companion object {
    init {
      HybridConfig.register()
    }

    private val ECIES_KEY_TEMPLATE = KeyTemplates.get("ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM")

    private fun KeysetHandle.toByteString(): ByteString {
      return ByteString.newOutput().use {
        this.writeNoSecret(BinaryKeysetWriter.withOutputStream(it))
        it.toByteString()
      }
    }
  }
}
