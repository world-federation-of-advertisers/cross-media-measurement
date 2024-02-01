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

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.hybrid.HybridConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.storage.toByteString

private const val PRIVATE_KEY_LABEL = "private-key-handle"
private const val ENCRYPTED_DATA_LABEL = "encrypted-data"
private const val DECRYPTED_DATA_LABEL = "decrypted-data"
private val NO_ASSOCIATED_DATA: ByteArray? = null

/** Hybrid decrypts input given a private key. */
class HybridDecryptTask : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.addToTaskLog("Executing hybrid decrypt task")

    val encryptedData = input.getValue(ENCRYPTED_DATA_LABEL).toByteString()
    // TODO: Read private key material from a KMS storage layer
    val keysetHandleData = input.getValue(PRIVATE_KEY_LABEL).toByteString()
    val privateKeysetHandle =
      CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(keysetHandleData.toByteArray()))
    val hybridDecrypt = privateKeysetHandle.getPrimitive(HybridDecrypt::class.java)
    val decryptedData =
      hybridDecrypt.decrypt(encryptedData.toByteArray(), NO_ASSOCIATED_DATA).toByteString()
    return mapOf(DECRYPTED_DATA_LABEL to flowOf(decryptedData))
  }

  companion object {
    init {
      HybridConfig.register()
    }

    private val logger by loggerFor()
  }
}
