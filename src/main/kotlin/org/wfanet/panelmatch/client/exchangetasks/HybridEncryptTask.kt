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

import com.google.crypto.tink.HybridEncrypt
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.hybrid.HybridConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.storage.toByteString

private const val PUBLIC_KEY_LABEL = "public-key-handle"
private const val PLAINTEXT_DATA_LABEL = "plaintext-data"
private const val ENCRYPTED_DATA_LABEL = "encrypted-data"
private val NO_ASSOCIATED_DATA: ByteArray? = null

/** Hybrid encrypts plaintext data given a public key. */
class HybridEncryptTask : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.addToTaskLog("Executing hybrid encrypt task")

    // TODO: Use PrivateKeystore instead of loading the KeysetHandle directly from blob storage
    // See https://github.com/world-federation-of-advertisers/panel-exchange-client/issues/322
    val inputData = input.getValue(PLAINTEXT_DATA_LABEL).toByteString()
    val publicKeyData = input.getValue(PUBLIC_KEY_LABEL).toByteString()
    val publicKeysetHandle =
      TinkProtoKeysetFormat.parseKeysetWithoutSecret(publicKeyData.toByteArray())
    val hybridEncrypt = publicKeysetHandle.getPrimitive(HybridEncrypt::class.java)
    val encryptedData =
      hybridEncrypt.encrypt(inputData.toByteArray(), NO_ASSOCIATED_DATA).toByteString()
    return mapOf(ENCRYPTED_DATA_LABEL to flowOf(encryptedData))
  }

  companion object {
    init {
      HybridConfig.register()
    }

    private val logger by loggerFor()
  }
}
