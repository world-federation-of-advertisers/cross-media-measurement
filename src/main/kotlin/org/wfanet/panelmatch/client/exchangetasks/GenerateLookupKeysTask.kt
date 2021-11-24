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

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.joinKeyOf
import org.wfanet.panelmatch.common.storage.toByteString

/**
 * Produces "lookup keys" from decrypted double blinded join keys.
 *
 * Refer to the README.md for more details on the different types of join keys. The output lookup
 * keys are salted, SHA256-hashed, EDP-encrypted identifiers.
 */
class GenerateLookupKeysTask : ExchangeTask {
  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    val serializedJoinKeys = input.getValue("decrypted-join-keys").toByteString()
    @Suppress("BlockingMethodInNonBlockingContext") // This is in-memory
    val joinkeys = JoinKeyAndIdCollection.parseFrom(serializedJoinKeys)
    val pepper = input.getValue("pepper").toByteString()
    val lookupKeys = joinKeyAndIdCollection {
      for (joinKeyAndId in joinkeys.joinKeysAndIdsList) {
        val joinKeyBytes = joinKeyAndId.joinKey.key
        val lookupKeyBytes = hashSha256(joinKeyBytes.concat(pepper)).substring(0, Long.SIZE_BYTES)
        joinKeysAndIds +=
          joinKeyAndId {
            this.joinKey = joinKeyOf(lookupKeyBytes)
            this.joinKeyIdentifier = joinKeyAndId.joinKeyIdentifier
          }
      }
    }
    return mapOf("lookup-keys" to flowOf(lookupKeys.toByteString()))
  }
}
