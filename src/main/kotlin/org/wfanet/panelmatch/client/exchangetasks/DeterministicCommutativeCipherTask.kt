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
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher
import org.wfanet.panelmatch.common.storage.toByteString

private const val INPUT_CRYPTO_KEY_LABEL = "encryption-key"

/**
 * Performs deterministic, commutative encryption on the items in a [JoinKeyAndIdCollection].
 *
 * The [operation] should output items in the same order that they are input.
 */
class DeterministicCommutativeCipherTask
internal constructor(
  private val operation: (ByteString, List<ByteString>) -> List<ByteString>,
  private val inputDataLabel: String,
  private val outputDataLabel: String,
) : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    // TODO See if it is worth updating this to not collect the inputs entirely at this step.
    //  It should be possible to process batches of them to balance memory usage and execution
    //  efficiency. If the inputs turn out to be small enough this shouldn't be an issue.
    //  Another reason to process them in one entire batch is to minimize the cost of
    //  serialization.
    val cryptoKey = input.getValue(INPUT_CRYPTO_KEY_LABEL).toByteString()
    val serializedInputs = input.getValue(inputDataLabel).toByteString()
    val inputList = JoinKeyAndIdCollection.parseFrom(serializedInputs).joinKeyAndIdsList

    val joinKeys = inputList.map { it.joinKey.key }
    require(joinKeys.toSet().size == joinKeys.size) { "JoinKeys are not distinct" }

    val joinKeyIds = inputList.map { it.joinKeyIdentifier }
    require(joinKeyIds.toSet().size == joinKeyIds.size) { "JoinKeyIdentifiers are not distinct" }

    val results = operation(cryptoKey, joinKeys)
    val serializedOutput =
      joinKeyAndIdCollection {
          joinKeyAndIds +=
            results.zip(joinKeyIds) { result: ByteString, joinKeyId: JoinKeyIdentifier ->
              joinKeyAndId {
                this.joinKey = joinKey { key = result }
                this.joinKeyIdentifier = joinKeyId
              }
            }
        }
        .toByteString()
    return mapOf(outputDataLabel to flowOf(serializedOutput))
  }

  companion object {
    /** Returns an [ExchangeTask] that removes encryption from data. */
    @JvmStatic
    fun forDecryption(cipher: DeterministicCommutativeCipher): ExchangeTask {
      return DeterministicCommutativeCipherTask(
        operation = cipher::decrypt,
        inputDataLabel = "encrypted-data",
        outputDataLabel = "decrypted-data",
      )
    }

    /** Returns an [ExchangeTask] that adds encryption to plaintext. */
    @JvmStatic
    fun forEncryption(cipher: DeterministicCommutativeCipher): ExchangeTask {
      return DeterministicCommutativeCipherTask(
        operation = cipher::encrypt,
        inputDataLabel = "unencrypted-data",
        outputDataLabel = "encrypted-data",
      )
    }

    /** Returns an [ExchangeTask] that adds another layer of encryption to data. */
    @JvmStatic
    fun forReEncryption(cipher: DeterministicCommutativeCipher): ExchangeTask {
      return DeterministicCommutativeCipherTask(
        operation = cipher::reEncrypt,
        inputDataLabel = "encrypted-data",
        outputDataLabel = "reencrypted-data",
      )
    }
  }
}
