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
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputFlow
import org.wfanet.panelmatch.protocol.common.parseSerializedSharedInputs

private const val DEFAULT_INPUT_KEY_LABEL: String = "encryption-key"

class CryptorExchangeTask
internal constructor(
  private val operation: (ByteString, List<ByteString>) -> List<ByteString>,
  private val inputDataLabel: String,
  private val outputDataLabel: String,
  private val inputKeyLabel: String = DEFAULT_INPUT_KEY_LABEL
) : ExchangeTask {

  override suspend fun execute(input: Map<String, VerifiedBlob>): Map<String, Flow<ByteString>> {
    logger.addToTaskLog("Executing operation: $operation")

    // TODO See if it is worth updating this to not collect the inputs entirely at this step.
    //  It should be possible to process batches of them to balance memory usage and execution
    //  efficiency. If the inputs turn out to be small enough this shouldn't be an issue.
    val key = input.getValue(inputKeyLabel).toByteString()
    val outBufferSize = input.getValue(inputDataLabel).defaultBufferSizeBytes
    val serializedInputs = input.getValue(inputDataLabel).toByteString()

    val inputs = parseSerializedSharedInputs(serializedInputs)
    val result = operation(key, inputs)
    return mapOf(outputDataLabel to makeSerializedSharedInputFlow(result, outBufferSize))
  }

  companion object {
    private val logger by loggerFor()

    /** Returns an [ExchangeTask] that removes encryption from data. */
    fun forDecryption(
      DeterministicCommutativeCipher: DeterministicCommutativeCipher
    ): ExchangeTask {
      return CryptorExchangeTask(
        operation = DeterministicCommutativeCipher::decrypt,
        inputDataLabel = "encrypted-data",
        outputDataLabel = "decrypted-data"
      )
    }

    /** Returns an [ExchangeTask] that adds encryption to plaintext. */
    fun forEncryption(
      DeterministicCommutativeCipher: DeterministicCommutativeCipher
    ): ExchangeTask {
      return CryptorExchangeTask(
        operation = DeterministicCommutativeCipher::encrypt,
        inputDataLabel = "unencrypted-data",
        outputDataLabel = "encrypted-data"
      )
    }

    /** Returns an [ExchangeTask] that adds another layer of encryption to data. */
    fun forReEncryption(
      DeterministicCommutativeCipher: DeterministicCommutativeCipher
    ): ExchangeTask {
      return CryptorExchangeTask(
        operation = DeterministicCommutativeCipher::reEncrypt,
        inputDataLabel = "encrypted-data",
        outputDataLabel = "reencrypted-data"
      )
    }
  }
}
