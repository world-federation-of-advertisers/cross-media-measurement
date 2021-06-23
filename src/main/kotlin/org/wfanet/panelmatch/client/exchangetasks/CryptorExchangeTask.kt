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
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.protocol.common.Cryptor
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs
import org.wfanet.panelmatch.protocol.common.parseSerializedSharedInputs

private const val DEFAULT_INPUT_KEY_LABEL: String = "encryption-key"

class CryptorExchangeTask
internal constructor(
  private val operation: (ByteString, List<ByteString>) -> List<ByteString>,
  private val inputDataLabel: String,
  private val outputDataLabel: String,
  private val inputKeyLabel: String = DEFAULT_INPUT_KEY_LABEL
) : ExchangeTask {

  override suspend fun execute(input: Map<String, ByteString>): Map<String, ByteString> {
    logger.addToTaskLog("Executing operation: $operation")

    val key = requireNotNull(input[inputKeyLabel]) { "Missing input label '$inputKeyLabel'" }
    val serializedInputs =
      requireNotNull(input[inputDataLabel]) { "Missing input label '$inputDataLabel'" }

    val inputs = parseSerializedSharedInputs(serializedInputs)
    val result = operation(key, inputs)
    return mapOf(outputDataLabel to makeSerializedSharedInputs(result))
  }

  companion object {
    val logger by loggerFor()

    /** Returns an [ExchangeTask] that removes encryption from data. */
    fun forDecryption(Cryptor: Cryptor): ExchangeTask {
      return CryptorExchangeTask(
        operation = Cryptor::decrypt,
        inputDataLabel = "encrypted-data",
        outputDataLabel = "decrypted-data"
      )
    }

    /** Returns an [ExchangeTask] that adds encryption to plaintext. */
    fun forEncryption(Cryptor: Cryptor): ExchangeTask {
      return CryptorExchangeTask(
        operation = Cryptor::encrypt,
        inputDataLabel = "unencrypted-data",
        outputDataLabel = "encrypted-data"
      )
    }

    /** Returns an [ExchangeTask] that adds another layer of encryption to data. */
    fun forReEncryption(Cryptor: Cryptor): ExchangeTask {
      return CryptorExchangeTask(
        operation = Cryptor::reEncrypt,
        inputDataLabel = "encrypted-data",
        outputDataLabel = "reencrypted-data"
      )
    }
  }
}
