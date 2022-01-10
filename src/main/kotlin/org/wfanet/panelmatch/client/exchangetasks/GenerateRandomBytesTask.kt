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
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.common.crypto.generateSecureRandomByteString
import org.wfanet.panelmatch.common.loggerFor

private const val RANDOM_BYTES_LABEL = "random-bytes"

/** Generates random bytes */
class GenerateRandomBytesTask(private val numBytes: Int) : ExchangeTask {

  init {
    require(numBytes > 0) { "The random bytes length must be greater than zero" }
  }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.addToTaskLog("Executing generate random bytes")
    val randomBytes = generateSecureRandomByteString(numBytes)
    return mapOf(RANDOM_BYTES_LABEL to flowOf(randomBytes))
  }

  companion object {
    private val logger by loggerFor()
  }
}
