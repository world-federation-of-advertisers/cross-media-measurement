// Copyright 2025 The Cross-Media Measurement Authors
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
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.toDelimitedByteString

/**
 * Streams JoinKeyAndId protos for BigQuery streaming ingestion.
 *
 * This task:
 * 1. Reads a JoinKeyAndIdCollection proto (containing all encrypted keys)
 * 2. Streams individual JoinKeyAndId messages as length-delimited protos
 * 3. Outputs a stream consumable by WriteKeysToBigQueryTask
 *
 * This replaces the AVRO conversion in the streaming workflow, eliminating the need for AVRO
 * conversion and intermediate file storage.
 *
 * Input: JoinKeyAndIdCollection proto blob Output: Stream of length-delimited JoinKeyAndId protos
 */
class StreamJoinKeysTask : ExchangeTask {

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val INPUT_LABEL = "edp-encrypted-keys"
    private const val OUTPUT_LABEL = "key-stream"
  }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    withContext(Dispatchers.IO) {
      logger.info("Starting StreamJoinKeysTask")

      // Get input JoinKeyAndIdCollection
      val joinKeysBlob =
        requireNotNull(input[INPUT_LABEL]) { "Missing required input: $INPUT_LABEL" }

      // Create streaming output
      val keyStream = streamJoinKeys(joinKeysBlob)

      mapOf(OUTPUT_LABEL to keyStream)
    }

  /**
   * Streams individual JoinKeyAndId messages from a JoinKeyAndIdCollection.
   *
   * This enables WriteKeysToBigQueryTask to consume keys one at a time for streaming ingestion,
   * rather than loading the entire collection into memory.
   */
  private suspend fun streamJoinKeys(joinKeysBlob: StorageClient.Blob): Flow<ByteString> = flow {
    withContext(Dispatchers.IO) {
      // Parse the JoinKeyAndIdCollection
      val collectionBytes = joinKeysBlob.toByteString()
      val collection = JoinKeyAndIdCollection.parseFrom(collectionBytes.toByteArray())

      // Stream each JoinKeyAndId as a length-delimited proto
      collection.joinKeyAndIdsList.forEach { joinKeyAndId ->
        // Emit as length-delimited proto for consumption by WriteKeysToBigQueryTask
        emit(joinKeyAndId.toDelimitedByteString())
      }
    }
    logger.info("Completed StreamJoinKeysTask")
  }

  override fun skipReadInput(): Boolean = false
}
