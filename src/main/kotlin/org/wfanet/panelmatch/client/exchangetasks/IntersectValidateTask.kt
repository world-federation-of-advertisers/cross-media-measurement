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
import org.wfanet.panelmatch.common.storage.toByteString

/**
 * Validates input data. In current iteration, it makes sure it is not empty, has less than a
 * maxSize number of items, and has a substantially overlapping membership to the previous validated
 * data.
 */
class IntersectValidateTask(
  private val maxSize: Int,
  private val maximumNewItemsAllowed: Int,
  private val isFirstExchange: Boolean,
) : ExchangeTask {
  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {

    // Flatten the Blob's underlying Flow and record the buffer size for output creation.
    val currentDataBytes = input.getValue("current-data").toByteString()
    val currentData = parseJoinKeyAndIds(currentDataBytes)

    require(currentData.size <= maxSize) {
      "${currentData.size} ids were provided, which exceeds the limit of $maxSize"
    }

    if (!isFirstExchange && maximumNewItemsAllowed < maxSize) {
      val oldDataBytes = input.getValue("previous-data").toByteString()
      val oldData: Set<JoinKeyAndId> = parseJoinKeyAndIds(oldDataBytes)
      validateIntersection(currentData, oldData)
    }

    return mapOf("current-data" to flowOf(currentDataBytes))
  }

  private fun parseJoinKeyAndIds(bytes: ByteString): Set<JoinKeyAndId> {
    return JoinKeyAndIdCollection.parseFrom(bytes).joinKeyAndIdsList.toSet()
  }

  private fun validateIntersection(currentData: Set<JoinKeyAndId>, oldData: Set<JoinKeyAndId>) {
    val overlap: Int = currentData.count { it in oldData }
    val newItems = currentData.size - overlap

    require(newItems <= maximumNewItemsAllowed) {
      "There are $newItems new ids, but only $maximumNewItemsAllowed are allowed"
    }
  }
}
