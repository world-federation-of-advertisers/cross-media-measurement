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
class IntersectValidateTask(val maxSize: Int, val minimumOverlap: Float) : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {

    // Flatten the Blob's underlying Flow and record the buffer size for output creation.
    val currentData: ByteString = input.getValue("current-data").toByteString()
    val currentSetData: Set<JoinKeyAndId> =
      JoinKeyAndIdCollection.parseFrom(currentData).joinKeysAndIdsList.toSet()
    val currentDataSize: Int = currentSetData.size

    require(currentDataSize < maxSize) {
      "Current data size of $currentDataSize is greater than $maxSize"
    }
    require(currentDataSize > 0) { "Current data size must be greater than zero" }

    val oldData: Set<JoinKeyAndId> =
      JoinKeyAndIdCollection.parseFrom(input.getValue("previous-data").toByteString())
        .joinKeysAndIdsList
        .toSet()
    val overlapItemsCount: Int = currentSetData.count { it in oldData }
    val currentOverlap: Float = overlapItemsCount.toFloat() / currentDataSize

    require(currentOverlap > minimumOverlap) {
      "Overlap of $currentOverlap is less than $minimumOverlap"
    }
    return mapOf("current-data" to flowOf(currentData)).toMap()
  }
}
