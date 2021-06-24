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
import org.wfanet.panelmatch.protocol.common.parseSerializedSharedInputs

/**
 * Validates that input data. In current iteration, it makes sure it is less than a maxSize and
 * compares it to the previous validated data to make sure it has minimum overlap.
 */
class IntersectValidateTask(val maxSize: Int, val minimumOverlap: Float) : ExchangeTask {

  override suspend fun execute(input: Map<String, ByteString>): Map<String, ByteString> {
    val currentData: Set<ByteString> =
      parseSerializedSharedInputs(requireNotNull(input["current-data"])).toSet()
    val currentDataSize: Int = currentData.size
    require(currentDataSize < maxSize) {
      "Current data size of $currentDataSize is greater than $maxSize"
    }
    val oldData: Set<ByteString> =
      parseSerializedSharedInputs(requireNotNull(input["previous-data"])).toSet()
    val overlapItemsCount: Int = currentData.count { it in oldData }
    require(currentDataSize > 0)
    val currentOverlap: Float = overlapItemsCount.toFloat() / currentDataSize
    require(currentOverlap > minimumOverlap) {
      "Overlap of $currentOverlap is less than $minimumOverlap"
    }
    return mapOf("current-data" to requireNotNull(input["current-data"])).toMap()
  }
}
