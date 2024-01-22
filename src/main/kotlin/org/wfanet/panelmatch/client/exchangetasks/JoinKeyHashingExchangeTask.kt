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
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.QueryPreparer
import org.wfanet.panelmatch.client.privatemembership.lookupKeyAndIdCollection
import org.wfanet.panelmatch.common.storage.toByteString

private const val INPUT_PEPPER_KEY_LABEL = "pepper"

/** Hashes a list of [JoinKeyAndId] given a pepper. */
class JoinKeyHashingExchangeTask
internal constructor(
  private val operation: (ByteString, List<JoinKeyAndId>) -> List<LookupKeyAndId>,
  private val inputDataLabel: String,
  private val outputDataLabel: String,
) : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    val cryptoKey = input.getValue(INPUT_PEPPER_KEY_LABEL).toByteString()
    val serializedInputs = input.getValue(inputDataLabel).toByteString()
    val results =
      operation(cryptoKey, JoinKeyAndIdCollection.parseFrom(serializedInputs).joinKeyAndIdsList)

    val serializedOutput = lookupKeyAndIdCollection { lookupKeyAndIds += results }.toByteString()
    return mapOf(outputDataLabel to flowOf(serializedOutput))
  }

  companion object {
    /** Returns an [ExchangeTask] that adds another layer of hashing to data. */
    @JvmStatic
    fun forHashing(QueryPreparer: QueryPreparer): ExchangeTask {
      return JoinKeyHashingExchangeTask(
        operation = QueryPreparer::prepareLookupKeys,
        inputDataLabel = "join-keys",
        outputDataLabel = "lookup-keys",
      )
    }
  }
}
