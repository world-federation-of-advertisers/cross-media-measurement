// Copyright 2023 The Cross-Media Measurement Authors
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
import com.google.protobuf.kotlin.toByteStringUtf8
import java.security.SecureRandom
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.storage.toByteString

/** AssignIds shuffles a set of join keys and assigns ids to each one. */
class AssignJoinKeyIdsTask() : ExchangeTask {
  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {

    val joinKeyBytes = input.getValue("join-keys").toByteString()
    val joinKeys = JoinKeyCollection.parseFrom(joinKeyBytes).joinKeysList
    require(joinKeys.size > 0) { "Number of join keys must be greater than 0" }
    val shuffledJoinKeys = joinKeys.shuffled(SecureRandom())
    val shuffledJoinKeyAndIds =
      shuffledJoinKeys.mapIndexed { index, joinKey ->
        joinKeyAndId {
          this.joinKey = joinKey
          joinKeyIdentifier = joinKeyIdentifier {
            id = "join-key-identifier-$index".toByteStringUtf8()
          }
        }
      }
    val shuffledJoinKeyAndIdBytes =
      joinKeyAndIdCollection { joinKeyAndIds += shuffledJoinKeyAndIds }.toByteString()

    return mapOf("shuffled-join-key-and-ids" to flowOf(shuffledJoinKeyAndIdBytes))
  }
}
