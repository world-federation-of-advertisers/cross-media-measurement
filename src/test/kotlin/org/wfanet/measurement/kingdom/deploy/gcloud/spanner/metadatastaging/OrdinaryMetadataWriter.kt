// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import com.google.protobuf.Struct
import io.grpc.Status
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.internal.kingdom.EventGroup

/** Prototype of an ordinary metadata replacement; protected keys come from hierarchy validation. */
class OrdinaryMetadataWriter(private val databaseClient: AsyncDatabaseClient) {
  private val guard = MetadataWriteGuard(databaseClient)

  suspend fun update(
    dataProviderId: Long,
    measurementConsumerId: Long,
    eventGroupId: Long,
    metadata: Struct,
    protectedKeys: Set<String>,
  ): Struct =
    guard.write(dataProviderId, measurementConsumerId, setOf(eventGroupId)) { transaction ->
      val row =
        requireNotNull(
          transaction.readRow(
            "EventGroups",
            Key.of(dataProviderId, eventGroupId),
            listOf("State", "IsLeaf"),
          )
        )
      if (
        row.getLong("State") == EventGroup.State.DELETED.number.toLong() ||
          !row.getBoolean("IsLeaf")
      ) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "Only ordinary, non-deleted EventGroups can be updated"
          )
          .asRuntimeException()
      }
      val current =
        checkNotNull(
          PublishedMetadataReader(transaction, dataProviderId, measurementConsumerId)
            .get(eventGroupId)
        )
      if (protectedKeys.any { current.fieldsMap[it] != metadata.fieldsMap[it] }) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "Metadata owned by another level or identifying the path cannot be changed"
          )
          .asRuntimeException()
      }
      val sequence =
        MetadataPublicationSequences.next(transaction, dataProviderId, measurementConsumerId)
      transaction.bufferUpdateMutation("EventGroups") {
        set("DataProviderId").to(dataProviderId)
        set("EventGroupId").to(eventGroupId)
        set("EntityMetadata").to(Value.protoMessage(metadata))
        set("EntityMetadataPublicationSequence").to(sequence)
        set("EntityMetadataValues").toStringArray(EntityMetadataValues.flatten(metadata))
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
      metadata
    }
}
