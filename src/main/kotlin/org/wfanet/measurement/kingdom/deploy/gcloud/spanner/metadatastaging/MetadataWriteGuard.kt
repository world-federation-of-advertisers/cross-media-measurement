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
import com.google.cloud.spanner.Mutation
import io.grpc.Status
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement

/** Serializes hierarchy writes and lock changes. Callers must declare every affected path. */
class MetadataWriteGuard(private val databaseClient: AsyncDatabaseClient) {
  suspend fun <T> write(
    dataProviderId: Long,
    measurementConsumerId: Long,
    existingEventGroupIds: Set<Long>,
    destinationPaths: Set<String> = emptySet(),
    action: suspend (AsyncDatabaseClient.TransactionContext) -> T,
  ): T =
    databaseClient.readWriteTransaction().run { transaction ->
      checkWrite(
        transaction,
        dataProviderId,
        measurementConsumerId,
        existingEventGroupIds,
        destinationPaths,
      )
      action(transaction)
    }

  /** Checks existing and destination paths in the same transaction as their mutations. */
  suspend fun checkWrite(
    transaction: AsyncDatabaseClient.TransactionContext,
    dataProviderId: Long,
    measurementConsumerId: Long,
    existingEventGroupIds: Set<Long>,
    destinationPaths: Set<String> = emptySet(),
  ) {
    require(existingEventGroupIds.isNotEmpty() || destinationPaths.isNotEmpty())
    val paths = destinationPaths.toMutableSet()
    for (id in existingEventGroupIds) {
      val row =
        requireNotNull(
          transaction.readRow(
            "EventGroups",
            Key.of(dataProviderId, id),
            listOf("MeasurementConsumerId", "EntityPath"),
          )
        ) {
          "Existing EventGroup not found"
        }
      require(row.getLong("MeasurementConsumerId") == measurementConsumerId)
      if (!row.isNull("EntityPath")) paths.add(row.getString("EntityPath"))
    }
    checkAvailable(transaction, dataProviderId, measurementConsumerId, paths)
  }

  /** Acquires a provider-wide guard for configuration changes or activation/backfill. */
  suspend fun beginHierarchyChange(dataProviderId: Long, changeId: String) {
    require(changeId.isNotBlank())
    databaseClient.readWriteTransaction().run { transaction ->
      touch(transaction, dataProviderId)
      checkNoHierarchyChange(transaction, dataProviderId)
      val locks =
        transaction
          .executeQuery(
            statement(
              "SELECT OperationId FROM EventGroupMetadataSubtreeLocks WHERE DataProviderId = @dataProviderId"
            ) {
              bind("dataProviderId").to(dataProviderId)
            }
          )
          .toList()
      if (locks.isNotEmpty())
        throw Status.ABORTED.withDescription("Metadata operations are running").asRuntimeException()
      transaction.bufferInsertMutation("EventGroupMetadataHierarchyChanges") {
        set("ChangeId").to(changeId)
        set("DataProviderId").to(dataProviderId)
        set("Active").to(true)
      }
    }
  }

  suspend fun finishHierarchyChange(changeId: String) {
    databaseClient.readWriteTransaction().run { transaction ->
      val change =
        requireNotNull(
          transaction.readRow(
            "EventGroupMetadataHierarchyChanges",
            Key.of(changeId),
            listOf("DataProviderId", "Active"),
          )
        )
      if (!change.getBoolean("Active")) return@run
      touch(transaction, change.getLong("DataProviderId"))
      transaction.bufferUpdateMutation("EventGroupMetadataHierarchyChanges") {
        set("ChangeId").to(changeId)
        set("Active").to(false)
      }
    }
  }

  internal suspend fun checkAvailable(
    transaction: AsyncDatabaseClient.TransactionContext,
    dataProviderId: Long,
    measurementConsumerId: Long,
    paths: Set<String>,
  ) {
    require(paths.all { it.isNotBlank() && it.endsWith("/") })
    touch(transaction, dataProviderId)
    checkNoHierarchyChange(transaction, dataProviderId)
    val locks =
      transaction
        .executeQuery(
          statement(
            "SELECT EntityPath FROM EventGroupMetadataSubtreeLocks WHERE DataProviderId = @dataProviderId AND MeasurementConsumerId = @measurementConsumerId"
          ) {
            bind("dataProviderId").to(dataProviderId)
            bind("measurementConsumerId").to(measurementConsumerId)
          }
        )
        .toList()
    if (
      locks.any { row ->
        paths.any { path ->
          val lockedPath = row.getString("EntityPath")
          path.startsWith(lockedPath) || lockedPath.startsWith(path)
        }
      }
    )
      throw Status.ABORTED.withDescription("An overlapping metadata operation is running")
        .asRuntimeException()
  }

  internal suspend fun touch(
    transaction: AsyncDatabaseClient.TransactionContext,
    dataProviderId: Long,
  ) {
    // All participating transactions serialize their guard checks with lock acquisition/release.
    val row =
      transaction.readRow(
        "EventGroupMetadataProviderGuards",
        Key.of(dataProviderId),
        listOf("Revision"),
      )
    val next = Math.addExact(row?.getLong("Revision") ?: 0L, 1L)
    transaction.buffer(
      Mutation.newInsertOrUpdateBuilder("EventGroupMetadataProviderGuards")
        .apply {
          set("DataProviderId").to(dataProviderId)
          set("Revision").to(next)
        }
        .build()
    )
  }

  private suspend fun checkNoHierarchyChange(
    transaction: AsyncDatabaseClient.TransactionContext,
    dataProviderId: Long,
  ) {
    val changes =
      transaction
        .executeQuery(
          statement(
            "SELECT ChangeId FROM EventGroupMetadataHierarchyChanges WHERE DataProviderId = @dataProviderId AND Active"
          ) {
            bind("dataProviderId").to(dataProviderId)
          }
        )
        .toList()
    if (changes.isNotEmpty())
      throw Status.ABORTED.withDescription("Hierarchy configuration or activation is running")
        .asRuntimeException()
  }
}
