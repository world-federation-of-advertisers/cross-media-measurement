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
import com.google.cloud.spanner.Struct as SpannerStruct
import com.google.cloud.spanner.Value
import com.google.protobuf.Struct
import com.google.protobuf.struct
import io.grpc.Status
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation

/**
 * Durable staging, worker fencing, and atomic metadata publication.
 *
 * DO_NOT_SUBMIT(world-federation-of-advertisers/cross-media-measurement#4542): Wire the activation
 * job and Operations before enabling ancestor updates.
 */
class EventGroupMetadataStore(private val databaseClient: AsyncDatabaseClient) {
  data class Request(
    val operationId: String,
    val dataProviderId: Long,
    val measurementConsumerId: Long,
    val entityPath: String,
    val ownedKeys: Set<String>,
    val metadata: Struct,
  )

  data class Progress(val stagedCount: Long, val complete: Boolean)

  data class WorkToken(val operationId: String, val fence: Long)

  enum class State(val number: Long) {
    RUNNING(1),
    SUCCEEDED(2),
    CANCELLED(3),
    FAILED(4),
  }

  data class Operation(val state: State, val progress: Progress, val publicationSequence: Long?)

  /** Persists the immutable staging request under a new operation ID. */
  suspend fun start(request: Request): WorkToken =
    databaseClient.readWriteTransaction().run { transaction -> start(transaction, request) }

  /** Acquires the lock in the same transaction as hierarchy validation and operation creation. */
  suspend fun start(
    transaction: AsyncDatabaseClient.TransactionContext,
    request: Request,
  ): WorkToken {
    require(request.operationId.isNotBlank() && request.operationId.length <= 64)
    require(request.entityPath.isNotBlank() && request.entityPath.endsWith("/"))
    require(request.ownedKeys.containsAll(request.metadata.fieldsMap.keys))
    MetadataWriteGuard.checkAvailable(
      transaction,
      request.dataProviderId,
      request.measurementConsumerId,
      setOf(request.entityPath),
    )
    transaction.bufferInsertMutation("EventGroupMetadataOperations") {
      set("OperationId").to(request.operationId)
      set("DataProviderId").to(request.dataProviderId)
      set("MeasurementConsumerId").to(request.measurementConsumerId)
      set("EntityPath").to(request.entityPath)
      set("OwnedKeys").toStringArray(request.ownedKeys)
      set("Metadata").to(Value.protoMessage(request.metadata))
      set("State").to(State.RUNNING.number)
      set("WorkerFence").to(1L)
      set("StagedCount").to(0L)
      set("Complete").to(false)
    }
    transaction.bufferInsertMutation("EventGroupMetadataSubtreeLocks") {
      set("OperationId").to(request.operationId)
      set("DataProviderId").to(request.dataProviderId)
      set("MeasurementConsumerId").to(request.measurementConsumerId)
      set("EntityPath").to(request.entityPath)
    }
    return WorkToken(request.operationId, 1L)
  }

  /** Commits at most [batchSize] staged rows and their checkpoint in one transaction. */
  suspend fun stageNextBatch(work: WorkToken, batchSize: Int): Progress {
    require(batchSize > 0)
    return databaseClient.readWriteTransaction().run { transaction ->
      val operation = readOperation(transaction, work.operationId)
      checkWorker(operation, work)
      val stagedCount = operation.getLong("StagedCount")
      if (operation.getBoolean("Complete")) {
        return@run Progress(stagedCount, complete = true)
      }
      val lastEventGroupId: Long? =
        if (operation.isNull("LastEventGroupId")) null else operation.getLong("LastEventGroupId")
      val rows =
        PublishedMetadataReader(
            transaction,
            operation.getLong("DataProviderId"),
            operation.getLong("MeasurementConsumerId"),
          )
          .readBatch(operation.getString("EntityPath"), lastEventGroupId, batchSize)
      val ownedKeys = operation.getStringList("OwnedKeys").toSet()
      val replacement = operation.getProtoMessage("Metadata", Struct.getDefaultInstance())
      for (row in rows) {
        val existing =
          if (row.isNull("EntityMetadata")) Struct.getDefaultInstance()
          else row.getProtoMessage("EntityMetadata", Struct.getDefaultInstance())
        val metadata = struct {
          fields.putAll(existing.fieldsMap.filterKeys { it !in ownedKeys })
          fields.putAll(replacement.fieldsMap)
        }
        transaction.bufferInsertMutation("EventGroupMetadataVersions") {
          set("OperationId").to(work.operationId)
          set("EventGroupId").to(row.getLong("EventGroupId"))
          set("Metadata").to(Value.protoMessage(metadata))
          set("MetadataValues").toStringArray(EntityMetadataValues.flatten(metadata))
        }
      }
      val progress = Progress(stagedCount + rows.size, complete = rows.size < batchSize)
      transaction.bufferUpdateMutation("EventGroupMetadataOperations") {
        set("OperationId").to(work.operationId)
        set("LastEventGroupId")
          .to(Value.int64(rows.lastOrNull()?.getLong("EventGroupId") ?: lastEventGroupId))
        set("StagedCount").to(progress.stagedCount)
        set("Complete").to(progress.complete)
      }
      progress
    }
  }

  /** Publishes a completed version without rewriting its EventGroups or search documents. */
  suspend fun publish(work: WorkToken): Long {
    return databaseClient.readWriteTransaction().run { transaction ->
      val operation = readOperation(transaction, work.operationId)
      if (operation.getLong("State") == State.SUCCEEDED.number) {
        return@run operation.getLong("PublicationSequence")
      }
      checkWorker(operation, work)
      check(operation.getBoolean("Complete")) { "Staging is not complete" }
      val dataProviderId = operation.getLong("DataProviderId")
      val measurementConsumerId = operation.getLong("MeasurementConsumerId")
      MetadataWriteGuard.touch(transaction, dataProviderId)
      val next =
        MetadataPublicationSequences.next(transaction, dataProviderId, measurementConsumerId)
      transaction.bufferUpdateMutation("EventGroupMetadataOperations") {
        set("OperationId").to(work.operationId)
        set("PublicationSequence").to(next)
        set("State").to(State.SUCCEEDED.number)
      }
      transaction.buffer(
        Mutation.delete("EventGroupMetadataSubtreeLocks", Key.of(work.operationId))
      )
      next
    }
  }

  /** Claims unfinished work after restart, invalidating older worker tokens. */
  suspend fun resume(operationId: String): WorkToken =
    databaseClient.readWriteTransaction().run { transaction ->
      val operation = readOperation(transaction, operationId)
      checkRunning(operation)
      val fence = Math.addExact(operation.getLong("WorkerFence"), 1L)
      transaction.bufferUpdateMutation("EventGroupMetadataOperations") {
        set("OperationId").to(operationId)
        set("WorkerFence").to(fence)
      }
      WorkToken(operationId, fence)
    }

  suspend fun cancel(operationId: String): State = finish(operationId, State.CANCELLED)

  suspend fun fail(work: WorkToken): State = finish(work.operationId, State.FAILED, work)

  suspend fun getOperation(operationId: String): Operation =
    databaseClient.singleUse().use { context ->
      val row = readOperation(context, operationId)
      Operation(
        STATES.getValue(row.getLong("State")),
        Progress(row.getLong("StagedCount"), row.getBoolean("Complete")),
        if (row.isNull("PublicationSequence")) null else row.getLong("PublicationSequence"),
      )
    }

  private suspend fun finish(operationId: String, state: State, work: WorkToken? = null): State =
    databaseClient.readWriteTransaction().run { transaction ->
      val operation = readOperation(transaction, operationId)
      val current = STATES.getValue(operation.getLong("State"))
      if (current != State.RUNNING) return@run current
      if (work != null) checkWorker(operation, work)
      MetadataWriteGuard.touch(transaction, operation.getLong("DataProviderId"))
      transaction.bufferUpdateMutation("EventGroupMetadataOperations") {
        set("OperationId").to(operationId)
        set("State").to(state.number)
      }
      transaction.buffer(Mutation.delete("EventGroupMetadataSubtreeLocks", Key.of(operationId)))
      state
    }

  private suspend fun readOperation(
    context: AsyncDatabaseClient.ReadContext,
    operationId: String,
  ): SpannerStruct =
    requireNotNull(
      context.readRow("EventGroupMetadataOperations", Key.of(operationId), OPERATION_COLUMNS)
    ) {
      "Staging operation not found"
    }

  private fun checkRunning(operation: SpannerStruct) {
    if (operation.getLong("State") != State.RUNNING.number) {
      throw Status.FAILED_PRECONDITION.withDescription("Metadata operation is terminal")
        .asRuntimeException()
    }
  }

  private fun checkWorker(operation: SpannerStruct, work: WorkToken) {
    checkRunning(operation)
    if (operation.getLong("WorkerFence") != work.fence) {
      throw Status.ABORTED.withDescription("Worker token was superseded").asRuntimeException()
    }
  }

  companion object {
    private val STATES = State.entries.associateBy { it.number }
    private val OPERATION_COLUMNS =
      listOf(
        "DataProviderId",
        "MeasurementConsumerId",
        "EntityPath",
        "OwnedKeys",
        "Metadata",
        "LastEventGroupId",
        "StagedCount",
        "Complete",
        "State",
        "WorkerFence",
        "PublicationSequence",
      )
  }
}
