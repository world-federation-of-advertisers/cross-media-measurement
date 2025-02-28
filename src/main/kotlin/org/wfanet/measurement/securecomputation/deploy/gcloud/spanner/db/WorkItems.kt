/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForInternalIdException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemNotFoundException

data class WorkItemResult(val workItemId: Long, val workItem: WorkItem)

/** Returns whether a [WorkItem] with the specified [workItemId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.workItemIdExists(workItemId: Long): Boolean {
  return readRow("WorkItems", Key.of(workItemId), listOf("WorkItemId")) != null
}

/** Returns whether a [WorkItem] with the specified [workItemResourceId] exists. */

suspend fun AsyncDatabaseClient.ReadContext.workItemResourceIdExists(workItemResourceId: Long): Boolean {
  val keySet = KeySet.singleKey(Key.of(workItemResourceId))
  return readUsingIndex("WorkItems", "WorkItemsByResourceId", keySet, listOf("WorkItemResourceId")).firstOrNull() != null
}

/**
 * Buffers an update mutation for the WorkItems table.
 * Set as FAILED all the child WorkItemAttempts
 * */
fun AsyncDatabaseClient.TransactionContext.failWorkItem(workItemId: Long): WorkItem.State {
  val state = WorkItem.State.FAILED
  bufferUpdateMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("State").toInt64(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  bufferUpdateMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("State").toInt64(WorkItemAttempt.State.FAILED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return state
}

/** Buffers an insert mutation for the WorkItems table. */
fun AsyncDatabaseClient.TransactionContext.insertWorkItem(workItemId: Long, workItemResourceId: Long, queueId: Long): WorkItem.State {
  val state = WorkItem.State.QUEUED
  bufferInsertMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("WorkItemResourceId").to(workItemResourceId)
    set("Queue").to(queueId)
    set("State").toInt64(state)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return state
}

/**
 * Reads a [WorkItem] by its [workItemResourceId].
 *
 * @throws WorkItemNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getWorkItemByResourceId(
  queueMapping: QueueMapping,
  workItemResourceId: Long
): WorkItemResult {
  val sql = buildString {
    appendLine(WorkItems.BASE_SQL)
    appendLine("WHERE WorkItemResourceId = @workItemResourceId")
  }
  val row: Struct =
    executeQuery(
      statement(sql) { bind("workItemResourceId").to(workItemResourceId) },
      Options.tag("action=getWorkItemByResourceId"),
    )
    .singleOrNullIfEmpty() ?: throw WorkItemNotFoundException(workItemResourceId)

  val queueId = row.getLong("QueueId")
  val queue = queueMapping.getQueueById(queueId) ?: throw QueueNotFoundForInternalIdException(queueId)

  return WorkItems.buildWorkItemResult(row, queue)
}

/**
 * Reads [WorkItem]s ordered by resource ID.
 *
 * @throws QueueNotFoundForInternalIdException
 */
fun AsyncDatabaseClient.ReadContext.readWorkItems(
  queueMapping: QueueMapping,
  limit: Int,
  after: ListWorkItemsPageToken.After? = null,
): Flow<WorkItemResult> {
  val sql = buildString {
    appendLine(WorkItems.BASE_SQL)
    if (after != null) {
      appendLine("WHERE WorkItemResourceId > @afterWorkItemResourceId")
    }
    appendLine("ORDER BY WorkItemResourceId")
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      if (after != null) {
        bind("afterWorkItemResourceId").to(after.workItemResourceId)
      }
      bind("limit").to(limit.toLong())
    }

  return executeQuery(query, Options.tag("action=readWorkItems")).map { row ->
    val queueId = row.getLong("QueueId")
    val queue = queueMapping.getQueueById(queueId) ?: throw QueueNotFoundForInternalIdException(queueId)

    WorkItems.buildWorkItemResult(row, queue)
  }
}

private object WorkItems {
  val BASE_SQL =
    """
    SELECT
      WorkItemId,
      WorkItemResourceId,
      Queue,
      State,
      CreateTime,
      UpdateTime,
    FROM
      WorkItems
    """
      .trimIndent()

  fun buildWorkItemResult(row: Struct, queue: QueueMapping.Queue): WorkItemResult {
    return WorkItemResult(
      row.getLong("WorkItemId"),
      workItem {
        workItemResourceId = row.getLong("WorkItemResourceId")
        queueResourceId = queue.queueResourceId
        state = WorkItem.State.forNumber(row.getLong("State").toInt())
        createTime = row.getTimestamp("CreateTime").toProto()
        updateTime = row.getTimestamp("UpdateTime").toProto()
      },
    )
  }

}
