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
import kotlinx.coroutines.flow.any
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForInternalIdException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptNotFoundException

data class WorkItemAttemptResult(val workItemId: Long, val workItemAttemptId: Long, val workItemAttempt: WorkItemAttempt)

suspend fun AsyncDatabaseClient.ReadContext.workItemAttemptResourceIdExists(
  workItemId: Long,
  workItemAttemptId: Long,
  workItemAttemptResourceId: Long
): Boolean {
  val keySet = KeySet.newBuilder()
    .addKey(Key.of(workItemId, workItemAttemptId))
    .build()

  val rows = read(
    "WorkItemAttempts",
    keySet,
    listOf("WorkItemAttemptResourceId")
  )

  return rows.any { it.getLong("WorkItemAttemptResourceId") == workItemAttemptResourceId }

}

suspend fun AsyncDatabaseClient.ReadContext.workItemAttemptExists(
  workItemId: Long,
  workItemAttemptId: Long
): Boolean {
  return readRow("WorkItemAttempts", Key.of(workItemId, workItemAttemptId), listOf("WorkItemId", "WorkItemAttemptId")) != null
}

/** Buffers an insert mutation for the WorkItemAttempts table. */
fun AsyncDatabaseClient.TransactionContext.insertWorkItemAttempt(
  workItemId: Long,
  workItemAttemptId: Long,
  workItemAttemptResourceId: Long,
  attemptNumber: Int
): WorkItemAttempt.State {
  val workItemAttemptstate = WorkItemAttempt.State.ACTIVE
  bufferInsertMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("WorkItemAttemptResourceId").to(workItemAttemptResourceId)
    set("State").toInt64(workItemAttemptstate)
    set("AttemptNumber").to(attemptNumber.toLong())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  bufferUpdateMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("State").toInt64(WorkItem.State.RUNNING)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return workItemAttemptstate
}

/**
 * Count [WorkItemAttempt]s for a given [WorkItem].
 *
 * @throws QueueNotFoundForInternalIdException
 */
suspend fun AsyncDatabaseClient.ReadContext.countWorkItemAttempts(
  workItemId: Long,
): Int {
  val sql = buildString {
    appendLine(
      """
        SELECT COUNT(*) FROM WorkItemAttempts
        WHERE WorkItemId = @workItemId
      """.trimIndent()
    )
  }
  val query =
    statement(sql) {
      bind("workItemId").to(workItemId)
    }

  return executeQuery(query, Options.tag("action=countWorkItemAttempts"))
    .map { row -> row.getLong(0).toInt() }
    .firstOrNull() ?: 0
}

/**
 * Reads a [WorkItemAttempt] by its [workItemResourceId] and [workItemAttemptResourceId].
 *
 * @throws WorkItemAttemptNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getWorkItemAttemptByResourceId(
  workItemResourceId: Long,
  workItemAttemptResourceId: Long
): WorkItemAttemptResult {
  val sql = buildString {
    appendLine(WorkItemAttempts.BASE_SQL)
    appendLine("WHERE WorkItems.WorkItemResourceId = @workItemResourceId AND WorkItemAttempts.WorkItemAttemptResourceId = @workItemAttemptResourceId")
  }
  val row: Struct =
    executeQuery(
      statement(sql) {
        bind("workItemResourceId").to(workItemResourceId)
        bind("workItemAttemptResourceId").to(workItemAttemptResourceId)
      },
      Options.tag("action=getWorkItemAttemptByResourceId"),
    )
      .singleOrNullIfEmpty() ?: throw WorkItemAttemptNotFoundException(workItemResourceId, workItemAttemptResourceId)

  return WorkItemAttempts.buildWorkItemAttemptResult(row)
}

/** Buffers an update mutation for the WorkItemAttempts table. */
fun AsyncDatabaseClient.TransactionContext.completeWorkItemAttempt(workItemId: Long, workItemAttemptId: Long): WorkItemAttempt.State {
  val state = WorkItemAttempt.State.SUCCEEDED
  bufferUpdateMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("State").toInt64(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  bufferUpdateMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("State").toInt64(WorkItem.State.SUCCEEDED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return state
}

/** Buffers an update mutation for the WorkItemAttempts table. */
fun AsyncDatabaseClient.TransactionContext.failWorkItemAttempt(workItemId: Long, workItemAttemptId: Long): WorkItemAttempt.State {
  val state = WorkItemAttempt.State.FAILED
  bufferUpdateMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("State").toInt64(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return state
}

/**
 * Reads [WorkItemAttempts]s ordered by resource ID.
 */
fun AsyncDatabaseClient.ReadContext.readWorkItemAttempts(
  limit: Int,
  workItemResourceId: Long,
  after: ListWorkItemAttemptsPageToken.After? = null,
): Flow<WorkItemAttemptResult> {
  val sql = buildString {
    appendLine(WorkItemAttempts.BASE_SQL)
    append("""
      WHERE WorkItems.WorkItemResourceId = @workItemResourceId
    """)
    if (after != null) {
      appendLine(
        """
        AND (
          (WorkItemAttempts.CreateTime > @createTime) OR
          (WorkItemAttempts.CreateTime = @createTime AND WorkItemAttempts.WorkItemAttemptResourceId > @workItemAttemptResourceId)
        )
        """.trimIndent()
      )
    }
    appendLine("""
      ORDER BY WorkItemAttempts.CreateTime ASC, WorkItemAttempts.WorkItemId ASC, WorkItemAttempts.WorkItemAttemptResourceId ASC
    """)
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      bind("workItemResourceId").to(workItemResourceId)
      if (after != null) {
        bind("createTime").to(after.createAfter.toGcloudTimestamp())
        bind("workItemAttemptResourceId").to(after.workItemAttemptResourceId)
      }
      bind("limit").to(limit.toLong())
    }
  return executeQuery(query, Options.tag("action=readWorkItemAttempts")).map { row ->
    WorkItemAttempts.buildWorkItemAttemptResult(row)
  }
}

private object WorkItemAttempts {
  val BASE_SQL =
    """
    SELECT
      WorkItemAttempts.WorkItemAttemptId,
      WorkItemAttempts.WorkItemId,
      WorkItems.WorkItemResourceId,
      WorkItemAttempts.WorkItemAttemptResourceId,
      WorkItemAttempts.State,
      WorkItemAttempts.AttemptNumber,
      WorkItemAttempts.ErrorMessage,
      WorkItemAttempts.CreateTime,
      WorkItemAttempts.UpdateTime
      FROM WorkItems
      JOIN WorkItemAttempts USING (WorkItemId)
    """
      .trimIndent()

  fun buildWorkItemAttemptResult(row: Struct): WorkItemAttemptResult {
    return WorkItemAttemptResult(
      row.getLong("WorkItemId"),
      row.getLong("WorkItemAttemptId"),
      workItemAttempt {
        workItemResourceId = row.getLong("WorkItemResourceId")
        workItemAttemptResourceId = row.getLong("WorkItemAttemptResourceId")
        state = WorkItemAttempt.State.forNumber(row.getLong("State").toInt())
        attemptNumber = row.getLong("AttemptNumber").toInt()
        errorMessage = if (row.isNull("ErrorMessage")) "" else row.getString("ErrorMessage")
        createTime = row.getTimestamp("CreateTime").toProto()
        updateTime = row.getTimestamp("UpdateTime").toProto()
      },
    )
  }

}
