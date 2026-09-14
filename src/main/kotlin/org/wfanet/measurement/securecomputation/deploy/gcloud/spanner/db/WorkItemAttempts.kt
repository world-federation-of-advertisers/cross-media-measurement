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
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.any
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getNullableString
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForWorkItem
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptNotFoundException

data class WorkItemAttemptResult(
  val workItemId: Long,
  val workItemAttemptId: Long,
  val queueId: Long,
  val generation: Long,
  val workItemAttempt: WorkItemAttempt,
)

data class ExpiredWorkItemAttemptKey(val workItemId: Long, val workItemAttemptId: Long)

enum class WorkItemAttemptRecoveryOutcome {
  REQUEUED,
  DEAD_LETTERED,
}

suspend fun AsyncDatabaseClient.ReadContext.workItemAttemptExists(
  workItemId: Long,
  workItemAttemptId: Long,
): Boolean {
  return readRow(
    "WorkItemAttempts",
    Key.of(workItemId, workItemAttemptId),
    listOf("WorkItemId", "WorkItemAttemptId"),
  ) != null
}

/** Returns whether [workItemId] has an ACTIVE attempt. */
suspend fun AsyncDatabaseClient.ReadContext.activeWorkItemAttemptExists(workItemId: Long): Boolean {
  return read("WorkItemAttempts", KeySet.prefixRange(Key.of(workItemId)), listOf("State")).any { row
    ->
    val state: WorkItemAttempt.State = row.getProtoEnum("State", WorkItemAttempt.State::forNumber)
    state == WorkItemAttempt.State.ACTIVE
  }
}

/** Returns the ACTIVE attempt for [workItemId], or `null` when none exists. */
suspend fun AsyncDatabaseClient.ReadContext.getActiveWorkItemAttempt(
  workItemId: Long
): WorkItemAttemptResult? {
  val sql = buildString {
    appendLine(WorkItemAttempts.BASE_SQL)
    appendLine("WHERE WorkItemAttempts.WorkItemId = @workItemId")
    appendLine("  AND WorkItemAttempts.State = @activeState")
  }
  return executeQuery(
      statement(sql) {
        bind("workItemId").to(workItemId)
        bind("activeState").to(WorkItemAttempt.State.ACTIVE.number.toLong())
      },
      Options.tag("action=getActiveWorkItemAttempt"),
    )
    .singleOrNullIfEmpty()
    ?.let(WorkItemAttempts::buildWorkItemAttemptResult)
}

/** Buffers a FAILED state update for every ACTIVE attempt belonging to [workItemId]. */
suspend fun AsyncDatabaseClient.TransactionContext.failActiveWorkItemAttempts(workItemId: Long) {
  executeUpdate(
    statement(
      """
      UPDATE WorkItemAttempts
      SET State = @failedState, UpdateTime = PENDING_COMMIT_TIMESTAMP()
      WHERE WorkItemId = @workItemId AND State = @activeState
      """
        .trimIndent()
    ) {
      bind("workItemId").to(workItemId)
      bind("activeState").to(WorkItemAttempt.State.ACTIVE.number.toLong())
      bind("failedState").to(WorkItemAttempt.State.FAILED.number.toLong())
    }
  )
}

/**
 * Buffers an insert mutation for the WorkItemAttempts table.
 *
 * @return a pair consisting of:
 *     - The `attemptNumber` assigned to the newly inserted row.
 *     - The resulting `State` of the `WorkItemAttempt` after insertion.
 */
suspend fun AsyncDatabaseClient.TransactionContext.insertWorkItemAttempt(
  workItemId: Long,
  workItemAttemptId: Long,
  workItemAttemptResourceId: String,
  leaseExpirationTime: Instant? = null,
): Pair<Int, WorkItemAttempt.State> {

  val attemptNumber =
    read("WorkItemAttempts", KeySet.prefixRange(Key.of(workItemId)), listOf("WorkItemId")).count() +
      1
  val workItemAttemptState = WorkItemAttempt.State.ACTIVE
  bufferInsertMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("WorkItemAttemptResourceId").to(workItemAttemptResourceId)
    set("State").to(workItemAttemptState)
    if (leaseExpirationTime != null) {
      set("LeaseExpirationTime").to(leaseExpirationTime.toGcloudTimestamp())
    }
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  bufferUpdateMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("State").to(WorkItem.State.RUNNING)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  deleteWorkItemPublication(workItemId)
  return Pair(attemptNumber, workItemAttemptState)
}

/**
 * Reads a [WorkItemAttempt] by its [workItemResourceId] and [workItemAttemptResourceId].
 *
 * A [WorkItemAttemptResult] containing the associated work item ID, work item attempt ID, and the
 * retrieved [WorkItemAttempt].
 *
 * @throws WorkItemAttemptNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getWorkItemAttemptByResourceId(
  workItemResourceId: String,
  workItemAttemptResourceId: String,
): WorkItemAttemptResult {
  val sql = buildString {
    appendLine(WorkItemAttempts.BASE_SQL)
    appendLine(
      "WHERE WorkItems.WorkItemResourceId = @workItemResourceId AND WorkItemAttempts.WorkItemAttemptResourceId = @workItemAttemptResourceId"
    )
  }
  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("workItemResourceId").to(workItemResourceId)
          bind("workItemAttemptResourceId").to(workItemAttemptResourceId)
        },
        Options.tag("action=getWorkItemAttemptByResourceId"),
      )
      .singleOrNullIfEmpty()
      ?: throw WorkItemAttemptNotFoundException(workItemResourceId, workItemAttemptResourceId)

  return WorkItemAttempts.buildWorkItemAttemptResult(row)
}

/**
 * Buffers an update mutation for the WorkItemAttempts table.
 *
 * @return the updated `WorkItemAttempt.State`.
 */
fun AsyncDatabaseClient.TransactionContext.completeWorkItemAttempt(
  workItemId: Long,
  workItemAttemptId: Long,
): WorkItemAttempt.State {
  val state = WorkItemAttempt.State.SUCCEEDED
  bufferUpdateMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  bufferUpdateMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("State").to(WorkItem.State.SUCCEEDED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return state
}

/**
 * Buffers an update mutation for the WorkItemAttempts table.
 *
 * @return the updated `WorkItemAttempt.State`.
 */
fun AsyncDatabaseClient.TransactionContext.failWorkItemAttempt(
  workItemId: Long,
  workItemAttemptId: Long,
  errorMessage: String,
): WorkItemAttempt.State {
  val state = WorkItemAttempt.State.FAILED
  bufferUpdateMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("State").to(state)
    set("ErrorMessage").to(errorMessage)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return state
}

/** Fails an attempt and creates a durable normal or dead-letter publication. */
fun AsyncDatabaseClient.TransactionContext.failWorkItemAttemptAndScheduleRecovery(
  result: WorkItemAttemptResult,
  queue: QueueMapping.Queue,
  errorMessage: String,
): WorkItemAttemptRecoveryOutcome {
  failWorkItemAttempt(result.workItemId, result.workItemAttemptId, errorMessage)
  return if (result.workItemAttempt.attemptNumber >= queue.maxWorkItemAttempts) {
    scheduleWorkItemDeadLetterPublication(result.workItemId, result.generation)
    WorkItemAttemptRecoveryOutcome.DEAD_LETTERED
  } else {
    retryWorkItem(result.workItemId, result.generation)
    WorkItemAttemptRecoveryOutcome.REQUEUED
  }
}

/** Extends the lease for an ACTIVE WorkItemAttempt. */
fun AsyncDatabaseClient.TransactionContext.renewWorkItemAttemptLease(
  workItemId: Long,
  workItemAttemptId: Long,
  leaseExpirationTime: Instant,
) {
  bufferUpdateMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("LeaseExpirationTime").to(leaseExpirationTime.toGcloudTimestamp())
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Reads ACTIVE WorkItemAttempts whose leases have expired. */
fun AsyncDatabaseClient.ReadContext.readExpiredWorkItemAttempts(
  now: Instant,
  limit: Int,
): Flow<ExpiredWorkItemAttemptKey> {
  val query =
    statement(
      """
      SELECT WorkItemId, WorkItemAttemptId
      FROM WorkItemAttempts@{FORCE_INDEX=WorkItemAttemptsByLeaseExpirationTime}
      WHERE State = @activeState
        AND LeaseExpirationTime IS NOT NULL
        AND LeaseExpirationTime <= @now
      ORDER BY LeaseExpirationTime ASC, WorkItemId ASC, WorkItemAttemptId ASC
      LIMIT @limit
      """
        .trimIndent()
    ) {
      bind("activeState").to(WorkItemAttempt.State.ACTIVE.number.toLong())
      bind("now").to(now.toGcloudTimestamp())
      bind("limit").to(limit.toLong())
    }
  return executeQuery(query, Options.tag("action=readExpiredWorkItemAttempts")).map { row ->
    ExpiredWorkItemAttemptKey(row.getLong("WorkItemId"), row.getLong("WorkItemAttemptId"))
  }
}

/** Fails an expired ACTIVE attempt and transactionally schedules recovery or dead-lettering. */
suspend fun AsyncDatabaseClient.TransactionContext.recoverExpiredWorkItemAttempt(
  key: ExpiredWorkItemAttemptKey,
  now: Instant,
  queueMapping: QueueMapping,
): WorkItemAttemptRecoveryOutcome? {
  val attemptRow =
    readRow(
      "WorkItemAttempts",
      Key.of(key.workItemId, key.workItemAttemptId),
      listOf("State", "LeaseExpirationTime"),
    ) ?: return null
  val state: WorkItemAttempt.State =
    attemptRow.getProtoEnum("State", WorkItemAttempt.State::forNumber)
  if (
    state != WorkItemAttempt.State.ACTIVE ||
      attemptRow.isNull("LeaseExpirationTime") ||
      attemptRow.getTimestamp("LeaseExpirationTime") > now.toGcloudTimestamp()
  ) {
    return null
  }

  val workItemRow =
    readRow(
      "WorkItems",
      Key.of(key.workItemId),
      listOf("WorkItemResourceId", "QueueId", "State", "Generation"),
    ) ?: return null
  val workItemState: WorkItem.State = workItemRow.getProtoEnum("State", WorkItem.State::forNumber)
  if (workItemState != WorkItem.State.RUNNING) {
    return null
  }
  val generation = if (workItemRow.isNull("Generation")) 1L else workItemRow.getLong("Generation")
  val queueId = workItemRow.getLong("QueueId")
  val queue =
    queueMapping.getQueueById(queueId)
      ?: throw QueueNotFoundForWorkItem(workItemRow.getString("WorkItemResourceId"))
  val attemptNumber =
    read("WorkItemAttempts", KeySet.prefixRange(Key.of(key.workItemId)), listOf("WorkItemId"))
      .count()
      .toInt()
  return failWorkItemAttemptAndScheduleRecovery(
    WorkItemAttemptResult(
      workItemId = key.workItemId,
      workItemAttemptId = key.workItemAttemptId,
      queueId = queueId,
      generation = generation,
      workItemAttempt = workItemAttempt { this.attemptNumber = attemptNumber },
    ),
    queue,
    "WorkItemAttempt lease expired",
  )
}

/**
 * Reads [WorkItemAttempts]s ordered by create time, work item id and work item attempt resource id.
 */
fun AsyncDatabaseClient.ReadContext.readWorkItemAttempts(
  limit: Int,
  workItemResourceId: String,
  after: ListWorkItemAttemptsPageToken.After? = null,
): Flow<WorkItemAttemptResult> {
  val sql = buildString {
    appendLine(WorkItemAttempts.BASE_SQL)
    append(
      """
      WHERE WorkItems.WorkItemResourceId = @workItemResourceId
    """
    )
    if (after != null) {
      appendLine(
        """
        AND (
          (WorkItemAttempts.CreateTime > @createTime) OR
          (WorkItemAttempts.CreateTime = @createTime AND WorkItemAttempts.WorkItemAttemptResourceId > @workItemAttemptResourceId)
        )
        """
          .trimIndent()
      )
    }
    appendLine(
      """
      ORDER BY WorkItemAttempts.CreateTime ASC, WorkItemAttempts.WorkItemId ASC, WorkItemAttempts.WorkItemAttemptResourceId ASC
    """
    )
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      bind("workItemResourceId").to(workItemResourceId)
      if (after != null) {
        bind("createTime").to(after.createdAfter.toGcloudTimestamp())
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
      WorkItems.QueueId,
      WorkItems.Generation,
      WorkItemAttempts.WorkItemAttemptResourceId,
      WorkItemAttempts.State,
      (
        SELECT COUNT(*)
        FROM WorkItemAttempts AS WIA
        WHERE WIA.WorkItemId = WorkItemAttempts.WorkItemId
          AND WIA.CreateTime <= WorkItemAttempts.CreateTime
      ) AS AttemptNumber,
      WorkItemAttempts.ErrorMessage,
      WorkItemAttempts.LeaseExpirationTime,
      WorkItemAttempts.CreateTime,
      WorkItemAttempts.UpdateTime
    FROM WorkItems
      JOIN WorkItemAttempts USING (WorkItemId)
    """
      .trimIndent()

  fun buildWorkItemAttemptResult(row: Struct): WorkItemAttemptResult {
    return WorkItemAttemptResult(
      workItemId = row.getLong("WorkItemId"),
      workItemAttemptId = row.getLong("WorkItemAttemptId"),
      queueId = row.getLong("QueueId"),
      generation = if (row.isNull("Generation")) 1L else row.getLong("Generation"),
      workItemAttempt =
        workItemAttempt {
          workItemResourceId = row.getString("WorkItemResourceId")
          workItemAttemptResourceId = row.getString("WorkItemAttemptResourceId")
          state = row.getProtoEnum("State", WorkItemAttempt.State::forNumber)
          attemptNumber = row.getLong("AttemptNumber").toInt()
          errorMessage = row.getNullableString("ErrorMessage") ?: ""
          if (!row.isNull("LeaseExpirationTime")) {
            leaseExpirationTime = row.getTimestamp("LeaseExpirationTime").toProto()
          }
          createTime = row.getTimestamp("CreateTime").toProto()
          updateTime = row.getTimestamp("UpdateTime").toProto()
        },
    )
  }
}
