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
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForInternalIdException

data class WorkItemAttemptResult(val workItemAttemptId: Long, val workItemAttempt: WorkItemAttempt)

suspend fun AsyncDatabaseClient.ReadContext.workItemAttemptResourceIdExists(
  workItemId: Long,
  workItemAttemptId: Long,
  workItemAttemptResourceId: Long
): Boolean {
  val keySet = KeySet.singleKey(Key.of(workItemAttemptResourceId))
  return readRow("WorkItemAttempts", Key.of(workItemId, workItemAttemptId, workItemAttemptResourceId), listOf("WorkItemId", "WorkItemAttemptId", "WorkItemAttemptResourceId")) != null
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
  workItemAttemptResourceId: Long
): WorkItemAttempt.State {
  val workItemAttemptstate = WorkItemAttempt.State.ACTIVE
  bufferInsertMutation("WorkItemAttempts") {
    set("WorkItemId").to(workItemId)
    set("WorkItemAttemptId").to(workItemAttemptId)
    set("WorkItemAttemptResourceId").to(workItemAttemptResourceId)
    set("State").toInt64(workItemAttemptstate)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  bufferUpdateMutation("WorkItems") {
    set("WorkItemId").to(workItemId)
    set("State").toInt64(WorkItem.State.CLAIMED)
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
    .map { row -> row.getLong("attemptCount").toInt() }
    .firstOrNull() ?: 0
}
