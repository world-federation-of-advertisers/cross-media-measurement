/*
 * Copyright 2026 The Cross-Media Measurement Authors
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
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import java.time.Instant
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping

data class WorkItemPublicationResult(
  val workItemId: Long,
  val workItem: WorkItem,
  val attemptCount: Long,
)

/** Buffers an insert mutation for a pending WorkItem publication. */
fun AsyncDatabaseClient.TransactionContext.insertWorkItemPublication(workItemId: Long) {
  bufferInsertMutation("WorkItemPublications") {
    set("WorkItemId").to(workItemId)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("AttemptCount").to(0L)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Removes a pending WorkItem publication. Deleting a missing row is a no-op. */
fun AsyncDatabaseClient.TransactionContext.deleteWorkItemPublication(workItemId: Long) {
  buffer(Mutation.delete("WorkItemPublications", Key.of(workItemId)))
}

/**
 * Claims a pending publication for [leaseOwner].
 *
 * If [workItemId] is specified, only that WorkItem is considered. A publication for a WorkItem
 * which is no longer QUEUED is removed because queue delivery has already been demonstrated or the
 * WorkItem has reached a terminal state.
 */
suspend fun AsyncDatabaseClient.TransactionContext.claimWorkItemPublication(
  queueMapping: QueueMapping,
  leaseOwner: String,
  now: Instant,
  leaseExpirationTime: Instant,
  workItemId: Long? = null,
): WorkItemPublicationResult? {
  val sql = buildString {
    appendLine(WORK_ITEM_PUBLICATION_SQL)
    appendLine("WHERE (LeaseExpirationTime IS NULL OR LeaseExpirationTime <= @now)")
    if (workItemId != null) {
      appendLine("  AND WorkItemPublications.WorkItemId = @workItemId")
    }
    appendLine("ORDER BY WorkItemPublications.CreateTime ASC, WorkItemPublications.WorkItemId ASC")
    appendLine("LIMIT 1")
  }
  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("now").to(now.toGcloudTimestamp())
          if (workItemId != null) {
            bind("workItemId").to(workItemId)
          }
        },
        Options.tag("action=claimWorkItemPublication"),
      )
      .singleOrNull() ?: return null

  val claimedWorkItemId = row.getLong("WorkItemId")
  val queueId = row.getLong("QueueId")
  val state = WorkItem.State.forNumber(row.getLong("State").toInt())

  if (state != WorkItem.State.QUEUED) {
    deleteWorkItemPublication(claimedWorkItemId)
    return null
  }

  val attemptCount = row.getLong("AttemptCount") + 1L
  val queue = queueMapping.getQueueById(queueId)
  bufferUpdateMutation("WorkItemPublications") {
    set("WorkItemId").to(claimedWorkItemId)
    set("LeaseOwner").to(if (queue == null) null else leaseOwner)
    set("LeaseExpirationTime").to(leaseExpirationTime.toGcloudTimestamp())
    set("AttemptCount").to(attemptCount)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  if (queue == null) {
    return null
  }

  val result = WorkItems.buildWorkItemResult(row, queue)
  return WorkItemPublicationResult(claimedWorkItemId, result.workItem, attemptCount)
}

/** Removes a publication if it is still leased by [leaseOwner]. */
suspend fun AsyncDatabaseClient.TransactionContext.completeWorkItemPublication(
  workItemId: Long,
  leaseOwner: String,
) {
  if (isLeaseOwner(workItemId, leaseOwner)) {
    deleteWorkItemPublication(workItemId)
  }
}

/** Releases a publication lease and schedules its next attempt. */
suspend fun AsyncDatabaseClient.TransactionContext.retryWorkItemPublication(
  workItemId: Long,
  leaseOwner: String,
  nextAttemptTime: Instant,
) {
  if (!isLeaseOwner(workItemId, leaseOwner)) {
    return
  }
  bufferUpdateMutation("WorkItemPublications") {
    set("WorkItemId").to(workItemId)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(nextAttemptTime.toGcloudTimestamp())
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private suspend fun AsyncDatabaseClient.ReadContext.isLeaseOwner(
  workItemId: Long,
  leaseOwner: String,
): Boolean {
  val row =
    readRow("WorkItemPublications", Key.of(workItemId), listOf("LeaseOwner")) ?: return false
  return !row.isNull("LeaseOwner") && row.getString("LeaseOwner") == leaseOwner
}

private val WORK_ITEM_PUBLICATION_SQL =
  """
  SELECT
    WorkItems.WorkItemId,
    WorkItems.WorkItemResourceId,
    WorkItems.QueueId,
    WorkItems.State,
    WorkItems.WorkItemParams,
    WorkItems.CreateTime,
    WorkItems.UpdateTime,
    WorkItemPublications.AttemptCount,
    WorkItemPublications.LeaseExpirationTime
  FROM WorkItemPublications
  JOIN WorkItems USING (WorkItemId)
  """
    .trimIndent()
