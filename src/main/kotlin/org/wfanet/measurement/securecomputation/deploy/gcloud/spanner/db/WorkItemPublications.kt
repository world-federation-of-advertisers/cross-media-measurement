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
  val queueResourceId: String,
  val attemptCount: Long,
  val leaseToken: String,
)

/** Returns whether [workItemId] has a pending publication. */
suspend fun AsyncDatabaseClient.ReadContext.workItemPublicationExists(workItemId: Long): Boolean {
  return readRow("WorkItemPublications", Key.of(workItemId), listOf("WorkItemId")) != null
}

sealed interface WorkItemPublicationClaimResult {
  data class Claimed(val publication: WorkItemPublicationResult) : WorkItemPublicationClaimResult

  data class Skipped(val workItemResourceId: String, val queueId: Long, val reason: Reason) :
    WorkItemPublicationClaimResult {
    enum class Reason {
      WORK_ITEM_STATE_MISMATCH,
      QUEUE_NOT_FOUND,
      LEGACY_DEAD_LETTER_TERMINALIZED,
    }
  }
}

/** Buffers an insert mutation for an immediately eligible WorkItem publication. */
fun AsyncDatabaseClient.TransactionContext.insertWorkItemPublication(workItemId: Long) {
  insertWorkItemPublication(workItemId, Instant.now())
}

/** Buffers an insert mutation for a WorkItem publication eligible at [nextAttemptTime]. */
fun AsyncDatabaseClient.TransactionContext.insertWorkItemPublication(
  workItemId: Long,
  nextAttemptTime: Instant,
) {
  bufferInsertMutation("WorkItemPublications") {
    set("WorkItemId").to(workItemId)
    // Retained during the rollout so new binaries can read rows written by the prior version.
    set("IsDeadLetter").to(false)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("NextAttemptTime").to(nextAttemptTime.toGcloudTimestamp())
    set("QueueResolutionFailed").to(false)
    set("AttemptCount").to(0L)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Schedules a publication, replacing any stale publication for an older generation. */
suspend fun AsyncDatabaseClient.TransactionContext.scheduleWorkItemPublication(
  workItemId: Long,
  nextAttemptTime: Instant,
) {
  if (!workItemPublicationExists(workItemId)) {
    insertWorkItemPublication(workItemId, nextAttemptTime)
    return
  }
  bufferUpdateMutation("WorkItemPublications") {
    set("WorkItemId").to(workItemId)
    set("IsDeadLetter").to(false)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("NextAttemptTime").to(nextAttemptTime.toGcloudTimestamp())
    set("QueueResolutionFailed").to(false)
    set("AttemptCount").to(0L)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

enum class WorkItemPublicationResetResult {
  MISSING,
  LEASED,
  RESET,
}

/** Makes an existing, unleased publication immediately eligible for an operator retry. */
suspend fun AsyncDatabaseClient.TransactionContext.resetWorkItemPublication(
  workItemId: Long,
  now: Instant,
): WorkItemPublicationResetResult {
  val row =
    readRow("WorkItemPublications", Key.of(workItemId), listOf("LeaseOwner"))
      ?: return WorkItemPublicationResetResult.MISSING
  if (!row.isNull("LeaseOwner")) {
    return WorkItemPublicationResetResult.LEASED
  }
  bufferUpdateMutation("WorkItemPublications") {
    set("WorkItemId").to(workItemId)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("NextAttemptTime").to(now.toGcloudTimestamp())
    set("QueueResolutionFailed").to(false)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return WorkItemPublicationResetResult.RESET
}

/** Removes a pending WorkItem publication. Deleting a missing row is a no-op. */
fun AsyncDatabaseClient.TransactionContext.deleteWorkItemPublication(workItemId: Long) {
  buffer(Mutation.delete("WorkItemPublications", Key.of(workItemId)))
}

/**
 * Claims a pending publication with [leaseToken].
 *
 * If [workItemId] is specified, only that WorkItem is considered. A publication for a WorkItem
 * whose state no longer matches the publication type is removed because the publication is stale.
 */
suspend fun AsyncDatabaseClient.TransactionContext.claimWorkItemPublication(
  queueMapping: QueueMapping,
  leaseToken: String,
  now: Instant,
  leaseExpirationTime: Instant,
  workItemId: Long? = null,
): WorkItemPublicationClaimResult? {
  val sql = buildString {
    appendLine(WORK_ITEM_PUBLICATION_SQL)
    appendLine("WHERE NextAttemptTime <= @now")
    appendLine("  AND (LeaseExpirationTime IS NULL OR LeaseExpirationTime <= @now)")
    if (workItemId != null) {
      appendLine("  AND WorkItemPublications.WorkItemId = @workItemId")
    }
    appendLine("ORDER BY WorkItemPublications.QueueResolutionFailed ASC,")
    appendLine("  WorkItemPublications.NextAttemptTime ASC,")
    appendLine("  WorkItemPublications.LeaseExpirationTime ASC,")
    appendLine("  WorkItemPublications.WorkItemId ASC")
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
  val workItemResourceId = row.getString("WorkItemResourceId")
  val queueId = row.getLong("QueueId")
  val state = WorkItem.State.forNumber(row.getLong("State").toInt())
  val isDeadLetter = !row.isNull("IsDeadLetter") && row.getBoolean("IsDeadLetter")

  if (isDeadLetter) {
    // Before Pub/Sub became the sole dead-letter mechanism, the application could leave a
    // pending dead-letter publication in this outbox. Finish that already-decided terminal
    // transition without publishing another message.
    failActiveWorkItemAttempts(claimedWorkItemId)
    failWorkItem(claimedWorkItemId)
    return WorkItemPublicationClaimResult.Skipped(
      workItemResourceId,
      queueId,
      WorkItemPublicationClaimResult.Skipped.Reason.LEGACY_DEAD_LETTER_TERMINALIZED,
    )
  }

  if (state != WorkItem.State.QUEUED) {
    deleteWorkItemPublication(claimedWorkItemId)
    return WorkItemPublicationClaimResult.Skipped(
      workItemResourceId,
      queueId,
      WorkItemPublicationClaimResult.Skipped.Reason.WORK_ITEM_STATE_MISMATCH,
    )
  }

  val attemptCount = row.getLong("AttemptCount") + 1L
  val queue = queueMapping.getQueueById(queueId)
  bufferUpdateMutation("WorkItemPublications") {
    set("WorkItemId").to(claimedWorkItemId)
    set("LeaseOwner").to(if (queue == null) null else leaseToken)
    set("LeaseExpirationTime").to(leaseExpirationTime.toGcloudTimestamp())
    set("QueueResolutionFailed").to(queue == null)
    if (queue == null) {
      set("NextAttemptTime").to(leaseExpirationTime.toGcloudTimestamp())
    }
    set("AttemptCount").to(attemptCount)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  if (queue == null) {
    return WorkItemPublicationClaimResult.Skipped(
      workItemResourceId,
      queueId,
      WorkItemPublicationClaimResult.Skipped.Reason.QUEUE_NOT_FOUND,
    )
  }

  val result = WorkItems.buildWorkItemResult(row, queue)
  return WorkItemPublicationClaimResult.Claimed(
    WorkItemPublicationResult(
      claimedWorkItemId,
      result.workItem,
      queue.queueResourceId,
      attemptCount,
      leaseToken,
    )
  )
}

/** Removes a publication if it is still leased with [leaseToken]. */
suspend fun AsyncDatabaseClient.TransactionContext.completeWorkItemPublication(
  workItemId: Long,
  leaseToken: String,
) {
  if (hasLeaseToken(workItemId, leaseToken)) {
    deleteWorkItemPublication(workItemId)
  }
}

/** Releases a publication lease and schedules its next attempt. */
suspend fun AsyncDatabaseClient.TransactionContext.retryWorkItemPublication(
  workItemId: Long,
  leaseToken: String,
  nextAttemptTime: Instant,
) {
  if (!hasLeaseToken(workItemId, leaseToken)) {
    return
  }
  bufferUpdateMutation("WorkItemPublications") {
    set("WorkItemId").to(workItemId)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(nextAttemptTime.toGcloudTimestamp())
    set("NextAttemptTime").to(nextAttemptTime.toGcloudTimestamp())
    set("QueueResolutionFailed").to(false)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private suspend fun AsyncDatabaseClient.ReadContext.hasLeaseToken(
  workItemId: Long,
  leaseToken: String,
): Boolean {
  val row =
    readRow("WorkItemPublications", Key.of(workItemId), listOf("LeaseOwner")) ?: return false
  return !row.isNull("LeaseOwner") && row.getString("LeaseOwner") == leaseToken
}

private val WORK_ITEM_PUBLICATION_SQL =
  """
  SELECT
    WorkItems.WorkItemId,
    WorkItems.WorkItemResourceId,
    WorkItems.QueueId,
    WorkItems.State,
    WorkItems.WorkItemParams,
    WorkItems.Generation,
    WorkItems.PublicationScheduledGeneration,
    WorkItems.CreateTime,
    WorkItems.UpdateTime,
    WorkItemPublications.IsDeadLetter,
    WorkItemPublications.AttemptCount,
    WorkItemPublications.LeaseExpirationTime,
    WorkItemPublications.NextAttemptTime,
    WorkItemPublications.QueueResolutionFailed
  FROM WorkItemPublications
  JOIN WorkItems USING (WorkItemId)
  """
    .trimIndent()
