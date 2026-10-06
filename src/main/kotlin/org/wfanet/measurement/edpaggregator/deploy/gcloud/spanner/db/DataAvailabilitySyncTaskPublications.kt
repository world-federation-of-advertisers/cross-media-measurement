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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import java.time.Instant
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskAttempts
import org.wfanet.measurement.edpaggregator.service.DataAvailabilitySyncTaskKey
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState

data class DataAvailabilitySyncTaskPublication(
  val dataProviderResourceId: String,
  val rawImpressionUploadId: Long,
  val taskResourceId: String,
  val taskName: String,
  val rawImpressionUploadName: String,
  val modelLine: String,
  val doneBlobPathHash: String,
  val doneBlobGeneration: Long,
  val taskState: DataAvailabilitySyncTaskState,
  val attemptCount: Long,
  val leaseToken: String,
)

enum class DataAvailabilitySyncTaskPublicationFailureResult {
  RETRY_SCHEDULED,
  DELIVERY_OBSERVED,
}

fun AsyncDatabaseClient.TransactionContext.insertDataAvailabilitySyncTaskPublication(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  taskResourceId: String,
  nextAttemptTime: Instant = Instant.now(),
) {
  bufferInsertMutation("DataAvailabilitySyncTaskPublication") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("DataAvailabilitySyncTaskResourceId").to(taskResourceId)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("ProviderSlot").to(null as Boolean?)
    set("NextAttemptTime").to(nextAttemptTime.toGcloudTimestamp())
    set("AttemptCount").to(0L)
    set("PublishedTime").to(null as com.google.cloud.Timestamp?)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

suspend fun AsyncDatabaseClient.TransactionContext.claimDataAvailabilitySyncTaskPublication(
  leaseToken: String,
  now: Instant,
  leaseExpirationTime: Instant,
): DataAvailabilitySyncTaskPublication? {
  val row: Struct =
    executeQuery(
        statement(
          """
          SELECT
            Publication.DataProviderResourceId,
            Publication.RawImpressionUploadId,
            Publication.DataAvailabilitySyncTaskResourceId,
            RawImpressionUpload.RawImpressionUploadResourceId,
            Task.CmmsModelLine,
            Task.DoneBlobPathHash,
            Task.DoneBlobGeneration,
            Task.State,
            Publication.AttemptCount
          FROM DataAvailabilitySyncTaskPublication@{FORCE_INDEX=DataAvailabilitySyncTaskPublicationByClaimPriority} AS Publication
          JOIN DataAvailabilitySyncTask AS Task USING (
            DataProviderResourceId,
            RawImpressionUploadId,
            DataAvailabilitySyncTaskResourceId
          )
          JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
          WHERE Publication.PublishedTime IS NULL
            AND Publication.NextAttemptTime <= @now
            AND (Publication.LeaseExpirationTime IS NULL OR Publication.LeaseExpirationTime <= @now)
            AND CAST(Task.State AS INT64) IN (@pendingState, @runningState, @failedState)
            AND NOT EXISTS (
              SELECT 1
              FROM DataAvailabilitySyncTaskPublication AS ActivePublication
              WHERE ActivePublication.DataProviderResourceId = Publication.DataProviderResourceId
                AND ActivePublication.ProviderSlot = TRUE
                AND (
                  ActivePublication.RawImpressionUploadId != Publication.RawImpressionUploadId
                  OR ActivePublication.DataAvailabilitySyncTaskResourceId !=
                    Publication.DataAvailabilitySyncTaskResourceId
                )
            )
          ORDER BY Publication.NextAttemptTime, Publication.LeaseExpirationTime,
            Publication.DataProviderResourceId, Publication.RawImpressionUploadId,
            Publication.DataAvailabilitySyncTaskResourceId
          LIMIT 1
          """
            .trimIndent()
        ) {
          bind("now").to(now.toGcloudTimestamp())
          bind("pendingState")
            .to(
              DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING.number
                .toLong()
            )
          bind("runningState")
            .to(
              DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING.number
                .toLong()
            )
          bind("failedState")
            .to(
              DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED.number.toLong()
            )
        },
        Options.tag("action=claimDataAvailabilitySyncTaskPublication"),
      )
      .singleOrNull() ?: return null

  val attemptCount = row.getLong("AttemptCount") + 1L
  bufferUpdateMutation("DataAvailabilitySyncTaskPublication") {
    set("DataProviderResourceId").to(row.getString("DataProviderResourceId"))
    set("RawImpressionUploadId").to(row.getLong("RawImpressionUploadId"))
    set("DataAvailabilitySyncTaskResourceId")
      .to(row.getString("DataAvailabilitySyncTaskResourceId"))
    set("LeaseOwner").to(leaseToken)
    set("LeaseExpirationTime").to(leaseExpirationTime.toGcloudTimestamp())
    set("ProviderSlot").to(true)
    set("AttemptCount").to(attemptCount)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  val taskResourceId = row.getString("DataAvailabilitySyncTaskResourceId")
  val taskKey =
    DataAvailabilitySyncTaskKey(
      row.getString("DataProviderResourceId"),
      row.getString("RawImpressionUploadResourceId"),
      taskResourceId,
    )
  return DataAvailabilitySyncTaskPublication(
    row.getString("DataProviderResourceId"),
    row.getLong("RawImpressionUploadId"),
    taskResourceId,
    taskKey.toName(),
    taskKey.parentKey.toName(),
    row.getString("CmmsModelLine"),
    row.getString("DoneBlobPathHash"),
    row.getLong("DoneBlobGeneration"),
    row.getProtoEnum("State", DataAvailabilitySyncTaskState::forNumber),
    attemptCount,
    leaseToken,
  )
}

suspend fun AsyncDatabaseClient.TransactionContext.completeDataAvailabilitySyncTaskPublication(
  publication: DataAvailabilitySyncTaskPublication
) {
  if (!hasLease(publication)) return
  bufferUpdateMutation("DataAvailabilitySyncTaskPublication") {
    set("DataProviderResourceId").to(publication.dataProviderResourceId)
    set("RawImpressionUploadId").to(publication.rawImpressionUploadId)
    set("DataAvailabilitySyncTaskResourceId").to(publication.taskResourceId)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("PublishedTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

suspend fun AsyncDatabaseClient.TransactionContext.retryDataAvailabilitySyncTaskPublication(
  publication: DataAvailabilitySyncTaskPublication,
  nextAttemptTime: Instant,
): DataAvailabilitySyncTaskPublicationFailureResult {
  if (!hasLease(publication)) {
    return DataAvailabilitySyncTaskPublicationFailureResult.DELIVERY_OBSERVED
  }
  val taskState =
    executeQuery(
        statement(
          """
          SELECT CAST(State AS INT64) AS State
          FROM DataAvailabilitySyncTask
          WHERE DataProviderResourceId = @dataProviderResourceId
            AND RawImpressionUploadId = @rawImpressionUploadId
            AND DataAvailabilitySyncTaskResourceId = @taskResourceId
          """
            .trimIndent()
        ) {
          bind("dataProviderResourceId").to(publication.dataProviderResourceId)
          bind("rawImpressionUploadId").to(publication.rawImpressionUploadId)
          bind("taskResourceId").to(publication.taskResourceId)
        }
      )
      .singleOrNull()
      ?.getLong("State")
  val persistedTaskState =
    checkNotNull(DataAvailabilitySyncTaskState.forNumber(checkNotNull(taskState).toInt()))
  if (
    persistedTaskState == DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING ||
      persistedTaskState ==
        DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUCCEEDED ||
      persistedTaskState ==
        DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUPERSEDED ||
      persistedTaskState ==
        DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_CANCELLED
  ) {
    completeDataAvailabilitySyncTaskPublication(publication)
    return DataAvailabilitySyncTaskPublicationFailureResult.DELIVERY_OBSERVED
  }
  if (
    persistedTaskState == DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING
  ) {
    bufferUpdateMutation("DataAvailabilitySyncTask") {
      set("DataProviderResourceId").to(publication.dataProviderResourceId)
      set("RawImpressionUploadId").to(publication.rawImpressionUploadId)
      set("DataAvailabilitySyncTaskResourceId").to(publication.taskResourceId)
      set("State").to(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED)
      set("FailureCategory")
        .to(
          DataAvailabilitySyncTaskFailureCategory
            .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_PUBLICATION
        )
      set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    }
  }
  bufferUpdateMutation("DataAvailabilitySyncTaskPublication") {
    set("DataProviderResourceId").to(publication.dataProviderResourceId)
    set("RawImpressionUploadId").to(publication.rawImpressionUploadId)
    set("DataAvailabilitySyncTaskResourceId").to(publication.taskResourceId)
    set("LeaseOwner").to(null as String?)
    set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
    set("ProviderSlot").to(null as Boolean?)
    set("NextAttemptTime").to(nextAttemptTime.toGcloudTimestamp())
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return DataAvailabilitySyncTaskPublicationFailureResult.RETRY_SCHEDULED
}

suspend fun AsyncDatabaseClient.TransactionContext.reconcileDataAvailabilitySyncTaskPublications(
  limit: Int,
  now: Instant,
  staleBefore: Instant,
): Int {
  val rows =
    executeQuery(
        statement(
          """
          SELECT Task.DataProviderResourceId, Task.RawImpressionUploadId,
            RawImpressionUpload.RawImpressionUploadResourceId,
            Task.DataAvailabilitySyncTaskResourceId, Task.AttemptCount,
            CAST(Task.State AS INT64) AS TaskState
          FROM DataAvailabilitySyncTask AS Task
          JOIN DataAvailabilitySyncTaskPublication AS Publication USING (
            DataProviderResourceId,
            RawImpressionUploadId,
            DataAvailabilitySyncTaskResourceId
          )
          JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
          WHERE CAST(Task.State AS INT64) IN (@pendingState, @runningState, @failedState)
            AND Publication.PublishedTime IS NOT NULL
            AND Publication.UpdateTime <= @staleBefore
            AND Task.UpdateTime <= @staleBefore
          ORDER BY Task.DataProviderResourceId, Task.RawImpressionUploadId,
            Task.DataAvailabilitySyncTaskResourceId
          LIMIT @limit
          """
            .trimIndent()
        ) {
          bind("pendingState")
            .to(
              DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING.number
                .toLong()
            )
          bind("runningState")
            .to(
              DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING.number
                .toLong()
            )
          bind("failedState")
            .to(
              DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED.number.toLong()
            )
          bind("staleBefore").to(staleBefore.toGcloudTimestamp())
          bind("limit").to(limit.toLong())
        },
        Options.tag("action=reconcileDataAvailabilitySyncTaskPublications"),
      )
      .toList()
  for (row in rows) {
    if (
      row.getLong("TaskState") ==
        DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING.number.toLong()
    ) {
      val taskKey =
        DataAvailabilitySyncTaskKey(
          row.getString("DataProviderResourceId"),
          row.getString("RawImpressionUploadResourceId"),
          row.getString("DataAvailabilitySyncTaskResourceId"),
        )
      if (
        isDataAvailabilitySyncLeaseActive(
          taskKey.dataProviderId,
          DataAvailabilitySyncTaskAttempts.leaseId(
            taskKey.toName(),
            row.getLong("AttemptCount").toInt(),
          ),
          now,
        )
      ) {
        continue
      }
      bufferUpdateMutation("DataAvailabilitySyncTask") {
        set("DataProviderResourceId").to(row.getString("DataProviderResourceId"))
        set("RawImpressionUploadId").to(row.getLong("RawImpressionUploadId"))
        set("DataAvailabilitySyncTaskResourceId")
          .to(row.getString("DataAvailabilitySyncTaskResourceId"))
        set("State").to(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED)
        set("FailureCategory")
          .to(
            DataAvailabilitySyncTaskFailureCategory
              .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_INTERNAL
          )
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }
    bufferUpdateMutation("DataAvailabilitySyncTaskPublication") {
      set("DataProviderResourceId").to(row.getString("DataProviderResourceId"))
      set("RawImpressionUploadId").to(row.getLong("RawImpressionUploadId"))
      set("DataAvailabilitySyncTaskResourceId")
        .to(row.getString("DataAvailabilitySyncTaskResourceId"))
      set("PublishedTime").to(null as com.google.cloud.Timestamp?)
      set("LeaseOwner").to(null as String?)
      set("LeaseExpirationTime").to(null as com.google.cloud.Timestamp?)
      set("ProviderSlot").to(null as Boolean?)
      set("NextAttemptTime").to(now.toGcloudTimestamp())
      set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    }
  }
  return rows.size
}

fun AsyncDatabaseClient.TransactionContext.releaseDataAvailabilitySyncTaskPublicationSlot(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  taskResourceId: String,
) {
  bufferUpdateMutation("DataAvailabilitySyncTaskPublication") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("DataAvailabilitySyncTaskResourceId").to(taskResourceId)
    set("ProviderSlot").to(null as Boolean?)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

suspend fun AsyncDatabaseClient.ReadContext.hasDataAvailabilitySyncTaskPublicationSlot(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  taskResourceId: String,
): Boolean {
  val row =
    readRow(
      "DataAvailabilitySyncTaskPublication",
      Key.of(dataProviderResourceId, rawImpressionUploadId, taskResourceId),
      listOf("ProviderSlot"),
    ) ?: return false
  return !row.isNull("ProviderSlot") && row.getBoolean("ProviderSlot")
}

private suspend fun AsyncDatabaseClient.ReadContext.hasLease(
  publication: DataAvailabilitySyncTaskPublication
): Boolean {
  val row =
    readRow(
      "DataAvailabilitySyncTaskPublication",
      Key.of(
        publication.dataProviderResourceId,
        publication.rawImpressionUploadId,
        publication.taskResourceId,
      ),
      listOf("LeaseOwner"),
    ) ?: return false
  return !row.isNull("LeaseOwner") && row.getString("LeaseOwner") == publication.leaseToken
}
