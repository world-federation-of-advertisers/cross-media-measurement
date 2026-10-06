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

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksPageToken
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksRequest
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTask

data class DataAvailabilitySyncTaskResult(
  val task: DataAvailabilitySyncTask,
  val rawImpressionUploadId: Long,
  val createRequestId: String,
)

suspend fun AsyncDatabaseClient.ReadContext.getDataAvailabilitySyncTaskByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  taskResourceId: String,
): DataAvailabilitySyncTaskResult? {
  return querySingle(
    """
    ${DataAvailabilitySyncTaskEntity.BASE_SQL}
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    WHERE DataAvailabilitySyncTask.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
      AND DataAvailabilitySyncTask.DataAvailabilitySyncTaskResourceId = @taskResourceId
    """,
    dataProviderResourceId,
    rawImpressionUploadResourceId,
    taskResourceId = taskResourceId,
  )
}

suspend fun AsyncDatabaseClient.ReadContext.findDataAvailabilitySyncTaskByCreateRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): DataAvailabilitySyncTaskResult? {
  if (requestId.isEmpty()) return null
  val sql =
    """
    ${DataAvailabilitySyncTaskEntity.BASE_SQL}
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    WHERE DataAvailabilitySyncTask.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
      AND DataAvailabilitySyncTask.CreateRequestId = @requestId
    """
      .trimIndent()
  val row =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("requestId").to(requestId)
        }
      )
      .singleOrNullIfEmpty() ?: return null
  return DataAvailabilitySyncTaskEntity.buildResult(row)
}

suspend fun AsyncDatabaseClient.ReadContext.findDataAvailabilitySyncTaskByDoneObject(
  dataProviderResourceId: String,
  doneBlobPathHash: String,
  doneBlobGeneration: Long,
): DataAvailabilitySyncTaskResult? {
  val sql =
    """
    ${DataAvailabilitySyncTaskEntity.BASE_SQL}
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    WHERE DataAvailabilitySyncTask.DataProviderResourceId = @dataProviderResourceId
      AND DataAvailabilitySyncTask.DoneBlobPathHash = @doneBlobPathHash
      AND DataAvailabilitySyncTask.DoneBlobGeneration = @doneBlobGeneration
    """
      .trimIndent()
  val row =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("doneBlobPathHash").to(doneBlobPathHash)
          bind("doneBlobGeneration").to(doneBlobGeneration)
        }
      )
      .singleOrNullIfEmpty() ?: return null
  return DataAvailabilitySyncTaskEntity.buildResult(row)
}

fun AsyncDatabaseClient.ReadContext.readDataAvailabilitySyncTasks(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListDataAvailabilitySyncTasksRequest.Filter?,
  limit: Int,
  after: ListDataAvailabilitySyncTasksPageToken.After? = null,
): Flow<DataAvailabilitySyncTaskResult> {
  val sql = buildString {
    appendLine(DataAvailabilitySyncTaskEntity.BASE_SQL)
    appendLine("JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)")
    val conjuncts =
      mutableListOf("DataAvailabilitySyncTask.DataProviderResourceId = @dataProviderResourceId")
    if (rawImpressionUploadResourceId != null) {
      conjuncts +=
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
    }
    if (filter != null) {
      if (
        filter.state != DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_UNSPECIFIED
      ) {
        conjuncts += "CAST(DataAvailabilitySyncTask.State AS INT64) = @state"
      }
      if (filter.hasCreateTimeInterval()) {
        if (filter.createTimeInterval.hasStartTime()) {
          conjuncts += "DataAvailabilitySyncTask.CreateTime >= @createTimeStart"
        }
        if (filter.createTimeInterval.hasEndTime()) {
          conjuncts += "DataAvailabilitySyncTask.CreateTime < @createTimeEnd"
        }
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts += "DataAvailabilitySyncTask.CmmsModelLine = @cmmsModelLine"
      }
    }
    if (after != null) {
      conjuncts +=
        "(DataAvailabilitySyncTask.CreateTime > @afterCreateTime OR " +
          "(DataAvailabilitySyncTask.CreateTime = @afterCreateTime AND " +
          "DataAvailabilitySyncTask.DataAvailabilitySyncTaskResourceId > @afterTaskResourceId))"
    }
    appendLine("WHERE ${conjuncts.joinToString(" AND ")}")
    appendLine(
      "ORDER BY DataAvailabilitySyncTask.CreateTime, " +
        "DataAvailabilitySyncTask.DataAvailabilitySyncTaskResourceId"
    )
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      if (rawImpressionUploadResourceId != null) {
        bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
      }
      bind("limit").to(limit.toLong())
      if (filter != null) {
        if (
          filter.state !=
            DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_UNSPECIFIED
        ) {
          bind("state").to(filter.state.number.toLong())
        }
        if (filter.hasCreateTimeInterval()) {
          if (filter.createTimeInterval.hasStartTime()) {
            bind("createTimeStart").to(filter.createTimeInterval.startTime.toGcloudTimestamp())
          }
          if (filter.createTimeInterval.hasEndTime()) {
            bind("createTimeEnd").to(filter.createTimeInterval.endTime.toGcloudTimestamp())
          }
        }
        if (filter.cmmsModelLine.isNotEmpty()) bind("cmmsModelLine").to(filter.cmmsModelLine)
      }
      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterTaskResourceId").to(after.dataAvailabilitySyncTaskResourceId)
      }
    }
  return executeQuery(query, Options.tag("action=readDataAvailabilitySyncTasks")).map {
    DataAvailabilitySyncTaskEntity.buildResult(it)
  }
}

fun AsyncDatabaseClient.TransactionContext.insertDataAvailabilitySyncTask(
  rawImpressionUploadId: Long,
  task: DataAvailabilitySyncTask,
  createRequestId: String,
) {
  bufferInsertMutation("DataAvailabilitySyncTask") {
    set("DataProviderResourceId").to(task.dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("DataAvailabilitySyncTaskResourceId").to(task.dataAvailabilitySyncTaskResourceId)
    if (createRequestId.isNotEmpty()) set("CreateRequestId").to(createRequestId)
    set("State").to(task.state)
    set("DoneBlobUri").to(task.doneBlobUri)
    set("DoneBlobPathHash").to(task.doneBlobPathHash)
    set("DoneBlobGeneration").to(task.doneBlobGeneration)
    set("CmmsModelLine").to(task.cmmsModelLine)
    set("EventDate").to(task.eventDate.toCloudDate())
    set("Traceparent").to(task.traceparent.ifEmpty { null })
    set("Tracestate").to(task.tracestate.ifEmpty { null })
    set("AttemptCount").to(task.attemptCount.toLong())
    set("FailureCategory").to(task.failureCategory)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private suspend fun AsyncDatabaseClient.ReadContext.querySingle(
  sql: String,
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  taskResourceId: String,
): DataAvailabilitySyncTaskResult? {
  val row: Struct =
    executeQuery(
        statement(sql.trimIndent()) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("taskResourceId").to(taskResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null
  return DataAvailabilitySyncTaskEntity.buildResult(row)
}

private object DataAvailabilitySyncTaskEntity {
  val BASE_SQL =
    """
    SELECT
      DataAvailabilitySyncTask.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      DataAvailabilitySyncTask.RawImpressionUploadId,
      DataAvailabilitySyncTask.DataAvailabilitySyncTaskResourceId,
      DataAvailabilitySyncTask.CreateRequestId,
      DataAvailabilitySyncTask.State,
      DataAvailabilitySyncTask.DoneBlobUri,
      DataAvailabilitySyncTask.DoneBlobPathHash,
      DataAvailabilitySyncTask.DoneBlobGeneration,
      DataAvailabilitySyncTask.CmmsModelLine,
      DataAvailabilitySyncTask.EventDate,
      DataAvailabilitySyncTask.Traceparent,
      DataAvailabilitySyncTask.Tracestate,
      DataAvailabilitySyncTask.AttemptCount,
      DataAvailabilitySyncTask.FailureCategory,
      DataAvailabilitySyncTask.CreateTime,
      DataAvailabilitySyncTask.UpdateTime,
    FROM DataAvailabilitySyncTask
    """
      .trimIndent()

  fun buildResult(row: Struct): DataAvailabilitySyncTaskResult {
    val task = dataAvailabilitySyncTask {
      dataProviderResourceId = row.getString("DataProviderResourceId")
      rawImpressionUploadResourceId = row.getString("RawImpressionUploadResourceId")
      dataAvailabilitySyncTaskResourceId = row.getString("DataAvailabilitySyncTaskResourceId")
      state = row.getProtoEnum("State", DataAvailabilitySyncTaskState::forNumber)
      doneBlobUri = row.getString("DoneBlobUri")
      doneBlobPathHash = row.getString("DoneBlobPathHash")
      doneBlobGeneration = row.getLong("DoneBlobGeneration")
      cmmsModelLine = row.getString("CmmsModelLine")
      eventDate = row.getDate("EventDate").toProtoDate()
      if (!row.isNull("Traceparent")) traceparent = row.getString("Traceparent")
      if (!row.isNull("Tracestate")) tracestate = row.getString("Tracestate")
      attemptCount = row.getLong("AttemptCount").toInt()
      failureCategory =
        row.getProtoEnum("FailureCategory", DataAvailabilitySyncTaskFailureCategory::forNumber)
      createTime = row.getTimestamp("CreateTime").toProto()
      updateTime = row.getTimestamp("UpdateTime").toProto()
      etag = ETags.computeETag(updateTime.toInstant())
    }
    return DataAvailabilitySyncTaskResult(
      task,
      row.getLong("RawImpressionUploadId"),
      if (row.isNull("CreateRequestId")) "" else row.getString("CreateRequestId"),
    )
  }
}
