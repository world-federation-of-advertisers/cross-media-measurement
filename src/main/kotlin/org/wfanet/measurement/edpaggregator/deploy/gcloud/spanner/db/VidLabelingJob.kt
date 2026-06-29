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
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsRequest
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJob
import org.wfanet.measurement.internal.edpaggregator.VidLabelingState as State
import org.wfanet.measurement.internal.edpaggregator.vidLabelingJob

data class VidLabelingJobResult(
  val vidLabelingJob: VidLabelingJob,
  val rawImpressionUploadId: Long,
  val vidLabelingJobId: Long,
  val markRequestId: String,
)

/** Returns whether a [VidLabelingJob] with the specified keys exists. */
suspend fun AsyncDatabaseClient.ReadContext.vidLabelingJobExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  vidLabelingJobId: Long,
): Boolean {
  return readRow(
    "VidLabelingJob",
    Key.of(dataProviderResourceId, rawImpressionUploadId, vidLabelingJobId),
    listOf("VidLabelingJobId"),
  ) != null
}

/**
 * Reads a [VidLabelingJob] by its resource ID.
 *
 * @return The [VidLabelingJobResult], or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getVidLabelingJobByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  vidLabelingJobResourceId: String,
): VidLabelingJobResult? {
  val sql = buildString {
    appendLine(VidLabelingJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE VidLabelingJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND VidLabelingJob.VidLabelingJobResourceId = @vidLabelingJobResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("vidLabelingJobResourceId").to(vidLabelingJobResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return VidLabelingJobEntity.buildResult(row)
}

/** Finds an existing [VidLabelingJob] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findVidLabelingJobByCreateRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): VidLabelingJobResult? {
  if (requestId.isEmpty()) return null

  val sql = buildString {
    appendLine(VidLabelingJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE VidLabelingJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND VidLabelingJob.CreateRequestId = @createRequestId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("createRequestId").to(requestId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return VidLabelingJobEntity.buildResult(row)
}

/** Finds existing [VidLabelingJob] entries by request IDs for batch idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findVidLabelingJobsByCreateRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestIds: List<String>,
): Map<String, VidLabelingJobResult> {
  if (requestIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(VidLabelingJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE VidLabelingJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND VidLabelingJob.CreateRequestId IN UNNEST(@createRequestIds)
      """
        .trimIndent()
    )
  }

  return buildMap {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("createRequestIds").toStringArray(requestIds)
        },
        Options.tag("action=findVidLabelingJobsByCreateRequestIds"),
      )
      .collect { row ->
        val result = VidLabelingJobEntity.buildResult(row)
        if (!row.isNull("CreateRequestId")) {
          put(row.getString("CreateRequestId"), result)
        }
      }
  }
}

/** Reads [VidLabelingJob] entries with filtering and pagination. */
fun AsyncDatabaseClient.ReadContext.readVidLabelingJobs(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListVidLabelingJobsRequest.Filter?,
  limit: Int,
  after: ListVidLabelingJobsPageToken.After? = null,
): Flow<VidLabelingJobResult> {
  val sql = buildString {
    appendLine(VidLabelingJobEntity.BASE_SQL)
    appendLine("JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)")

    val conjuncts = mutableListOf("VidLabelingJob.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId != null) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    if (filter != null) {
      if (filter.state != State.VID_LABELING_STATE_UNSPECIFIED) {
        conjuncts.add("CAST(VidLabelingJob.State AS INT64) = @state")
      }
      if (filter.hasCreateTimeInterval()) {
        if (filter.createTimeInterval.hasStartTime()) {
          conjuncts.add("VidLabelingJob.CreateTime >= @createTimeStart")
        }
        if (filter.createTimeInterval.hasEndTime()) {
          conjuncts.add("VidLabelingJob.CreateTime < @createTimeEnd")
        }
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts.add("@cmmsModelLine IN UNNEST(VidLabelingJob.CmmsModelLines)")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(VidLabelingJob.CreateTime > @afterCreateTime OR " +
          "(VidLabelingJob.CreateTime = @afterCreateTime AND " +
          "VidLabelingJob.VidLabelingJobResourceId > @afterVidLabelingJobResourceId))"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine(
      "ORDER BY VidLabelingJob.CreateTime ASC, VidLabelingJob.VidLabelingJobResourceId ASC"
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
        if (filter.state != State.VID_LABELING_STATE_UNSPECIFIED) {
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
        if (filter.cmmsModelLine.isNotEmpty()) {
          bind("cmmsModelLine").to(filter.cmmsModelLine)
        }
      }

      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterVidLabelingJobResourceId").to(after.vidLabelingJobResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readVidLabelingJobs")).map { row ->
    VidLabelingJobEntity.buildResult(row)
  }
}

/**
 * Counts the [VidLabelingJob] entries under the same upload that cover [cmmsModelLine] and are not
 * yet SUCCEEDED, excluding [excludeVidLabelingJobId].
 *
 * This detects whether a model line is complete in O(1) per model line, without materializing every
 * job row for the upload. The current job's buffered state update is not visible within the same
 * transaction, so it is excluded by ID rather than relying on its persisted state.
 */
suspend fun AsyncDatabaseClient.ReadContext.countOtherNonSucceededVidLabelingJobsForModelLine(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
  excludeVidLabelingJobId: Long,
): Long {
  val sql =
    """
    SELECT COUNT(*) AS Cnt
    FROM VidLabelingJob
    WHERE VidLabelingJob.DataProviderResourceId = @dataProviderResourceId
      AND VidLabelingJob.RawImpressionUploadId = @rawImpressionUploadId
      AND @cmmsModelLine IN UNNEST(VidLabelingJob.CmmsModelLines)
      AND VidLabelingJob.VidLabelingJobId != @excludeVidLabelingJobId
      AND CAST(VidLabelingJob.State AS INT64) != @succeededState
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
          bind("excludeVidLabelingJobId").to(excludeVidLabelingJobId)
          bind("succeededState").to(State.VID_LABELING_STATE_SUCCEEDED.number.toLong())
        },
        Options.tag("action=countOtherNonSucceededVidLabelingJobsForModelLine"),
      )
      .single()

  return row.getLong("Cnt")
}

/** Buffers an insert mutation for a [VidLabelingJob] row. */
fun AsyncDatabaseClient.TransactionContext.insertVidLabelingJob(
  rawImpressionUploadId: Long,
  vidLabelingJobId: Long,
  vidLabelingJobResourceId: String,
  dataProviderResourceId: String,
  cmmsModelLines: List<String>,
  rawImpressionUploadFiles: List<String>,
  createRequestId: String,
  etag: String,
) {
  bufferInsertMutation("VidLabelingJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("VidLabelingJobId").to(vidLabelingJobId)
    set("VidLabelingJobResourceId").to(vidLabelingJobResourceId)
    set("CmmsModelLines").toStringArray(cmmsModelLines)
    set("RawImpressionUploadFiles").toStringArray(rawImpressionUploadFiles)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("State").to(State.VID_LABELING_STATE_CREATED)
    set("Etag").to(etag)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update to a [VidLabelingJob] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updateVidLabelingJobState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  vidLabelingJobId: Long,
  state: State,
  etag: String,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("VidLabelingJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("VidLabelingJobId").to(vidLabelingJobId)
    set("State").to(state)
    set("Etag").to(etag)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (block != null) {
      block()
    }
  }
}

private object VidLabelingJobEntity {
  val BASE_SQL =
    """
    SELECT
      VidLabelingJob.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      VidLabelingJob.RawImpressionUploadId,
      VidLabelingJob.VidLabelingJobId,
      VidLabelingJob.VidLabelingJobResourceId,
      VidLabelingJob.CmmsModelLines,
      VidLabelingJob.RawImpressionUploadFiles,
      VidLabelingJob.CreateRequestId,
      VidLabelingJob.MarkRequestId,
      VidLabelingJob.State,
      VidLabelingJob.ErrorMessage,
      VidLabelingJob.Etag,
      VidLabelingJob.CreateTime,
      VidLabelingJob.UpdateTime,
    FROM
      VidLabelingJob
    """
      .trimIndent()

  fun buildResult(struct: Struct): VidLabelingJobResult {
    return VidLabelingJobResult(
      vidLabelingJob {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
        vidLabelingJobResourceId = struct.getString("VidLabelingJobResourceId")
        cmmsModelLines += struct.getStringList("CmmsModelLines")
        rawImpressionUploadFiles += struct.getStringList("RawImpressionUploadFiles")
        state = struct.getProtoEnum("State", State::forNumber)
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        if (!struct.isNull("ErrorMessage")) {
          errorMessage = struct.getString("ErrorMessage")
        }
        if (!struct.isNull("Etag")) {
          etag = struct.getString("Etag")
        }
      },
      struct.getLong("RawImpressionUploadId"),
      struct.getLong("VidLabelingJobId"),
      if (struct.isNull("MarkRequestId")) "" else struct.getString("MarkRequestId"),
    )
  }
}
