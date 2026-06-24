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
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState as State
import org.wfanet.measurement.internal.edpaggregator.poolAssignmentJob

data class PoolAssignmentJobResult(
  val poolAssignmentJob: PoolAssignmentJob,
  val rawImpressionUploadId: Long,
  val poolAssignmentJobId: Long,
  val markRequestId: String,
)

/** Returns whether a [PoolAssignmentJob] with the specified keys exists. */
suspend fun AsyncDatabaseClient.ReadContext.poolAssignmentJobExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  poolAssignmentJobId: Long,
): Boolean {
  return readRow(
    "PoolAssignmentJob",
    Key.of(dataProviderResourceId, rawImpressionUploadId, poolAssignmentJobId),
    listOf("PoolAssignmentJobId"),
  ) != null
}

/**
 * Resolves a [RawImpressionUpload]'s internal ID from its resource ID.
 *
 * @return The internal `RawImpressionUploadId`, or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadIdForPoolAssignment(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
): Long? {
  val sql =
    """
    SELECT RawImpressionUploadId
    FROM RawImpressionUpload
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadResourceId = @rawImpressionUploadResourceId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return row.getLong("RawImpressionUploadId")
}

/**
 * Reads a [PoolAssignmentJob] by its resource ID.
 *
 * @return The [PoolAssignmentJobResult], or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getPoolAssignmentJobByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  poolAssignmentJobResourceId: String,
): PoolAssignmentJobResult? {
  val sql = buildString {
    appendLine(PoolAssignmentJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE PoolAssignmentJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND PoolAssignmentJob.PoolAssignmentJobResourceId = @poolAssignmentJobResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("poolAssignmentJobResourceId").to(poolAssignmentJobResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return PoolAssignmentJobEntity.buildResult(row)
}

/** Finds an existing [PoolAssignmentJob] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findPoolAssignmentJobByRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): PoolAssignmentJobResult? {
  if (requestId.isEmpty()) return null

  val sql = buildString {
    appendLine(PoolAssignmentJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE PoolAssignmentJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND PoolAssignmentJob.CreateRequestId = @createRequestId
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

  return PoolAssignmentJobEntity.buildResult(row)
}

/** Finds existing [PoolAssignmentJob] entries by request IDs for batch idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findPoolAssignmentJobsByRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestIds: List<String>,
): Map<String, PoolAssignmentJobResult> {
  if (requestIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(PoolAssignmentJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE PoolAssignmentJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND PoolAssignmentJob.CreateRequestId IN UNNEST(@createRequestIds)
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
        Options.tag("action=findPoolAssignmentJobsByRequestIds"),
      )
      .collect { row ->
        val result = PoolAssignmentJobEntity.buildResult(row)
        if (!row.isNull("CreateRequestId")) {
          put(row.getString("CreateRequestId"), result)
        }
      }
  }
}

/** Reads [PoolAssignmentJob] entries with filtering and pagination. */
fun AsyncDatabaseClient.ReadContext.readPoolAssignmentJobs(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListPoolAssignmentJobsRequest.Filter?,
  limit: Int,
  after: ListPoolAssignmentJobsPageToken.After? = null,
): Flow<PoolAssignmentJobResult> {
  val sql = buildString {
    appendLine(PoolAssignmentJobEntity.BASE_SQL)
    appendLine("JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)")

    val conjuncts =
      mutableListOf("PoolAssignmentJob.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId != null) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    if (filter != null) {
      if (filter.stateInList.isNotEmpty()) {
        conjuncts.add("CAST(PoolAssignmentJob.State AS INT64) IN UNNEST(@state_in)")
      }
      if (filter.hasCreateTimeIn()) {
        if (filter.createTimeIn.hasStartTime()) {
          conjuncts.add("PoolAssignmentJob.CreateTime >= @createTimeStart")
        }
        if (filter.createTimeIn.hasEndTime()) {
          conjuncts.add("PoolAssignmentJob.CreateTime < @createTimeEnd")
        }
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts.add("PoolAssignmentJob.CmmsModelLine = @cmmsModelLine")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(PoolAssignmentJob.CreateTime > @afterCreateTime OR " +
          "(PoolAssignmentJob.CreateTime = @afterCreateTime AND " +
          "PoolAssignmentJob.PoolAssignmentJobResourceId > @afterPoolAssignmentJobResourceId))"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine(
      "ORDER BY PoolAssignmentJob.CreateTime ASC, " +
        "PoolAssignmentJob.PoolAssignmentJobResourceId ASC"
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
        if (filter.stateInList.isNotEmpty()) {
          bind("state_in").toInt64Array(filter.stateInList.map { it.number.toLong() })
        }
        if (filter.hasCreateTimeIn()) {
          if (filter.createTimeIn.hasStartTime()) {
            bind("createTimeStart").to(filter.createTimeIn.startTime.toGcloudTimestamp())
          }
          if (filter.createTimeIn.hasEndTime()) {
            bind("createTimeEnd").to(filter.createTimeIn.endTime.toGcloudTimestamp())
          }
        }
        if (filter.cmmsModelLine.isNotEmpty()) {
          bind("cmmsModelLine").to(filter.cmmsModelLine)
        }
      }

      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterPoolAssignmentJobResourceId").to(after.poolAssignmentJobResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readPoolAssignmentJobs")).map { row ->
    PoolAssignmentJobEntity.buildResult(row)
  }
}

/**
 * Counts the number of non-SUCCEEDED [PoolAssignmentJob] rows for the given (upload, model line)
 * group.
 *
 * Used by MarkSucceeded to detect the "last shard" scenario.
 */
suspend fun AsyncDatabaseClient.ReadContext.countNonSucceededPoolAssignmentJobs(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): Long {
  val sql =
    """
    SELECT COUNT(*) AS cnt
    FROM PoolAssignmentJob
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND CmmsModelLine = @cmmsModelLine
      AND CAST(State AS INT64) != @succeededState
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
          bind("succeededState").to(State.POOL_ASSIGNMENT_STATE_SUCCEEDED.number.toLong())
        }
      )
      .singleOrNullIfEmpty() ?: return 0

  return row.getLong("cnt")
}

/**
 * Reads the `PoolOffsets` of a [RawImpressionUploadModelLine] for the given (upload, model line).
 *
 * Used by MarkSucceeded to populate the last-shard pool offsets that trigger Phase 1.
 *
 * @return the pool offsets (empty if the column is null), or `null` if the row does not exist
 */
suspend fun AsyncDatabaseClient.ReadContext.getPoolOffsetsForModelLine(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): List<Long>? {
  val sql =
    """
    SELECT PoolOffsets
    FROM RawImpressionUploadModelLine
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND CmmsModelLine = @cmmsModelLine
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return if (row.isNull("PoolOffsets")) emptyList() else row.getLongList("PoolOffsets")
}

/** Buffers an insert mutation for a [PoolAssignmentJob] row. */
fun AsyncDatabaseClient.TransactionContext.insertPoolAssignmentJob(
  rawImpressionUploadId: Long,
  poolAssignmentJobId: Long,
  poolAssignmentJobResourceId: String,
  dataProviderResourceId: String,
  cmmsModelLine: String,
  shardIndex: Long,
  createRequestId: String,
  etag: String,
) {
  bufferInsertMutation("PoolAssignmentJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("PoolAssignmentJobId").to(poolAssignmentJobId)
    set("PoolAssignmentJobResourceId").to(poolAssignmentJobResourceId)
    set("CmmsModelLine").to(cmmsModelLine)
    set("ShardIndex").to(shardIndex)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("State").to(State.POOL_ASSIGNMENT_STATE_CREATED)
    set("Etag").to(etag)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update to a [PoolAssignmentJob] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updatePoolAssignmentJobState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  poolAssignmentJobId: Long,
  state: State,
  etag: String,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("PoolAssignmentJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("PoolAssignmentJobId").to(poolAssignmentJobId)
    set("State").to(state)
    set("Etag").to(etag)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (block != null) {
      block()
    }
  }
}

private object PoolAssignmentJobEntity {
  val BASE_SQL =
    """
    SELECT
      PoolAssignmentJob.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      PoolAssignmentJob.RawImpressionUploadId,
      PoolAssignmentJob.PoolAssignmentJobId,
      PoolAssignmentJob.PoolAssignmentJobResourceId,
      PoolAssignmentJob.CmmsModelLine,
      PoolAssignmentJob.ShardIndex,
      PoolAssignmentJob.CreateRequestId,
      PoolAssignmentJob.MarkRequestId,
      PoolAssignmentJob.State,
      PoolAssignmentJob.ErrorMessage,
      PoolAssignmentJob.EncryptedDek,
      PoolAssignmentJob.Etag,
      PoolAssignmentJob.CreateTime,
      PoolAssignmentJob.UpdateTime,
    FROM
      PoolAssignmentJob
    """
      .trimIndent()

  fun buildResult(struct: Struct): PoolAssignmentJobResult {
    return PoolAssignmentJobResult(
      poolAssignmentJob {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
        poolAssignmentJobResourceId = struct.getString("PoolAssignmentJobResourceId")
        cmmsModelLine = struct.getString("CmmsModelLine")
        shardIndex = struct.getLong("ShardIndex").toInt()
        state = struct.getProtoEnum("State", State::forNumber)
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        if (!struct.isNull("ErrorMessage")) {
          errorMessage = struct.getString("ErrorMessage")
        }
        if (!struct.isNull("EncryptedDek")) {
          encryptedDek = struct.getProtoMessage("EncryptedDek", EncryptedDek.getDefaultInstance())
        }
        etag = struct.getString("Etag")
      },
      struct.getLong("RawImpressionUploadId"),
      struct.getLong("PoolAssignmentJobId"),
      if (struct.isNull("MarkRequestId")) "" else struct.getString("MarkRequestId"),
    )
  }
}
