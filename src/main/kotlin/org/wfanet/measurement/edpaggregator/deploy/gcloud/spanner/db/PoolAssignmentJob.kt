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
import com.google.protobuf.Timestamp
import com.google.type.Date
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
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek as InternalEncryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState as State
import org.wfanet.measurement.internal.edpaggregator.poolAssignmentJob

data class PoolAssignmentJobResult(
  val poolAssignmentJob: PoolAssignmentJob,
  val rawImpressionUploadId: Long,
  val poolAssignmentJobId: Long,
  val markSucceededRequestId: String,
  val markFailedRequestId: String,
  val lastShard: Boolean?,
  val lastShardPoolOffsets: List<Long>,
  val lastShardMaxEventDate: Date?,
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
 * Counts SUCCEEDED [PoolAssignmentJob] sibling rows (excluding [poolAssignmentJobId]) in the given
 * (upload, model line) group whose `UpdateTime` is strictly later than [updateTime].
 *
 * Used by MarkSucceeded replay to reconstruct whether a shard was the original last to reach
 * SUCCEEDED: it was iff no sibling transitioned to SUCCEEDED after it. Commit timestamps are
 * strictly ordered because every mark writes the shared parent model line row, serializing the
 * txns.
 */
suspend fun AsyncDatabaseClient.ReadContext.countPoolAssignmentJobSiblingsSucceededAfter(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
  poolAssignmentJobId: Long,
  updateTime: Timestamp,
): Long {
  val sql =
    """
    SELECT COUNT(*) AS cnt
    FROM PoolAssignmentJob
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND CmmsModelLine = @cmmsModelLine
      AND PoolAssignmentJobId != @poolAssignmentJobId
      AND CAST(State AS INT64) = @succeededState
      AND UpdateTime > @updateTime
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
          bind("poolAssignmentJobId").to(poolAssignmentJobId)
          bind("succeededState").to(State.POOL_ASSIGNMENT_STATE_SUCCEEDED.number.toLong())
          bind("updateTime").to(updateTime.toGcloudTimestamp())
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
data class RawImpressionUploadModelLineMergeState(
  val rawImpressionUploadModelLineId: Long,
  val poolOffsets: List<Long>,
  val maxEventDate: Date?,
  val encryptedMergedDek: InternalEncryptedDek?,
  val poolAssignmentComplete: Boolean,
  val outputKekUri: String,
)

/**
 * Reads the parent [RawImpressionUploadModelLine]'s merge state — its surrogate ID, the pool
 * offsets unioned so far, and the max event date reduced so far — for the given (upload, model
 * line).
 *
 * @return the merge state, or `null` if the model line row does not exist
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadModelLineMergeState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): RawImpressionUploadModelLineMergeState? {
  val sql =
    """
    SELECT RawImpressionUploadModelLineId, PoolOffsets, MaxEventDate,
      EncryptedMergedDek, PoolAssignmentComplete, OutputKekUri
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

  return RawImpressionUploadModelLineMergeState(
    rawImpressionUploadModelLineId = row.getLong("RawImpressionUploadModelLineId"),
    poolOffsets = if (row.isNull("PoolOffsets")) emptyList() else row.getLongList("PoolOffsets"),
    maxEventDate =
      if (row.isNull("MaxEventDate")) null else row.getDate("MaxEventDate").toProtoDate(),
    encryptedMergedDek =
      if (row.isNull("EncryptedMergedDek")) null
      else row.getProtoMessage("EncryptedMergedDek", InternalEncryptedDek.getDefaultInstance()),
    poolAssignmentComplete = row.getBoolean("PoolAssignmentComplete"),
    outputKekUri = if (row.isNull("OutputKekUri")) "" else row.getString("OutputKekUri"),
  )
}

/** Buffers accumulated Phase-0 outputs and, on final completion, its independent metadata. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionUploadModelLineMergedOutputs(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
  poolOffsets: List<Long>,
  maxEventDate: Date?,
  poolAssignmentComplete: Boolean,
  outputKekUri: String,
  mergedDek: InternalEncryptedDek?,
  preserveUpdateTime: Boolean = false,
) {
  bufferUpdateMutation("RawImpressionUploadModelLine") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RawImpressionUploadModelLineId").to(rawImpressionUploadModelLineId)
    set("PoolOffsets").toInt64Array(poolOffsets)
    set("MaxEventDate").to(maxEventDate?.toCloudDate())
    if (poolAssignmentComplete) {
      set("PoolAssignmentComplete").to(true)
      set("OutputKekUri").to(outputKekUri)
      if (mergedDek != null) {
        set("EncryptedMergedDek").toProtoBytes(mergedDek)
      } else {
        set("EncryptedMergedDek").to(null as com.google.cloud.ByteArray?)
      }
    }
    if (!preserveUpdateTime) set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Materializes one completed legacy group before its metadata or membership can change. Legacy
 * completion commits are uniquely ordered because every mark writes the shared parent.
 */
suspend fun AsyncDatabaseClient.TransactionContext.materializeLegacyPoolAssignmentCompletion(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): RawImpressionUploadModelLineMergeState? {
  val mergeState =
    getRawImpressionUploadModelLineMergeState(
      dataProviderResourceId,
      rawImpressionUploadId,
      cmmsModelLine,
    ) ?: return null
  val legacyDek = mergeState.encryptedMergedDek ?: return null
  if (
    countNonSucceededPoolAssignmentJobs(
      dataProviderResourceId,
      rawImpressionUploadId,
      cmmsModelLine,
    ) != 0L
  )
    return null
  val row =
    executeQuery(
        statement(
          """
    ${PoolAssignmentJobEntity.BASE_SQL}
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    WHERE PoolAssignmentJob.DataProviderResourceId = @provider
      AND PoolAssignmentJob.RawImpressionUploadId = @upload
      AND PoolAssignmentJob.CmmsModelLine = @model
    ORDER BY PoolAssignmentJob.UpdateTime DESC
    LIMIT 1
    """
            .trimIndent()
        ) {
          bind("provider").to(dataProviderResourceId)
          bind("upload").to(rawImpressionUploadId)
          bind("model").to(cmmsModelLine)
        }
      )
      .singleOrNullIfEmpty() ?: return null
  val finalizer = PoolAssignmentJobEntity.buildResult(row)
  val needsSnapshot = finalizer.lastShard == null
  val needsParent = !mergeState.poolAssignmentComplete || mergeState.poolOffsets.isEmpty()
  if (!needsSnapshot && !needsParent) return null
  if (needsSnapshot)
    persistPoolAssignmentJobCompletion(
      dataProviderResourceId,
      rawImpressionUploadId,
      finalizer.poolAssignmentJobId,
      lastShard = true,
      poolOffsets = mergeState.poolOffsets,
      maxEventDate = mergeState.maxEventDate,
    )
  val mergedDek = if (mergeState.poolOffsets.isNotEmpty()) legacyDek else null
  val outputKekUri = mergeState.outputKekUri.ifEmpty { legacyDek.kekUri }
  if (needsParent)
    updateRawImpressionUploadModelLineMergedOutputs(
      dataProviderResourceId,
      rawImpressionUploadId,
      mergeState.rawImpressionUploadModelLineId,
      mergeState.poolOffsets,
      mergeState.maxEventDate,
      poolAssignmentComplete = true,
      outputKekUri = outputKekUri,
      mergedDek = mergedDek,
      preserveUpdateTime = true,
    )
  return mergeState.copy(
    encryptedMergedDek = mergedDek,
    poolAssignmentComplete = true,
    outputKekUri = outputKekUri,
  )
}

/** Reads an existing output key for a final shard with no shard blobs of its own. */
suspend fun AsyncDatabaseClient.ReadContext.getSucceededPoolAssignmentJobDek(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): InternalEncryptedDek? {
  val row =
    executeQuery(
        statement(
          """
      SELECT EncryptedDek
      FROM PoolAssignmentJob
      WHERE DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUploadId = @rawImpressionUploadId
        AND CmmsModelLine = @cmmsModelLine
        AND CAST(State AS INT64) = @succeededState
        AND EncryptedDek IS NOT NULL
      ORDER BY ShardIndex
      LIMIT 1
      """
            .trimIndent()
        ) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
          bind("succeededState").to(State.POOL_ASSIGNMENT_STATE_SUCCEEDED.number.toLong())
        }
      )
      .singleOrNullIfEmpty() ?: return null
  return row.getProtoMessage("EncryptedDek", InternalEncryptedDek.getDefaultInstance())
}

/** Stores replay data for a successful mark written by an older metadata service. */
fun AsyncDatabaseClient.TransactionContext.persistPoolAssignmentJobCompletion(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  poolAssignmentJobId: Long,
  lastShard: Boolean,
  poolOffsets: List<Long>,
  maxEventDate: Date?,
) {
  bufferUpdateMutation("PoolAssignmentJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("PoolAssignmentJobId").to(poolAssignmentJobId)
    set("LastShard").to(lastShard)
    set("LastShardPoolOffsets").toInt64Array(poolOffsets)
    set("LastShardMaxEventDate").to(maxEventDate?.toCloudDate())
  }
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
) {
  bufferInsertMutation("PoolAssignmentJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("PoolAssignmentJobId").to(poolAssignmentJobId)
    set("PoolAssignmentJobResourceId").to(poolAssignmentJobResourceId)
    set("CmmsModelLine").to(cmmsModelLine)
    set("ShardIndex").to(shardIndex)
    // request_id is REQUIRED on Create (enforced by the service), so set it unconditionally.
    set("CreateRequestId").to(createRequestId)
    set("State").to(State.POOL_ASSIGNMENT_STATE_CREATED)
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
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("PoolAssignmentJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("PoolAssignmentJobId").to(poolAssignmentJobId)
    set("State").to(state)
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
      PoolAssignmentJob.MarkSucceededRequestId,
      PoolAssignmentJob.MarkFailedRequestId,
      PoolAssignmentJob.LastShard,
      PoolAssignmentJob.LastShardPoolOffsets,
      PoolAssignmentJob.LastShardMaxEventDate,
      PoolAssignmentJob.State,
      PoolAssignmentJob.ErrorMessage,
      PoolAssignmentJob.EncryptedDek,
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
          encryptedDek =
            struct.getProtoMessage("EncryptedDek", InternalEncryptedDek.getDefaultInstance())
        }
        etag = ETags.computeETag(updateTime.toInstant())
      },
      struct.getLong("RawImpressionUploadId"),
      struct.getLong("PoolAssignmentJobId"),
      if (struct.isNull("MarkSucceededRequestId")) ""
      else struct.getString("MarkSucceededRequestId"),
      if (struct.isNull("MarkFailedRequestId")) "" else struct.getString("MarkFailedRequestId"),
      if (struct.isNull("LastShard")) null else struct.getBoolean("LastShard"),
      if (struct.isNull("LastShardPoolOffsets")) emptyList()
      else struct.getLongList("LastShardPoolOffsets"),
      if (struct.isNull("LastShardMaxEventDate")) null
      else struct.getDate("LastShardMaxEventDate").toProtoDate(),
    )
  }
}
