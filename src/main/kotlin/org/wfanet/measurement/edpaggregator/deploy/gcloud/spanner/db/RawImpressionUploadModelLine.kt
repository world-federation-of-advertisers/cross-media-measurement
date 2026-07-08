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
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState as State
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine

data class RawImpressionUploadModelLineResult(
  val rawImpressionUploadModelLine: RawImpressionUploadModelLine,
  val rawImpressionUploadId: Long,
  val rawImpressionUploadModelLineId: Long,
)

/** Returns whether a [RawImpressionUploadModelLine] with the specified keys exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadModelLineExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
): Boolean {
  return readRow(
    "RawImpressionUploadModelLine",
    Key.of(dataProviderResourceId, rawImpressionUploadId, rawImpressionUploadModelLineId),
    listOf("RawImpressionUploadModelLineId"),
  ) != null
}

/**
 * Returns the parent [RawImpressionUpload]'s current [RawImpressionUploadState], or `null` if the
 * row is missing.
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
): RawImpressionUploadState? {
  val row =
    readRow(
      "RawImpressionUpload",
      Key.of(dataProviderResourceId, rawImpressionUploadId),
      listOf("State"),
    ) ?: return null
  return row.getProtoEnum("State", RawImpressionUploadState::forNumber)
}

/**
 * Counts the [RawImpressionUploadModelLine] rows under the given upload that are not yet COMPLETED,
 * excluding [excludeRawImpressionUploadModelLineId] (its terminal write is buffered and not visible
 * to reads in the same transaction).
 */
suspend fun AsyncDatabaseClient.ReadContext.countNonCompletedRawImpressionUploadModelLines(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  excludeRawImpressionUploadModelLineId: Long,
): Long {
  val sql =
    """
    SELECT COUNT(*) AS NonCompletedCount
    FROM RawImpressionUploadModelLine
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND RawImpressionUploadModelLineId != @excludeRawImpressionUploadModelLineId
      AND CAST(State AS INT64) != @completedState
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("excludeRawImpressionUploadModelLineId").to(excludeRawImpressionUploadModelLineId)
          bind("completedState")
            .to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED.number.toLong())
        }
      )
      .singleOrNullIfEmpty() ?: return 0
  return row.getLong("NonCompletedCount")
}

/** A conflicting in-progress model-line row: the owning upload's resource ID and its state. */
data class InProgressModelLine(val rawImpressionUploadResourceId: String, val state: State)

/**
 * Finds up to [limit] model-line rows for ([dataProviderResourceId], [cmmsModelLine]) that are in a
 * processing state (POOL_ASSIGNING/RANKING/LABELING) under an upload OTHER than
 * [excludeRawImpressionUploadId], returning each conflicting upload's resource ID and state.
 *
 * Used to enforce the one-in-flight-upload-per-(DataProvider, model line) rule and to name the
 * blocking upload(s) when the rule rejects a transition.
 */
suspend fun AsyncDatabaseClient.ReadContext.findInProgressModelLinesForModelLine(
  dataProviderResourceId: String,
  cmmsModelLine: String,
  excludeRawImpressionUploadId: Long,
  limit: Int = 5,
): List<InProgressModelLine> {
  val sql =
    """
    SELECT
      RawImpressionUpload.RawImpressionUploadResourceId AS RawImpressionUploadResourceId,
      RawImpressionUploadModelLine.State AS State
    FROM RawImpressionUploadModelLine
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadModelLine.CmmsModelLine = @cmmsModelLine
      AND RawImpressionUploadModelLine.RawImpressionUploadId != @excludeRawImpressionUploadId
      AND CAST(RawImpressionUploadModelLine.State AS INT64) IN UNNEST(@inProgressStates)
    LIMIT @limit
    """
      .trimIndent()

  return buildList {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("cmmsModelLine").to(cmmsModelLine)
          bind("excludeRawImpressionUploadId").to(excludeRawImpressionUploadId)
          bind("limit").to(limit.toLong())
          bind("inProgressStates")
            .toInt64Array(
              listOf(
                State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING.number.toLong(),
                State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING.number.toLong(),
                State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING.number.toLong(),
              )
            )
        }
      )
      .collect { row ->
        add(
          InProgressModelLine(
            row.getString("RawImpressionUploadResourceId"),
            row.getProtoEnum("State", State::forNumber),
          )
        )
      }
  }
}

/** Buffers an update to the parent [RawImpressionUpload] row's state. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionUploadState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  state: RawImpressionUploadState,
) {
  bufferUpdateMutation("RawImpressionUpload") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Reads a [RawImpressionUploadModelLine] by its composite resource IDs.
 *
 * @return The [RawImpressionUploadModelLineResult], or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadModelLineByResourceIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  rawImpressionUploadModelLineResourceId: String,
): RawImpressionUploadModelLineResult? {
  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RawImpressionUploadModelLine.RawImpressionUploadModelLineResourceId =
          @rawImpressionUploadModelLineResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("rawImpressionUploadModelLineResourceId").to(rawImpressionUploadModelLineResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return RawImpressionUploadModelLineEntity.buildResult(row)
}

/** Finds an existing [RawImpressionUploadModelLine] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRawImpressionUploadModelLineByRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): RawImpressionUploadModelLineResult? {
  if (requestId.isEmpty()) return null

  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RawImpressionUploadModelLine.CreateRequestId = @createRequestId
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

  return RawImpressionUploadModelLineEntity.buildResult(row)
}

/** Finds existing [RawImpressionUploadModelLine] entries by request IDs for batch idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRawImpressionUploadModelLinesByRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestIds: List<String>,
): Map<String, RawImpressionUploadModelLineResult> {
  if (requestIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RawImpressionUploadModelLine.CreateRequestId IN UNNEST(@createRequestIds)
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
        Options.tag("action=findRawImpressionUploadModelLinesByRequestIds"),
      )
      .collect { row ->
        val result = RawImpressionUploadModelLineEntity.buildResult(row)
        if (!row.isNull("CreateRequestId")) {
          put(row.getString("CreateRequestId"), result)
        }
      }
  }
}

/** Reads [RawImpressionUploadModelLine] entries with filtering and pagination. */
fun AsyncDatabaseClient.ReadContext.readRawImpressionUploadModelLines(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListRawImpressionUploadModelLinesRequest.Filter?,
  limit: Int,
  after: ListRawImpressionUploadModelLinesPageToken.After? = null,
): Flow<RawImpressionUploadModelLineResult> {
  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine("JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)")

    val conjuncts =
      mutableListOf("RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId != null) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    if (filter != null) {
      if (filter.stateInList.isNotEmpty()) {
        conjuncts.add("CAST(RawImpressionUploadModelLine.State AS INT64) IN UNNEST(@state_in)")
      }
      if (filter.hasCreateTimeIn()) {
        if (filter.createTimeIn.hasStartTime()) {
          conjuncts.add("RawImpressionUploadModelLine.CreateTime >= @createTimeStart")
        }
        if (filter.createTimeIn.hasEndTime()) {
          conjuncts.add("RawImpressionUploadModelLine.CreateTime < @createTimeEnd")
        }
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts.add("RawImpressionUploadModelLine.CmmsModelLine = @cmmsModelLine")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(RawImpressionUploadModelLine.CreateTime > @afterCreateTime OR (RawImpressionUploadModelLine.CreateTime = @afterCreateTime AND RawImpressionUploadModelLine.CmmsModelLine > @afterCmmsModelLine) OR (RawImpressionUploadModelLine.CreateTime = @afterCreateTime AND RawImpressionUploadModelLine.CmmsModelLine = @afterCmmsModelLine AND RawImpressionUpload.RawImpressionUploadResourceId > @afterRawImpressionUploadResourceId))"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine(
      "ORDER BY RawImpressionUploadModelLine.CreateTime ASC, " +
        "RawImpressionUploadModelLine.CmmsModelLine ASC, " +
        "RawImpressionUpload.RawImpressionUploadResourceId ASC"
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
        bind("afterCmmsModelLine").to(after.cmmsModelLine)
        bind("afterRawImpressionUploadResourceId").to(after.rawImpressionUploadResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRawImpressionUploadModelLines")).map { row ->
    RawImpressionUploadModelLineEntity.buildResult(row)
  }
}

/** Buffers an insert mutation for a [RawImpressionUploadModelLine] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionUploadModelLine(
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
  rawImpressionUploadModelLineResourceId: String,
  dataProviderResourceId: String,
  cmmsModelLine: String,
  createRequestId: String,
) {
  bufferInsertMutation("RawImpressionUploadModelLine") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RawImpressionUploadModelLineId").to(rawImpressionUploadModelLineId)
    set("RawImpressionUploadModelLineResourceId").to(rawImpressionUploadModelLineResourceId)
    set("CmmsModelLine").to(cmmsModelLine)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("State").to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update to a [RawImpressionUploadModelLine] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionUploadModelLineState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
  state: State,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("RawImpressionUploadModelLine") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RawImpressionUploadModelLineId").to(rawImpressionUploadModelLineId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (block != null) {
      block()
    }
  }
}

private object RawImpressionUploadModelLineEntity {
  val BASE_SQL =
    """
    SELECT
      RawImpressionUploadModelLine.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      RawImpressionUploadModelLine.RawImpressionUploadId,
      RawImpressionUploadModelLine.RawImpressionUploadModelLineId,
      RawImpressionUploadModelLine.RawImpressionUploadModelLineResourceId,
      RawImpressionUploadModelLine.CmmsModelLine,
      RawImpressionUploadModelLine.CreateRequestId,
      RawImpressionUploadModelLine.State,
      RawImpressionUploadModelLine.ErrorMessage,
      RawImpressionUploadModelLine.CreateTime,
      RawImpressionUploadModelLine.UpdateTime,
      RawImpressionUploadModelLine.PoolOffsets,
      RawImpressionUploadModelLine.MaxEventDate,
      RawImpressionUploadModelLine.EncryptedMergedDek,
    FROM
      RawImpressionUploadModelLine
    """
      .trimIndent()

  fun buildResult(struct: Struct): RawImpressionUploadModelLineResult {
    return RawImpressionUploadModelLineResult(
      rawImpressionUploadModelLine {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
        rawImpressionUploadModelLineResourceId =
          struct.getString("RawImpressionUploadModelLineResourceId")
        cmmsModelLine = struct.getString("CmmsModelLine")
        state = struct.getProtoEnum("State", State::forNumber)
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        etag = ETags.computeETag(updateTime.toInstant())
        if (!struct.isNull("ErrorMessage")) {
          errorMessage = struct.getString("ErrorMessage")
        }
        // Phase-0 last-shard-out outputs on the parent row, read back by a retrying
        // SubpoolAssigner (see SubpoolAssigner.recoverIfLastShardOut). All three are
        // populated together on last-shard-out; null until then.
        if (!struct.isNull("PoolOffsets")) {
          poolOffsets += struct.getLongList("PoolOffsets")
        }
        if (!struct.isNull("MaxEventDate")) {
          maxEventDate = struct.getDate("MaxEventDate").toProtoDate()
        }
        if (!struct.isNull("EncryptedMergedDek")) {
          encryptedMergedDek =
            struct.getProtoMessage("EncryptedMergedDek", EncryptedDek.getDefaultInstance())
        }
      },
      struct.getLong("RawImpressionUploadId"),
      struct.getLong("RawImpressionUploadModelLineId"),
    )
  }
}
