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
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsRequest
import org.wfanet.measurement.internal.edpaggregator.RankerJob
import org.wfanet.measurement.internal.edpaggregator.RankerState as State
import org.wfanet.measurement.internal.edpaggregator.rankerJob

/**
 * Result of reading a [RankerJob] row, exposing internal identifiers and the [markRequestId] that
 * are needed by the service layer but not part of the public resource.
 */
data class RankerJobResult(
  val rankerJob: RankerJob,
  val rawImpressionUploadId: Long,
  val rankerJobId: Long,
  val markRequestId: String,
)

/** Returns whether a [RankerJob] with the specified keys exists. */
suspend fun AsyncDatabaseClient.ReadContext.rankerJobExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rankerJobId: Long,
): Boolean {
  return readRow(
    "RankerJob",
    Key.of(dataProviderResourceId, rawImpressionUploadId, rankerJobId),
    listOf("RankerJobId"),
  ) != null
}

/**
 * Resolves a [RawImpressionUpload]'s internal ID from its resource ID.
 *
 * @return The internal `RawImpressionUploadId`, or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadIdForRanker(
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
 * Reads a [RankerJob] by its resource ID.
 *
 * @return The [RankerJobResult], or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getRankerJobByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  rankerJobResourceId: String,
): RankerJobResult? {
  val sql = buildString {
    appendLine(RankerJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RankerJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankerJob.RankerJobResourceId = @rankerJobResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("rankerJobResourceId").to(rankerJobResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return RankerJobEntity.buildResult(row)
}

/** Finds an existing [RankerJob] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRankerJobByRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): RankerJobResult? {
  if (requestId.isEmpty()) return null

  val sql = buildString {
    appendLine(RankerJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RankerJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankerJob.CreateRequestId = @createRequestId
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

  return RankerJobEntity.buildResult(row)
}

/** Finds existing [RankerJob] entries by request IDs for batch idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRankerJobsByRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestIds: List<String>,
): Map<String, RankerJobResult> {
  if (requestIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(RankerJobEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RankerJob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankerJob.CreateRequestId IN UNNEST(@createRequestIds)
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
        Options.tag("action=findRankerJobsByRequestIds"),
      )
      .collect { row ->
        val result = RankerJobEntity.buildResult(row)
        if (!row.isNull("CreateRequestId")) {
          put(row.getString("CreateRequestId"), result)
        }
      }
  }
}

/** Reads [RankerJob] entries for the same (upload, model line) group. */
suspend fun AsyncDatabaseClient.ReadContext.readRankerJobsForModelLine(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): List<RankerJobResult> {
  val sql =
    """
    SELECT
      RankerJob.RankerJobId,
      RankerJob.State,
    FROM RankerJob
    WHERE RankerJob.DataProviderResourceId = @dataProviderResourceId
      AND RankerJob.RawImpressionUploadId = @rawImpressionUploadId
      AND RankerJob.CmmsModelLine = @cmmsModelLine
    """
      .trimIndent()

  return buildList {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
        },
        Options.tag("action=readRankerJobsForModelLine"),
      )
      .collect { row ->
        add(
          RankerJobResult(
            rankerJob { state = row.getProtoEnum("State", State::forNumber) },
            rawImpressionUploadId,
            row.getLong("RankerJobId"),
            "",
          )
        )
      }
  }
}

/** Reads [RankerJob] entries with filtering and pagination. */
fun AsyncDatabaseClient.ReadContext.readRankerJobs(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListRankerJobsRequest.Filter?,
  limit: Int,
  after: ListRankerJobsPageToken.After? = null,
): Flow<RankerJobResult> {
  val sql = buildString {
    appendLine(RankerJobEntity.BASE_SQL)
    appendLine("JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)")

    val conjuncts = mutableListOf("RankerJob.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId != null) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    if (filter != null) {
      if (filter.stateInList.isNotEmpty()) {
        conjuncts.add("CAST(RankerJob.State AS INT64) IN UNNEST(@state_in)")
      }
      if (filter.hasCreateTimeIn()) {
        if (filter.createTimeIn.hasStartTime()) {
          conjuncts.add("RankerJob.CreateTime >= @createTimeStart")
        }
        if (filter.createTimeIn.hasEndTime()) {
          conjuncts.add("RankerJob.CreateTime < @createTimeEnd")
        }
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts.add("RankerJob.CmmsModelLine = @cmmsModelLine")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(RankerJob.CreateTime > @afterCreateTime OR " +
          "(RankerJob.CreateTime = @afterCreateTime AND " +
          "RankerJob.RankerJobResourceId > @afterRankerJobResourceId))"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY RankerJob.CreateTime ASC, RankerJob.RankerJobResourceId ASC")
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
        bind("afterRankerJobResourceId").to(after.rankerJobResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRankerJobs")).map { row ->
    RankerJobEntity.buildResult(row)
  }
}

/** Buffers an insert mutation for a [RankerJob] row. */
fun AsyncDatabaseClient.TransactionContext.insertRankerJob(
  rawImpressionUploadId: Long,
  rankerJobId: Long,
  rankerJobResourceId: String,
  dataProviderResourceId: String,
  cmmsModelLine: String,
  poolOffsets: List<Long>,
  createRequestId: String,
  etag: String,
) {
  bufferInsertMutation("RankerJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RankerJobId").to(rankerJobId)
    set("RankerJobResourceId").to(rankerJobResourceId)
    set("CmmsModelLine").to(cmmsModelLine)
    set("PoolOffsets").toInt64Array(poolOffsets)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("State").to(State.RANKER_STATE_CREATED)
    set("Etag").to(etag)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update to a [RankerJob] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updateRankerJobState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rankerJobId: Long,
  state: State,
  etag: String,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("RankerJob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RankerJobId").to(rankerJobId)
    set("State").to(state)
    set("Etag").to(etag)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (block != null) {
      block()
    }
  }
}

private object RankerJobEntity {
  val BASE_SQL =
    """
    SELECT
      RankerJob.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      RankerJob.RawImpressionUploadId,
      RankerJob.RankerJobId,
      RankerJob.RankerJobResourceId,
      RankerJob.CmmsModelLine,
      RankerJob.PoolOffsets,
      RankerJob.CreateRequestId,
      RankerJob.MarkRequestId,
      RankerJob.State,
      RankerJob.ErrorMessage,
      RankerJob.Etag,
      RankerJob.CreateTime,
      RankerJob.UpdateTime,
    FROM
      RankerJob
    """
      .trimIndent()

  fun buildResult(struct: Struct): RankerJobResult {
    return RankerJobResult(
      rankerJob {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
        rankerJobResourceId = struct.getString("RankerJobResourceId")
        cmmsModelLine = struct.getString("CmmsModelLine")
        poolOffsets += struct.getLongList("PoolOffsets")
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
      struct.getLong("RankerJobId"),
      if (struct.isNull("MarkRequestId")) "" else struct.getString("MarkRequestId"),
    )
  }
}
