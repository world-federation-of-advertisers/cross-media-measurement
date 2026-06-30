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

import com.google.cloud.Date
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlob
import org.wfanet.measurement.internal.edpaggregator.rankIndexBlob

/**
 * Result of reading a [RankIndexBlob] row, exposing internal identifiers needed by the service
 * layer but not part of the public resource.
 */
data class RankIndexBlobResult(
  val rankIndexBlob: RankIndexBlob,
  val rawImpressionUploadId: Long,
  val rankIndexBlobId: Long,
)

/** Returns whether a [RankIndexBlob] with the specified keys exists. */
suspend fun AsyncDatabaseClient.ReadContext.rankIndexBlobExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rankIndexBlobId: Long,
): Boolean {
  return readRow(
    "RankIndexBlob",
    Key.of(dataProviderResourceId, rawImpressionUploadId, rankIndexBlobId),
    listOf("RankIndexBlobId"),
  ) != null
}

/**
 * Reads a [RankIndexBlob] by its resource ID.
 *
 * @return The [RankIndexBlobResult], or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getRankIndexBlobByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  rankIndexBlobResourceId: String,
): RankIndexBlobResult? {
  val sql = buildString {
    appendLine(RankIndexBlobEntity.BASE_SQL)
    appendLine(
      """
      WHERE RankIndexBlob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankIndexBlob.RankIndexBlobResourceId = @rankIndexBlobResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("rankIndexBlobResourceId").to(rankIndexBlobResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return RankIndexBlobEntity.buildResult(row)
}

/** Reads multiple [RankIndexBlob] entries by their resource IDs, keyed by resource ID. */
suspend fun AsyncDatabaseClient.ReadContext.getRankIndexBlobsByResourceIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  rankIndexBlobResourceIds: List<String>,
): Map<String, RankIndexBlobResult> {
  if (rankIndexBlobResourceIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(RankIndexBlobEntity.BASE_SQL)
    appendLine(
      """
      WHERE RankIndexBlob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankIndexBlob.RankIndexBlobResourceId IN UNNEST(@rankIndexBlobResourceIds)
      """
        .trimIndent()
    )
  }

  return buildMap {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("rankIndexBlobResourceIds").toStringArray(rankIndexBlobResourceIds)
        },
        Options.tag("action=getRankIndexBlobsByResourceIds"),
      )
      .collect { row ->
        put(row.getString("RankIndexBlobResourceId"), RankIndexBlobEntity.buildResult(row))
      }
  }
}

/**
 * Finds an existing [RankIndexBlob] by request ID for idempotency.
 *
 * Soft-deleted rows are intentionally NOT filtered out: a create replayed with the same
 * `request_id` returns the original row even if it was later soft-deleted, so the response stays
 * stable across retries. In practice this is unreachable because request IDs are fresh per attempt
 * and deletion happens via the retention sweep long after the create completes.
 */
suspend fun AsyncDatabaseClient.ReadContext.findRankIndexBlobByRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): RankIndexBlobResult? {
  if (requestId.isEmpty()) return null

  val sql = buildString {
    appendLine(RankIndexBlobEntity.BASE_SQL)
    appendLine(
      """
      WHERE RankIndexBlob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankIndexBlob.CreateRequestId = @createRequestId
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

  return RankIndexBlobEntity.buildResult(row)
}

/** Finds existing [RankIndexBlob] entries by request IDs for batch idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRankIndexBlobsByRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestIds: List<String>,
): Map<String, RankIndexBlobResult> {
  val nonEmptyRequestIds = requestIds.filter { it.isNotEmpty() }
  if (nonEmptyRequestIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(RankIndexBlobEntity.BASE_SQL)
    appendLine(
      """
      WHERE RankIndexBlob.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RankIndexBlob.CreateRequestId IN UNNEST(@createRequestIds)
      """
        .trimIndent()
    )
  }

  return buildMap {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("createRequestIds").toStringArray(nonEmptyRequestIds)
        },
        Options.tag("action=findRankIndexBlobsByRequestIds"),
      )
      .collect { row ->
        if (!row.isNull("CreateRequestId")) {
          put(row.getString("CreateRequestId"), RankIndexBlobEntity.buildResult(row))
        }
      }
  }
}

/** Reads [RankIndexBlob] entries with filtering and pagination. */
fun AsyncDatabaseClient.ReadContext.readRankIndexBlobs(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListRankIndexBlobsRequest.Filter?,
  showDeleted: Boolean,
  limit: Int,
  after: ListRankIndexBlobsPageToken.After? = null,
): Flow<RankIndexBlobResult> {
  val sql = buildString {
    appendLine(RankIndexBlobEntity.BASE_SQL)

    val conjuncts = mutableListOf("RankIndexBlob.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId != null) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    // show_deleted=true includes soft-deleted rows alongside active ones (AIP-164); the default
    // (show_deleted=false) returns only active rows.
    if (!showDeleted) {
      conjuncts.add("RankIndexBlob.DeleteTime IS NULL")
    }

    if (filter != null) {
      if (filter.blobType != BlobType.BLOB_TYPE_UNSPECIFIED) {
        conjuncts.add("CAST(RankIndexBlob.BlobType AS INT64) = @blobType")
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts.add("RankIndexBlob.CmmsModelLine = @cmmsModelLine")
      }
      if (filter.hasPoolOffset()) {
        conjuncts.add("RankIndexBlob.PoolOffset = @poolOffset")
      }
      if (filter.hasMaxEventDateOnOrBefore()) {
        conjuncts.add("RankIndexBlob.MaxEventDate <= @maxEventDateOnOrBefore")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(RankIndexBlob.CreateTime > @afterCreateTime OR " +
          "(RankIndexBlob.CreateTime = @afterCreateTime AND " +
          "RankIndexBlob.RankIndexBlobResourceId > @afterRankIndexBlobResourceId))"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY RankIndexBlob.CreateTime ASC, RankIndexBlob.RankIndexBlobResourceId ASC")
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
        if (filter.blobType != BlobType.BLOB_TYPE_UNSPECIFIED) {
          bind("blobType").to(filter.blobType.number.toLong())
        }
        if (filter.cmmsModelLine.isNotEmpty()) {
          bind("cmmsModelLine").to(filter.cmmsModelLine)
        }
        if (filter.hasPoolOffset()) {
          bind("poolOffset").to(filter.poolOffset)
        }
        if (filter.hasMaxEventDateOnOrBefore()) {
          bind("maxEventDateOnOrBefore").to(filter.maxEventDateOnOrBefore.toCloudDate())
        }
      }

      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterRankIndexBlobResourceId").to(after.rankIndexBlobResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRankIndexBlobs")).map { row ->
    RankIndexBlobEntity.buildResult(row)
  }
}

/** Buffers an insert mutation for a [RankIndexBlob] row. */
fun AsyncDatabaseClient.TransactionContext.insertRankIndexBlob(
  rawImpressionUploadId: Long,
  rankIndexBlobId: Long,
  rankIndexBlobResourceId: String,
  dataProviderResourceId: String,
  createRequestId: String,
  rankIndexBlob: RankIndexBlob,
) {
  bufferInsertMutation("RankIndexBlob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RankIndexBlobId").to(rankIndexBlobId)
    set("RankIndexBlobResourceId").to(rankIndexBlobResourceId)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("CmmsModelLine").to(rankIndexBlob.cmmsModelLine)
    set("BlobType").to(rankIndexBlob.blobType)
    set("PoolOffset").to(rankIndexBlob.poolOffset)
    set("BlobUri").to(rankIndexBlob.blobUri)
    set("EncryptedDek").to(rankIndexBlob.encryptedDek)
    if (rankIndexBlob.hasMaxEventDate()) {
      set("MaxEventDate").to(rankIndexBlob.maxEventDate.toCloudDate())
    }
    if (!rankIndexBlob.blobChecksum.isEmpty()) {
      set("BlobChecksum").to(rankIndexBlob.blobChecksum.toGcloudByteArray())
    }
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers a soft-delete of a [RankIndexBlob] row by setting its `DeleteTime`. */
fun AsyncDatabaseClient.TransactionContext.softDeleteRankIndexBlob(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rankIndexBlobId: Long,
) {
  bufferUpdateMutation("RankIndexBlob") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RankIndexBlobId").to(rankIndexBlobId)
    set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun com.google.type.Date.toCloudDate(): Date = Date.fromYearMonthDay(year, month, day)

private object RankIndexBlobEntity {
  val BASE_SQL =
    """
    SELECT
      RankIndexBlob.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      RankIndexBlob.RawImpressionUploadId,
      RankIndexBlob.RankIndexBlobId,
      RankIndexBlob.RankIndexBlobResourceId,
      RankIndexBlob.CreateRequestId,
      RankIndexBlob.CmmsModelLine,
      RankIndexBlob.BlobType,
      RankIndexBlob.PoolOffset,
      RankIndexBlob.BlobUri,
      RankIndexBlob.EncryptedDek,
      RankIndexBlob.MaxEventDate,
      RankIndexBlob.BlobChecksum,
      RankIndexBlob.CreateTime,
      RankIndexBlob.DeleteTime,
    FROM
      RankIndexBlob
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    """
      .trimIndent()

  fun buildResult(struct: Struct): RankIndexBlobResult {
    return RankIndexBlobResult(
      rankIndexBlob {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
        rankIndexBlobResourceId = struct.getString("RankIndexBlobResourceId")
        cmmsModelLine = struct.getString("CmmsModelLine")
        blobType = struct.getProtoEnum("BlobType", BlobType::forNumber)
        poolOffset = struct.getLong("PoolOffset")
        blobUri = struct.getString("BlobUri")
        encryptedDek = struct.getProtoMessage("EncryptedDek", EncryptedDek.getDefaultInstance())
        if (!struct.isNull("MaxEventDate")) {
          maxEventDate = struct.getDate("MaxEventDate").toProtoDate()
        }
        if (!struct.isNull("BlobChecksum")) {
          blobChecksum = struct.getBytes("BlobChecksum").toByteArray().toByteString()
        }
        createTime = struct.getTimestamp("CreateTime").toProto()
        if (!struct.isNull("DeleteTime")) {
          deleteTime = struct.getTimestamp("DeleteTime").toProto()
        }
      },
      struct.getLong("RawImpressionUploadId"),
      struct.getLong("RankIndexBlobId"),
    )
  }
}
