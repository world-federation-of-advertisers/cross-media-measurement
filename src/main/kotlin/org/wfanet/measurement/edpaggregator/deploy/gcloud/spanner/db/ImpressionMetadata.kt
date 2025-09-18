/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.type.Interval
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.impressionMetadata

data class ImpressionMetadataResult(
  val impressionMetadata: ImpressionMetadata,
  val impressionMetadataId: Long,
)

/** Returns whether the [ImpressionMetadata] with the specified [impressionMetadataId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.impressionMetadataExists(
  dataProviderResourceId: String,
  impressionMetadataId: Long,
): Boolean {
  return readRow(
    "ImpressionMetadata",
    Key.of(dataProviderResourceId, impressionMetadataId),
    listOf("ImpressionMetadataId"),
  ) != null
}

/**
 * Reads a [ImpressionMetadata] by its public resource ID.
 *
 * @return the [ImpressionMetadataResult]
 * @throws ImpressionMetadataNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getImpressionMetadataByResourceId(
  dataProviderResourceId: String,
  impressionMetadataResourceId: String,
): ImpressionMetadataResult {
  val sql = buildString {
    appendLine(ImpressionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE DataProviderResourceId = @dataProviderResourceId
      AND ImpressionMetadataResourceId = @impressionMetadataResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("impressionMetadataResourceId").to(impressionMetadataResourceId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw ImpressionMetadataNotFoundException(
        dataProviderResourceId,
        impressionMetadataResourceId,
      )

  return ImpressionMetadataEntity.buildImpressionMetadataResult(row)
}

suspend fun AsyncDatabaseClient.ReadContext.getImpressionMetadataByCreateRequestId(
  dataProviderResourceId: String,
  createRequestId: String,
): ImpressionMetadataResult? {
  val sql = buildString {
    appendLine(ImpressionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE DataProviderResourceId = @dataProviderResourceId
        AND CreateRequestId = @createRequestId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("createRequestId").to(createRequestId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return ImpressionMetadataEntity.buildImpressionMetadataResult(row)
}

/** Buffers an insert mutation for a [ImpressionMetadata] row. */
fun AsyncDatabaseClient.TransactionContext.insertImpressionMetadata(
  impressionMetadataId: Long,
  impressionMetadataResourceId: String,
  state: State,
  impressionMetadata: ImpressionMetadata,
  createRequestId: String? = null,
) {
  bufferInsertMutation("ImpressionMetadata") {
    set("DataProviderResourceId").to(impressionMetadata.dataProviderResourceId)
    set("ImpressionMetadataId").to(impressionMetadataId)
    set("ImpressionMetadataResourceId").to(impressionMetadataResourceId)
    set("CreateRequestId").to(createRequestId)
    set("BlobUri").to(impressionMetadata.blobUri)
    set("BlobTypeUrl").to(impressionMetadata.blobTypeUrl)
    set("EventGroupReferenceId").to(impressionMetadata.eventGroupReferenceId)
    set("CmmsModelLine").to(impressionMetadata.cmmsModelLine)
    set("IntervalStartTime").to(impressionMetadata.interval.startTime.toGcloudTimestamp())
    set("IntervalEndTime").to(impressionMetadata.interval.endTime.toGcloudTimestamp())
    set("State").to(state)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers a delete mutation for the ImpressionMetadata table. */
fun AsyncDatabaseClient.TransactionContext.deleteImpressionMetadata(
  dataProviderResourceId: String,
  impressionMetadataId: Long,
) {
  buffer(
    Mutation.delete("ImpressionMetadata", Key.of(dataProviderResourceId, impressionMetadataId))
  )
}

/**
 * Reads the ID of the [ImpressionMetadata] with the specified resource ID.
 *
 * @throws ImpressionMetadataNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getImpressionMetadataIdByResourceId(
  dataProviderResourceId: String,
  impressionMetadataResourceId: String,
): Long {
  val row =
    readRowUsingIndex(
      "ImpressionMetadata",
      "ImpressionMetadataByResourceId",
      Key.of(dataProviderResourceId, impressionMetadataResourceId),
      "ImpressionMetadataId",
    )
      ?: throw ImpressionMetadataNotFoundException(
        dataProviderResourceId,
        impressionMetadataResourceId,
      )
  return row.getLong("ImpressionMetadataId")
}

/** Reads [ImpressionMetadata] ordered by resource ID. */
fun AsyncDatabaseClient.ReadContext.readImpressionMetadata(
  limit: Int,
  after: ListImpressionMetadataPageToken.After? = null,
): Flow<ImpressionMetadataResult> {
  val sql = buildString {
    appendLine(
      """
      SELECT
        ImpressionMetadata.*
      FROM
        ImpressionMetadata
      """
        .trimIndent()
    )

    if (after != null) {
      appendLine("WHERE ImpressionMetadataResourceId > @afterImpressionMetadataResourceId")
    }
    appendLine("ORDER BY ImpressionMetadataResourceId")
    appendLine("LIMIT @limit")
  }

  val query =
    statement(sql) {
      if (after != null) {
        bind("afterImpressionMetadataResourceId").to(after.impressionMetadataResourceId)
      }
      bind("limit").to(limit.toLong())
    }

  return executeQuery(query, Options.tag("action=readImpressionMetadata")).map { row ->
    ImpressionMetadataEntity.buildImpressionMetadataResult(row)
  }
}

private object ImpressionMetadataEntity {
  val BASE_SQL =
    """
  SELECT
    DataProviderResourceId,
    ImpressionMetadataId,
    ImpressionMetadataResourceId,
    CreateRequestId,
    BlobUri,
    BlobTypeUrl,
    EventGroupReferenceId,
    CmmsModelLine,
    IntervalStartTime,
    IntervalEndTime,
    State,
    CreateTime,
    UpdateTime,
  FROM
    ImpressionMetadata
  """
      .trimIndent()

  fun buildImpressionMetadataResult(struct: Struct): ImpressionMetadataResult {
    return ImpressionMetadataResult(
      impressionMetadata {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        impressionMetadataResourceId = struct.getString("ImpressionMetadataResourceId")
        blobUri = struct.getString("BlobUri")
        blobTypeUrl = struct.getString("BlobTypeUrl")
        eventGroupReferenceId = struct.getString("EventGroupReferenceId")
        cmmsModelLine = struct.getString("CmmsModelLine")
        interval =
          Interval.newBuilder()
            .setStartTime(struct.getTimestamp("IntervalStartTime").toProto())
            .setEndTime(struct.getTimestamp("IntervalEndTime").toProto())
            .build()
        state = struct.getProtoEnum("State", State::forNumber)
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        etag = ETags.computeETag(updateTime.toInstant())
      },
      struct.getLong("ImpressionMetadataId"),
    )
  }
}
