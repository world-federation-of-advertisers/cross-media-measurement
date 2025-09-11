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

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.type.Interval
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataNotFoundException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
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
    set("IntervalStartTime").to(Timestamp.fromProto(impressionMetadata.interval.startTime))
    set("IntervalEndTime").to(Timestamp.fromProto(impressionMetadata.interval.endTime))
    set("State").to(state)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
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
