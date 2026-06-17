// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload

data class RawImpressionUploadResult(
  val rawImpressionUpload: RawImpressionUpload,
  val rawImpressionUploadId: Long,
)

/** Reads a [RawImpressionUpload] by its resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
): RawImpressionUploadResult {
  val sql: String =
    """
    SELECT
      DataProviderResourceId,
      RawImpressionUploadId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      DoneBlobUri,
      State,
      CreateTime,
      UpdateTime,
    FROM
      RawImpressionUpload
    WHERE
      DataProviderResourceId = @dataProviderResourceId
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
      .singleOrNullIfEmpty()
      ?: throw RawImpressionUploadNotFoundException(
        dataProviderResourceId,
        rawImpressionUploadResourceId,
      )

  return buildRawImpressionUploadResult(row)
}

/** Finds an existing [RawImpressionUpload] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findExistingUploadByRequestId(
  dataProviderResourceId: String,
  requestId: String,
): RawImpressionUploadResult? {
  if (requestId.isEmpty()) return null

  val sql: String =
    """
    SELECT
      DataProviderResourceId,
      RawImpressionUploadId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      DoneBlobUri,
      State,
      CreateTime,
      UpdateTime,
    FROM
      RawImpressionUpload
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND CreateRequestId = @createRequestId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("createRequestId").to(requestId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return buildRawImpressionUploadResult(row)
}

/** Checks whether an upload with the given internal ID exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
): Boolean {
  return readRow(
    "RawImpressionUpload",
    Key.of(dataProviderResourceId, rawImpressionUploadId),
    listOf("RawImpressionUploadId"),
  ) != null
}

/** Buffers an insert mutation for a [RawImpressionUpload] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionUpload(
  rawImpressionUploadId: Long,
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  doneBlobUri: String,
  createRequestId: String,
  state: RawImpressionUploadState,
) {
  bufferInsertMutation("RawImpressionUpload") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("DoneBlobUri").to(doneBlobUri)
    set("State").to(state)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Reads [RawImpressionUpload] entries ordered by CreateTime then resource ID. */
fun AsyncDatabaseClient.ReadContext.readRawImpressionUploads(
  dataProviderResourceId: String,
  filter: ListRawImpressionUploadsRequest.Filter,
  limit: Int,
  after: ListRawImpressionUploadsPageToken.After? = null,
): Flow<RawImpressionUploadResult> {
  val sql: String = buildString {
    appendLine(
      """
      SELECT
        DataProviderResourceId,
        RawImpressionUploadId,
        RawImpressionUploadResourceId,
        CreateRequestId,
        DoneBlobUri,
        State,
        CreateTime,
        UpdateTime,
      FROM
        RawImpressionUpload
      """
        .trimIndent()
    )

    val conjuncts: MutableList<String> =
      mutableListOf("DataProviderResourceId = @dataProviderResourceId")

    if (filter.stateInList.isNotEmpty()) {
      conjuncts.add("CAST(State AS INT64) IN UNNEST(@stateIn)")
    }

    if (filter.hasCreateTimeIn()) {
      if (filter.createTimeIn.hasStartTime()) {
        conjuncts.add("CreateTime >= @createTimeStart")
      }
      if (filter.createTimeIn.hasEndTime()) {
        conjuncts.add("CreateTime < @createTimeEnd")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(CreateTime > @afterCreateTime) OR (CreateTime = @afterCreateTime AND RawImpressionUploadResourceId > @afterRawImpressionUploadResourceId)"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY CreateTime ASC, RawImpressionUploadResourceId ASC")
    appendLine("LIMIT @limit")
  }

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())

      if (filter.stateInList.isNotEmpty()) {
        bind("stateIn").toInt64Array(filter.stateInList.map { it.number.toLong() })
      }

      if (filter.hasCreateTimeIn()) {
        if (filter.createTimeIn.hasStartTime()) {
          bind("createTimeStart").to(filter.createTimeIn.startTime.toGcloudTimestamp())
        }
        if (filter.createTimeIn.hasEndTime()) {
          bind("createTimeEnd").to(filter.createTimeIn.endTime.toGcloudTimestamp())
        }
      }

      if (after != null) {
        bind("afterRawImpressionUploadResourceId").to(after.rawImpressionUploadResourceId)
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
      }
    }

  return executeQuery(query, Options.tag("action=readRawImpressionUploads")).map { row ->
    buildRawImpressionUploadResult(row)
  }
}

private fun buildRawImpressionUploadResult(struct: Struct): RawImpressionUploadResult {
  return RawImpressionUploadResult(
    rawImpressionUpload {
      dataProviderResourceId = struct.getString("DataProviderResourceId")
      rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
      doneBlobUri = struct.getString("DoneBlobUri")
      state = struct.getProtoEnum("State", RawImpressionUploadState::forNumber)
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
    },
    struct.getLong("RawImpressionUploadId"),
  )
}
