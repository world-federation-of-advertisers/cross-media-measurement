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
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadFileNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFile
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadFile

private const val SELECT_COLUMNS =
  """
  SELECT
    DataProviderResourceId,
    RawImpressionUploadId,
    FileResourceId,
    CreateRequestId,
    BlobUri,
    CreateTime,
    UpdateTime,
    DeleteTime,
  FROM
    RawImpressionUploadFile
  """

/** Checks whether the parent [RawImpressionUpload] exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadExists(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
): Boolean {
  return readRow(
    "RawImpressionUpload",
    Key.of(dataProviderResourceId, rawImpressionUploadResourceId),
    listOf("RawImpressionUploadId"),
  ) != null
}

/** Reads a [RawImpressionUploadFile] by its resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadFileByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceId: String,
): RawImpressionUploadFile {
  val sql: String =
    """
    $SELECT_COLUMNS
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadResourceId
      AND FileResourceId = @fileResourceId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("fileResourceId").to(fileResourceId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw RawImpressionUploadFileNotFoundException(
        dataProviderResourceId,
        rawImpressionUploadResourceId,
        fileResourceId,
      )

  return buildRawImpressionUploadFile(row)
}

/** Finds existing [RawImpressionUploadFile] entries by request IDs for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findExistingUploadFilesByRequestIds(
  dataProviderResourceId: String,
  requestIds: List<String>,
): Map<String, RawImpressionUploadFile> {
  val nonEmptyRequestIds: List<String> = requestIds.filter { it.isNotEmpty() }
  if (nonEmptyRequestIds.isEmpty()) return emptyMap()

  val sql: String =
    """
    $SELECT_COLUMNS
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND CreateRequestId IN UNNEST(@createRequestIds)
    """
      .trimIndent()

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("createRequestIds").toStringArray(nonEmptyRequestIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=findExistingUploadFilesByRequestIds")).collect { row ->
      if (!row.isNull("CreateRequestId")) {
        put(row.getString("CreateRequestId"), buildRawImpressionUploadFile(row))
      }
    }
  }
}

/** Reads multiple [RawImpressionUploadFile] entries by their resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getUploadFilesByResourceIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceIds: List<String>,
): Map<String, RawImpressionUploadFile> {
  if (fileResourceIds.isEmpty()) return emptyMap()

  val sql: String =
    """
    $SELECT_COLUMNS
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadResourceId
      AND FileResourceId IN UNNEST(@fileResourceIds)
    """
      .trimIndent()

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
      bind("fileResourceIds").toStringArray(fileResourceIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=getUploadFilesByResourceIds")).collect { row ->
      put(row.getString("FileResourceId"), buildRawImpressionUploadFile(row))
    }
  }
}

/** Checks whether an upload file with the given resource ID exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadFileExists(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceId: String,
): Boolean {
  return readRow(
    "RawImpressionUploadFile",
    Key.of(dataProviderResourceId, rawImpressionUploadResourceId, fileResourceId),
    listOf("FileResourceId"),
  ) != null
}

/** Buffers an insert mutation for a [RawImpressionUploadFile] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionUploadFile(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceId: String,
  blobUri: String,
  createRequestId: String,
) {
  bufferInsertMutation("RawImpressionUploadFile") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadResourceId)
    set("FileResourceId").to(fileResourceId)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("BlobUri").to(blobUri)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Soft-deletes a [RawImpressionUploadFile] by setting DeleteTime. */
fun AsyncDatabaseClient.TransactionContext.softDeleteRawImpressionUploadFile(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceId: String,
) {
  bufferUpdateMutation("RawImpressionUploadFile") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadResourceId)
    set("FileResourceId").to(fileResourceId)
    set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Reads [RawImpressionUploadFile] entries ordered by CreateTime, RawImpressionUploadId and
 * FileResourceId.
 *
 * When [rawImpressionUploadResourceId] is empty, files across all uploads for the DataProvider are
 * returned (AIP-159 wildcard).
 */
fun AsyncDatabaseClient.ReadContext.readRawImpressionUploadFiles(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  filter: ListRawImpressionUploadFilesRequest.Filter,
  limit: Int,
  showDeleted: Boolean,
  after: ListRawImpressionUploadFilesPageToken.After? = null,
): Flow<RawImpressionUploadFile> {
  val sql: String = buildString {
    appendLine(SELECT_COLUMNS.trimIndent())

    val conjuncts: MutableList<String> =
      mutableListOf("DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId.isNotEmpty()) {
      conjuncts.add("RawImpressionUploadId = @rawImpressionUploadResourceId")
    }

    if (!showDeleted) {
      conjuncts.add("DeleteTime IS NULL")
    }

    if (filter.blobUriInList.isNotEmpty()) {
      conjuncts.add("BlobUri IN UNNEST(@blobUriIn)")
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
        """
        (CreateTime > @afterCreateTime)
        OR (CreateTime = @afterCreateTime
            AND RawImpressionUploadId > @afterRawImpressionUploadResourceId)
        OR (CreateTime = @afterCreateTime
            AND RawImpressionUploadId = @afterRawImpressionUploadResourceId
            AND FileResourceId > @afterFileResourceId)
        """
          .trimIndent()
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY CreateTime ASC, RawImpressionUploadId ASC, FileResourceId ASC")
    appendLine("LIMIT @limit")
  }

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())

      if (rawImpressionUploadResourceId.isNotEmpty()) {
        bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
      }

      if (filter.blobUriInList.isNotEmpty()) {
        bind("blobUriIn").toStringArray(filter.blobUriInList)
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
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterRawImpressionUploadResourceId").to(after.rawImpressionUploadResourceId)
        bind("afterFileResourceId").to(after.fileResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRawImpressionUploadFiles")).map { row ->
    buildRawImpressionUploadFile(row)
  }
}

private fun buildRawImpressionUploadFile(struct: Struct): RawImpressionUploadFile {
  return rawImpressionUploadFile {
    dataProviderResourceId = struct.getString("DataProviderResourceId")
    rawImpressionUploadResourceId = struct.getString("RawImpressionUploadId")
    fileResourceId = struct.getString("FileResourceId")
    blobUri = struct.getString("BlobUri")
    createTime = struct.getTimestamp("CreateTime").toProto()
    updateTime = struct.getTimestamp("UpdateTime").toProto()
    if (!struct.isNull("DeleteTime")) {
      deleteTime = struct.getTimestamp("DeleteTime").toProto()
    }
  }
}
