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

import com.google.cloud.Date
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadFileNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFile
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadFile

/**
 * A [RawImpressionUploadFile] read result together with the internal numeric IDs (surrogate primary
 * keys) of the file and its parent upload.
 */
data class RawImpressionUploadFileResult(
  val rawImpressionUploadFile: RawImpressionUploadFile,
  val rawImpressionUploadId: Long,
  val fileId: Long,
)

private const val BASE_SQL =
  """
  SELECT
    RawImpressionUploadFile.DataProviderResourceId,
    RawImpressionUploadFile.RawImpressionUploadId,
    RawImpressionUploadFile.FileId,
    RawImpressionUploadFile.FileResourceId,
    RawImpressionUploadFile.CreateRequestId,
    RawImpressionUploadFile.BlobUri,
    RawImpressionUploadFile.SizeBytes,
    RawImpressionUploadFile.EventDate,
    RawImpressionUploadFile.CreateTime,
    RawImpressionUploadFile.UpdateTime,
    RawImpressionUploadFile.DeleteTime,
    RawImpressionUpload.RawImpressionUploadResourceId,
  FROM
    RawImpressionUploadFile
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
  """

/** Resolves the internal numeric ID of a [RawImpressionUpload] from its resource ID. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
): Long {
  val sql: String =
    """
    SELECT RawImpressionUploadId
    FROM RawImpressionUpload
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

  return row.getLong("RawImpressionUploadId")
}

/** Reads a [RawImpressionUploadFile] by its resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadFileByResourceId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceId: String,
): RawImpressionUploadFileResult {
  val sql: String =
    """
    $BASE_SQL
    WHERE
      RawImpressionUploadFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
      AND RawImpressionUploadFile.FileResourceId = @fileResourceId
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

  return buildRawImpressionUploadFileResult(row)
}

/** Finds existing [RawImpressionUploadFile] entries by request IDs for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findExistingUploadFilesByRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  requestIds: List<String>,
): Map<String, RawImpressionUploadFileResult> {
  if (requestIds.isEmpty()) return emptyMap()

  val sql: String =
    """
    $BASE_SQL
    WHERE
      RawImpressionUploadFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadFile.RawImpressionUploadId = @rawImpressionUploadId
      AND RawImpressionUploadFile.CreateRequestId IN UNNEST(@createRequestIds)
    """
      .trimIndent()

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("rawImpressionUploadId").to(rawImpressionUploadId)
      bind("createRequestIds").toStringArray(requestIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=findExistingUploadFilesByRequestIds")).collect { row ->
      if (!row.isNull("CreateRequestId")) {
        put(row.getString("CreateRequestId"), buildRawImpressionUploadFileResult(row))
      }
    }
  }
}

/** Reads multiple [RawImpressionUploadFile] entries by their resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getUploadFilesByResourceIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  fileResourceIds: List<String>,
): Map<String, RawImpressionUploadFileResult> {
  if (fileResourceIds.isEmpty()) return emptyMap()

  val sql: String =
    """
    $BASE_SQL
    WHERE
      RawImpressionUploadFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
      AND RawImpressionUploadFile.FileResourceId IN UNNEST(@fileResourceIds)
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
      put(row.getString("FileResourceId"), buildRawImpressionUploadFileResult(row))
    }
  }
}

/** Checks whether an upload file with the given numeric ID exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadFileExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  fileId: Long,
): Boolean {
  return readRow(
    "RawImpressionUploadFile",
    Key.of(dataProviderResourceId, rawImpressionUploadId, fileId),
    listOf("FileId"),
  ) != null
}

/** Buffers an insert mutation for a [RawImpressionUploadFile] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionUploadFile(
  fileId: Long,
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  fileResourceId: String,
  blobUri: String,
  sizeBytes: Long,
  eventDate: com.google.type.Date,
  createRequestId: String,
) {
  bufferInsertMutation("RawImpressionUploadFile") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("FileId").to(fileId)
    set("FileResourceId").to(fileResourceId)
    set("CreateRequestId").to(createRequestId)
    set("BlobUri").to(blobUri)
    set("SizeBytes").to(sizeBytes)
    set("EventDate").to(eventDate.toCloudDate())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Soft-deletes a [RawImpressionUploadFile] by setting DeleteTime. */
fun AsyncDatabaseClient.TransactionContext.softDeleteRawImpressionUploadFile(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  fileId: Long,
) {
  bufferUpdateMutation("RawImpressionUploadFile") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("FileId").to(fileId)
    set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Reads [RawImpressionUploadFile] entries ordered by CreateTime, RawImpressionUploadResourceId and
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
): Flow<RawImpressionUploadFileResult> {
  val sql: String = buildString {
    appendLine(BASE_SQL.trimIndent())

    val conjuncts: MutableList<String> =
      mutableListOf("RawImpressionUploadFile.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId.isNotEmpty()) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    if (!showDeleted) {
      conjuncts.add("RawImpressionUploadFile.DeleteTime IS NULL")
    }

    if (filter.blobUriInList.isNotEmpty()) {
      conjuncts.add("RawImpressionUploadFile.BlobUri IN UNNEST(@blobUriIn)")
    }

    if (filter.hasCreateTimeIn()) {
      if (filter.createTimeIn.hasStartTime()) {
        conjuncts.add("RawImpressionUploadFile.CreateTime >= @createTimeStart")
      }
      if (filter.createTimeIn.hasEndTime()) {
        conjuncts.add("RawImpressionUploadFile.CreateTime < @createTimeEnd")
      }
    }

    if (after != null) {
      conjuncts.add(
        """
        (
          (RawImpressionUploadFile.CreateTime > @afterCreateTime)
          OR (RawImpressionUploadFile.CreateTime = @afterCreateTime
              AND RawImpressionUpload.RawImpressionUploadResourceId > @afterRawImpressionUploadResourceId)
          OR (RawImpressionUploadFile.CreateTime = @afterCreateTime
              AND RawImpressionUpload.RawImpressionUploadResourceId = @afterRawImpressionUploadResourceId
              AND RawImpressionUploadFile.FileResourceId > @afterFileResourceId)
        )
        """
          .trimIndent()
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine(
      "ORDER BY RawImpressionUploadFile.CreateTime ASC, " +
        "RawImpressionUpload.RawImpressionUploadResourceId ASC, " +
        "RawImpressionUploadFile.FileResourceId ASC"
    )
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
    buildRawImpressionUploadFileResult(row)
  }
}

private fun com.google.type.Date.toCloudDate(): Date = Date.fromYearMonthDay(year, month, day)

private fun buildRawImpressionUploadFileResult(struct: Struct): RawImpressionUploadFileResult {
  return RawImpressionUploadFileResult(
    rawImpressionUploadFile {
      dataProviderResourceId = struct.getString("DataProviderResourceId")
      rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
      fileResourceId = struct.getString("FileResourceId")
      blobUri = struct.getString("BlobUri")
      sizeBytes = struct.getLong("SizeBytes")
      eventDate = struct.getDate("EventDate").toProtoDate()
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      if (!struct.isNull("DeleteTime")) {
        deleteTime = struct.getTimestamp("DeleteTime").toProto()
      }
    },
    struct.getLong("RawImpressionUploadId"),
    struct.getLong("FileId"),
  )
}
