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
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchFileNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFile
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataBatchFile

data class RawImpressionMetadataBatchFileResult(
  val rawImpressionMetadataBatchFile: RawImpressionMetadataBatchFile,
  val batchId: Long,
  val fileId: Long,
)

/** Reads a [RawImpressionMetadataBatchFile] by its resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionMetadataBatchFileByResourceId(
  dataProviderResourceId: String,
  batchResourceId: String,
  fileResourceId: String,
): RawImpressionMetadataBatchFileResult {
  val sql: String =
    """
    SELECT
      RawImpressionMetadataBatchFile.DataProviderResourceId,
      RawImpressionMetadataBatchFile.BatchId,
      RawImpressionMetadataBatchFile.FileId,
      RawImpressionMetadataBatchFile.FileResourceId,
      RawImpressionMetadataBatchFile.CreateRequestId,
      RawImpressionMetadataBatchFile.BlobUri,
      RawImpressionMetadataBatchFile.CreateTime,
      RawImpressionMetadataBatchFile.UpdateTime,
      RawImpressionMetadataBatchFile.DeleteTime,
      RawImpressionMetadataBatch.BatchResourceId,
    FROM
      RawImpressionMetadataBatchFile
      JOIN RawImpressionMetadataBatch USING (DataProviderResourceId, BatchId)
    WHERE
      RawImpressionMetadataBatchFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionMetadataBatch.BatchResourceId = @batchResourceId
      AND RawImpressionMetadataBatchFile.FileResourceId = @fileResourceId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("batchResourceId").to(batchResourceId)
          bind("fileResourceId").to(fileResourceId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw RawImpressionMetadataBatchFileNotFoundException(
        dataProviderResourceId,
        batchResourceId,
        fileResourceId,
      )

  return buildRawImpressionMetadataBatchFileResult(row)
}

/** Finds existing [RawImpressionMetadataBatchFile] entries by request IDs for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findExistingBatchFilesByRequestIds(
  dataProviderResourceId: String,
  batchId: Long,
  requestIds: List<String>,
): Map<String, RawImpressionMetadataBatchFileResult> {
  val nonEmptyRequestIds: List<String> = requestIds.filter { it.isNotEmpty() }
  if (nonEmptyRequestIds.isEmpty()) return emptyMap()

  val sql: String =
    """
    SELECT
      RawImpressionMetadataBatchFile.DataProviderResourceId,
      RawImpressionMetadataBatchFile.BatchId,
      RawImpressionMetadataBatchFile.FileId,
      RawImpressionMetadataBatchFile.FileResourceId,
      RawImpressionMetadataBatchFile.CreateRequestId,
      RawImpressionMetadataBatchFile.BlobUri,
      RawImpressionMetadataBatchFile.CreateTime,
      RawImpressionMetadataBatchFile.UpdateTime,
      RawImpressionMetadataBatchFile.DeleteTime,
      RawImpressionMetadataBatch.BatchResourceId,
    FROM
      RawImpressionMetadataBatchFile
      JOIN RawImpressionMetadataBatch USING (DataProviderResourceId, BatchId)
    WHERE
      RawImpressionMetadataBatchFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionMetadataBatchFile.BatchId = @batchId
      AND RawImpressionMetadataBatchFile.CreateRequestId IN UNNEST(@createRequestIds)
    """
      .trimIndent()

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("batchId").to(batchId)
      bind("createRequestIds").toStringArray(nonEmptyRequestIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=findExistingBatchFilesByRequestIds")).collect { row ->
      val result: RawImpressionMetadataBatchFileResult =
        buildRawImpressionMetadataBatchFileResult(row)
      if (!row.isNull("CreateRequestId")) {
        put(row.getString("CreateRequestId"), result)
      }
    }
  }
}

/** Finds existing [RawImpressionMetadataBatchFile] entries by blob URIs. */
suspend fun AsyncDatabaseClient.ReadContext.findExistingBatchFilesByBlobUris(
  dataProviderResourceId: String,
  blobUris: List<String>,
): Map<String, RawImpressionMetadataBatchFileResult> {
  if (blobUris.isEmpty()) return emptyMap()

  val sql: String =
    """
    SELECT
      RawImpressionMetadataBatchFile.DataProviderResourceId,
      RawImpressionMetadataBatchFile.BatchId,
      RawImpressionMetadataBatchFile.FileId,
      RawImpressionMetadataBatchFile.FileResourceId,
      RawImpressionMetadataBatchFile.CreateRequestId,
      RawImpressionMetadataBatchFile.BlobUri,
      RawImpressionMetadataBatchFile.CreateTime,
      RawImpressionMetadataBatchFile.UpdateTime,
      RawImpressionMetadataBatchFile.DeleteTime,
      RawImpressionMetadataBatch.BatchResourceId,
    FROM
      RawImpressionMetadataBatchFile
      JOIN RawImpressionMetadataBatch USING (DataProviderResourceId, BatchId)
    WHERE
      RawImpressionMetadataBatchFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionMetadataBatchFile.BlobUri IN UNNEST(@blobUris)
    """
      .trimIndent()

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("blobUris").toStringArray(blobUris)
    }

  return executeQuery(query, Options.tag("action=findExistingBatchFilesByBlobUris"))
    .map { buildRawImpressionMetadataBatchFileResult(it) }
    .toList()
    .associateBy { it.rawImpressionMetadataBatchFile.blobUri }
}

/** Reads multiple [RawImpressionMetadataBatchFile] entries by their resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getBatchFilesByResourceIds(
  dataProviderResourceId: String,
  batchResourceId: String,
  fileResourceIds: List<String>,
): Map<String, RawImpressionMetadataBatchFileResult> {
  val sql: String =
    """
    SELECT
      RawImpressionMetadataBatchFile.DataProviderResourceId,
      RawImpressionMetadataBatchFile.BatchId,
      RawImpressionMetadataBatchFile.FileId,
      RawImpressionMetadataBatchFile.FileResourceId,
      RawImpressionMetadataBatchFile.CreateRequestId,
      RawImpressionMetadataBatchFile.BlobUri,
      RawImpressionMetadataBatchFile.CreateTime,
      RawImpressionMetadataBatchFile.UpdateTime,
      RawImpressionMetadataBatchFile.DeleteTime,
      RawImpressionMetadataBatch.BatchResourceId,
    FROM
      RawImpressionMetadataBatchFile
      JOIN RawImpressionMetadataBatch USING (DataProviderResourceId, BatchId)
    WHERE
      RawImpressionMetadataBatchFile.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionMetadataBatch.BatchResourceId = @batchResourceId
      AND RawImpressionMetadataBatchFile.FileResourceId IN UNNEST(@fileResourceIds)
    """
      .trimIndent()

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("batchResourceId").to(batchResourceId)
      bind("fileResourceIds").toStringArray(fileResourceIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=getBatchFilesByResourceIds")).collect { row ->
      val result: RawImpressionMetadataBatchFileResult =
        buildRawImpressionMetadataBatchFileResult(row)
      put(row.getString("FileResourceId"), result)
    }
  }
}

/** Checks whether a batch file with the given ID exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionMetadataBatchFileExists(
  dataProviderResourceId: String,
  batchId: Long,
  fileId: Long,
): Boolean {
  return readRow(
    "RawImpressionMetadataBatchFile",
    Key.of(dataProviderResourceId, batchId, fileId),
    listOf("FileId"),
  ) != null
}

/** Buffers an insert mutation for a [RawImpressionMetadataBatchFile] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionMetadataBatchFile(
  fileId: Long,
  dataProviderResourceId: String,
  batchId: Long,
  fileResourceId: String,
  blobUri: String,
  createRequestId: String,
) {
  bufferInsertMutation("RawImpressionMetadataBatchFile") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("BatchId").to(batchId)
    set("FileId").to(fileId)
    set("FileResourceId").to(fileResourceId)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("BlobUri").to(blobUri)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Soft-deletes a [RawImpressionMetadataBatchFile] by setting DeleteTime. */
fun AsyncDatabaseClient.TransactionContext.softDeleteRawImpressionMetadataBatchFile(
  dataProviderResourceId: String,
  batchId: Long,
  fileId: Long,
) {
  bufferUpdateMutation("RawImpressionMetadataBatchFile") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("BatchId").to(batchId)
    set("FileId").to(fileId)
    set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Reads [RawImpressionMetadataBatchFile] entries ordered by FileResourceId. */
fun AsyncDatabaseClient.ReadContext.readRawImpressionMetadataBatchFiles(
  dataProviderResourceId: String,
  batchResourceId: String,
  filter: ListRawImpressionMetadataBatchFilesRequest.Filter,
  limit: Int,
  showDeleted: Boolean,
  after: ListRawImpressionMetadataBatchFilesPageToken.After? = null,
): Flow<RawImpressionMetadataBatchFileResult> {
  val sql: String = buildString {
    appendLine(
      """
      SELECT
        RawImpressionMetadataBatchFile.DataProviderResourceId,
        RawImpressionMetadataBatchFile.BatchId,
        RawImpressionMetadataBatchFile.FileId,
        RawImpressionMetadataBatchFile.FileResourceId,
        RawImpressionMetadataBatchFile.CreateRequestId,
        RawImpressionMetadataBatchFile.BlobUri,
        RawImpressionMetadataBatchFile.CreateTime,
        RawImpressionMetadataBatchFile.UpdateTime,
        RawImpressionMetadataBatchFile.DeleteTime,
        RawImpressionMetadataBatch.BatchResourceId,
      FROM
        RawImpressionMetadataBatchFile
        JOIN RawImpressionMetadataBatch USING (DataProviderResourceId, BatchId)
      """
        .trimIndent()
    )

    val conjuncts: MutableList<String> =
      mutableListOf(
        "RawImpressionMetadataBatchFile.DataProviderResourceId = @dataProviderResourceId",
        "RawImpressionMetadataBatch.BatchResourceId = @batchResourceId",
      )

    if (!showDeleted) {
      conjuncts.add("RawImpressionMetadataBatchFile.DeleteTime IS NULL")
    }

    if (filter.blobUriInList.isNotEmpty()) {
      conjuncts.add("RawImpressionMetadataBatchFile.BlobUri IN UNNEST(@blobUriIn)")
    }

    if (after != null) {
      conjuncts.add(
        """
        (RawImpressionMetadataBatchFile.CreateTime > @afterCreateTime)
        OR (RawImpressionMetadataBatchFile.CreateTime = @afterCreateTime
            AND RawImpressionMetadataBatch.BatchResourceId > @afterBatchResourceId)
        OR (RawImpressionMetadataBatchFile.CreateTime = @afterCreateTime
            AND RawImpressionMetadataBatch.BatchResourceId = @afterBatchResourceId
            AND RawImpressionMetadataBatchFile.FileResourceId > @afterFileResourceId)
        """
          .trimIndent()
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine(
      "ORDER BY RawImpressionMetadataBatchFile.CreateTime ASC, RawImpressionMetadataBatch.BatchResourceId ASC, RawImpressionMetadataBatchFile.FileResourceId ASC"
    )
    appendLine("LIMIT @limit")
  }

  val query: Statement =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("batchResourceId").to(batchResourceId)
      bind("limit").to(limit.toLong())

      if (filter.blobUriInList.isNotEmpty()) {
        bind("blobUriIn").toStringArray(filter.blobUriInList)
      }

      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterBatchResourceId").to(after.batchResourceId)
        bind("afterFileResourceId").to(after.fileResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRawImpressionMetadataBatchFiles")).map { row ->
    buildRawImpressionMetadataBatchFileResult(row)
  }
}

private fun buildRawImpressionMetadataBatchFileResult(
  struct: Struct
): RawImpressionMetadataBatchFileResult {
  return RawImpressionMetadataBatchFileResult(
    rawImpressionMetadataBatchFile {
      dataProviderResourceId = struct.getString("DataProviderResourceId")
      batchResourceId = struct.getString("BatchResourceId")
      fileResourceId = struct.getString("FileResourceId")
      blobUri = struct.getString("BlobUri")
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      if (!struct.isNull("DeleteTime")) {
        deleteTime = struct.getTimestamp("DeleteTime").toProto()
      }
    },
    struct.getLong("BatchId"),
    struct.getLong("FileId"),
  )
}
