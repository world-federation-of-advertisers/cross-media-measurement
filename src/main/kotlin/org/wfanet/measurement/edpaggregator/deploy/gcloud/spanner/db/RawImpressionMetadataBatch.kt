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
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionBatchState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatch
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataBatch

data class RawImpressionMetadataBatchResult(
  val rawImpressionMetadataBatch: RawImpressionMetadataBatch,
  val batchId: Long,
)

/** Reads a [RawImpressionMetadataBatch] by its resource IDs. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionMetadataBatchByResourceId(
  dataProviderResourceId: String,
  batchResourceId: String,
): RawImpressionMetadataBatchResult {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      BatchId,
      BatchResourceId,
      CreateRequestId,
      State,
      CreateTime,
      UpdateTime,
      DeleteTime,
    FROM
      RawImpressionMetadataBatch
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND BatchResourceId = @batchResourceId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("batchResourceId").to(batchResourceId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw RawImpressionMetadataBatchNotFoundException(dataProviderResourceId, batchResourceId)

  return buildRawImpressionMetadataBatchResult(row)
}

/** Finds an existing [RawImpressionMetadataBatch] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findExistingBatchByRequestId(
  dataProviderResourceId: String,
  requestId: String,
): RawImpressionMetadataBatchResult? {
  if (requestId.isBlank()) return null

  val sql =
    """
    SELECT
      DataProviderResourceId,
      BatchId,
      BatchResourceId,
      CreateRequestId,
      State,
      CreateTime,
      UpdateTime,
      DeleteTime,
    FROM
      RawImpressionMetadataBatch
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

  return buildRawImpressionMetadataBatchResult(row)
}

/** Checks whether a batch with the given ID exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionMetadataBatchExists(
  dataProviderResourceId: String,
  batchId: Long,
): Boolean {
  return readRow(
    "RawImpressionMetadataBatch",
    Key.of(dataProviderResourceId, batchId),
    listOf("BatchId"),
  ) != null
}

/** Buffers an insert mutation for a [RawImpressionMetadataBatch] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionMetadataBatch(
  batchId: Long,
  dataProviderResourceId: String,
  batchResourceId: String,
  createRequestId: String,
): String {
  val resolvedBatchResourceId = batchResourceId.ifBlank { "batch-${UUID.randomUUID()}" }
  bufferInsertMutation("RawImpressionMetadataBatch") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("BatchId").to(batchId)
    set("BatchResourceId").to(resolvedBatchResourceId)
    if (createRequestId.isNotBlank()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("State").to(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  return resolvedBatchResourceId
}

/** Buffers an update to a [RawImpressionMetadataBatch] row's state. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionMetadataBatchState(
  dataProviderResourceId: String,
  batchId: Long,
  state: RawImpressionBatchState,
) {
  bufferUpdateMutation("RawImpressionMetadataBatch") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("BatchId").to(batchId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Soft-deletes a [RawImpressionMetadataBatch] by setting DeleteTime. */
fun AsyncDatabaseClient.TransactionContext.softDeleteRawImpressionMetadataBatch(
  dataProviderResourceId: String,
  batchId: Long,
) {
  bufferUpdateMutation("RawImpressionMetadataBatch") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("BatchId").to(batchId)
    set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Reads [RawImpressionMetadataBatch] entries ordered by BatchResourceId. */
fun AsyncDatabaseClient.ReadContext.readRawImpressionMetadataBatches(
  dataProviderResourceId: String,
  filter: ListRawImpressionMetadataBatchesRequest.Filter,
  limit: Int,
  showDeleted: Boolean,
  after: ListRawImpressionMetadataBatchesPageToken.After? = null,
): Flow<RawImpressionMetadataBatchResult> {
  val sql = buildString {
    appendLine(
      """
      SELECT
        DataProviderResourceId,
        BatchId,
        BatchResourceId,
        CreateRequestId,
        State,
        CreateTime,
        UpdateTime,
        DeleteTime,
      FROM
        RawImpressionMetadataBatch
      """
        .trimIndent()
    )

    val conjuncts = mutableListOf("DataProviderResourceId = @dataProviderResourceId")

    if (!showDeleted) {
      conjuncts.add("DeleteTime IS NULL")
    }

    if (filter.state != RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_UNSPECIFIED) {
      conjuncts.add("State = @state")
    }

    if (after != null) {
      conjuncts.add(
        "(CreateTime > @afterCreateTime) OR (CreateTime = @afterCreateTime AND BatchResourceId > @afterBatchResourceId)"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY CreateTime ASC, BatchResourceId ASC")
    appendLine("LIMIT @limit")
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())

      if (filter.state != RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_UNSPECIFIED) {
        bind("state").to(filter.state.number.toLong())
      }

      if (after != null) {
        bind("afterBatchResourceId").to(after.batchResourceId)
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
      }
    }

  return executeQuery(query, Options.tag("action=readRawImpressionMetadataBatches")).map { row ->
    buildRawImpressionMetadataBatchResult(row)
  }
}

private fun buildRawImpressionMetadataBatchResult(
  struct: Struct
): RawImpressionMetadataBatchResult {
  return RawImpressionMetadataBatchResult(
    rawImpressionMetadataBatch {
      dataProviderResourceId = struct.getString("DataProviderResourceId")
      batchResourceId = struct.getString("BatchResourceId")
      state = struct.getProtoEnum("State", RawImpressionBatchState::forNumber)
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      if (!struct.isNull("DeleteTime")) {
        deleteTime = struct.getTimestamp("DeleteTime").toProto()
      }
    },
    struct.getLong("BatchId"),
  )
}
