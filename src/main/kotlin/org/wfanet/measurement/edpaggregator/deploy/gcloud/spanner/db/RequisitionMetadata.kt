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
import com.google.protobuf.Timestamp
import kotlin.text.trimIndent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataNotFoundByCmmsRequisitionException
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata

data class RequisitionMetadataResult(
  val requisitionMetadata: RequisitionMetadata,
  val requisitionMetadataId: Long,
)

/**
 * Returns whether the [RequisitionMetadata] with the specified [dataProviderResourceId] and
 * [requisitionMetadataId] exists.
 */
suspend fun AsyncDatabaseClient.ReadContext.requisitionMetadataExists(
  dataProviderResourceId: String,
  requisitionMetadataId: Long,
): Boolean {
  return readRow(
    "RequisitionMetadata",
    Key.of(dataProviderResourceId, requisitionMetadataId),
    listOf("RequisitionMetadataId"),
  ) != null
}

/**
 * Reads a [RequisitionMetadata] by its public resource ID.
 *
 * @return The [RequisitionMetadataResult]
 * @throws RequisitionMetadataNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByResourceId(
  dataProviderResourceId: String,
  requisitionMetadataResourceId: String,
): RequisitionMetadataResult {
  val sql = buildString {
    appendLine(RequisitionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE DataProviderResourceId = @dataProviderResourceId
      AND RequisitionMetadataResourceId = @requisitionMetadataResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("requisitionMetadataResourceId").to(requisitionMetadataResourceId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw RequisitionMetadataNotFoundException(
        dataProviderResourceId,
        requisitionMetadataResourceId,
      )
  return RequisitionMetadataEntity.buildRequisitionMetadataResult(row)
}

suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByCmmsRequisition(
  dataProviderResourceId: String,
  cmmsRequisition: String,
): RequisitionMetadataResult {
  val sql = buildString {
    appendLine(RequisitionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE DataProviderResourceId = @dataProviderResourceId
        AND CmmsRequisition = @cmmsRequisition
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("cmmsRequisition").to(cmmsRequisition)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw RequisitionMetadataNotFoundByCmmsRequisitionException(
        dataProviderResourceId,
        cmmsRequisition,
      )

  return RequisitionMetadataEntity.buildRequisitionMetadataResult(row)
}

fun AsyncDatabaseClient.ReadContext.readRequisitionMetadata(
  dataProviderResourceId: String,
  filter: ListRequisitionMetadataRequest.Filter?,
  limit: Int,
  after: ListRequisitionMetadataPageToken.After? = null,
): Flow<RequisitionMetadataResult> {
  val sql = buildString {
    appendLine(RequisitionMetadataEntity.BASE_SQL)

    val conjuncts = mutableListOf("DataProviderResourceId = @dataProviderResourceId")

    if (filter != null) {
      if (filter.state != RequisitionMetadataState.REQUISITION_METADATA_STATE_UNSPECIFIED) {
        conjuncts.add("State = @state")
      }
      if (filter.groupId.isNotEmpty()) {
        conjuncts.add("GroupId = @groupId")
      }
      if (filter.report.isNotEmpty()) {
        conjuncts.add("Report = @report")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(UpdateTime > @afterUpdateTime OR (UpdateTime = @afterUpdateTime AND RequisitionMetadataResourceId > @afterRequisitionMetadataResourceId))"
      )
    }

    if (conjuncts.isNotEmpty()) {
      appendLine("WHERE " + conjuncts.joinToString(" AND "))
    }

    appendLine("ORDER BY UpdateTime ASC, RequisitionMetadataResourceId ASC")
    appendLine("LIMIT @limit")
  }

  val query =
    statement(sql) {
      bind("limit").to(limit.toLong())
      bind("dataProviderResourceId").to(dataProviderResourceId)

      if (filter != null) {
        if (filter.state != RequisitionMetadataState.REQUISITION_METADATA_STATE_UNSPECIFIED) {
          bind("state").to(filter.state.number.toLong())
        }
        if (filter.groupId.isNotEmpty()) {
          bind("groupId").to(filter.groupId)
        }
        if (filter.report.isNotEmpty()) {
          bind("report").to(filter.report)
        }
      }

      if (after != null) {
        bind("afterUpdateTime").to(after.updateTime.toGcloudTimestamp())
        bind("afterRequisitionMetadataResourceId").to(after.requisitionMetadataResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRequisitionMetadata")).map { row ->
    RequisitionMetadataEntity.buildRequisitionMetadataResult(row)
  }
}

suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByCreateRequestId(
  dataProviderResourceId: String,
  createRequestId: String,
): RequisitionMetadataResult? {
  val sql = buildString {
    appendLine(RequisitionMetadataEntity.BASE_SQL)
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

  return RequisitionMetadataEntity.buildRequisitionMetadataResult(row)
}

/** Buffers an insert mutation for a [RequisitionMetadata] row. */
fun AsyncDatabaseClient.TransactionContext.insertRequisitionMetadata(
  requisitionMetadataId: Long,
  requisitionMetadataResourceId: String,
  state: State,
  requisitionMetadata: RequisitionMetadata,
  createRequestId: String,
) {
  bufferInsertMutation("RequisitionMetadata") {
    set("DataProviderResourceId").to(requisitionMetadata.dataProviderResourceId)
    set("RequisitionMetadataId").to(requisitionMetadataId)
    set("RequisitionMetadataResourceId").to(requisitionMetadataResourceId)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("CmmsRequisition").to(requisitionMetadata.cmmsRequisition)
    set("BlobUri").to(requisitionMetadata.blobUri)
    set("BlobTypeUrl").to(requisitionMetadata.blobTypeUrl)
    set("GroupId").to(requisitionMetadata.groupId)
    set("CmmsCreateTime").to(requisitionMetadata.cmmsCreateTime.toGcloudTimestamp())
    set("Report").to(requisitionMetadata.report)
    set("State").to(state)
    if (requisitionMetadata.workItem.isNotEmpty()) {
      set("WorkItem").to(requisitionMetadata.workItem)
    }
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (requisitionMetadata.refusalMessage.isNotEmpty()) {
      set("RefusalMessage").to(requisitionMetadata.refusalMessage)
    }
  }
}

/** Buffers an update to a [RequisitionMetadata] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updateRequisitionMetadataState(
  dataProviderResourceId: String,
  requisitionMetadataId: Long,
  state: State,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("RequisitionMetadata") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RequisitionMetadataId").to(requisitionMetadataId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (block != null) {
      block()
    }
  }
}

/** Returns the latest cmms create time for a given [dataProviderResourceId] */
suspend fun AsyncDatabaseClient.ReadContext.fetchLatestCmmsCreateTime(
  dataProviderResourceId: String
): Timestamp {
  val sql =
    """
      SELECT CmmsCreateTime FROM RequisitionMetadata
      WHERE DataProviderResourceId = @dataProviderResourceId
      ORDER BY CmmsCreateTime DESC
      LIMIT 1
      """
      .trimIndent()

  val row: Struct =
    executeQuery(statement(sql) { bind("dataProviderResourceId").to(dataProviderResourceId) })
      .singleOrNullIfEmpty() ?: return Timestamp.getDefaultInstance()

  return row.getTimestamp("CmmsCreateTime").toProto()
}

private object RequisitionMetadataEntity {
  val BASE_SQL =
    """
  SELECT
    DataProviderResourceId,
    RequisitionMetadataResourceId,
    RequisitionMetadataId,
    CreateRequestId,
    CmmsRequisition,
    BlobUri,
    BlobTypeUrl,
    GroupId,
    CmmsCreateTime,
    Report,
    State,
    WorkItem,
    CreateTime,
    UpdateTime,
    RefusalMessage,
  FROM
    RequisitionMetadata
  """
      .trimIndent()

  fun buildRequisitionMetadataResult(struct: Struct): RequisitionMetadataResult {
    return RequisitionMetadataResult(
      requisitionMetadata {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        requisitionMetadataResourceId = struct.getString("RequisitionMetadataResourceId")
        cmmsRequisition = struct.getString("CmmsRequisition")
        blobUri = struct.getString("BlobUri")
        blobTypeUrl = struct.getString("BlobTypeUrl")
        groupId = struct.getString("GroupId")
        cmmsCreateTime = struct.getTimestamp("CmmsCreateTime").toProto()
        report = struct.getString("Report")
        state = struct.getProtoEnum("State", State::forNumber)
        if (!struct.isNull("WorkItem")) {
          workItem = struct.getString("WorkItem")
        }
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        if (!struct.isNull("RefusalMessage")) {
          refusalMessage = struct.getString("RefusalMessage")
        }
        etag = ETags.computeETag(updateTime.toInstant())
      },
      struct.getLong("RequisitionMetadataId"),
    )
  }
}
