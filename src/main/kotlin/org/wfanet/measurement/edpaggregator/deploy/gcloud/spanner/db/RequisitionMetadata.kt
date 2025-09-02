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

import com.google.cloud.spanner.Struct
import kotlin.text.trimIndent
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata

data class RequisitionMetadataResult(
  val dataProviderResourceId: String,
  val requisitionMetadataResourceId: String,
  val requisitionMetadata: RequisitionMetadata,
)

/**
 * Reads a [RequisitionMetadata] by its public resource ID.
 *
 * @return The [RequisitionMetadataResult] or null if not found.
 */
suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByResourceId(
  dataProviderResourceId: String,
  requisitionMetadataResourceId: String,
): RequisitionMetadataResult? {
  val sql = buildString {
    appendLine(RequisitionMetadata.BASE_SQL)
    appendLine(
      """
      $READER_BASE_SQL
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
      .singleOrNullIfEmpty() ?: return null

  return RequisitionMetadataResult(
    row.getString("DataProviderResourceId"),
    row.getString("RequisitionMetadataResourceId"),
    buildRequisitionMetadata(row),
  )
}

suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByCmmsRequisition(
  dataProviderResourceId: String,
  cmmsRequisition: String,
): RequisitionMetadataResult? {
  val sql = buildString {
    appendLine(READER_BASE_SQL)
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
      .singleOrNullIfEmpty() ?: return null

  return RequisitionMetadataResult(
    row.getString("DataProviderResourceId"),
    row.getString("RequisitionMetadataResourceId"),
    buildRequisitionMetadata(row),
  )
}

suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByBlobUri(
  dataProviderResourceId: String,
  blobUri: String,
): RequisitionMetadataResult? {
  val sql = buildString {
    appendLine(READER_BASE_SQL)
    appendLine(
      """
      WHERE DataProviderResourceId = @dataProviderResourceId
        AND BlobUri = @blobUri
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("blobUri").to(blobUri)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return RequisitionMetadataResult(
    row.getString("DataProviderResourceId"),
    row.getString("RequisitionMetadataResourceId"),
    buildRequisitionMetadata(row),
  )
}

suspend fun AsyncDatabaseClient.ReadContext.getRequisitionMetadataByCreateRequestId(
  dataProviderResourceId: String,
  createRequestId: String,
): RequisitionMetadataResult? {
  val sql = buildString {
    appendLine(READER_BASE_SQL)
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

  return RequisitionMetadataResult(
    row.getString("DataProviderResourceId"),
    row.getString("RequisitionMetadataResourceId"),
    buildRequisitionMetadata(row),
  )
}

private object RequisitionMetadata {
  private val BASE_SQL =
    """
  SELECT
    DataProviderResourceId,
    RequisitionMetadataResourceId,
    CmmsRequisition,
    BlobUri,
    GroupId,
    CmmsCreateTime,
    Report,
    State,
    WorkItem,
    CreateTime,
    UpdateTime,
    RefusalMessage
  FROM
  """
      .trimIndent()

  private fun buildRequisitionMetadata(struct: Struct): RequisitionMetadataResult {
    return RequisitionMetadataResult(
      struct.getString("DataProviderResourceId"),
      struct.getString("RequisitionMetadataResourceId"),
      requisitionMetadata {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        requisitionMetadataResourceId = struct.getString("RequisitionMetadataResourceId")
        cmmsRequisition = struct.getString("CmmsRequisition")
        blobUri = struct.getString("BlobUri")
        groupId = struct.getString("GroupId")
        cmmsCreateTime = struct.getTimestamp("CmmsCreateTime").toProto()
        report = struct.getString("Report")
        state = struct.getProtoEnum("State", RequisitionMetadataState::forNumber)
        if (!struct.isNull("WorkItem")) {
          workItem = struct.getString("WorkItem")
        }
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        if (!struct.isNull("RefusalMessage")) {
          refusalMessage = struct.getString("RefusalMessage")
        }
      },
    )
  }
}
