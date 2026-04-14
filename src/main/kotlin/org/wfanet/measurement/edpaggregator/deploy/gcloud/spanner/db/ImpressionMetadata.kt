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
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataNotFoundException
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.CreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataKey
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.impressionMetadata
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataKey

private const val IMPRESSION_METADATA_RESOURCE_ID_PREFIX = "imp"

data class ImpressionMetadataResult(
  val impressionMetadata: ImpressionMetadata,
  val impressionMetadataId: Long,
)

data class ModelLineBoundResult(val cmmsModelLine: String, val bound: Interval)

/** Resolved internal Spanner IDs for a RawImpressionMetadata FK reference. */
data class RawImpressionInternalKey(val batchId: Long, val fileId: Long)

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
      WHERE ImpressionMetadata.DataProviderResourceId = @dataProviderResourceId
      AND ImpressionMetadata.ImpressionMetadataResourceId = @impressionMetadataResourceId
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

/**
 * Reads multiple [ImpressionMetadata] entities by their public resource IDs.
 *
 * @returns a list of [ImpressionMetadataResult]s for the found entities
 */
suspend fun AsyncDatabaseClient.ReadContext.getImpressionMetadataByResourceIds(
  dataProviderResourceId: String,
  impressionMetadataResourceIds: List<String>,
): Map<String, ImpressionMetadataResult> {
  val sql = buildString {
    appendLine(ImpressionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE ImpressionMetadata.DataProviderResourceId = @dataProviderResourceId
      AND ImpressionMetadata.ImpressionMetadataResourceId IN UNNEST(@impressionMetadataResourceIds)
      """
        .trimIndent()
    )
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("impressionMetadataResourceIds").toStringArray(impressionMetadataResourceIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=getImpressionMetadataByResourceIds")).collect { row ->
      val result = ImpressionMetadataEntity.buildImpressionMetadataResult(row)
      put(row.getString("ImpressionMetadataResourceId"), result)
    }
  }
}

/**
 * Finds existing [ImpressionMetadata] for a list of create requests.
 *
 * @param dataProviderResourceId the resource ID of the parent DataProvider
 * @return a [Map] of create request ID to [ImpressionMetadataResult]
 */
suspend fun AsyncDatabaseClient.ReadContext.findExistingImpressionMetadataByRequestIds(
  dataProviderResourceId: String,
  requestIds: List<String>,
): Map<String, ImpressionMetadataResult> {
  val sql = buildString {
    appendLine(ImpressionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE ImpressionMetadata.DataProviderResourceId = @dataProviderResourceId
        AND ImpressionMetadata.CreateRequestId IS NOT NULL
        AND ImpressionMetadata.CreateRequestId IN UNNEST(@createRequestIds)
      """
        .trimIndent()
    )
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("createRequestIds").toStringArray(requestIds)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=findExistingImpressionMetadataByRequestIds")).collect {
      row ->
      val result = ImpressionMetadataEntity.buildImpressionMetadataResult(row)
      put(row.getString("CreateRequestId"), result)
    }
  }
}

/**
 * Finds existing [ImpressionMetadata] for a list of blob URIs.
 *
 * @param dataProviderResourceId the resource ID of the parent DataProvider
 * @param blobUris the list of blob URIs to check
 * @return a [Map] of blob URI to [ImpressionMetadataResult]
 */
suspend fun AsyncDatabaseClient.ReadContext.findExistingImpressionMetadataByBlobUris(
  dataProviderResourceId: String,
  blobUris: List<String>,
): Map<String, ImpressionMetadataResult> {
  val sql = buildString {
    appendLine(ImpressionMetadataEntity.BASE_SQL)
    appendLine(
      """
      WHERE ImpressionMetadata.DataProviderResourceId = @dataProviderResourceId
        AND ImpressionMetadata.BlobUri IN UNNEST(@blobUris)
      """
        .trimIndent()
    )
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("blobUris").toStringArray(blobUris)
    }

  return executeQuery(query, Options.tag("action=findExistingImpressionMetadataByBlobUris"))
    .map { ImpressionMetadataEntity.buildImpressionMetadataResult(it) }
    .toList()
    .associateBy { it.impressionMetadata.blobUri }
}

/**
 * Resolves resource IDs from a [RawImpressionMetadataKey] to internal Spanner IDs.
 *
 * @throws RawImpressionMetadataNotFoundException if the referenced RawImpressionMetadata does not
 *   exist
 */
suspend fun AsyncDatabaseClient.ReadContext.resolveRawImpressionInternalKey(
  dataProviderResourceId: String,
  key: RawImpressionMetadataKey,
): RawImpressionInternalKey {
  val sql =
    """
    SELECT
      RawImpressionMetadataBatch.BatchId,
      RawImpressionMetadataBatchFile.FileId,
    FROM
      RawImpressionMetadataBatch
      JOIN RawImpressionMetadataBatchFile USING (DataProviderResourceId, BatchId)
    WHERE
      RawImpressionMetadataBatch.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionMetadataBatch.BatchResourceId = @batchResourceId
      AND RawImpressionMetadataBatchFile.FileResourceId = @fileResourceId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("batchResourceId").to(key.batchResourceId)
          bind("fileResourceId").to(key.fileResourceId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw RawImpressionMetadataNotFoundException(
        dataProviderResourceId,
        key.batchResourceId,
        key.fileResourceId,
      )

  return RawImpressionInternalKey(batchId = row.getLong("BatchId"), fileId = row.getLong("FileId"))
}

/** Buffers an insert mutation for a [ImpressionMetadata] row. */
fun AsyncDatabaseClient.TransactionContext.insertImpressionMetadata(
  impressionMetadataId: Long,
  impressionMetadata: ImpressionMetadata,
  createRequestId: String,
  rawImpressionInternalKey: RawImpressionInternalKey? = null,
) {
  bufferInsertMutation("ImpressionMetadata") {
    set("DataProviderResourceId").to(impressionMetadata.dataProviderResourceId)
    set("ImpressionMetadataId").to(impressionMetadataId)
    set("ImpressionMetadataResourceId").to(impressionMetadata.impressionMetadataResourceId)
    if (createRequestId.isNotEmpty()) {
      set("CreateRequestId").to(createRequestId)
    }
    set("BlobUri").to(impressionMetadata.blobUri)
    set("BlobTypeUrl").to(impressionMetadata.blobTypeUrl)
    set("EventGroupReferenceId").to(impressionMetadata.eventGroupReferenceId)
    set("CmmsModelLine").to(impressionMetadata.cmmsModelLine)
    set("IntervalStartTime").to(impressionMetadata.interval.startTime.toGcloudTimestamp())
    set("IntervalEndTime").to(impressionMetadata.interval.endTime.toGcloudTimestamp())
    set("State").to(impressionMetadata.state)
    if (rawImpressionInternalKey != null) {
      set("RawImpressionBatchId").to(rawImpressionInternalKey.batchId)
      set("RawImpressionFileId").to(rawImpressionInternalKey.fileId)
    }
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Buffers a batch of [ImpressionMetadata] to be inserted in a single transaction.
 *
 * This will check for existence prior to insertion. If an entity with the same create_request_id
 * already exists and is ACTIVE, it will be returned as-is. If it exists but is DELETED, it will be
 * reactivated (state set back to ACTIVE). Similarly, if an entity with the same blob_uri exists and
 * is DELETED, it will be reactivated. If it exists and is ACTIVE, an ALREADY_EXISTS error is
 * thrown.
 *
 * For newly-created or reactivated entities, the `update_time` and "etag" fields will not be set in
 * the returned [ImpressionMetadata]. The caller is responsible for populating these from the commit
 * timestamp.
 *
 * @return a list of [ImpressionMetadata], containing newly created, reactivated, and existing
 *   entities.
 */
suspend fun AsyncDatabaseClient.TransactionContext.batchCreateImpressionMetadata(
  requests: List<CreateImpressionMetadataRequest>
): List<ImpressionMetadata> {
  if (requests.isEmpty()) {
    return emptyList()
  }

  val dataProviderResourceId = requests.first().impressionMetadata.dataProviderResourceId

  val existingRequestIdToImpressionMetadata: Map<String, ImpressionMetadataResult> =
    findExistingImpressionMetadataByRequestIds(
      dataProviderResourceId,
      requests.map { it.requestId },
    )

  val existingBlobUriToImpressionMetadata: Map<String, ImpressionMetadataResult> =
    findExistingImpressionMetadataByBlobUris(
      dataProviderResourceId,
      requests.map { it.impressionMetadata.blobUri },
    )

  // Reactivate DELETED records that were matched by requestId.
  val reactivatedByRequestId: Map<String, ImpressionMetadata> = buildMap {
    for ((requestId, result) in existingRequestIdToImpressionMetadata) {
      if (result.impressionMetadata.state == State.IMPRESSION_METADATA_STATE_DELETED) {
        updateImpressionMetadataState(
          dataProviderResourceId,
          result.impressionMetadataId,
          State.IMPRESSION_METADATA_STATE_ACTIVE,
        )
        put(
          requestId,
          result.impressionMetadata.copy {
            state = State.IMPRESSION_METADATA_STATE_ACTIVE
            clearUpdateTime()
            clearEtag()
          },
        )
      }
    }
  }

  val creations: List<ImpressionMetadata> =
    requests
      .filter { !existingRequestIdToImpressionMetadata.containsKey(it.requestId) }
      .map { request ->
        val existingByBlobUri =
          existingBlobUriToImpressionMetadata[request.impressionMetadata.blobUri]
        if (existingByBlobUri != null) {
          if (
            existingByBlobUri.impressionMetadata.state == State.IMPRESSION_METADATA_STATE_DELETED
          ) {
            // Reactivate the DELETED record found by blobUri.
            updateImpressionMetadataState(
              dataProviderResourceId,
              existingByBlobUri.impressionMetadataId,
              State.IMPRESSION_METADATA_STATE_ACTIVE,
            )
            return@map existingByBlobUri.impressionMetadata.copy {
              state = State.IMPRESSION_METADATA_STATE_ACTIVE
              clearUpdateTime()
              clearEtag()
            }
          }
          throw ImpressionMetadataAlreadyExistsException(request.impressionMetadata.blobUri)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }

        val impressionMetadataId =
          IdGenerator.Default.generateNewId { id ->
            impressionMetadataExists(request.impressionMetadata.dataProviderResourceId, id)
          }

        val impressionMetadataResourceId =
          request.impressionMetadata.impressionMetadataResourceId.ifBlank {
            "$IMPRESSION_METADATA_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
          }

        val rawImpressionInternalKey =
          if (request.impressionMetadata.hasRawImpressionMetadataKey()) {
            resolveRawImpressionInternalKey(
              dataProviderResourceId,
              request.impressionMetadata.rawImpressionMetadataKey,
            )
          } else {
            null
          }

        val created =
          request.impressionMetadata.copy {
            this.impressionMetadataResourceId = impressionMetadataResourceId
            state = State.IMPRESSION_METADATA_STATE_ACTIVE
          }

        insertImpressionMetadata(
          impressionMetadataId,
          created,
          request.requestId,
          rawImpressionInternalKey,
        )
        created
      }

  val newCreationIterator = creations.iterator()

  return requests.map {
    reactivatedByRequestId[it.requestId]
      ?: existingRequestIdToImpressionMetadata[it.requestId]?.impressionMetadata
      ?: newCreationIterator.next()
  }
}

/** Buffers an update to a [ImpressionMetadata] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updateImpressionMetadataState(
  dataProviderResourceId: String,
  impressionMetadataId: Long,
  state: State,
) {
  bufferUpdateMutation("ImpressionMetadata") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("ImpressionMetadataId").to(impressionMetadataId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Reads [ImpressionMetadata] ordered by resource ID. */
fun AsyncDatabaseClient.ReadContext.readImpressionMetadata(
  dataProviderResourceId: String,
  filter: ListImpressionMetadataRequest.Filter,
  limit: Int,
  after: ListImpressionMetadataPageToken.After? = null,
): Flow<ImpressionMetadataResult> {
  val sql = buildString {
    appendLine(ImpressionMetadataEntity.BASE_SQL)

    val conjuncts =
      mutableListOf("ImpressionMetadata.DataProviderResourceId = @dataProviderResourceId")

    if (filter.state != State.IMPRESSION_METADATA_STATE_UNSPECIFIED) {
      conjuncts.add("ImpressionMetadata.State = @state")
    }

    if (filter.cmmsModelLine.isNotEmpty()) {
      conjuncts.add("ImpressionMetadata.CmmsModelLine = @cmmsModelLine")
    }

    if (filter.eventGroupReferenceId.isNotEmpty()) {
      conjuncts.add("ImpressionMetadata.EventGroupReferenceId = @eventGroupReferenceId")
    }

    if (filter.hasIntervalOverlaps()) {
      conjuncts.add(
        "ImpressionMetadata.IntervalStartTime < @intervalOverlapsEndTime AND ImpressionMetadata.IntervalEndTime > @intervalOverlapsStartTime"
      )
    }

    if (filter.blobUriPrefix.isNotEmpty()) {
      conjuncts.add("STARTS_WITH(ImpressionMetadata.BlobUri, @blobUriPrefix)")
    }

    if (after != null) {
      conjuncts.add(
        "ImpressionMetadata.ImpressionMetadataResourceId > @afterImpressionMetadataResourceId"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY ImpressionMetadata.ImpressionMetadataResourceId ASC")
    appendLine("LIMIT @limit")
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())

      if (filter.state != State.IMPRESSION_METADATA_STATE_UNSPECIFIED) {
        bind("state").to(filter.state.number.toLong())
      }

      if (filter.cmmsModelLine.isNotEmpty()) {
        bind("cmmsModelLine").to(filter.cmmsModelLine)
      }

      if (filter.eventGroupReferenceId.isNotEmpty()) {
        bind("eventGroupReferenceId").to(filter.eventGroupReferenceId)
      }

      if (filter.hasIntervalOverlaps()) {
        bind("intervalOverlapsStartTime").to(filter.intervalOverlaps.startTime.toGcloudTimestamp())
        bind("intervalOverlapsEndTime").to(filter.intervalOverlaps.endTime.toGcloudTimestamp())
      }

      if (filter.blobUriPrefix.isNotEmpty()) {
        bind("blobUriPrefix").to(filter.blobUriPrefix)
      }

      if (after != null) {
        bind("afterImpressionMetadataResourceId").to(after.impressionMetadataResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readImpressionMetadata")).map { row ->
    ImpressionMetadataEntity.buildImpressionMetadataResult(row)
  }
}

suspend fun AsyncDatabaseClient.ReadContext.readModelLinesBounds(
  dataProviderResourceId: String
): List<ModelLineBoundResult> {
  val sql =
    """
      SELECT
        DataProviderResourceId,
        CmmsModelLine,
        MIN(IntervalStartTime) AS StartTime,
        MAX(IntervalEndTime) AS EndTime
      FROM
        ImpressionMetadata
      WHERE
        DataProviderResourceId = @dataProviderResourceId
        AND State = @state
      GROUP BY
        DataProviderResourceId,
        CmmsModelLine
    """
      .trimIndent()
  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("state").to(State.IMPRESSION_METADATA_STATE_ACTIVE.number.toLong())
    }
  return executeQuery(query, Options.tag("action=readModelLinesBounds"))
    .map { row ->
      ModelLineBoundResult(
        row.getString("CmmsModelLine"),
        interval {
          startTime = row.getTimestamp("StartTime").toProto()
          endTime = row.getTimestamp("EndTime").toProto()
        },
      )
    }
    .toList()
}

private object ImpressionMetadataEntity {
  val BASE_SQL =
    """
    SELECT
      ImpressionMetadata.DataProviderResourceId,
      ImpressionMetadata.ImpressionMetadataId,
      ImpressionMetadata.ImpressionMetadataResourceId,
      ImpressionMetadata.CreateRequestId,
      ImpressionMetadata.BlobUri,
      ImpressionMetadata.BlobTypeUrl,
      ImpressionMetadata.EventGroupReferenceId,
      ImpressionMetadata.CmmsModelLine,
      ImpressionMetadata.IntervalStartTime,
      ImpressionMetadata.IntervalEndTime,
      ImpressionMetadata.State,
      ImpressionMetadata.CreateTime,
      ImpressionMetadata.UpdateTime,
      RawImpressionMetadataBatch.BatchResourceId AS RawImpressionBatchResourceId,
      RawImpressionMetadataBatchFile.FileResourceId AS RawImpressionFileResourceId,
    FROM
      ImpressionMetadata
    LEFT JOIN (
      RawImpressionMetadataBatchFile
      JOIN RawImpressionMetadataBatch USING (DataProviderResourceId, BatchId)
    ) ON ImpressionMetadata.DataProviderResourceId = RawImpressionMetadataBatchFile.DataProviderResourceId
      AND ImpressionMetadata.RawImpressionBatchId = RawImpressionMetadataBatchFile.BatchId
      AND ImpressionMetadata.RawImpressionFileId = RawImpressionMetadataBatchFile.FileId
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
        if (!struct.isNull("RawImpressionBatchResourceId")) {
          rawImpressionMetadataKey = rawImpressionMetadataKey {
            batchResourceId = struct.getString("RawImpressionBatchResourceId")
            fileResourceId = struct.getString("RawImpressionFileResourceId")
          }
        }
      },
      struct.getLong("ImpressionMetadataId"),
    )
  }
}
