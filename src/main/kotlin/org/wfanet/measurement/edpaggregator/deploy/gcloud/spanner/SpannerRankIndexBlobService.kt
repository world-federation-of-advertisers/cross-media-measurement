/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankIndexBlobResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRankIndexBlobByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRankIndexBlobsByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRankIndexBlobByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRankIndexBlobsByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadIdForRankIndexBlob
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRankIndexBlob
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rankIndexBlobExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRankIndexBlobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.softDeleteRankIndexBlob
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RankIndexBlobAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RankIndexBlobNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRankIndexBlobsResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRankIndexBlobsResponse
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.CreateRankIndexBlobRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteRankIndexBlobRequest
import org.wfanet.measurement.internal.edpaggregator.GetRankIndexBlobRequest
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsResponse
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlob
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.batchCreateRankIndexBlobsResponse
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRankIndexBlobsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRankIndexBlobsPageToken
import org.wfanet.measurement.internal.edpaggregator.listRankIndexBlobsResponse

/**
 * Cloud Spanner implementation of the internal [RankIndexBlob] service.
 *
 * Persists RankIndexBlob rows interleaved under their parent RawImpressionUpload. Blobs are
 * immutable apart from soft-deletion (the `DeleteTime` column), with `request_id` idempotency on
 * creation.
 */
class SpannerRankIndexBlobService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RankIndexBlobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRankIndexBlob(request: CreateRankIndexBlobRequest): RankIndexBlob {
    try {
      validateCreateRequest(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRankIndexBlob"))
    val createdBlob: RankIndexBlob =
      try {
        transactionRunner.run { txn ->
          val existing =
            txn.findRankIndexBlobByRequestId(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestId,
            )
          if (existing != null) {
            return@run existing.rankIndexBlob
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForRankIndexBlob(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          insertNewRankIndexBlob(
            txn,
            dataProviderResourceId,
            rawImpressionUploadResourceId,
            rawImpressionUploadId,
            request.requestId,
            request.rankIndexBlob,
          )
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RankIndexBlobAlreadyExistsException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              e,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    return if (createdBlob.hasCreateTime()) {
      createdBlob
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      createdBlob.copy { createTime = commitTimestamp }
    }
  }

  override suspend fun batchCreateRankIndexBlobs(
    request: BatchCreateRankIndexBlobsRequest
  ): BatchCreateRankIndexBlobsResponse {
    if (request.requestsList.size > MAX_BATCH_CREATE_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_CREATE_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.isEmpty()) {
      return BatchCreateRankIndexBlobsResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId

    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val requestIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      if (
        subRequest.dataProviderResourceId.isNotEmpty() &&
          subRequest.dataProviderResourceId != dataProviderResourceId
      ) {
        throw InvalidFieldValueException("requests[$index].data_provider_resource_id") {
            "$it does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (
        subRequest.rawImpressionUploadResourceId.isNotEmpty() &&
          subRequest.rawImpressionUploadResourceId != rawImpressionUploadResourceId
      ) {
        throw InvalidFieldValueException("requests[$index].raw_impression_upload_resource_id") {
            "$it does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      try {
        validateRankIndexBlob(subRequest, "requests[$index].")
      } catch (e: RequiredFieldNotSetException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      val requestId = subRequest.requestId
      if (!requestIdSet.add(requestId)) {
        throw InvalidFieldValueException("requests[$index].request_id") {
            "Duplicate request_id $requestId in batch"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreateRankIndexBlobs"))

    val results: List<RankIndexBlob> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForRankIndexBlob(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val existingByRequestId: Map<String, RankIndexBlobResult> =
            txn.findRankIndexBlobsByRequestIds(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestsList.map { it.requestId },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (existing != null) {
              existing.rankIndexBlob
            } else {
              insertNewRankIndexBlob(
                txn,
                dataProviderResourceId,
                rawImpressionUploadResourceId,
                rawImpressionUploadId,
                subRequest.requestId,
                subRequest.rankIndexBlob,
              )
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RankIndexBlobAlreadyExistsException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              e,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateRankIndexBlobsResponse {
      rankIndexBlobs +=
        results.map { result ->
          if (result.hasCreateTime()) result else result.copy { createTime = commitTimestamp }
        }
    }
  }

  override suspend fun getRankIndexBlob(request: GetRankIndexBlobRequest): RankIndexBlob {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankIndexBlobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("rank_index_blob_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn
        .getRankIndexBlobByResourceId(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.rankIndexBlobResourceId,
        )
        ?.rankIndexBlob
        ?: throw RankIndexBlobNotFoundException(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.rankIndexBlobResourceId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRankIndexBlobs(
    request: ListRankIndexBlobsRequest
  ): ListRankIndexBlobsResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val rows: Flow<RankIndexBlobResult> =
        txn.readRankIndexBlobs(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId.ifEmpty { null },
          filter = if (request.hasFilter()) request.filter else null,
          showDeleted = request.showDeleted,
          limit = pageSize + 1,
          after = after,
        )

      return listRankIndexBlobsResponse {
        rows.collectIndexed { index, result ->
          if (index == pageSize) {
            val lastIncluded = rankIndexBlobs.last()
            nextPageToken = listRankIndexBlobsPageToken {
              this.after =
                ListRankIndexBlobsPageTokenKt.after {
                  createTime = lastIncluded.createTime
                  rankIndexBlobResourceId = lastIncluded.rankIndexBlobResourceId
                }
            }
          } else {
            rankIndexBlobs += result.rankIndexBlob
          }
        }
      }
    }
  }

  override suspend fun deleteRankIndexBlob(request: DeleteRankIndexBlobRequest): RankIndexBlob {
    validateDeleteRequest(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.rankIndexBlobResourceId,
    )

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=deleteRankIndexBlob"))

    val deletedBlob: RankIndexBlob =
      transactionRunner.run { txn ->
        deleteOneRankIndexBlob(
          txn,
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.rankIndexBlobResourceId,
        )
      }

    // If the row was already soft-deleted, deleteOneRankIndexBlob returns it unchanged (with its
    // original delete_time) for idempotency; otherwise stamp the new delete_time from the commit.
    return if (deletedBlob.hasDeleteTime()) {
      deletedBlob
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      deletedBlob.copy { deleteTime = commitTimestamp }
    }
  }

  override suspend fun batchDeleteRankIndexBlobs(
    request: BatchDeleteRankIndexBlobsRequest
  ): BatchDeleteRankIndexBlobsResponse {
    if (request.requestsList.size > MAX_BATCH_DELETE_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_DELETE_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.isEmpty()) {
      return BatchDeleteRankIndexBlobsResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val resourceIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      if (subRequest.rankIndexBlobResourceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests[$index].rank_index_blob_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (
        subRequest.dataProviderResourceId.isNotEmpty() &&
          subRequest.dataProviderResourceId != dataProviderResourceId
      ) {
        throw InvalidFieldValueException("requests[$index].data_provider_resource_id") {
            "$it does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (
        subRequest.rawImpressionUploadResourceId.isNotEmpty() &&
          subRequest.rawImpressionUploadResourceId != rawImpressionUploadResourceId
      ) {
        throw InvalidFieldValueException("requests[$index].raw_impression_upload_resource_id") {
            "$it does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!resourceIdSet.add(subRequest.rankIndexBlobResourceId)) {
        throw InvalidFieldValueException("requests[$index].rank_index_blob_resource_id") {
            "Duplicate rank_index_blob_resource_id ${subRequest.rankIndexBlobResourceId} in batch"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchDeleteRankIndexBlobs"))

    val deletedBlobs: List<RankIndexBlob> =
      transactionRunner.run { txn ->
        val byResourceId: Map<String, RankIndexBlobResult> =
          txn.getRankIndexBlobsByResourceIds(
            dataProviderResourceId,
            rawImpressionUploadResourceId,
            request.requestsList.map { it.rankIndexBlobResourceId },
          )

        request.requestsList.map { subRequest ->
          val result =
            byResourceId[subRequest.rankIndexBlobResourceId]
              ?: throw RankIndexBlobNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                  subRequest.rankIndexBlobResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)
          // Idempotent (AIP-135): an already soft-deleted row is returned unchanged (with its
          // original delete_time) rather than treated as not-found; only a row that never existed
          // is NOT_FOUND.
          if (!result.rankIndexBlob.hasDeleteTime()) {
            txn.softDeleteRankIndexBlob(
              dataProviderResourceId,
              result.rawImpressionUploadId,
              result.rankIndexBlobId,
            )
          }
          result.rankIndexBlob
        }
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchDeleteRankIndexBlobsResponse {
      // Newly soft-deleted rows take the commit timestamp; already-deleted rows (idempotent
      // replays) keep their original delete_time.
      rankIndexBlobs +=
        deletedBlobs.map { blob ->
          if (blob.hasDeleteTime()) blob else blob.copy { deleteTime = commitTimestamp }
        }
    }
  }

  /**
   * Inserts a new [RankIndexBlob] row within [txn] and returns the corresponding resource with its
   * internal/parent IDs populated but `create_time` left unset (filled by the caller from the
   * commit timestamp).
   */
  private suspend fun insertNewRankIndexBlob(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    rawImpressionUploadId: Long,
    requestId: String,
    rankIndexBlob: RankIndexBlob,
  ): RankIndexBlob {
    val rankIndexBlobId =
      idGenerator.generateNewId { id ->
        txn.rankIndexBlobExists(dataProviderResourceId, rawImpressionUploadId, id)
      }
    val resourceId = "$RANK_INDEX_BLOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"

    txn.insertRankIndexBlob(
      rawImpressionUploadId = rawImpressionUploadId,
      rankIndexBlobId = rankIndexBlobId,
      rankIndexBlobResourceId = resourceId,
      dataProviderResourceId = dataProviderResourceId,
      createRequestId = requestId,
      rankIndexBlob = rankIndexBlob,
    )

    return rankIndexBlob.copy {
      this.dataProviderResourceId = dataProviderResourceId
      this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
      rankIndexBlobResourceId = resourceId
      clearCreateTime()
      clearDeleteTime()
    }
  }

  /** Soft-deletes a single [RankIndexBlob] within [txn], returning the pre-delete resource. */
  private suspend fun deleteOneRankIndexBlob(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    rankIndexBlobResourceId: String,
  ): RankIndexBlob {
    val result =
      txn.getRankIndexBlobByResourceId(
        dataProviderResourceId,
        rawImpressionUploadResourceId,
        rankIndexBlobResourceId,
      )
        ?: throw RankIndexBlobNotFoundException(
            dataProviderResourceId,
            rawImpressionUploadResourceId,
            rankIndexBlobResourceId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)

    // Idempotent (AIP-135): a row that is already soft-deleted is returned unchanged with its
    // original delete_time, rather than treated as not-found.
    if (result.rankIndexBlob.hasDeleteTime()) {
      return result.rankIndexBlob
    }

    txn.softDeleteRankIndexBlob(
      dataProviderResourceId,
      result.rawImpressionUploadId,
      result.rankIndexBlobId,
    )
    return result.rankIndexBlob
  }

  private fun validateCreateRequest(request: CreateRankIndexBlobRequest) {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
    }
    validateRankIndexBlob(request, "")
  }

  /** Validates the [RankIndexBlob] payload and required `request_id` of a create sub-request. */
  private fun validateRankIndexBlob(request: CreateRankIndexBlobRequest, fieldPathPrefix: String) {
    if (!request.hasRankIndexBlob()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob")
    }
    val rankIndexBlob = request.rankIndexBlob
    if (rankIndexBlob.blobType == BlobType.BLOB_TYPE_UNSPECIFIED) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.blob_type")
    }
    if (rankIndexBlob.blobType == BlobType.UNRECOGNIZED) {
      throw InvalidFieldValueException("${fieldPathPrefix}rank_index_blob.blob_type")
    }
    if (rankIndexBlob.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.cmms_model_line")
    }
    if (rankIndexBlob.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.blob_uri")
    }
    if (!rankIndexBlob.hasEncryptedDek()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.encrypted_dek")
    }
    if (request.requestId.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}request_id")
    }
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("${fieldPathPrefix}request_id", e)
    }
  }

  private fun validateDeleteRequest(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    rankIndexBlobResourceId: String,
  ) {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rankIndexBlobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("rank_index_blob_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  companion object {
    private const val RANK_INDEX_BLOB_RESOURCE_ID_PREFIX = "rankIndexBlob"
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_BATCH_CREATE_SIZE = 50
    private const val MAX_BATCH_DELETE_SIZE = 1000
  }
}
