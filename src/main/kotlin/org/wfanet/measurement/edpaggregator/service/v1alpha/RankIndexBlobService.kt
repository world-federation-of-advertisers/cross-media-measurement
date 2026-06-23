/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.IOException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RankIndexBlobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.GetRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.internal.edpaggregator.BlobType as InternalBlobType
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek as InternalEncryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsPageToken as InternalListPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsRequestKt as InternalListRankIndexBlobsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsResponse as InternalListResponse
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlob as InternalRankIndexBlob
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub as InternalRankIndexBlobServiceStub
import org.wfanet.measurement.internal.edpaggregator.batchCreateRankIndexBlobsRequest as internalBatchCreateRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRankIndexBlobsRequest as internalBatchDeleteRequest
import org.wfanet.measurement.internal.edpaggregator.createRankIndexBlobRequest as internalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRankIndexBlobRequest as internalDeleteRequest
import org.wfanet.measurement.internal.edpaggregator.encryptedDek as internalEncryptedDek
import org.wfanet.measurement.internal.edpaggregator.getRankIndexBlobRequest as internalGetRequest
import org.wfanet.measurement.internal.edpaggregator.listRankIndexBlobsRequest as internalListRequest
import org.wfanet.measurement.internal.edpaggregator.rankIndexBlob as internalRankIndexBlob

/**
 * Public v1alpha implementation of the [RankIndexBlob] service.
 *
 * Validates and translates public RankIndexBlob API requests into the internal RankIndexBlob
 * service, mapping resource names to internal resource IDs and internal error reasons to gRPC
 * statuses.
 */
class RankIndexBlobService(
  private val internalRankIndexBlobStub: InternalRankIndexBlobServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RankIndexBlobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRankIndexBlob(request: CreateRankIndexBlobRequest): RankIndexBlob {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    validateCreateRequest(request, "")

    val internalResponse: InternalRankIndexBlob =
      try {
        internalRankIndexBlobStub.createRankIndexBlob(
          internalCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            rankIndexBlob = request.rankIndexBlob.toInternal()
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchCreateRankIndexBlobs(
    request: BatchCreateRankIndexBlobsRequest
  ): BatchCreateRankIndexBlobsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.requestsList.isEmpty()) {
      throw RequiredFieldNotSetException("requests")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.size > MAX_BATCH_CREATE_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_CREATE_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val seenRequestIds = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, createRequest ->
      if (createRequest.parent.isNotEmpty() && createRequest.parent != request.parent) {
        throw InvalidFieldValueException("requests.$index.parent") {
            "$it does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      validateCreateRequest(createRequest, "requests.$index.")
      if (!seenRequestIds.add(createRequest.requestId)) {
        throw InvalidFieldValueException("requests.$index.request_id") {
            "$it is duplicate in the batch of requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val internalResponse =
      try {
        internalRankIndexBlobStub.batchCreateRankIndexBlobs(
          internalBatchCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests +=
              request.requestsList.map { createRequest ->
                internalCreateRequest {
                  dataProviderResourceId = uploadKey.dataProviderId
                  rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
                  rankIndexBlob = createRequest.rankIndexBlob.toInternal()
                  requestId = createRequest.requestId
                }
              }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return batchCreateRankIndexBlobsResponse {
      rankIndexBlobs += internalResponse.rankIndexBlobsList.map { it.toPublic() }
    }
  }

  override suspend fun getRankIndexBlob(request: GetRankIndexBlobRequest): RankIndexBlob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      RankIndexBlobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRankIndexBlob =
      try {
        internalRankIndexBlobStub.getRankIndexBlob(
          internalGetRequest {
            dataProviderResourceId = key.dataProviderId
            rawImpressionUploadResourceId = key.rawImpressionUploadId
            rankIndexBlobResourceId = key.rankIndexBlobId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun listRankIndexBlobs(
    request: ListRankIndexBlobsRequest
  ): ListRankIndexBlobsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    // Coerce page size at the public layer (the validation boundary): 0 -> default, capped at max.
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    if (request.hasFilter() && request.filter.blobType == RankIndexBlob.BlobType.UNRECOGNIZED) {
      throw InvalidFieldValueException("filter.blob_type")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalPageToken: InternalListPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListResponse =
      try {
        internalRankIndexBlobStub.listRankIndexBlobs(
          internalListRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId =
              if (uploadKey.rawImpressionUploadId == ResourceKey.WILDCARD_ID) ""
              else uploadKey.rawImpressionUploadId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            showDeleted = request.showDeleted
            if (request.hasFilter()) {
              filter =
                InternalListRankIndexBlobsRequestKt.filter {
                  if (request.filter.blobType != RankIndexBlob.BlobType.BLOB_TYPE_UNSPECIFIED) {
                    blobType = request.filter.blobType.toInternal()
                  }
                  if (request.filter.cmmsModelLine.isNotEmpty()) {
                    cmmsModelLine = request.filter.cmmsModelLine
                  }
                  if (request.filter.hasPoolOffset()) {
                    poolOffset = request.filter.poolOffset
                  }
                  if (request.filter.hasMaxEventDateOnOrBefore()) {
                    maxEventDateOnOrBefore = request.filter.maxEventDateOnOrBefore
                  }
                }
            }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return listRankIndexBlobsResponse {
      rankIndexBlobs += internalResponse.rankIndexBlobsList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun deleteRankIndexBlob(request: DeleteRankIndexBlobRequest): RankIndexBlob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      RankIndexBlobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRankIndexBlob =
      try {
        internalRankIndexBlobStub.deleteRankIndexBlob(
          internalDeleteRequest {
            dataProviderResourceId = key.dataProviderId
            rawImpressionUploadResourceId = key.rawImpressionUploadId
            rankIndexBlobResourceId = key.rankIndexBlobId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchDeleteRankIndexBlobs(
    request: BatchDeleteRankIndexBlobsRequest
  ): BatchDeleteRankIndexBlobsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.requestsList.isEmpty()) {
      throw RequiredFieldNotSetException("requests")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.size > MAX_BATCH_DELETE_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_DELETE_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val seenNames = mutableSetOf<String>()
    val internalDeleteRequests =
      request.requestsList.mapIndexed { index, deleteRequest ->
        if (deleteRequest.name.isEmpty()) {
          throw RequiredFieldNotSetException("requests.$index.name")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
        val key =
          RankIndexBlobKey.fromName(deleteRequest.name)
            ?: throw InvalidFieldValueException("requests.$index.name")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        if (
          key.dataProviderId != uploadKey.dataProviderId ||
            key.rawImpressionUploadId != uploadKey.rawImpressionUploadId
        ) {
          throw InvalidFieldValueException("requests.$index.name") {
              "$it does not match the parent"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
        if (!seenNames.add(deleteRequest.name)) {
          throw InvalidFieldValueException("requests.$index.name") {
              "$it is duplicate in the batch of requests"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
        internalDeleteRequest {
          dataProviderResourceId = key.dataProviderId
          rawImpressionUploadResourceId = key.rawImpressionUploadId
          rankIndexBlobResourceId = key.rankIndexBlobId
        }
      }

    val internalResponse =
      try {
        internalRankIndexBlobStub.batchDeleteRankIndexBlobs(
          internalBatchDeleteRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests += internalDeleteRequests
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return batchDeleteRankIndexBlobsResponse {
      rankIndexBlobs += internalResponse.rankIndexBlobsList.map { it.toPublic() }
    }
  }

  /** Validates the payload and required `request_id` of a [CreateRankIndexBlobRequest]. */
  private fun validateCreateRequest(request: CreateRankIndexBlobRequest, fieldPathPrefix: String) {
    if (!request.hasRankIndexBlob()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankIndexBlob.blobType == RankIndexBlob.BlobType.BLOB_TYPE_UNSPECIFIED) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.blob_type")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankIndexBlob.blobType == RankIndexBlob.BlobType.UNRECOGNIZED) {
      throw InvalidFieldValueException("${fieldPathPrefix}rank_index_blob.blob_type")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankIndexBlob.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankIndexBlob.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.blob_uri")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!request.rankIndexBlob.hasEncryptedDek()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}rank_index_blob.encrypted_dek")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    // request_id is REQUIRED on every create so retries are safe (AIP-155).
    if (request.requestId.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("${fieldPathPrefix}request_id", e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  companion object {
    private const val MAX_BATCH_CREATE_SIZE = 50
    private const val MAX_BATCH_DELETE_SIZE = 1000
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100

    private fun handleInternalError(e: StatusException): StatusRuntimeException {
      return when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.RANK_INDEX_BLOB_NOT_FOUND,
        InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND ->
          Status.NOT_FOUND.withCause(e).asRuntimeException()
        InternalErrors.Reason.RANK_INDEX_BLOB_ALREADY_EXISTS ->
          Status.ALREADY_EXISTS.withCause(e).asRuntimeException()
        InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
        InternalErrors.Reason.INVALID_FIELD_VALUE ->
          Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
        InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
        InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
        InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
        InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
        InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND,
        InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID,
        InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND,
        InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
        InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
        InternalErrors.Reason.ETAG_MISMATCH,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }
}

/** Converts an internal [InternalRankIndexBlob] to a public [RankIndexBlob]. */
internal fun InternalRankIndexBlob.toPublic(): RankIndexBlob {
  val source = this
  return rankIndexBlob {
    name =
      RankIndexBlobKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.rankIndexBlobResourceId,
        )
        .toName()
    blobType = source.blobType.toPublic()
    cmmsModelLine = source.cmmsModelLine
    poolOffset = source.poolOffset
    blobUri = source.blobUri
    if (!source.blobChecksum.isEmpty()) {
      blobChecksum = source.blobChecksum
    }
    encryptedDek = source.encryptedDek.toPublic()
    if (source.hasMaxEventDate()) {
      maxEventDate = source.maxEventDate
    }
    createTime = source.createTime
    if (source.hasDeleteTime()) {
      deleteTime = source.deleteTime
    }
  }
}

/** Converts a public [RankIndexBlob] to an internal [InternalRankIndexBlob] for creation. */
internal fun RankIndexBlob.toInternal(): InternalRankIndexBlob {
  val source = this
  return internalRankIndexBlob {
    blobType = source.blobType.toInternal()
    cmmsModelLine = source.cmmsModelLine
    poolOffset = source.poolOffset
    blobUri = source.blobUri
    if (!source.blobChecksum.isEmpty()) {
      blobChecksum = source.blobChecksum
    }
    encryptedDek = source.encryptedDek.toInternal()
    if (source.hasMaxEventDate()) {
      maxEventDate = source.maxEventDate
    }
  }
}

/** Converts an internal [InternalBlobType] to a public [RankIndexBlob.BlobType]. */
internal fun InternalBlobType.toPublic(): RankIndexBlob.BlobType {
  return when (this) {
    InternalBlobType.BLOB_TYPE_DAY_ONLY -> RankIndexBlob.BlobType.DAY_ONLY
    InternalBlobType.BLOB_TYPE_SNAPSHOT -> RankIndexBlob.BlobType.SNAPSHOT
    InternalBlobType.BLOB_TYPE_UNSPECIFIED -> RankIndexBlob.BlobType.BLOB_TYPE_UNSPECIFIED
    InternalBlobType.UNRECOGNIZED -> error("Unrecognized blob type")
  }
}

/** Converts a public [RankIndexBlob.BlobType] to an internal [InternalBlobType]. */
internal fun RankIndexBlob.BlobType.toInternal(): InternalBlobType {
  return when (this) {
    RankIndexBlob.BlobType.DAY_ONLY -> InternalBlobType.BLOB_TYPE_DAY_ONLY
    RankIndexBlob.BlobType.SNAPSHOT -> InternalBlobType.BLOB_TYPE_SNAPSHOT
    RankIndexBlob.BlobType.BLOB_TYPE_UNSPECIFIED -> InternalBlobType.BLOB_TYPE_UNSPECIFIED
    RankIndexBlob.BlobType.UNRECOGNIZED -> error("Unrecognized blob type")
  }
}

/** Converts an internal [InternalEncryptedDek] to a public [EncryptedDek]. */
internal fun InternalEncryptedDek.toPublic(): EncryptedDek {
  val source = this
  return encryptedDek {
    kekUri = source.kekUri
    typeUrl = source.typeUrl
    protobufFormat = source.protobufFormat.toPublicProtobufFormat()
    ciphertext = source.ciphertext
  }
}

/** Converts a public [EncryptedDek] to an internal [InternalEncryptedDek]. */
internal fun EncryptedDek.toInternal(): InternalEncryptedDek {
  val source = this
  return internalEncryptedDek {
    kekUri = source.kekUri
    typeUrl = source.typeUrl
    protobufFormat = source.protobufFormat.toInternalProtobufFormat()
    ciphertext = source.ciphertext
  }
}

private fun InternalEncryptedDek.ProtobufFormat.toPublicProtobufFormat():
  EncryptedDek.ProtobufFormat {
  return when (this) {
    InternalEncryptedDek.ProtobufFormat.BINARY -> EncryptedDek.ProtobufFormat.BINARY
    InternalEncryptedDek.ProtobufFormat.JSON -> EncryptedDek.ProtobufFormat.JSON
    InternalEncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED ->
      EncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED
    InternalEncryptedDek.ProtobufFormat.UNRECOGNIZED -> error("Unrecognized protobuf format")
  }
}

private fun EncryptedDek.ProtobufFormat.toInternalProtobufFormat():
  InternalEncryptedDek.ProtobufFormat {
  return when (this) {
    EncryptedDek.ProtobufFormat.BINARY -> InternalEncryptedDek.ProtobufFormat.BINARY
    EncryptedDek.ProtobufFormat.JSON -> InternalEncryptedDek.ProtobufFormat.JSON
    EncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED ->
      InternalEncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED
    EncryptedDek.ProtobufFormat.UNRECOGNIZED -> error("Unrecognized protobuf format")
  }
}
