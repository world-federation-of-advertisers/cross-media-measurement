// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadFileKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadFileNotFoundException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionUploadFilesResponse as InternalBatchCreateFilesResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionUploadFilesResponse as InternalBatchDeleteFilesResponse
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageToken as InternalListFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesRequestKt as InternalListFilesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesResponse as InternalListFilesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFile as InternalRawImpressionUploadFile
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub as InternalFileServiceStub
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionUploadFilesRequest as internalBatchCreateFilesRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRawImpressionUploadFilesRequest as internalBatchDeleteFilesRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadFileRequest as internalCreateFileRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRawImpressionUploadFileRequest as internalDeleteFileRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadFileRequest as internalGetFileRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesRequest as internalListFilesRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadFile as internalFile

class RawImpressionUploadFileService(
  private val internalFileStub: InternalFileServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionUploadFileServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUploadFile(
    request: CreateRawImpressionUploadFileRequest
  ): RawImpressionUploadFile {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasRawImpressionUploadFile()) {
      throw RequiredFieldNotSetException("raw_impression_upload_file")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.rawImpressionUploadFile.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_file.blob_uri")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val internalResponse: InternalRawImpressionUploadFile =
      try {
        internalFileStub.createRawImpressionUploadFile(
          internalCreateFileRequest {
            rawImpressionUploadFile = internalFile {
              dataProviderResourceId = uploadKey.dataProviderId
              rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
              blobUri = request.rawImpressionUploadFile.blobUri
            }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw mapCreateError(e, request.parent)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchCreateRawImpressionUploadFiles(
    request: BatchCreateRawImpressionUploadFilesRequest
  ): BatchCreateRawImpressionUploadFilesResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val requestIdSet = mutableSetOf<String>()

    val internalRequests =
      request.requestsList.mapIndexed { index, childRequest ->
        if (childRequest.parent.isNotEmpty() && childRequest.parent != request.parent) {
          throw InvalidFieldValueException("requests.$index.parent") {
              "Parent ${childRequest.parent} does not match top-level parent ${request.parent}"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (!childRequest.hasRawImpressionUploadFile()) {
          throw RequiredFieldNotSetException("requests.$index.raw_impression_upload_file")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val blobUri = childRequest.rawImpressionUploadFile.blobUri
        if (blobUri.isEmpty()) {
          throw RequiredFieldNotSetException("requests.$index.raw_impression_upload_file.blob_uri")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val requestId = childRequest.requestId
        if (requestId.isNotEmpty()) {
          try {
            UUID.fromString(requestId)
          } catch (e: IllegalArgumentException) {
            throw InvalidFieldValueException("requests.$index.request_id", e)
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }
          if (!requestIdSet.add(requestId)) {
            throw InvalidFieldValueException("requests.$index.request_id") {
                "request Id $requestId is duplicate in the batch of requests"
              }
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }
        }

        internalCreateFileRequest {
          rawImpressionUploadFile = internalFile {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            this.blobUri = blobUri
          }
          this.requestId = requestId
        }
      }

    val internalResponse: InternalBatchCreateFilesResponse =
      try {
        internalFileStub.batchCreateRawImpressionUploadFiles(
          internalBatchCreateFilesRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests += internalRequests
          }
        )
      } catch (e: StatusException) {
        throw mapCreateError(e, request.parent)
      }

    return batchCreateRawImpressionUploadFilesResponse {
      rawImpressionUploadFiles +=
        internalResponse.rawImpressionUploadFilesList.map { it.toPublic() }
    }
  }

  override suspend fun getRawImpressionUploadFile(
    request: GetRawImpressionUploadFileRequest
  ): RawImpressionUploadFile {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val fileKey =
      RawImpressionUploadFileKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionUploadFile =
      try {
        internalFileStub.getRawImpressionUploadFile(
          internalGetFileRequest {
            dataProviderResourceId = fileKey.dataProviderId
            rawImpressionUploadResourceId = fileKey.rawImpressionUploadId
            fileResourceId = fileKey.fileId
          }
        )
      } catch (e: StatusException) {
        throw mapFileNotFoundError(e, request.name)
      }

    return internalResponse.toPublic()
  }

  override suspend fun listRawImpressionUploadFiles(
    request: ListRawImpressionUploadFilesRequest
  ): ListRawImpressionUploadFilesResponse {
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

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val internalPageToken: InternalListFilesPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListFilesPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    // Support AIP-159 wildcard "-" for listing across all uploads.
    val rawImpressionUploadResourceId =
      if (uploadKey.rawImpressionUploadId == ResourceKey.WILDCARD_ID) {
        ""
      } else {
        uploadKey.rawImpressionUploadId
      }

    val internalResponse: InternalListFilesResponse =
      try {
        internalFileStub.listRawImpressionUploadFiles(
          internalListFilesRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (request.hasFilter()) {
              filter =
                InternalListFilesRequestKt.filter {
                  blobUriIn += request.filter.blobUriInList
                  if (request.filter.hasCreateTimeIn()) {
                    createTimeIn = request.filter.createTimeIn
                  }
                }
            }
            showDeleted = request.showDeleted
          }
        )
      } catch (e: StatusException) {
        throw mapListError(e)
      }

    return listRawImpressionUploadFilesResponse {
      rawImpressionUploadFiles +=
        internalResponse.rawImpressionUploadFilesList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun deleteRawImpressionUploadFile(
    request: DeleteRawImpressionUploadFileRequest
  ): RawImpressionUploadFile {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val fileKey =
      RawImpressionUploadFileKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionUploadFile =
      try {
        internalFileStub.deleteRawImpressionUploadFile(
          internalDeleteFileRequest {
            dataProviderResourceId = fileKey.dataProviderId
            rawImpressionUploadResourceId = fileKey.rawImpressionUploadId
            fileResourceId = fileKey.fileId
          }
        )
      } catch (e: StatusException) {
        throw mapFileNotFoundError(e, request.name)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchDeleteRawImpressionUploadFiles(
    request: BatchDeleteRawImpressionUploadFilesRequest
  ): BatchDeleteRawImpressionUploadFilesResponse {
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

    val nameSet = HashSet<String>()
    val internalDeleteRequests =
      request.requestsList.mapIndexed { index, childRequest ->
        val name = childRequest.name
        if (name.isEmpty()) {
          throw RequiredFieldNotSetException("requests.$index.name")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val fileKey =
          RawImpressionUploadFileKey.fromName(name)
            ?: throw InvalidFieldValueException("requests.$index.name")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

        if (
          fileKey.dataProviderId != uploadKey.dataProviderId ||
            fileKey.rawImpressionUploadId != uploadKey.rawImpressionUploadId
        ) {
          throw InvalidFieldValueException("requests.$index.name") {
              "File $name does not belong to parent ${request.parent}"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (!nameSet.add(name)) {
          throw InvalidFieldValueException("requests.$index.name")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        internalDeleteFileRequest {
          dataProviderResourceId = fileKey.dataProviderId
          rawImpressionUploadResourceId = fileKey.rawImpressionUploadId
          fileResourceId = fileKey.fileId
        }
      }

    val internalResponse: InternalBatchDeleteFilesResponse =
      try {
        internalFileStub.batchDeleteRawImpressionUploadFiles(
          internalBatchDeleteFilesRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests += internalDeleteRequests
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND ->
            RawImpressionUploadFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return batchDeleteRawImpressionUploadFilesResponse {
      rawImpressionUploadFiles +=
        internalResponse.rawImpressionUploadFilesList.map { it.toPublic() }
    }
  }

  private fun mapCreateError(e: StatusException, parent: String): Throwable {
    return when (InternalErrors.getReason(e)) {
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND ->
        RawImpressionUploadNotFoundException(parent, e)
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
      else -> Status.INTERNAL.withCause(e).asRuntimeException()
    }
  }

  private fun mapFileNotFoundError(e: StatusException, name: String): Throwable {
    return when (InternalErrors.getReason(e)) {
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND ->
        RawImpressionUploadFileNotFoundException(name, e)
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
      else -> Status.INTERNAL.withCause(e).asRuntimeException()
    }
  }

  private fun mapListError(e: StatusException): Throwable {
    return Status.INTERNAL.withCause(e).asRuntimeException()
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

/** Converts an internal [InternalRawImpressionUploadFile] to a public one. */
fun InternalRawImpressionUploadFile.toPublic(): RawImpressionUploadFile {
  val source = this
  return rawImpressionUploadFile {
    name =
      RawImpressionUploadFileKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.fileResourceId,
        )
        .toName()
    blobUri = source.blobUri
    createTime = source.createTime
    updateTime = source.updateTime
    if (source.hasDeleteTime()) {
      deleteTime = source.deleteTime
    }
  }
}
