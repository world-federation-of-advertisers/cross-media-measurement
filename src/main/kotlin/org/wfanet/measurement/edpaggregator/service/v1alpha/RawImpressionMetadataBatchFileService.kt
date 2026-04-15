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
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchFileAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchFileKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchFileNotFoundException
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchNotFoundException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatchFile
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionMetadataBatchFilesResponse as InternalBatchCreateFilesResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionMetadataBatchFilesResponse as InternalBatchDeleteFilesResponse
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesPageToken as InternalListFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesResponse as InternalListFilesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFile as InternalRawImpressionMetadataBatchFile
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineStub as InternalFileServiceStub
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionMetadataBatchFilesRequest as internalBatchCreateFilesRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRawImpressionMetadataBatchFilesRequest as internalBatchDeleteFilesRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionMetadataBatchFileRequest as internalCreateFileRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRawImpressionMetadataBatchFileRequest as internalDeleteFileRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionMetadataBatchFileRequest as internalGetFileRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchFilesRequest as internalListFilesRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataBatchFile as internalFile

class RawImpressionMetadataBatchFileService(
  private val internalFileStub: InternalFileServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionMetadataBatchFileServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionMetadataBatchFile(
    request: CreateRawImpressionMetadataBatchFileRequest
  ): RawImpressionMetadataBatchFile {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasRawImpressionMetadataBatchFile()) {
      throw RequiredFieldNotSetException("raw_impression_metadata_batch_file")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.rawImpressionMetadataBatchFile.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_metadata_batch_file.blob_uri")
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

    val internalResponse: InternalRawImpressionMetadataBatchFile =
      try {
        internalFileStub.createRawImpressionMetadataBatchFile(
          internalCreateFileRequest {
            dataProviderResourceId = batchKey.dataProviderId
            batchResourceId = batchKey.rawImpressionMetadataBatchId
            rawImpressionMetadataBatchFile = internalFile {
              blobUri = request.rawImpressionMetadataBatchFile.blobUri
            }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND ->
            RawImpressionMetadataBatchNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID ->
            Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND ->
            RawImpressionMetadataBatchFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS ->
            RawImpressionMetadataBatchFileAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
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

    return internalResponse.toPublic()
  }

  override suspend fun batchCreateRawImpressionMetadataBatchFiles(
    request: BatchCreateRawImpressionMetadataBatchFilesRequest
  ): BatchCreateRawImpressionMetadataBatchFilesResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val blobUriSet = mutableSetOf<String>()
    val requestIdSet = mutableSetOf<String>()

    val internalRequests =
      request.requestsList.mapIndexed { index, childRequest ->
        if (childRequest.parent.isNotEmpty() && childRequest.parent != request.parent) {
          throw InvalidFieldValueException("requests.$index.parent") {
              "Parent ${childRequest.parent} does not match top-level parent ${request.parent}"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (!childRequest.hasRawImpressionMetadataBatchFile()) {
          throw RequiredFieldNotSetException("requests.$index.raw_impression_metadata_batch_file")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val blobUri = childRequest.rawImpressionMetadataBatchFile.blobUri
        if (blobUri.isEmpty()) {
          throw RequiredFieldNotSetException(
              "requests.$index.raw_impression_metadata_batch_file.blob_uri"
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (!blobUriSet.add(blobUri)) {
          throw InvalidFieldValueException("requests.$index.blob_uri") {
              "blob uri $blobUri is duplicate in the batch of requests"
            }
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
          dataProviderResourceId = batchKey.dataProviderId
          batchResourceId = batchKey.rawImpressionMetadataBatchId
          rawImpressionMetadataBatchFile = internalFile { this.blobUri = blobUri }
          this.requestId = requestId
        }
      }

    val internalResponse: InternalBatchCreateFilesResponse =
      try {
        internalFileStub.batchCreateRawImpressionMetadataBatchFiles(
          internalBatchCreateFilesRequest {
            dataProviderResourceId = batchKey.dataProviderId
            batchResourceId = batchKey.rawImpressionMetadataBatchId
            requests += internalRequests
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND ->
            RawImpressionMetadataBatchNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID ->
            Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND ->
            RawImpressionMetadataBatchFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS ->
            RawImpressionMetadataBatchFileAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
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

    return batchCreateRawImpressionMetadataBatchFilesResponse {
      rawImpressionMetadataBatchFiles +=
        internalResponse.rawImpressionMetadataBatchFilesList.map { it.toPublic() }
    }
  }

  override suspend fun getRawImpressionMetadataBatchFile(
    request: GetRawImpressionMetadataBatchFileRequest
  ): RawImpressionMetadataBatchFile {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val fileKey =
      RawImpressionMetadataBatchFileKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionMetadataBatchFile =
      try {
        internalFileStub.getRawImpressionMetadataBatchFile(
          internalGetFileRequest {
            dataProviderResourceId = fileKey.dataProviderId
            batchResourceId = fileKey.rawImpressionMetadataBatchId
            fileResourceId = fileKey.fileId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND ->
            RawImpressionMetadataBatchNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID ->
            Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND ->
            RawImpressionMetadataBatchFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS ->
            RawImpressionMetadataBatchFileAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
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

    return internalResponse.toPublic()
  }

  override suspend fun listRawImpressionMetadataBatchFiles(
    request: ListRawImpressionMetadataBatchFilesRequest
  ): ListRawImpressionMetadataBatchFilesResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.parent)
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

    // Support AIP-159 wildcard "-" for listing across all batches.
    val batchResourceId =
      if (batchKey.rawImpressionMetadataBatchId == ResourceKey.WILDCARD_ID) {
        ""
      } else {
        batchKey.rawImpressionMetadataBatchId
      }

    val internalResponse: InternalListFilesResponse =
      try {
        internalFileStub.listRawImpressionMetadataBatchFiles(
          internalListFilesRequest {
            dataProviderResourceId = batchKey.dataProviderId
            this.batchResourceId = batchResourceId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (request.hasFilter() && request.filter.blobUriInList.isNotEmpty()) {
              filter =
                org.wfanet.measurement.internal.edpaggregator
                  .ListRawImpressionMetadataBatchFilesRequestKt
                  .filter { blobUriIn += request.filter.blobUriInList }
            }
            showDeleted = request.showDeleted
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND ->
            RawImpressionMetadataBatchNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID ->
            Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND ->
            RawImpressionMetadataBatchFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS ->
            RawImpressionMetadataBatchFileAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
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

    return listRawImpressionMetadataBatchFilesResponse {
      rawImpressionMetadataBatchFiles +=
        internalResponse.rawImpressionMetadataBatchFilesList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun deleteRawImpressionMetadataBatchFile(
    request: DeleteRawImpressionMetadataBatchFileRequest
  ): RawImpressionMetadataBatchFile {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val fileKey =
      RawImpressionMetadataBatchFileKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionMetadataBatchFile =
      try {
        internalFileStub.deleteRawImpressionMetadataBatchFile(
          internalDeleteFileRequest {
            dataProviderResourceId = fileKey.dataProviderId
            batchResourceId = fileKey.rawImpressionMetadataBatchId
            fileResourceId = fileKey.fileId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND ->
            RawImpressionMetadataBatchNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID ->
            Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND ->
            RawImpressionMetadataBatchFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS ->
            RawImpressionMetadataBatchFileAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
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

    return internalResponse.toPublic()
  }

  override suspend fun batchDeleteRawImpressionMetadataBatchFiles(
    request: BatchDeleteRawImpressionMetadataBatchFilesRequest
  ): BatchDeleteRawImpressionMetadataBatchFilesResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.namesList.isEmpty()) {
      throw RequiredFieldNotSetException("names")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val nameSet = HashSet<String>()
    val internalDeleteRequests =
      request.namesList.mapIndexed { index, name ->
        if (name.isEmpty()) {
          throw RequiredFieldNotSetException("names.$index")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val fileKey =
          RawImpressionMetadataBatchFileKey.fromName(name)
            ?: throw InvalidFieldValueException("names.$index")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

        if (
          fileKey.dataProviderId != batchKey.dataProviderId ||
            fileKey.rawImpressionMetadataBatchId != batchKey.rawImpressionMetadataBatchId
        ) {
          throw InvalidFieldValueException("names.$index") {
              "File $name does not belong to parent ${request.parent}"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (!nameSet.add(name)) {
          throw InvalidFieldValueException("names.$index")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        internalDeleteFileRequest {
          dataProviderResourceId = fileKey.dataProviderId
          batchResourceId = fileKey.rawImpressionMetadataBatchId
          fileResourceId = fileKey.fileId
        }
      }

    val internalResponse: InternalBatchDeleteFilesResponse =
      try {
        internalFileStub.batchDeleteRawImpressionMetadataBatchFiles(
          internalBatchDeleteFilesRequest { requests += internalDeleteRequests }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND ->
            RawImpressionMetadataBatchNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID ->
            Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND ->
            RawImpressionMetadataBatchFileNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS ->
            RawImpressionMetadataBatchFileAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
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

    return batchDeleteRawImpressionMetadataBatchFilesResponse {
      rawImpressionMetadataBatchFiles +=
        internalResponse.rawImpressionMetadataBatchFilesList.map { it.toPublic() }
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

/** Converts an internal [InternalRawImpressionMetadataBatchFile] to a public one. */
fun InternalRawImpressionMetadataBatchFile.toPublic(): RawImpressionMetadataBatchFile {
  val source = this
  return rawImpressionMetadataBatchFile {
    name =
      RawImpressionMetadataBatchFileKey(
          source.dataProviderResourceId,
          source.batchResourceId,
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
