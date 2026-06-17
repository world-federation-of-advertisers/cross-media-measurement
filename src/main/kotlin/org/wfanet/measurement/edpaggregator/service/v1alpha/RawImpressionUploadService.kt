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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsPageToken as InternalListUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsRequestKt as InternalListUploadsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsResponse as InternalListUploadsResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload as InternalRawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub as InternalUploadServiceStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadRequest as internalCreateUploadRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadRequest as internalGetUploadRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsRequest as internalListUploadsRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload as internalRawImpressionUpload

class RawImpressionUploadService(
  private val internalUploadStub: InternalUploadServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionUploadServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUpload(
    request: CreateRawImpressionUploadRequest
  ): RawImpressionUpload {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasRawImpressionUpload()) {
      throw RequiredFieldNotSetException("raw_impression_upload")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.rawImpressionUpload.doneBlobUri.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload.done_blob_uri")
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

    val internalResponse: InternalRawImpressionUpload =
      try {
        internalUploadStub.createRawImpressionUpload(
          internalCreateUploadRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            rawImpressionUpload = internalRawImpressionUpload {
              doneBlobUri = request.rawImpressionUpload.doneBlobUri
            }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  override suspend fun getRawImpressionUpload(
    request: GetRawImpressionUploadRequest
  ): RawImpressionUpload {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionUpload =
      try {
        internalUploadStub.getRawImpressionUpload(
          internalGetUploadRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND ->
            RawImpressionUploadNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
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
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  override suspend fun listRawImpressionUploads(
    request: ListRawImpressionUploadsRequest
  ): ListRawImpressionUploadsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.hasFilter()) {
      for (state in request.filter.stateInList) {
        if (
          state == RawImpressionUpload.State.STATE_UNSPECIFIED ||
            state == RawImpressionUpload.State.UNRECOGNIZED
        ) {
          throw InvalidFieldValueException("filter.state_in")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val internalPageToken: InternalListUploadsPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListUploadsPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListUploadsResponse =
      try {
        internalUploadStub.listRawImpressionUploads(
          internalListUploadsRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (request.hasFilter()) {
              filter =
                InternalListUploadsRequestKt.filter {
                  stateIn += request.filter.stateInList.map { it.toInternal() }
                  if (request.filter.hasCreateTimeIn()) {
                    createTimeIn = request.filter.createTimeIn
                  }
                }
            }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return listRawImpressionUploadsResponse {
      rawImpressionUploads += internalResponse.rawImpressionUploadsList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

/** Converts an internal [InternalRawImpressionUpload] to a public one. */
fun InternalRawImpressionUpload.toPublic(): RawImpressionUpload {
  val source = this
  return rawImpressionUpload {
    name =
      RawImpressionUploadKey(source.dataProviderResourceId, source.rawImpressionUploadResourceId)
        .toName()
    state = source.state.toPublic()
    doneBlobUri = source.doneBlobUri
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

/** Converts an internal [RawImpressionUploadState] to a public [RawImpressionUpload.State]. */
internal fun RawImpressionUploadState.toPublic(): RawImpressionUpload.State {
  return when (this) {
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED ->
      RawImpressionUpload.State.CREATED
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE -> RawImpressionUpload.State.ACTIVE
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED ->
      RawImpressionUpload.State.COMPLETED
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED -> RawImpressionUpload.State.FAILED
    RawImpressionUploadState.UNRECOGNIZED,
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}

/** Converts a public [RawImpressionUpload.State] to an internal [RawImpressionUploadState]. */
internal fun RawImpressionUpload.State.toInternal(): RawImpressionUploadState {
  return when (this) {
    RawImpressionUpload.State.CREATED ->
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
    RawImpressionUpload.State.ACTIVE -> RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE
    RawImpressionUpload.State.COMPLETED ->
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED
    RawImpressionUpload.State.FAILED -> RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED
    RawImpressionUpload.State.UNRECOGNIZED,
    RawImpressionUpload.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
