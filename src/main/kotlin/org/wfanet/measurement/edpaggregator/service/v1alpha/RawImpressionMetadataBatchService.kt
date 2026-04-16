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
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchFileAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchFileNotFoundException
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchNotFoundException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionMetadataBatchesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionMetadataBatchesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionMetadataBatchFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionMetadataBatchProcessedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatch
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesPageToken as InternalListBatchesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesResponse as InternalListBatchesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionBatchState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatch as InternalRawImpressionMetadataBatch
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub as InternalBatchServiceStub
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionMetadataBatchRequest as internalCreateBatchRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRawImpressionMetadataBatchRequest as internalDeleteBatchRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionMetadataBatchRequest as internalGetBatchRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchesRequest as internalListBatchesRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionMetadataBatchFailedRequest as internalMarkBatchFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionMetadataBatchProcessedRequest as internalMarkBatchProcessedRequest

class RawImpressionMetadataBatchService(
  private val internalBatchStub: InternalBatchServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionMetadataBatchServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionMetadataBatch(
    request: CreateRawImpressionMetadataBatchRequest
  ): RawImpressionMetadataBatch {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasRawImpressionMetadataBatch()) {
      throw RequiredFieldNotSetException("raw_impression_metadata_batch")
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

    val internalResponse: InternalRawImpressionMetadataBatch =
      try {
        internalBatchStub.createRawImpressionMetadataBatch(
          internalCreateBatchRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
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
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  override suspend fun getRawImpressionMetadataBatch(
    request: GetRawImpressionMetadataBatchRequest
  ): RawImpressionMetadataBatch {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionMetadataBatch =
      try {
        internalBatchStub.getRawImpressionMetadataBatch(
          internalGetBatchRequest {
            dataProviderResourceId = batchKey.dataProviderId
            batchResourceId = batchKey.rawImpressionMetadataBatchId
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
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  override suspend fun listRawImpressionMetadataBatches(
    request: ListRawImpressionMetadataBatchesRequest
  ): ListRawImpressionMetadataBatchesResponse {
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

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val internalPageToken: InternalListBatchesPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListBatchesPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListBatchesResponse =
      try {
        internalBatchStub.listRawImpressionMetadataBatches(
          internalListBatchesRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (
              request.hasFilter() &&
                request.filter.state != RawImpressionMetadataBatch.State.STATE_UNSPECIFIED
            ) {
              filter =
                org.wfanet.measurement.internal.edpaggregator
                  .ListRawImpressionMetadataBatchesRequestKt
                  .filter { state = request.filter.state.toInternal() }
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
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return listRawImpressionMetadataBatchesResponse {
      rawImpressionMetadataBatches +=
        internalResponse.rawImpressionMetadataBatchesList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun deleteRawImpressionMetadataBatch(
    request: DeleteRawImpressionMetadataBatchRequest
  ): RawImpressionMetadataBatch {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionMetadataBatch =
      try {
        internalBatchStub.deleteRawImpressionMetadataBatch(
          internalDeleteBatchRequest {
            dataProviderResourceId = batchKey.dataProviderId
            batchResourceId = batchKey.rawImpressionMetadataBatchId
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
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  override suspend fun markRawImpressionMetadataBatchProcessed(
    request: MarkRawImpressionMetadataBatchProcessedRequest
  ): RawImpressionMetadataBatch {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionMetadataBatch =
      try {
        internalBatchStub.markRawImpressionMetadataBatchProcessed(
          internalMarkBatchProcessedRequest {
            dataProviderResourceId = batchKey.dataProviderId
            batchResourceId = batchKey.rawImpressionMetadataBatchId
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
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  override suspend fun markRawImpressionMetadataBatchFailed(
    request: MarkRawImpressionMetadataBatchFailedRequest
  ): RawImpressionMetadataBatch {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val batchKey =
      RawImpressionMetadataBatchKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionMetadataBatch =
      try {
        internalBatchStub.markRawImpressionMetadataBatchFailed(
          internalMarkBatchFailedRequest {
            dataProviderResourceId = batchKey.dataProviderId
            batchResourceId = batchKey.rawImpressionMetadataBatchId
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
          else -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPublic()
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

/** Converts an internal [InternalRawImpressionMetadataBatch] to a public one. */
fun InternalRawImpressionMetadataBatch.toPublic(): RawImpressionMetadataBatch {
  val source = this
  return rawImpressionMetadataBatch {
    name =
      RawImpressionMetadataBatchKey(source.dataProviderResourceId, source.batchResourceId).toName()
    state = source.state.toPublic()
    createTime = source.createTime
    updateTime = source.updateTime
    if (source.hasDeleteTime()) {
      deleteTime = source.deleteTime
    }
  }
}

/**
 * Converts an internal [RawImpressionBatchState] to a public [RawImpressionMetadataBatch.State].
 */
internal fun RawImpressionBatchState.toPublic(): RawImpressionMetadataBatch.State {
  return when (this) {
    RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED ->
      RawImpressionMetadataBatch.State.CREATED
    RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_PROCESSED ->
      RawImpressionMetadataBatch.State.PROCESSED
    RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_FAILED ->
      RawImpressionMetadataBatch.State.FAILED
    RawImpressionBatchState.UNRECOGNIZED,
    RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}

/**
 * Converts a public [RawImpressionMetadataBatch.State] to an internal [RawImpressionBatchState].
 */
internal fun RawImpressionMetadataBatch.State.toInternal(): RawImpressionBatchState {
  return when (this) {
    RawImpressionMetadataBatch.State.CREATED ->
      RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED
    RawImpressionMetadataBatch.State.PROCESSED ->
      RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_PROCESSED
    RawImpressionMetadataBatch.State.FAILED ->
      RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_FAILED
    RawImpressionMetadataBatch.State.UNRECOGNIZED,
    RawImpressionMetadataBatch.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
