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
import io.grpc.StatusRuntimeException
import java.io.IOException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesPageToken as InternalListPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesResponse as InternalListResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine as InternalRawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub as InternalModelLineServiceStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionUploadModelLinesRequest as internalBatchCreateRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadModelLineRequest as internalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadModelLineRequest as internalGetRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadModelLinesRequest as internalListRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineCompletedRequest as internalMarkCompletedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineFailedRequest as internalMarkFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineLabelingRequest as internalMarkLabelingRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLinePoolAssigningRequest as internalMarkPoolAssigningRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineRankingRequest as internalMarkRankingRequest

class RawImpressionUploadModelLineService(
  private val internalModelLineStub: InternalModelLineServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionUploadModelLineServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUploadModelLine(
    request: CreateRawImpressionUploadModelLineRequest
  ): RawImpressionUploadModelLine {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasRawImpressionUploadModelLine()) {
      throw RequiredFieldNotSetException("raw_impression_upload_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.rawImpressionUploadModelLine.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_model_line.cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    ModelLineKey.fromName(request.rawImpressionUploadModelLine.cmmsModelLine)
      ?: throw InvalidFieldValueException("raw_impression_upload_model_line.cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.createRawImpressionUploadModelLine(
          internalCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            rawImpressionUploadModelLine =
              org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine {
                cmmsModelLine = request.rawImpressionUploadModelLine.cmmsModelLine
              }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchCreateRawImpressionUploadModelLines(
    request: BatchCreateRawImpressionUploadModelLinesRequest
  ): BatchCreateRawImpressionUploadModelLinesResponse {
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

    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val seenRequestIds = mutableSetOf<String>()
    val seenModelLines = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, createRequest ->
      if (createRequest.requestId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.request_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      try {
        UUID.fromString(createRequest.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("requests.$index.request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!seenRequestIds.add(createRequest.requestId)) {
        throw InvalidFieldValueException("requests.$index.request_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.parent.isNotEmpty() && createRequest.parent != request.parent) {
        throw InvalidFieldValueException("requests.$index.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.rawImpressionUploadModelLine.cmmsModelLine.isEmpty()) {
        throw RequiredFieldNotSetException(
            "requests.$index.raw_impression_upload_model_line.cmms_model_line"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      ModelLineKey.fromName(createRequest.rawImpressionUploadModelLine.cmmsModelLine)
        ?: throw InvalidFieldValueException(
            "requests.$index.raw_impression_upload_model_line.cmms_model_line"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      if (!seenModelLines.add(createRequest.rawImpressionUploadModelLine.cmmsModelLine)) {
        throw InvalidFieldValueException(
            "requests.$index.raw_impression_upload_model_line.cmms_model_line"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val internalResponse =
      try {
        internalModelLineStub.batchCreateRawImpressionUploadModelLines(
          internalBatchCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests +=
              request.requestsList.map { createRequest ->
                internalCreateRequest {
                  dataProviderResourceId = uploadKey.dataProviderId
                  rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
                  rawImpressionUploadModelLine =
                    org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine {
                      cmmsModelLine = createRequest.rawImpressionUploadModelLine.cmmsModelLine
                    }
                  requestId = createRequest.requestId
                }
              }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return batchCreateRawImpressionUploadModelLinesResponse {
      rawImpressionUploadModelLines +=
        internalResponse.rawImpressionUploadModelLinesList.map { it.toPublic() }
    }
  }

  override suspend fun getRawImpressionUploadModelLine(
    request: GetRawImpressionUploadModelLineRequest
  ): RawImpressionUploadModelLine {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLineKey =
      RawImpressionUploadModelLineKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.getRawImpressionUploadModelLine(
          internalGetRequest {
            dataProviderResourceId = modelLineKey.dataProviderId
            rawImpressionUploadResourceId = modelLineKey.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLineKey.rawImpressionUploadModelLineId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun listRawImpressionUploadModelLines(
    request: ListRawImpressionUploadModelLinesRequest
  ): ListRawImpressionUploadModelLinesResponse {
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
        internalModelLineStub.listRawImpressionUploadModelLines(
          internalListRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId =
              if (uploadKey.rawImpressionUploadId == WILDCARD_ID) ""
              else uploadKey.rawImpressionUploadId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (request.hasFilter()) {
              filter =
                org.wfanet.measurement.internal.edpaggregator
                  .ListRawImpressionUploadModelLinesRequestKt
                  .filter {
                    if (request.filter.stateInList.isNotEmpty()) {
                      stateIn += request.filter.stateInList.map { it.toInternal() }
                    }
                    if (request.filter.hasCreateTimeIn()) {
                      createTimeIn = request.filter.createTimeIn
                    }
                    if (request.filter.cmmsModelLine.isNotEmpty()) {
                      cmmsModelLine = request.filter.cmmsModelLine
                    }
                  }
            }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return listRawImpressionUploadModelLinesResponse {
      rawImpressionUploadModelLines +=
        internalResponse.rawImpressionUploadModelLinesList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun markRawImpressionUploadModelLinePoolAssigning(
    request: MarkRawImpressionUploadModelLinePoolAssigningRequest
  ): RawImpressionUploadModelLine {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLineKey =
      RawImpressionUploadModelLineKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    validateEtagAndRequestId(request.etag, request.requestId)
    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.markRawImpressionUploadModelLinePoolAssigning(
          internalMarkPoolAssigningRequest {
            dataProviderResourceId = modelLineKey.dataProviderId
            rawImpressionUploadResourceId = modelLineKey.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLineKey.rawImpressionUploadModelLineId
            etag = request.etag
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun markRawImpressionUploadModelLineRanking(
    request: MarkRawImpressionUploadModelLineRankingRequest
  ): RawImpressionUploadModelLine {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLineKey =
      RawImpressionUploadModelLineKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    validateEtagAndRequestId(request.etag, request.requestId)
    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.markRawImpressionUploadModelLineRanking(
          internalMarkRankingRequest {
            dataProviderResourceId = modelLineKey.dataProviderId
            rawImpressionUploadResourceId = modelLineKey.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLineKey.rawImpressionUploadModelLineId
            etag = request.etag
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun markRawImpressionUploadModelLineLabeling(
    request: MarkRawImpressionUploadModelLineLabelingRequest
  ): RawImpressionUploadModelLine {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLineKey =
      RawImpressionUploadModelLineKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    validateEtagAndRequestId(request.etag, request.requestId)
    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.markRawImpressionUploadModelLineLabeling(
          internalMarkLabelingRequest {
            dataProviderResourceId = modelLineKey.dataProviderId
            rawImpressionUploadResourceId = modelLineKey.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLineKey.rawImpressionUploadModelLineId
            etag = request.etag
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun markRawImpressionUploadModelLineCompleted(
    request: MarkRawImpressionUploadModelLineCompletedRequest
  ): RawImpressionUploadModelLine {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLineKey =
      RawImpressionUploadModelLineKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    validateEtagAndRequestId(request.etag, request.requestId)
    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.markRawImpressionUploadModelLineCompleted(
          internalMarkCompletedRequest {
            dataProviderResourceId = modelLineKey.dataProviderId
            rawImpressionUploadResourceId = modelLineKey.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLineKey.rawImpressionUploadModelLineId
            etag = request.etag
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun markRawImpressionUploadModelLineFailed(
    request: MarkRawImpressionUploadModelLineFailedRequest
  ): RawImpressionUploadModelLine {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLineKey =
      RawImpressionUploadModelLineKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    validateEtagAndRequestId(request.etag, request.requestId)
    val internalResponse: InternalRawImpressionUploadModelLine =
      try {
        internalModelLineStub.markRawImpressionUploadModelLineFailed(
          internalMarkFailedRequest {
            dataProviderResourceId = modelLineKey.dataProviderId
            rawImpressionUploadResourceId = modelLineKey.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLineKey.rawImpressionUploadModelLineId
            etag = request.etag
            requestId = request.requestId
            errorMessage = request.errorMessage
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  /**
   * Validates the `etag` and `request_id` shared by every `Mark*` RPC.
   *
   * @throws io.grpc.StatusRuntimeException with [Status.Code.INVALID_ARGUMENT] if `etag` or
   *   `request_id` is empty, or if `request_id` is not a valid UUID.
   */
  private fun validateEtagAndRequestId(etag: String, requestId: String) {
    if (etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      UUID.fromString(requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun handleInternalError(e: StatusException): StatusRuntimeException {
    return when (InternalErrors.getReason(e)) {
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NOT_FOUND ->
        Status.NOT_FOUND.withCause(e).asRuntimeException()
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_INVALID,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_CONCURRENT ->
        Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
      InternalErrors.Reason.ETAG_MISMATCH -> Status.ABORTED.withCause(e).asRuntimeException()
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
      InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
      InternalErrors.Reason.INVALID_FIELD_VALUE,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_ALREADY_EXISTS,
      InternalErrors.Reason.VID_LABELING_JOB_NOT_FOUND,
      InternalErrors.Reason.VID_LABELING_JOB_STATE_INVALID,
      InternalErrors.Reason.VID_LABELING_JOB_ALREADY_EXISTS,
      InternalErrors.Reason.RANKER_JOB_NOT_FOUND,
      InternalErrors.Reason.RANKER_JOB_ALREADY_EXISTS,
      InternalErrors.Reason.RANKER_JOB_STATE_INVALID,
      InternalErrors.Reason.RANK_INDEX_BLOB_NOT_FOUND,
      InternalErrors.Reason.RANK_INDEX_BLOB_ALREADY_EXISTS,
      InternalErrors.Reason.POOL_ASSIGNMENT_JOB_NOT_FOUND,
      InternalErrors.Reason.POOL_ASSIGNMENT_JOB_STATE_INVALID,
      InternalErrors.Reason.POOL_ASSIGNMENT_JOB_ALREADY_EXISTS,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_ALREADY_EXISTS,
      null -> Status.INTERNAL.withCause(e).asRuntimeException()
    }
  }

  companion object {
    private const val WILDCARD_ID = "-"
    private const val MAX_BATCH_SIZE = 50
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

/** Converts an internal [InternalRawImpressionUploadModelLine] to a public one. */
fun InternalRawImpressionUploadModelLine.toPublic(): RawImpressionUploadModelLine {
  val source = this
  return rawImpressionUploadModelLine {
    name =
      RawImpressionUploadModelLineKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.rawImpressionUploadModelLineResourceId,
        )
        .toName()
    state = source.state.toPublic()
    cmmsModelLine = source.cmmsModelLine
    createTime = source.createTime
    updateTime = source.updateTime
    etag = source.etag
    if (source.errorMessage.isNotEmpty()) {
      errorMessage = source.errorMessage
    }
    // Phase-0 last-shard-out outputs (OUTPUT_ONLY), consumed by a retrying SubpoolAssigner.
    poolOffsets += source.poolOffsetsList
    if (source.hasMaxEventDate()) {
      maxEventDate = source.maxEventDate
    }
    if (source.hasEncryptedMergedDek()) {
      encryptedMergedDek = source.encryptedMergedDek.toPublic()
    }
  }
}

/**
 * Converts an internal [RawImpressionUploadModelLineState] to a public
 * [RawImpressionUploadModelLine.State].
 */
internal fun RawImpressionUploadModelLineState.toPublic(): RawImpressionUploadModelLine.State {
  return when (this) {
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED ->
      RawImpressionUploadModelLine.State.CREATED
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING ->
      RawImpressionUploadModelLine.State.POOL_ASSIGNING
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING ->
      RawImpressionUploadModelLine.State.RANKING
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING ->
      RawImpressionUploadModelLine.State.LABELING
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED ->
      RawImpressionUploadModelLine.State.COMPLETED
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED ->
      RawImpressionUploadModelLine.State.FAILED
    RawImpressionUploadModelLineState.UNRECOGNIZED,
    RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_UNSPECIFIED ->
      error("Unrecognized state")
  }
}

/**
 * Converts a public [RawImpressionUploadModelLine.State] to an internal
 * [RawImpressionUploadModelLineState].
 */
internal fun RawImpressionUploadModelLine.State.toInternal(): RawImpressionUploadModelLineState {
  return when (this) {
    RawImpressionUploadModelLine.State.CREATED ->
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED
    RawImpressionUploadModelLine.State.POOL_ASSIGNING ->
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
    RawImpressionUploadModelLine.State.RANKING ->
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING
    RawImpressionUploadModelLine.State.LABELING ->
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
    RawImpressionUploadModelLine.State.COMPLETED ->
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
    RawImpressionUploadModelLine.State.FAILED ->
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED
    RawImpressionUploadModelLine.State.UNRECOGNIZED,
    RawImpressionUploadModelLine.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
