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
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RankerJobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRankerJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRankerJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRankerJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsPageToken as InternalListPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsRequestKt as InternalListRankerJobsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsResponse as InternalListResponse
import org.wfanet.measurement.internal.edpaggregator.MarkRankerJobSucceededResponse as InternalMarkSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.RankerJob as InternalRankerJob
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub as InternalRankerJobServiceStub
import org.wfanet.measurement.internal.edpaggregator.RankerState
import org.wfanet.measurement.internal.edpaggregator.batchCreateRankerJobsRequest as internalBatchCreateRequest
import org.wfanet.measurement.internal.edpaggregator.createRankerJobRequest as internalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.getRankerJobRequest as internalGetRequest
import org.wfanet.measurement.internal.edpaggregator.listRankerJobsRequest as internalListRequest
import org.wfanet.measurement.internal.edpaggregator.markRankerJobFailedRequest as internalMarkFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRankerJobSucceededRequest as internalMarkSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.rankerJob as internalRankerJob

/**
 * Public v1alpha implementation of the [RankerJob] service.
 *
 * Validates and translates public RankerJob API requests into the internal RankerJob service,
 * mapping resource names to internal resource IDs and internal error reasons to gRPC statuses.
 */
class RankerJobService(
  private val internalRankerJobStub: InternalRankerJobServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RankerJobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRankerJob(request: CreateRankerJobRequest): RankerJob {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasRankerJob()) {
      throw RequiredFieldNotSetException("ranker_job")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankerJob.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("ranker_job.cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankerJob.poolOffsetsList.isEmpty()) {
      throw RequiredFieldNotSetException("ranker_job.pool_offsets")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
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

    val internalResponse: InternalRankerJob =
      try {
        internalRankerJobStub.createRankerJob(
          internalCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            rankerJob = internalRankerJob {
              cmmsModelLine = request.rankerJob.cmmsModelLine
              poolOffsets += request.rankerJob.poolOffsetsList
            }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchCreateRankerJobs(
    request: BatchCreateRankerJobsRequest
  ): BatchCreateRankerJobsResponse {
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
      throw InvalidFieldValueException("requests")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val seenRequestIds = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, createRequest ->
      if (createRequest.parent.isNotEmpty() && createRequest.parent != request.parent) {
        throw InvalidFieldValueException("requests.$index.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!createRequest.hasRankerJob()) {
        throw RequiredFieldNotSetException("requests.$index.ranker_job")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.rankerJob.cmmsModelLine.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.ranker_job.cmms_model_line")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.rankerJob.poolOffsetsList.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.ranker_job.pool_offsets")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
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
    }

    val internalResponse =
      try {
        internalRankerJobStub.batchCreateRankerJobs(
          internalBatchCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests +=
              request.requestsList.map { createRequest ->
                internalCreateRequest {
                  dataProviderResourceId = uploadKey.dataProviderId
                  rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
                  rankerJob = internalRankerJob {
                    cmmsModelLine = createRequest.rankerJob.cmmsModelLine
                    poolOffsets += createRequest.rankerJob.poolOffsetsList
                  }
                  requestId = createRequest.requestId
                }
              }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return batchCreateRankerJobsResponse {
      rankerJobs += internalResponse.rankerJobsList.map { it.toPublic() }
    }
  }

  override suspend fun getRankerJob(request: GetRankerJobRequest): RankerJob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      RankerJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRankerJob =
      try {
        internalRankerJobStub.getRankerJob(
          internalGetRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            rankerJobResourceId = jobKey.rankerJobId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun listRankerJobs(request: ListRankerJobsRequest): ListRankerJobsResponse {
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

    if (request.hasFilter()) {
      for (state in request.filter.stateInList) {
        if (state == RankerJob.State.STATE_UNSPECIFIED || state == RankerJob.State.UNRECOGNIZED) {
          throw InvalidFieldValueException("filter.state_in")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }

    val internalResponse: InternalListResponse =
      try {
        internalRankerJobStub.listRankerJobs(
          internalListRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId =
              if (uploadKey.rawImpressionUploadId == ResourceKey.WILDCARD_ID) ""
              else uploadKey.rawImpressionUploadId
            this.pageSize = request.pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (request.hasFilter()) {
              filter =
                InternalListRankerJobsRequestKt.filter {
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

    return listRankerJobsResponse {
      rankerJobs += internalResponse.rankerJobsList.map { it.toPublic() }
      totalSize = internalResponse.totalSize
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun markRankerJobSucceeded(
    request: MarkRankerJobSucceededRequest
  ): MarkRankerJobSucceededResponse {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      RankerJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

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

    val internalResponse: InternalMarkSucceededResponse =
      try {
        internalRankerJobStub.markRankerJobSucceeded(
          internalMarkSucceededRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            rankerJobResourceId = jobKey.rankerJobId
            etag = request.etag
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return markRankerJobSucceededResponse {
      rankerJob = internalResponse.rankerJob.toPublic()
      isLastJob = internalResponse.isLastJob
    }
  }

  override suspend fun markRankerJobFailed(request: MarkRankerJobFailedRequest): RankerJob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      RankerJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

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

    val internalResponse: InternalRankerJob =
      try {
        internalRankerJobStub.markRankerJobFailed(
          internalMarkFailedRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            rankerJobResourceId = jobKey.rankerJobId
            etag = request.etag
            errorMessage = request.errorMessage
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  companion object {
    private const val MAX_BATCH_SIZE = 50

    private fun handleInternalError(e: StatusException): StatusRuntimeException {
      return when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.RANKER_JOB_NOT_FOUND,
        InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND ->
          Status.NOT_FOUND.withCause(e).asRuntimeException()
        InternalErrors.Reason.RANKER_JOB_ALREADY_EXISTS ->
          Status.ALREADY_EXISTS.withCause(e).asRuntimeException()
        InternalErrors.Reason.RANKER_JOB_STATE_INVALID ->
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
        InternalErrors.Reason.VID_LABELING_JOB_NOT_FOUND,
        InternalErrors.Reason.VID_LABELING_JOB_STATE_INVALID,
        InternalErrors.Reason.VID_LABELING_JOB_ALREADY_EXISTS,
        InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
        InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_ALREADY_EXISTS,
        InternalErrors.Reason.RANK_INDEX_BLOB_NOT_FOUND,
        InternalErrors.Reason.RANK_INDEX_BLOB_ALREADY_EXISTS,
        InternalErrors.Reason.POOL_ASSIGNMENT_JOB_NOT_FOUND,
        InternalErrors.Reason.POOL_ASSIGNMENT_JOB_STATE_INVALID,
        InternalErrors.Reason.POOL_ASSIGNMENT_JOB_ALREADY_EXISTS,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }
}

/** Converts an internal [InternalRankerJob] to a public one. */
internal fun InternalRankerJob.toPublic(): RankerJob {
  val source = this
  return rankerJob {
    name =
      RankerJobKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.rankerJobResourceId,
        )
        .toName()
    state = source.state.toPublic()
    cmmsModelLine = source.cmmsModelLine
    poolOffsets += source.poolOffsetsList
    createTime = source.createTime
    updateTime = source.updateTime
    if (source.errorMessage.isNotEmpty()) {
      errorMessage = source.errorMessage
    }
    if (source.etag.isNotEmpty()) {
      etag = source.etag
    }
  }
}

/** Converts an internal [RankerState] to a public [RankerJob.State]. */
internal fun RankerState.toPublic(): RankerJob.State {
  return when (this) {
    RankerState.RANKER_STATE_CREATED -> RankerJob.State.CREATED
    RankerState.RANKER_STATE_SUCCEEDED -> RankerJob.State.SUCCEEDED
    RankerState.RANKER_STATE_FAILED -> RankerJob.State.FAILED
    RankerState.UNRECOGNIZED,
    RankerState.RANKER_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}

/** Converts a public [RankerJob.State] to an internal [RankerState]. */
internal fun RankerJob.State.toInternal(): RankerState {
  return when (this) {
    RankerJob.State.CREATED -> RankerState.RANKER_STATE_CREATED
    RankerJob.State.SUCCEEDED -> RankerState.RANKER_STATE_SUCCEEDED
    RankerJob.State.FAILED -> RankerState.RANKER_STATE_FAILED
    RankerJob.State.UNRECOGNIZED,
    RankerJob.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
