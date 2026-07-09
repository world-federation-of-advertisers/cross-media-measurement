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
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.VidLabelingJobKey
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreateVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsPageToken as InternalListPageToken
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsRequestKt as InternalListRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsResponse as InternalListResponse
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededResponse as InternalMarkSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJob as InternalVidLabelingJob
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub as InternalVidLabelingJobServiceStub
import org.wfanet.measurement.internal.edpaggregator.VidLabelingState
import org.wfanet.measurement.internal.edpaggregator.batchCreateVidLabelingJobsRequest as internalBatchCreateRequest
import org.wfanet.measurement.internal.edpaggregator.createVidLabelingJobRequest as internalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.getVidLabelingJobRequest as internalGetRequest
import org.wfanet.measurement.internal.edpaggregator.listVidLabelingJobsRequest as internalListRequest
import org.wfanet.measurement.internal.edpaggregator.markVidLabelingJobFailedRequest as internalMarkFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markVidLabelingJobSucceededRequest as internalMarkSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.vidLabelingJob as internalVidLabelingJob

/**
 * Public API service for [VidLabelingJob] resources.
 *
 * Validates public requests, translates resource names to resource IDs for the internal
 * [InternalVidLabelingJobServiceStub], and maps internal errors to public gRPC statuses.
 */
class VidLabelingJobService(
  private val internalVidLabelingJobStub: InternalVidLabelingJobServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : VidLabelingJobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createVidLabelingJob(request: CreateVidLabelingJobRequest): VidLabelingJob {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasVidLabelingJob()) {
      throw RequiredFieldNotSetException("vid_labeling_job")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.vidLabelingJob.cmmsModelLinesList.isEmpty()) {
      throw RequiredFieldNotSetException("vid_labeling_job.cmms_model_lines")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.vidLabelingJob.rawImpressionUploadFilesList.isEmpty()) {
      throw RequiredFieldNotSetException("vid_labeling_job.raw_impression_upload_files")
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

    val internalResponse: InternalVidLabelingJob =
      try {
        internalVidLabelingJobStub.createVidLabelingJob(
          internalCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            vidLabelingJob = internalVidLabelingJob {
              cmmsModelLines += request.vidLabelingJob.cmmsModelLinesList
              rawImpressionUploadFiles += request.vidLabelingJob.rawImpressionUploadFilesList
            }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun batchCreateVidLabelingJobs(
    request: BatchCreateVidLabelingJobsRequest
  ): BatchCreateVidLabelingJobsResponse {
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

    request.requestsList.forEachIndexed { index, createRequest ->
      if (createRequest.parent.isNotEmpty() && createRequest.parent != request.parent) {
        throw InvalidFieldValueException("requests.$index.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.vidLabelingJob.cmmsModelLinesList.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.vid_labeling_job.cmms_model_lines")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.vidLabelingJob.rawImpressionUploadFilesList.isEmpty()) {
        throw RequiredFieldNotSetException(
            "requests.$index.vid_labeling_job.raw_impression_upload_files"
          )
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
    }

    val internalResponse =
      try {
        internalVidLabelingJobStub.batchCreateVidLabelingJobs(
          internalBatchCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests +=
              request.requestsList.map { createRequest ->
                internalCreateRequest {
                  dataProviderResourceId = uploadKey.dataProviderId
                  rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
                  vidLabelingJob = internalVidLabelingJob {
                    cmmsModelLines += createRequest.vidLabelingJob.cmmsModelLinesList
                    rawImpressionUploadFiles +=
                      createRequest.vidLabelingJob.rawImpressionUploadFilesList
                  }
                  requestId = createRequest.requestId
                }
              }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return batchCreateVidLabelingJobsResponse {
      vidLabelingJobs += internalResponse.vidLabelingJobsList.map { it.toPublic() }
    }
  }

  override suspend fun getVidLabelingJob(request: GetVidLabelingJobRequest): VidLabelingJob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      VidLabelingJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalVidLabelingJob =
      try {
        internalVidLabelingJobStub.getVidLabelingJob(
          internalGetRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            vidLabelingJobResourceId = jobKey.vidLabelingJobId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun listVidLabelingJobs(
    request: ListVidLabelingJobsRequest
  ): ListVidLabelingJobsResponse {
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
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

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
        internalVidLabelingJobStub.listVidLabelingJobs(
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
                InternalListRequestKt.filter {
                  if (request.filter.state == VidLabelingJob.State.UNRECOGNIZED) {
                    throw InvalidFieldValueException("filter.state")
                      .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
                  }
                  if (request.filter.state != VidLabelingJob.State.STATE_UNSPECIFIED) {
                    state = request.filter.state.toInternal()
                  }
                  if (request.filter.hasCreateTimeInterval()) {
                    createTimeInterval = request.filter.createTimeInterval
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

    return listVidLabelingJobsResponse {
      vidLabelingJobs += internalResponse.vidLabelingJobsList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun markVidLabelingJobSucceeded(
    request: MarkVidLabelingJobSucceededRequest
  ): MarkVidLabelingJobSucceededResponse {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      VidLabelingJobKey.fromName(request.name)
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
        internalVidLabelingJobStub.markVidLabelingJobSucceeded(
          internalMarkSucceededRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            vidLabelingJobResourceId = jobKey.vidLabelingJobId
            etag = request.etag
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return markVidLabelingJobSucceededResponse {
      vidLabelingJob = internalResponse.vidLabelingJob.toPublic()
      if (internalResponse.hasLastVidLabelingJobResult()) {
        lastVidLabelingJobResult = lastVidLabelingJobResult {
          completedModelLines += internalResponse.lastVidLabelingJobResult.completedModelLinesList
        }
      }
    }
  }

  override suspend fun markVidLabelingJobFailed(
    request: MarkVidLabelingJobFailedRequest
  ): VidLabelingJob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      VidLabelingJobKey.fromName(request.name)
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

    val internalResponse: InternalVidLabelingJob =
      try {
        internalVidLabelingJobStub.markVidLabelingJobFailed(
          internalMarkFailedRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            vidLabelingJobResourceId = jobKey.vidLabelingJobId
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

  private fun handleInternalError(e: StatusException): StatusRuntimeException {
    return when (InternalErrors.getReason(e)) {
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
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_ALREADY_EXISTS,
      InternalErrors.Reason.RANKER_JOB_NOT_FOUND,
      InternalErrors.Reason.RANKER_JOB_ALREADY_EXISTS,
      InternalErrors.Reason.RANKER_JOB_STATE_INVALID,
      InternalErrors.Reason.RANK_INDEX_BLOB_NOT_FOUND,
      InternalErrors.Reason.RANK_INDEX_BLOB_ALREADY_EXISTS,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NOT_FOUND,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_INVALID,
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_CONCURRENT,
      InternalErrors.Reason.POOL_ASSIGNMENT_JOB_NOT_FOUND,
      InternalErrors.Reason.POOL_ASSIGNMENT_JOB_STATE_INVALID,
      InternalErrors.Reason.POOL_ASSIGNMENT_JOB_ALREADY_EXISTS,
      null -> Status.INTERNAL.withCause(e).asRuntimeException()
      InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND,
      InternalErrors.Reason.VID_LABELING_JOB_NOT_FOUND ->
        Status.NOT_FOUND.withCause(e).asRuntimeException()
      InternalErrors.Reason.VID_LABELING_JOB_STATE_INVALID ->
        Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
      InternalErrors.Reason.VID_LABELING_JOB_ALREADY_EXISTS ->
        Status.ALREADY_EXISTS.withCause(e).asRuntimeException()
      InternalErrors.Reason.ETAG_MISMATCH -> Status.ABORTED.withCause(e).asRuntimeException()
      InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
      InternalErrors.Reason.INVALID_FIELD_VALUE ->
        Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
    }
  }

  companion object {
    private const val WILDCARD_ID = "-"
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
    private const val MAX_BATCH_SIZE = 50
  }
}

/** Converts an internal [InternalVidLabelingJob] to a public one. */
internal fun InternalVidLabelingJob.toPublic(): VidLabelingJob {
  val source = this
  return vidLabelingJob {
    name =
      VidLabelingJobKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.vidLabelingJobResourceId,
        )
        .toName()
    state = source.state.toPublic()
    cmmsModelLines += source.cmmsModelLinesList
    rawImpressionUploadFiles += source.rawImpressionUploadFilesList
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

/** Converts an internal [VidLabelingState] to a public [VidLabelingJob.State]. */
internal fun VidLabelingState.toPublic(): VidLabelingJob.State {
  return when (this) {
    VidLabelingState.VID_LABELING_STATE_CREATED -> VidLabelingJob.State.CREATED
    VidLabelingState.VID_LABELING_STATE_SUCCEEDED -> VidLabelingJob.State.SUCCEEDED
    VidLabelingState.VID_LABELING_STATE_FAILED -> VidLabelingJob.State.FAILED
    VidLabelingState.UNRECOGNIZED,
    VidLabelingState.VID_LABELING_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}

/** Converts a public [VidLabelingJob.State] to an internal [VidLabelingState]. */
internal fun VidLabelingJob.State.toInternal(): VidLabelingState {
  return when (this) {
    VidLabelingJob.State.CREATED -> VidLabelingState.VID_LABELING_STATE_CREATED
    VidLabelingJob.State.SUCCEEDED -> VidLabelingState.VID_LABELING_STATE_SUCCEEDED
    VidLabelingJob.State.FAILED -> VidLabelingState.VID_LABELING_STATE_FAILED
    VidLabelingJob.State.UNRECOGNIZED,
    VidLabelingJob.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
